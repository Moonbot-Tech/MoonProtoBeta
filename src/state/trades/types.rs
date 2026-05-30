/// Result of applying the TradesStream packet-number state.
#[derive(Debug, Clone)]
pub enum TradesEvent {
    /// Packet was applied.
    ///
    /// Active Lib has already written rows into market state and retained
    /// `SeqRing` storage before this event is emitted. The event is a light
    /// "new rows are available" signal; it intentionally does not carry an
    /// owned `TradesPacket`, so the hot path does not build a `Vec` only for
    /// the public callback.
    Applied { packet_num: u16, base_time: f64 },
    /// A packet-number gap was detected: `[start..=end]` is missing. The
    /// recovery bucket was created; retry is driven by `tick()`.
    GapDetected { start: u16, end: u16 },
    /// Packet number was a duplicate (`packet_num == last`).
    /// Delphi does not advance gap-state for it, but still applies the payload.
    Duplicate,
    /// Packet number was outside the accepted range, usually after a reset.
    OutOfOrder { packet_num: u16 },
    /// An out-of-order packet filled one slot in an existing gap bucket.
    GapFilled {
        packet_num: u16,
        bucket_seq_range: (u16, u16),
    },
    /// Recovery tick requested these packet numbers through `emk_TradesResend`.
    ///
    /// This is diagnostic only. The active client sends the request
    /// automatically; applications must not send their own duplicate request
    /// because they saw this event.
    ResendRequested { packet_nums: Vec<u16> },
    /// Recovery bucket was closed: all packets arrived or retry limit expired.
    BucketClosed {
        start: u16,
        end: u16,
        all_received: bool,
        retry_count: u8,
    },
}

/// Packet-number effect produced before row/state application.
///
/// Delphi decides gap/duplicate/resend bookkeeping from `PacketNum` first and
/// then continues reading the stream rows. Keeping this separate lets the
/// dispatcher iterate decoded sections in wire order and emit only a lightweight
/// applied signal after Active Lib state/storage has been updated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TradesPacketEffect {
    Apply,
    GapDetected {
        start: u16,
        end: u16,
    },
    Duplicate,
    OutOfOrder {
        packet_num: u16,
    },
    GapFilled {
        packet_num: u16,
        bucket_seq_range: (u16, u16),
    },
}

/// Small stack buffer for the few bookkeeping effects one trades packet can
/// produce before row application.
///
/// This keeps the high-rate trades stream on the latency-oriented path: the
/// wire rows stay 10-byte compact records and packet-number bookkeeping does
/// not allocate a heap `Vec` just to say "apply" or "gap filled + apply".
#[derive(Debug, Clone, Copy)]
pub(crate) struct TradesPacketEffects {
    items: [Option<TradesPacketEffect>; 4],
    len: usize,
}

impl TradesPacketEffects {
    pub(crate) fn new() -> Self {
        Self {
            items: [None; 4],
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, effect: TradesPacketEffect) {
        debug_assert!(
            self.len < self.items.len(),
            "TradesPacketEffects capacity must cover all packet branches"
        );
        if self.len < self.items.len() {
            self.items[self.len] = Some(effect);
            self.len += 1;
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn has_gap_detected(&self) -> bool {
        self.items[..self.len]
            .iter()
            .flatten()
            .any(|effect| matches!(effect, TradesPacketEffect::GapDetected { .. }))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = TradesPacketEffect> + '_ {
        self.items[..self.len].iter().flatten().copied()
    }
}

impl TradesPacketEffect {
    pub(crate) fn into_event(self, packet_num: u16, base_time: f64) -> TradesEvent {
        match self {
            Self::Apply => TradesEvent::Applied {
                packet_num,
                base_time,
            },
            Self::GapDetected { start, end } => TradesEvent::GapDetected { start, end },
            Self::Duplicate => TradesEvent::Duplicate,
            Self::OutOfOrder { packet_num } => TradesEvent::OutOfOrder { packet_num },
            Self::GapFilled {
                packet_num,
                bucket_seq_range,
            } => TradesEvent::GapFilled {
                packet_num,
                bucket_seq_range,
            },
        }
    }
}
