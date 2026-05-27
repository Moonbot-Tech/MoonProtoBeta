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
#[derive(Debug, Clone, PartialEq, Eq)]
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
