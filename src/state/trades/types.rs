/// Результат применения TradesStream packet-number state.
#[derive(Debug, Clone)]
pub enum TradesEvent {
    /// Пакет применён.
    ///
    /// Active Lib уже раздаёт rows по market state и retained `SeqRing`
    /// storage до эмита этого события. Событие является лёгким сигналом
    /// "новые rows доступны"; оно намеренно не содержит owned `TradesPacket`,
    /// чтобы hot path не собирал `Vec` ради public callback.
    Applied { packet_num: u16, base_time: f64 },
    /// Обнаружен gap: пропущены packet_num в `[start..=end]`. Bucket создан, retry проверяется через `tick()`.
    GapDetected { start: u16, end: u16 },
    /// Пакет был фактически дубликат (packet_num == last).
    /// Delphi не двигает gap-state для него, но всё равно применяет payload дальше.
    Duplicate,
    /// Пакет пришёл вне диапазона — может быть после reset, отображает packet_num.
    OutOfOrder { packet_num: u16 },
    /// Принят out-of-order пакет, который был помечен в одном из gap-bucket'ов (`recvd[i]=true`).
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
    /// Bucket закрыт: получены все trades или исчерпан retry лимит.
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
