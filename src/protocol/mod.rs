//! Low-level MoonProto protocol primitives.
//!
//! This module exposes command ordinals plus helpers for crypted payloads,
//! handshake packets, sliced datagrams, and ACK sliders. Most applications do
//! not need these APIs directly; use `Client`, `EventDispatcher`, and
//! `commands::*` builders unless you are writing protocol diagnostics or a
//! custom transport tool.

pub mod control;
pub mod crypted;
pub mod handshake;
pub mod slicing;
pub mod slider;

/// MoonProto command enum matching Delphi `TMoonProtoCommand` ordinals.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    None = 0,
    Test = 1,
    Hello = 2,
    WhoAreYou = 3,
    ImFriend = 4,
    Fine = 5,
    HelloAgain = 6,
    TestWantCrypted = 7,
    TestCrypted = 8,
    TestStopCrypted = 9,
    Ping = 10,
    Data = 11,
    Grouped = 12,
    LogOff = 13,
    SizeTest = 14,
    SizeAck = 15,
    PTMUReq = 16,
    Sliced = 17,
    SlicedACK = 18,
    Crypted = 19,
    Echo = 20,
    EchoReply = 21,
    WrongHello = 22,
    WantNewHello = 23,
    NeedHelloAgain = 24,
    ProbeMTU = 25,
    ProbeMTUAck = 26,
    LogMsg = 27,
    Order = 28,
    UI = 29,
    Strat = 30,
    API = 31,
    Balance = 32,
    TradesStream = 33,
    TradesResend = 34,
    TradesResendResponse = 35,
    OrderBook = 36,
    Reserved1 = 37,
}

impl Command {
    /// Convert a wire command byte into the typed enum.
    ///
    /// Unknown non-zero bytes map to `Command::None` after a throttled warning.
    /// This keeps the UDP receive path tolerant of corrupted packets and future
    /// server-side extensions.
    pub fn from_byte(b: u8) -> Self {
        match b & 0x7F {
            0 => Self::None,
            1 => Self::Test,
            2 => Self::Hello,
            3 => Self::WhoAreYou,
            4 => Self::ImFriend,
            5 => Self::Fine,
            6 => Self::HelloAgain,
            7 => Self::TestWantCrypted,
            8 => Self::TestCrypted,
            9 => Self::TestStopCrypted,
            10 => Self::Ping,
            11 => Self::Data,
            12 => Self::Grouped,
            13 => Self::LogOff,
            14 => Self::SizeTest,
            15 => Self::SizeAck,
            16 => Self::PTMUReq,
            17 => Self::Sliced,
            18 => Self::SlicedACK,
            19 => Self::Crypted,
            20 => Self::Echo,
            21 => Self::EchoReply,
            22 => Self::WrongHello,
            23 => Self::WantNewHello,
            24 => Self::NeedHelloAgain,
            25 => Self::ProbeMTU,
            26 => Self::ProbeMTUAck,
            27 => Self::LogMsg,
            28 => Self::Order,
            29 => Self::UI,
            30 => Self::Strat,
            31 => Self::API,
            32 => Self::Balance,
            33 => Self::TradesStream,
            34 => Self::TradesResend,
            35 => Self::TradesResendResponse,
            36 => Self::OrderBook,
            37 => Self::Reserved1,
            // 0 уже покрыт в `0 => Self::None` выше; здесь — всё прочее (unknown).
            _ => {
                // A-V2-06 fix: throttle warn — атакующий мог бы залить лог штормом
                // пакетов с unknown cmd. Логируем максимум 1 раз в секунду.
                use std::sync::atomic::{AtomicI64, Ordering};
                static LAST_LOGGED_MS: AtomicI64 = AtomicI64::new(0);
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let last = LAST_LOGGED_MS.load(Ordering::Relaxed);
                if now_ms.saturating_sub(last) > 1000 {
                    LAST_LOGGED_MS.store(now_ms, Ordering::Relaxed);
                    log::warn!(target: "moonproto::cmd",
                        "unknown Command byte: {} (server-side extension? / corrupted pkt? / DoS attempt?)",
                        b & 0x7F);
                }
                Self::None
            }
        }
    }
}
