pub mod handshake;
pub mod slider;
pub mod slicing;
pub mod crypted;

/// MoonProto command enum (matches Delphi TMoonProtoCommand ordinals)
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
    /// Транспортный enum — silent drop через `Command::None` для unknown (стандарт UDP
    /// "не понял пакет — игнорирую"). При unknown byte > 0 — warn для диагностики
    /// server-side новых команд (A-02).
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
                log::warn!(target: "moonproto::cmd", "unknown Command byte: {} (server-side extension?)", b & 0x7F);
                Self::None
            }
        }
    }
}
