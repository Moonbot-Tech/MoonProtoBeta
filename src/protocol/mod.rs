//! Low-level MoonProto protocol primitives.
//!
//! This module exposes command ordinals plus helpers for crypted payloads,
//! handshake packets, sliced datagrams, and ACK sliders. Applications should
//! use `MoonClient`, events, and snapshots. Use these primitives only for
//! protocol diagnostics or a custom transport/runtime tool.

pub mod control;
pub mod crypted;
pub mod handshake;
pub mod slicing;
pub mod slider;

/// MoonProto command ordinal matching Delphi `TMoonProtoCommand`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Command(u8);

#[allow(non_upper_case_globals)]
impl Command {
    pub const None: Self = Self(0);
    pub const Test: Self = Self(1);
    pub const Hello: Self = Self(2);
    pub const WhoAreYou: Self = Self(3);
    pub const ImFriend: Self = Self(4);
    pub const Fine: Self = Self(5);
    pub const HelloAgain: Self = Self(6);
    pub const TestWantCrypted: Self = Self(7);
    pub const TestCrypted: Self = Self(8);
    pub const TestStopCrypted: Self = Self(9);
    pub const Ping: Self = Self(10);
    pub const Data: Self = Self(11);
    pub const Grouped: Self = Self(12);
    pub const LogOff: Self = Self(13);
    pub const SizeTest: Self = Self(14);
    pub const SizeAck: Self = Self(15);
    pub const PTMUReq: Self = Self(16);
    pub const Sliced: Self = Self(17);
    pub const SlicedACK: Self = Self(18);
    pub const Crypted: Self = Self(19);
    pub const Echo: Self = Self(20);
    pub const EchoReply: Self = Self(21);
    pub const WrongHello: Self = Self(22);
    pub const WantNewHello: Self = Self(23);
    pub const NeedHelloAgain: Self = Self(24);
    pub const ProbeMTU: Self = Self(25);
    pub const ProbeMTUAck: Self = Self(26);
    pub const LogMsg: Self = Self(27);
    pub const Order: Self = Self(28);
    pub const UI: Self = Self(29);
    pub const Strat: Self = Self(30);
    pub const API: Self = Self(31);
    pub const Balance: Self = Self(32);
    pub const TradesStream: Self = Self(33);
    pub const TradesResend: Self = Self(34);
    pub const TradesResendResponse: Self = Self(35);
    pub const OrderBook: Self = Self(36);
    pub const Reserved1: Self = Self(37);

    /// Convert a wire command byte into the raw Delphi ordinal wrapper.
    /// The compressed flag is stripped exactly like Delphi `GetRealCommand`.
    pub fn from_byte(b: u8) -> Self {
        Self(b & 0x7F)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Reserved1.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Test => "Test",
            Self::Hello => "Hello",
            Self::WhoAreYou => "WhoAreYou",
            Self::ImFriend => "ImFriend",
            Self::Fine => "Fine",
            Self::HelloAgain => "HelloAgain",
            Self::TestWantCrypted => "TestWantCrypted",
            Self::TestCrypted => "TestCrypted",
            Self::TestStopCrypted => "TestStopCrypted",
            Self::Ping => "Ping",
            Self::Data => "Data",
            Self::Grouped => "Grouped",
            Self::LogOff => "LogOff",
            Self::SizeTest => "SizeTest",
            Self::SizeAck => "SizeAck",
            Self::PTMUReq => "PTMUReq",
            Self::Sliced => "Sliced",
            Self::SlicedACK => "SlicedACK",
            Self::Crypted => "Crypted",
            Self::Echo => "Echo",
            Self::EchoReply => "EchoReply",
            Self::WrongHello => "WrongHello",
            Self::WantNewHello => "WantNewHello",
            Self::NeedHelloAgain => "NeedHelloAgain",
            Self::ProbeMTU => "ProbeMTU",
            Self::ProbeMTUAck => "ProbeMTUAck",
            Self::LogMsg => "LogMsg",
            Self::Order => "Order",
            Self::UI => "UI",
            Self::Strat => "Strat",
            Self::API => "API",
            Self::Balance => "Balance",
            Self::TradesStream => "TradesStream",
            Self::TradesResend => "TradesResend",
            Self::TradesResendResponse => "TradesResendResponse",
            Self::OrderBook => "OrderBook",
            Self::Reserved1 => "Reserved1",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Command;

    #[test]
    fn command_from_byte_preserves_unknown_raw_ordinal_like_delphi() {
        let cmd = Command::from_byte(99);
        assert_eq!(cmd.to_byte(), 99);
        assert_eq!(cmd.name(), "Unknown");
        assert!(!cmd.is_known());
    }

    #[test]
    fn command_from_byte_strips_compressed_flag_without_losing_unknown_ordinal() {
        assert_eq!(
            Command::from_byte(Command::UI.to_byte() | 0x80),
            Command::UI
        );

        let cmd = Command::from_byte(99 | 0x80);
        assert_eq!(cmd.to_byte(), 99);
        assert_eq!(cmd.name(), "Unknown");
    }
}
