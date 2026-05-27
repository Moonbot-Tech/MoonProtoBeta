//! Delphi `_eps` / `_epsStep` / `_epsM` profile table.
//!
//! Hidden Active Lib policy: users do not configure these thresholds. The
//! server sends `Ord(cfg.Header.Current)` in BaseCheck and Rust selects the
//! same constants as `Unit1.pas:4715-4780`. Missing/unknown exchange falls back
//! to the small HTX/Huobi-class profile, by project decision.

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct EpsProfile {
    pub(crate) eps: f64,
    pub(crate) eps_step: f64,
    pub(crate) eps_m: f64,
}

impl EpsProfile {
    pub(crate) const HUOBI: Self = Self {
        eps: 0.000000000001,
        eps_step: 0.0000000000015,
        eps_m: 0.0000000000001,
    };

    pub(crate) const FUTURES_ALT: Self = Self {
        eps: 0.0000000001,
        eps_step: 0.00000000015,
        eps_m: 0.000000000005,
    };

    pub(crate) const HYPER: Self = Self {
        eps: 0.00000000001,
        eps_step: 0.000000000015,
        eps_m: 0.000000000001,
    };

    pub(crate) const BINANCE: Self = Self {
        eps: 0.00000001,
        eps_step: 0.000000015,
        eps_m: 0.000000009,
    };

    pub(crate) const fn from_exchange_code(exchange_code: Option<u8>) -> Self {
        match exchange_code {
            Some(DELPHI_PLATFORM_BINANCE)
            | Some(DELPHI_PLATFORM_FBINANCE)
            | Some(DELPHI_PLATFORM_QBINANCE) => Self::BINANCE,
            Some(DELPHI_PLATFORM_HUOBI)
            | Some(DELPHI_PLATFORM_BYBIT)
            | Some(DELPHI_PLATFORM_GATE)
            | Some(DELPHI_PLATFORM_BITGET) => Self::HUOBI,
            Some(DELPHI_PLATFORM_FBYBIT)
            | Some(DELPHI_PLATFORM_FGATE)
            | Some(DELPHI_PLATFORM_FBITGET) => Self::FUTURES_ALT,
            Some(DELPHI_PLATFORM_HYPER) | Some(DELPHI_PLATFORM_FHYPER) => Self::HYPER,
            _ => Self::HUOBI,
        }
    }
}

impl Default for EpsProfile {
    fn default() -> Self {
        Self::HUOBI
    }
}

pub(crate) const DELPHI_PLATFORM_FBYBIT: u8 = 2;
pub(crate) const DELPHI_PLATFORM_BINANCE: u8 = 3;
pub(crate) const DELPHI_PLATFORM_FBINANCE: u8 = 4;
pub(crate) const DELPHI_PLATFORM_HUOBI: u8 = 5;
pub(crate) const DELPHI_PLATFORM_QBINANCE: u8 = 6;
pub(crate) const DELPHI_PLATFORM_BYBIT: u8 = 7;
pub(crate) const DELPHI_PLATFORM_GATE: u8 = 8;
pub(crate) const DELPHI_PLATFORM_FGATE: u8 = 9;
pub(crate) const DELPHI_PLATFORM_BITGET: u8 = 10;
pub(crate) const DELPHI_PLATFORM_FBITGET: u8 = 11;
pub(crate) const DELPHI_PLATFORM_HYPER: u8 = 12;
pub(crate) const DELPHI_PLATFORM_FHYPER: u8 = 13;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eps_profile_matches_delphi_unit1_table() {
        assert_eq!(EpsProfile::from_exchange_code(Some(3)), EpsProfile::BINANCE);
        assert_eq!(EpsProfile::from_exchange_code(Some(4)), EpsProfile::BINANCE);
        assert_eq!(EpsProfile::from_exchange_code(Some(6)), EpsProfile::BINANCE);

        assert_eq!(EpsProfile::from_exchange_code(Some(5)), EpsProfile::HUOBI);
        assert_eq!(EpsProfile::from_exchange_code(Some(7)), EpsProfile::HUOBI);
        assert_eq!(EpsProfile::from_exchange_code(Some(8)), EpsProfile::HUOBI);
        assert_eq!(EpsProfile::from_exchange_code(Some(10)), EpsProfile::HUOBI);

        assert_eq!(
            EpsProfile::from_exchange_code(Some(2)),
            EpsProfile::FUTURES_ALT
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(9)),
            EpsProfile::FUTURES_ALT
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(11)),
            EpsProfile::FUTURES_ALT
        );

        assert_eq!(EpsProfile::from_exchange_code(Some(12)), EpsProfile::HYPER);
        assert_eq!(EpsProfile::from_exchange_code(Some(13)), EpsProfile::HYPER);

        assert_eq!(EpsProfile::from_exchange_code(None), EpsProfile::HUOBI);
        assert_eq!(EpsProfile::from_exchange_code(Some(255)), EpsProfile::HUOBI);
    }
}
