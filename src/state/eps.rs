//! Delphi `_eps` / `_epsStep` / `_epsM` profile table.
//!
//! Hidden Active Lib policy: users do not configure these thresholds. The
//! server sends `Ord(cfg.Header.Current)` in BaseCheck and Rust selects the
//! same constants as `Unit1.pas:4715-4780`. Missing/unknown exchange falls back
//! to the small HTX/Huobi-class profile, by project decision.

use crate::commands::market::ExchangeCode;

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

    pub(crate) const fn from_exchange_code(exchange_code: Option<ExchangeCode>) -> Self {
        match exchange_code {
            Some(ExchangeCode::Binance)
            | Some(ExchangeCode::FBinance)
            | Some(ExchangeCode::QBinance) => Self::BINANCE,
            Some(ExchangeCode::Huobi)
            | Some(ExchangeCode::ByBit)
            | Some(ExchangeCode::Gate)
            | Some(ExchangeCode::BitGet) => Self::HUOBI,
            Some(ExchangeCode::FBybit)
            | Some(ExchangeCode::FGate)
            | Some(ExchangeCode::FBitGet) => Self::FUTURES_ALT,
            Some(ExchangeCode::Hyper) | Some(ExchangeCode::FHyper) => Self::HYPER,
            _ => Self::HUOBI,
        }
    }
}

impl Default for EpsProfile {
    fn default() -> Self {
        Self::HUOBI
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eps_profile_matches_delphi_unit1_table() {
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::Binance)),
            EpsProfile::BINANCE
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::FBinance)),
            EpsProfile::BINANCE
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::QBinance)),
            EpsProfile::BINANCE
        );

        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::Huobi)),
            EpsProfile::HUOBI
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::ByBit)),
            EpsProfile::HUOBI
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::Gate)),
            EpsProfile::HUOBI
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::BitGet)),
            EpsProfile::HUOBI
        );

        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::FBybit)),
            EpsProfile::FUTURES_ALT
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::FGate)),
            EpsProfile::FUTURES_ALT
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::FBitGet)),
            EpsProfile::FUTURES_ALT
        );

        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::Hyper)),
            EpsProfile::HYPER
        );
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::FHyper)),
            EpsProfile::HYPER
        );

        assert_eq!(EpsProfile::from_exchange_code(None), EpsProfile::HUOBI);
        assert_eq!(
            EpsProfile::from_exchange_code(Some(ExchangeCode::from_byte(255))),
            EpsProfile::HUOBI
        );
    }
}
