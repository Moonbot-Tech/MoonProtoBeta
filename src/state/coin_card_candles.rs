//! CoinCard deep-history candles maintained on demand by Active Lib.
//!
//! These are not the retained 5m candles loaded by `RequestCandlesData` and
//! then updated from trades. Active Lib stores this demand-driven UI history
//! after a background request completes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::candles::{DeepHistoryKind, DeepPrice};

#[derive(Debug, Clone, PartialEq)]
pub enum CoinCardCandlesEvent {
    Updated {
        market: String,
        kind: DeepHistoryKind,
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        request_uid: u64,
        count: usize,
        revision: u64,
    },
    UpdateFailed {
        market: String,
        kind: DeepHistoryKind,
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        request_uid: Option<u64>,
        error: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct CoinCardCandlesState {
    by_market: HashMap<String, HashMap<DeepHistoryKind, CoinCardCandlesEntry>>,
    revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveCandleApply {
    Applied { count: usize, revision: u64 },
    NoBaseHistory,
    BadCandle,
    LateCandle,
}

#[derive(Debug, Clone, Default)]
struct CoinCardCandlesEntry {
    candles: Arc<Vec<DeepPrice>>,
    revision: u64,
}

impl CoinCardCandlesState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the last loaded CoinCard candles for one market/history kind.
    pub fn get(&self, market: &str, kind: DeepHistoryKind) -> Option<&[DeepPrice]> {
        self.by_market
            .get(market)?
            .get(&kind)
            .map(|e| e.candles.as_slice())
    }

    /// Last global update revision. Zero means no successful update yet.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Last revision for one market/history kind. Zero means never updated.
    pub fn entry_revision(&self, market: &str, kind: DeepHistoryKind) -> u64 {
        self.by_market
            .get(market)
            .and_then(|m| m.get(&kind))
            .map(|e| e.revision)
            .unwrap_or(0)
    }

    pub(crate) fn apply_update(
        &mut self,
        market: String,
        kind: DeepHistoryKind,
        request_uid: u64,
        candles: Vec<DeepPrice>,
    ) -> CoinCardCandlesEvent {
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = request_uid;
        self.revision = self.revision.wrapping_add(1).max(1);
        let count = candles.len();
        self.by_market.entry(market.clone()).or_default().insert(
            kind,
            CoinCardCandlesEntry {
                candles: Arc::new(candles),
                revision: self.revision,
            },
        );
        CoinCardCandlesEvent::Updated {
            market,
            kind,
            #[cfg(any(test, feature = "diagnostics"))]
            request_uid,
            count,
            revision: self.revision,
        }
    }

    pub(crate) fn apply_live_update(
        &mut self,
        market: &str,
        kind: DeepHistoryKind,
        candle: DeepPrice,
    ) -> LiveCandleApply {
        if candle.low() <= 0.0 || candle.high() <= 0.0 {
            return LiveCandleApply::BadCandle;
        }
        let Some(entry) = self
            .by_market
            .get_mut(market)
            .and_then(|m| m.get_mut(&kind))
        else {
            return LiveCandleApply::NoBaseHistory;
        };
        let mut candles = entry.candles.as_ref().clone();
        if let Some(last) = candles.last().copied() {
            let half_bar_days = kind.minutes() as f64 / 1440.0 * 0.5;
            if candle.time < last.time - half_bar_days {
                return LiveCandleApply::LateCandle;
            }
            if candle.time > last.time + half_bar_days {
                candles.push(candle);
            } else if let Some(last_mut) = candles.last_mut() {
                *last_mut = candle;
            }
        } else {
            candles.push(candle);
        }
        self.revision = self.revision.wrapping_add(1).max(1);
        entry.candles = Arc::new(candles);
        entry.revision = self.revision;
        LiveCandleApply::Applied {
            count: entry.candles.len(),
            revision: self.revision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(close: f32) -> DeepPrice {
        DeepPrice {
            open: close,
            close,
            high: close,
            low: close,
            volume: 1.0,
            time: 45_000.0,
        }
    }

    #[test]
    fn coin_card_candles_keep_separate_history_kinds() {
        let mut state = CoinCardCandlesState::new();
        state.apply_update(
            "BTCUSDT".to_string(),
            DeepHistoryKind::Hour4,
            10,
            vec![candle(1.0)],
        );
        state.apply_update(
            "BTCUSDT".to_string(),
            DeepHistoryKind::Day1,
            11,
            vec![candle(2.0), candle(3.0)],
        );

        assert_eq!(
            state.get("BTCUSDT", DeepHistoryKind::Hour4).unwrap()[0].close(),
            1.0
        );
        assert_eq!(
            state.get("BTCUSDT", DeepHistoryKind::Day1).unwrap().len(),
            2
        );
        assert_eq!(state.entry_revision("BTCUSDT", DeepHistoryKind::Hour4), 1);
        assert_eq!(state.entry_revision("BTCUSDT", DeepHistoryKind::Day1), 2);
    }

    #[test]
    fn snapshot_cow_updating_one_coin_card_history_keeps_other_candle_vec_shared() {
        let mut state = CoinCardCandlesState::new();
        state.apply_update(
            "BTCUSDT".to_string(),
            DeepHistoryKind::Hour4,
            10,
            vec![candle(1.0)],
        );
        state.apply_update(
            "BTCUSDT".to_string(),
            DeepHistoryKind::Day1,
            11,
            vec![candle(2.0)],
        );

        let snapshot = state.clone();
        let live_day = &state.by_market["BTCUSDT"][&DeepHistoryKind::Day1].candles;
        let snap_day = &snapshot.by_market["BTCUSDT"][&DeepHistoryKind::Day1].candles;
        assert!(Arc::ptr_eq(live_day, snap_day));

        state.apply_update(
            "BTCUSDT".to_string(),
            DeepHistoryKind::Hour4,
            12,
            vec![candle(3.0), candle(4.0)],
        );

        let live_hour = &state.by_market["BTCUSDT"][&DeepHistoryKind::Hour4].candles;
        let snap_hour = &snapshot.by_market["BTCUSDT"][&DeepHistoryKind::Hour4].candles;
        let live_day = &state.by_market["BTCUSDT"][&DeepHistoryKind::Day1].candles;
        let snap_day = &snapshot.by_market["BTCUSDT"][&DeepHistoryKind::Day1].candles;
        assert!(!Arc::ptr_eq(live_hour, snap_hour));
        assert!(Arc::ptr_eq(live_day, snap_day));
    }

    #[test]
    fn live_update_replaces_or_appends_loaded_tf_history_only() {
        let mut state = CoinCardCandlesState::new();
        let base = DeepPrice {
            open: 100.0,
            close: 101.0,
            high: 102.0,
            low: 99.0,
            volume: 10.0,
            time: 45_000.0,
        };
        state.apply_update("BTCUSDT".to_string(), DeepHistoryKind::Hour1, 1, vec![base]);

        let replace = DeepPrice {
            close: 105.0,
            time: base.time + (20.0 / 1440.0),
            ..base
        };
        assert_eq!(
            state.apply_live_update("BTCUSDT", DeepHistoryKind::Hour1, replace),
            LiveCandleApply::Applied {
                count: 1,
                revision: 2
            }
        );
        assert_eq!(
            state.get("BTCUSDT", DeepHistoryKind::Hour1).unwrap()[0].close(),
            105.0
        );

        let append = DeepPrice {
            close: 110.0,
            time: base.time + (60.0 / 1440.0),
            ..base
        };
        assert_eq!(
            state.apply_live_update("BTCUSDT", DeepHistoryKind::Hour1, append),
            LiveCandleApply::Applied {
                count: 2,
                revision: 3
            }
        );
        assert_eq!(
            state
                .get("BTCUSDT", DeepHistoryKind::Hour1)
                .unwrap()
                .last()
                .unwrap()
                .close(),
            110.0
        );

        let late = DeepPrice {
            close: 90.0,
            time: base.time - (40.0 / 1440.0),
            ..base
        };
        assert_eq!(
            state.apply_live_update("BTCUSDT", DeepHistoryKind::Hour1, late),
            LiveCandleApply::LateCandle
        );
        assert_eq!(
            state.apply_live_update("ETHUSDT", DeepHistoryKind::Hour1, append),
            LiveCandleApply::NoBaseHistory
        );
    }
}
