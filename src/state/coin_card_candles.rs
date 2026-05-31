//! CoinCard deep-history candles maintained on demand by Active Lib.
//!
//! These are not the retained 5m candles loaded by `RequestCandlesData` and
//! then updated from trades. Delphi stores this demand-driven UI history in
//! `TMarket.CoinCardCandles` after a background worker calls
//! `Engine.getDeepHistory(hk_4h, ...)`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::candles::{DeepHistoryKind, DeepPrice};

#[derive(Debug, Clone, PartialEq)]
pub enum CoinCardCandlesEvent {
    Updated {
        market: String,
        kind: DeepHistoryKind,
        #[doc(hidden)]
        request_uid: u64,
        count: usize,
        revision: u64,
    },
    UpdateFailed {
        market: String,
        kind: DeepHistoryKind,
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
            request_uid,
            count,
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
}
