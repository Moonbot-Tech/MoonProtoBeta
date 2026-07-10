use super::*;
use crate::state::history::{CandleVolumeSnapshot, DerivedDeltaSnapshot};
use crate::time::SECONDS_PER_DAY;

fn trade(time: f64, price: f32, qty: f32) -> TradeHistoryRow {
    TradeHistoryRow {
        time: mt(time),
        price,
        qty,
    }
}

fn mt(days: f64) -> MoonTime {
    crate::state::history::moon_time_from_delphi_days(days)
}

#[test]
fn memory_sized_config_stays_inside_budget() {
    let total = 16 * GIB;
    let market_count = 1_000;
    let cfg = MarketHistoryConfig::from_total_memory_bytes(total, market_count);
    let total_estimated = cfg.estimated_bytes_per_market() * market_count;

    assert!(cfg.futures_trades_capacity > cfg.spot_trades_capacity);
    assert!(
        total_estimated <= MarketHistoryConfig::history_budget_bytes(total),
        "history defaults should fit the configured memory budget"
    );
}

#[test]
fn small_memory_config_uses_larger_fraction() {
    let small = 4 * GIB;
    let large = 16 * GIB;
    assert_eq!(MarketHistoryConfig::history_budget_bytes(small), small / 4);
    assert_eq!(MarketHistoryConfig::history_budget_bytes(large), large / 5);
}

#[test]
fn auto_budget_percent_clamps_and_scales_memory_budget() {
    let total = 16 * GIB;

    assert_eq!(
        MarketHistorySizing::clamp_budget_percent(1),
        MarketHistorySizing::MIN_BUDGET_PERCENT
    );
    assert_eq!(
        MarketHistorySizing::clamp_budget_percent(900),
        MarketHistorySizing::MAX_BUDGET_PERCENT
    );
    assert_eq!(
        MarketHistoryConfig::history_budget_bytes_with_budget_percent(total, 250),
        MarketHistoryConfig::history_budget_bytes(total) * 250 / 100
    );
    assert_eq!(
        MarketHistoryConfig::history_budget_bytes_with_budget_percent(total, 1),
        MarketHistoryConfig::history_budget_bytes(total)
    );
    assert_eq!(
        MarketHistoryConfig::history_budget_bytes_with_budget_percent(total, 900),
        MarketHistoryConfig::history_budget_bytes(total) * 800 / 100
    );
}

#[test]
fn auto_sizing_with_budget_percent_preserves_fixed_variant_compatibility() {
    let total = 16 * GIB;
    let market_count = 500;
    let base = MarketHistoryConfig::from_total_memory_bytes(total, market_count);
    let bigger =
        MarketHistoryConfig::from_total_memory_bytes_with_budget_percent(total, market_count, 400);

    assert!(bigger.futures_trades_capacity >= base.futures_trades_capacity);
    assert!(bigger.last_price_capacity >= base.last_price_capacity);

    let fixed = MarketHistoryConfig::default();
    assert_eq!(
        MarketHistorySizing::fixed(fixed).resolve(market_count),
        fixed
    );
}

#[test]
fn default_config_is_safe_for_all_markets_subscription() {
    let cfg = MarketHistoryConfig::default();
    let assumed_large_market_universe = 1_000;
    assert!(
            cfg.estimated_bytes_per_market() * assumed_large_market_universe
                < MarketHistoryConfig::history_budget_bytes(8 * GIB),
            "default config must not become multi-GB when subscribe_all_trades creates stores for all markets"
        );
}

#[test]
fn registry_configures_trade_storage_scope_from_known_markets() {
    let mut registry = MarketHistoryRegistry::new(MarketHistoryConfig {
        futures_trades_capacity: 1,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });
    let markets = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
    ];

    assert_eq!(
        registry.configure_markets(&markets, Some(&TradeStorageScope::All)),
        3
    );
    assert!(registry.contains_market("BTCUSDT"));
    assert!(registry.contains_market("ETHUSDT"));

    let scope = TradeStorageScope::from_markets(["ETHUSDT"]);
    assert_eq!(registry.configure_markets(&markets, Some(&scope)), 1);
    assert!(!registry.contains_market("BTCUSDT"));
    assert!(registry.contains_market("ETHUSDT"));

    assert_eq!(registry.configure_markets(&markets, None), 0);
    assert!(registry.is_empty());
}

#[test]
fn registry_resolves_stream_sections_by_configured_server_index() {
    let mut registry = MarketHistoryRegistry::new(MarketHistoryConfig {
        futures_trades_capacity: 1,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });
    let markets = vec![
        "ETHUSDT".to_string(),
        "BTCUSDT".to_string(),
        "SOLUSDT".to_string(),
    ];

    registry.configure_markets(&markets, Some(&TradeStorageScope::All));
    assert!(registry.get_mut_by_server_index(0).is_some());
    assert!(registry.get_mut_by_server_index(1).is_some());
    assert!(registry.get_mut_by_server_index(3).is_none());

    let scope = TradeStorageScope::from_markets(["BTCUSDT"]);
    registry.configure_markets(&markets, Some(&scope));
    assert!(registry.get_mut_by_server_index(0).is_none());
    assert!(registry.get_mut_by_server_index(1).is_some());
    assert!(registry.get_mut_by_server_index(2).is_none());

    registry.configure_market_index_slots(
        &[
            None,
            Some("BTCUSDT".to_string()),
            Some("SOLUSDT".to_string()),
        ],
        Some(&TradeStorageScope::All),
    );
    assert!(registry.get_mut_by_server_index(0).is_none());
    assert!(registry.get_mut_by_server_index(1).is_some());
    assert!(registry.get_mut_by_server_index(2).is_some());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (reuses existing TMarket on re-list)
fn registry_reconfigure_preserves_existing_store() {
    let mut registry = MarketHistoryRegistry::new(MarketHistoryConfig {
        futures_trades_capacity: 2,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });
    let first = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    registry.configure_markets(&first, Some(&TradeStorageScope::All));
    registry
        .get_mut("BTCUSDT")
        .unwrap()
        .append_futures_trade(trade(45_000.0, 100.0, 1.0));

    let after_listing = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
    ];
    registry.configure_markets(&after_listing, Some(&TradeStorageScope::All));

    assert!(registry.contains_market("SOLUSDT"));
    let mut out = Vec::new();
    registry
        .readers("BTCUSDT")
        .unwrap()
        .futures_trades
        .unwrap()
        .copy_last(10, &mut out);
    assert_eq!(out, vec![trade(45_000.0, 100.0, 1.0)]);
}

#[test]
fn registry_allocates_market_history_only_from_configured_scope() {
    let mut registry = MarketHistoryRegistry::new(MarketHistoryConfig {
        futures_trades_capacity: 2,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 2,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    assert!(registry.is_empty());
    assert!(registry.readers("BTCUSDT").is_none());

    registry.configure_markets(
        &["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        Some(&TradeStorageScope::All),
    );
    registry.get_mut("BTCUSDT").unwrap().append_last_price(
        100.0,
        mt(45_000.0),
        99.0,
        101.0,
        true,
        false,
    );
    registry
        .get_mut("ETHUSDT")
        .unwrap()
        .append_futures_trade(trade(45_000.0, 10.0, 1.0));

    assert_eq!(registry.len(), 2);
    assert!(registry.contains_market("BTCUSDT"));
    assert!(registry.contains_market("ETHUSDT"));

    let mut last_prices = Vec::new();
    registry
        .readers("BTCUSDT")
        .unwrap()
        .last_prices
        .unwrap()
        .copy_last(10, &mut last_prices);
    assert_eq!(
        last_prices,
        vec![LastPricePoint {
            current: 100.0,
            time: mt(45_000.0),
        }]
    );
}

#[test]
fn last_price_appends_only_delphi_history_price_markets() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 4,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    assert_eq!(
        store.append_last_price(10.0, mt(45_000.0), 9.0, 11.0, false, false),
        None
    );
    assert_eq!(
        store.append_last_price(0.0, mt(45_000.0), 9.0, 11.0, true, false),
        None
    );
    assert_eq!(
        store.append_last_price(10.0, mt(45_000.0), 0.0, 0.0, true, false),
        None
    );
    assert_eq!(
        store.append_last_price(10.0, mt(45_000.0), 9.0, 11.0, true, false),
        Some(0)
    );

    let mut out = Vec::new();
    store.readers().last_prices.unwrap().copy_last(10, &mut out);
    assert_eq!(
        out,
        vec![LastPricePoint {
            current: 10.0,
            time: mt(45_000.0)
        }]
    );
}

#[test]
fn last_price_history_feeds_delphi_hourly_delta_windows() {
    let now = 45_000.0;
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 8,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    store.append_last_price(
        100.0,
        mt(now - 50.0 / SECONDS_PER_DAY),
        99.0,
        101.0,
        true,
        false,
    );
    store.append_last_price(130.0, mt(now - 14.0 / 1440.0), 129.0, 131.0, true, false);
    store.append_last_price(170.0, mt(now - 59.0 / 1440.0), 169.0, 171.0, true, false);
    store.append_last_price(250.0, mt(now - 60.0 / 1440.0), 249.0, 251.0, true, false);

    store.refresh_derived_analytics(mt(now));
    let derived = store.derived_snapshot();

    assert!((derived.last_price_deltas.one_minute - 0.0).abs() < 1e-9);
    assert!((derived.last_price_deltas.fifteen_minutes - 30.0).abs() < 1e-9);
    assert!((derived.last_price_deltas.thirty_minutes - 30.0).abs() < 1e-9);
    assert!((derived.last_price_deltas.one_hour - 70.0).abs() < 1e-9);
    assert!((derived.deltas.one_hour - 70.0).abs() < 1e-9);
}

#[test]
fn futures_trades_append_directly_and_update_volumes() {
    let base = 45_000.0;
    let sec = |s: f64| base + s / 86_400.0;
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    assert_eq!(
        store.append_futures_trade(trade(sec(10.0), 100.0, 1.0)),
        Some(0)
    );

    assert_eq!(
        store.append_futures_trade(trade(sec(9.0), 90.0, 1.0)),
        Some(1)
    );
    assert_eq!(
        store.append_futures_trade(trade(sec(12.0), 120.0, -2.0)),
        Some(2)
    );
    assert_eq!(
        store.append_futures_trade(trade(sec(11.0), 110.0, 3.0)),
        Some(3)
    );

    let mut out = Vec::new();
    store
        .readers()
        .futures_trades
        .unwrap()
        .copy_last(8, &mut out);
    assert_eq!(
        out,
        vec![
            trade(sec(10.0), 100.0, 1.0),
            trade(sec(9.0), 90.0, 1.0),
            trade(sec(12.0), 120.0, -2.0),
            trade(sec(11.0), 110.0, 3.0),
        ]
    );

    let volumes = store.rolling_volumes_snapshot(mt(sec(12.0)));
    assert_eq!(volumes.five_minutes.buy_value, 520.0);
    assert_eq!(volumes.five_minutes.sell_value, 240.0);
    assert_eq!(volumes.five_minutes.trade_count, 4);
}

#[test]
fn stream_append_helpers_share_delphi_packet_time_shift() {
    let base = 45_000.0;
    let now = base + 2.0 / 24.0 + 3.0 / 86_400.0;
    let mut shift = TradesPacketTimeShift::new();
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 8,
        liquidation_capacity: 8,
        mm_orders_capacity: 8,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let fut_time = store.append_futures_stream_trade(base, 100, now, 100.0, 1.0, &mut shift);
    let taker = [7u8; 20];
    let (mm_time, mm_seq) =
        store.append_mm_stream_order(base, 200, base - 10.0, 5.0, -2.0, Some(taker), &mut shift);
    let (spot_time, spot_seq) =
        store.append_spot_stream_trade(base, -300, base - 10.0, 90.0, -1.0, &mut shift);
    assert_eq!(shift.shift_days(), Some(2.0 / 24.0));
    assert_eq!(fut_time, mt(base + 100.0 / 86_400_000.0 + 2.0 / 24.0));
    assert_eq!(mm_time, mt(base + 200.0 / 86_400_000.0 + 2.0 / 24.0));
    assert_eq!(spot_time, mt(base - 300.0 / 86_400_000.0 + 2.0 / 24.0));
    assert_eq!(mm_seq, Some(0));
    assert_eq!(spot_seq, Some(0));

    let readers = store.readers();
    let mut trades = Vec::new();
    readers.futures_trades.unwrap().copy_last(1, &mut trades);
    assert_eq!(trades[0].time, fut_time);

    let mut mm_orders = Vec::new();
    readers.mm_orders.unwrap().copy_last(1, &mut mm_orders);
    assert_eq!(
        mm_orders,
        vec![MMOrderHistoryRow {
            time: mm_time,
            volume: 5.0,
            q: -2.0,
        }]
    );

    let mut companions = Vec::new();
    readers
        .mm_order_companion
        .unwrap()
        .copy_last(1, &mut companions);
    assert_eq!(
        companions,
        vec![MMOrderCompanionData {
            taker,
            color: hl_address_color(taker),
        }]
    );
}

#[test]
fn evicted_futures_compact_to_mini_candles() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 2,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 8,
        candles_5m_capacity: 0,
    });

    for i in 0..4 {
        store.append_futures_trade(trade(10.0 + i as f64 / 86_400.0, 100.0 + i as f32, 1.0));
    }
    assert_eq!(store.pending_evicted_futures_for_compaction(), 2);
    assert_eq!(store.compact_evicted_futures(mt(20.0)), 1);

    let mut out = Vec::new();
    store.readers().mini_candles.unwrap().copy_last(8, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].cnt, 2);
    assert_eq!(out[0].min_price, 100.0);
    assert_eq!(out[0].max_price, 101.0);
    assert_eq!(out[0].buy_vol, 201.0);
}

#[test]
fn candles_snapshot_replaces_retained_5m_rows_and_feeds_deltas() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    let now = 45_000.0;
    store.replace_candles_5m_from_snapshot(
        &[
            Candle5mRow {
                time: mt(now - 10.0 / 1440.0),
                low: 90.0,
                high: 110.0,
                close: 100.0,
                open: 95.0,
                volume: 1_000.0,
            },
            Candle5mRow {
                time: mt(now - 4.0 / 1440.0),
                low: 95.0,
                high: 118.0,
                close: 112.0,
                open: 100.0,
                volume: 1_500.0,
            },
            Candle5mRow {
                time: mt(now),
                low: 100.0,
                high: 120.0,
                close: 115.0,
                open: 105.0,
                volume: 2_000.0,
            },
        ],
        mt(now),
    );

    let mut candles = Vec::new();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(candles.len(), 3);

    store.append_futures_trade(trade(now + 1.0 / 86_400.0, 125.0, 2.0));
    candles.clear();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(candles.len(), 3);
    // Snapshot candles are sealed — a trade does NOT touch them (reference: the live candle is separate from the ring).
    assert_eq!(candles[2].close, 115.0);
    assert_eq!(candles[2].high, 120.0);
    assert_eq!(candles[2].volume, 2_000.0);

    // refresh with a time >= the trade (in prod `now` is always >= the time of the last trade),
    // otherwise the live candle (now+1s) would fall outside the delta window.
    store.refresh_derived_analytics(mt(now + 1.0 / 86_400.0));
    let derived = store.derived_snapshot();
    // The trade went into the live candle (Delphi `FCandle`), exposed separately from the sealed ring.
    let live = derived.current_candle.expect("live candle from trade");
    assert_eq!(live.close, 125.0);
    assert_eq!(live.high, 125.0);
    assert_eq!(live.volume, 250.0);
    assert!((derived.candle_deltas.fifteen_minutes - 38.8888888889).abs() < 1e-6);
    assert_eq!(derived.candle_volumes.fifteen_minutes, 4_750.0);
    assert_eq!(derived.candle_volumes.one_hour, 4_750.0);
    assert_eq!(derived.trade_deltas.fifteen_minutes, 0.0);
    assert!((derived.deltas.fifteen_minutes - 38.8888888889).abs() < 1e-6);
}

#[test]
// parity: MoonBot MarketsU.pas:TMarket.RecalcPumpQ guard `High(Deep5m) < 2`
fn candle_derived_requires_three_sealed_candles() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    let now = 45_000.0;
    store.replace_candles_5m_from_snapshot(
        &[
            Candle5mRow {
                time: mt(now - 5.0 / 1440.0),
                low: 90.0,
                high: 110.0,
                close: 100.0,
                open: 95.0,
                volume: 1_000.0,
            },
            Candle5mRow {
                time: mt(now),
                low: 100.0,
                high: 120.0,
                close: 115.0,
                open: 105.0,
                volume: 2_000.0,
            },
        ],
        mt(now),
    );

    store.append_futures_trade(trade(now + 1.0 / 86_400.0, 125.0, 2.0));
    store.refresh_derived_analytics(mt(now + 1.0 / 86_400.0));
    let derived = store.derived_snapshot();
    assert!(
        derived.current_candle.is_some(),
        "live FCandle stays visible"
    );
    assert_eq!(derived.candle_deltas, DerivedDeltaSnapshot::default());
    assert_eq!(derived.candle_volumes, CandleVolumeSnapshot::default());
}

#[test]
// parity: MoonBot MarketsU.pas:TMarkets.ApplyRecvdStream clears old Deep5m holes
fn candles_snapshot_older_than_eleven_minutes_clears_ring() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    let now = 45_000.0;
    store.replace_candles_5m_from_snapshot(
        &[
            Candle5mRow {
                time: mt(now - 20.0 / 1440.0),
                low: 90.0,
                high: 110.0,
                close: 100.0,
                open: 95.0,
                volume: 1_000.0,
            },
            Candle5mRow {
                time: mt(now - 12.0 / 1440.0),
                low: 100.0,
                high: 120.0,
                close: 115.0,
                open: 105.0,
                volume: 2_000.0,
            },
        ],
        mt(now),
    );

    let mut candles = Vec::new();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert!(
        candles.is_empty(),
        "stale snapshot is a hole, not partial history to keep"
    );
    assert_eq!(
        store.derived_snapshot().candle_deltas,
        DerivedDeltaSnapshot::default()
    );
}

#[test]
fn futures_trades_roll_current_candle_after_five_minutes() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    let now = 45_000.0;
    store.replace_candles_5m_from_snapshot(
        &[Candle5mRow {
            time: mt(now),
            low: 100.0,
            high: 110.0,
            close: 105.0,
            open: 101.0,
            volume: 1_000.0,
        }],
        mt(now),
    );

    // The first trade of the next period — accumulates into a separate live
    // accumulator (Delphi `FCandle`), is NOT pushed into the sealed ring.
    let t1 = now + 6.0 / 1440.0;
    store.append_futures_trade(trade(t1, 120.0, 2.0));

    let mut candles = Vec::new();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(
        candles.len(),
        1,
        "snapshot candle is sealed; live candle is separate, not in the ring"
    );
    assert_eq!(candles[0].time, mt(now));
    assert_eq!(candles[0].close, 105.0);
    store.refresh_derived_analytics(mt(t1));
    let live = store
        .derived_snapshot()
        .current_candle
        .expect("live candle accumulating");
    assert_eq!(live.open, 120.0);
    assert_eq!(live.close, 120.0);

    // The second trade after >5 min — the current candle is sealed into the ring
    // (end-stamped with the seal time), a new live candle starts (Delphi Recalc5mCandle roll).
    let t2 = t1 + 6.0 / 1440.0;
    store.append_futures_trade(trade(t2, 130.0, 1.0));
    candles.clear();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(
        candles.len(),
        2,
        "first live candle is sealed and added to the ring"
    );
    assert_eq!(candles[0].time, mt(now));
    assert_eq!(
        candles[1].time,
        mt(t2),
        "sealed candle is stamped with the seal time (end of period)"
    );
    assert_eq!(candles[1].open, 120.0);
    assert_eq!(candles[1].close, 120.0);
    assert_eq!(candles[1].volume, 240.0);
    store.refresh_derived_analytics(mt(t2));
    let live2 = store
        .derived_snapshot()
        .current_candle
        .expect("new live candle after roll");
    assert_eq!(live2.open, 130.0);
}

#[test]
fn retained_trades_update_current_candle_and_derived_volumes() {
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    let now = 45_000.0;
    store.append_futures_trade(trade(now - 10.0 / 86_400.0, 100.0, 2.0));
    store.append_futures_trade(trade(now - 5.0 / 86_400.0, 110.0, -1.0));
    store.refresh_derived_analytics(mt(now));

    let derived = store.derived_snapshot();
    assert_eq!(derived.trade_volumes.one_minute.buy_value, 200.0);
    assert_eq!(derived.trade_volumes.one_minute.sell_value, 110.0);
    assert_eq!(derived.trade_volumes.one_minute.min_price, 100.0);
    assert_eq!(derived.trade_volumes.one_minute.max_price, 110.0);
    assert_eq!(
        derived.trade_deltas.one_minute, 0.0,
        "cfg.DeltasByTrades defaults to false in the core"
    );
    assert_eq!(derived.candle_deltas.one_minute, 0.0);
    assert_eq!(
        derived.candle_volumes.five_minutes, 0.0,
        "Delphi RecalcPumpQ exits before candle-derived values while Deep5m has fewer than 3 sealed candles"
    );
    assert_eq!(derived.deltas.one_minute, 0.0);

    store.set_deltas_by_trades(true);
    store.refresh_derived_analytics(mt(now));
    let derived = store.derived_snapshot();
    assert!((derived.trade_deltas.one_minute - 10.0).abs() < 1e-9);
    assert!((derived.deltas.one_minute - 10.0).abs() < 1e-9);
}

#[test]
// parity: MoonBot MarketsU.pas:TMarket.RecalcPumpQ
fn combined_long_deltas_do_not_drop_below_one_hour() {
    let trade = DerivedDeltaSnapshot {
        one_hour: 12.0,
        ..DerivedDeltaSnapshot::default()
    };
    let candles = DerivedDeltaSnapshot {
        two_hours: 4.0,
        three_hours: 5.0,
        twenty_four_hours: 6.0,
        seventy_two_hours: 7.0,
        ..DerivedDeltaSnapshot::default()
    };
    let last_price = DerivedDeltaSnapshot {
        fifteen_minutes: 13.0,
        one_hour: 14.0,
        ..DerivedDeltaSnapshot::default()
    };

    let combined = combine_deltas(trade, candles, last_price);

    assert_eq!(combined.fifteen_minutes, 13.0);
    assert_eq!(combined.one_hour, 14.0);
    assert_eq!(combined.two_hours, 14.0);
    assert_eq!(combined.three_hours, 14.0);
    assert_eq!(combined.twenty_four_hours, 14.0);
    assert_eq!(
        combined.seventy_two_hours, 7.0,
        "Delphi RecalcPumpQ only floors 2h/3h/24h by Last1hDelta; 72h stays its own source"
    );
}

#[test]
fn candle_long_delta_windows_match_delphi_trunc_hour_buckets() {
    let now = 45_000.0;
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    store.replace_candles_5m_from_snapshot(
        &[
            Candle5mRow {
                time: mt(now - 25.0 / 24.0),
                low: 100.0,
                high: 220.0,
                close: 100.0,
                open: 100.0,
                volume: 16.0,
            },
            Candle5mRow {
                time: mt(now - 24.5 / 24.0),
                low: 100.0,
                high: 150.0,
                close: 100.0,
                open: 100.0,
                volume: 4.0,
            },
            Candle5mRow {
                time: mt(now - 3.5 / 24.0),
                low: 100.0,
                high: 140.0,
                close: 100.0,
                open: 100.0,
                volume: 2.0,
            },
            Candle5mRow {
                time: mt(now - 3.0 / 24.0),
                low: 100.0,
                high: 190.0,
                close: 100.0,
                open: 100.0,
                volume: 8.0,
            },
            Candle5mRow {
                time: mt(now - 2.5 / 24.0),
                low: 100.0,
                high: 130.0,
                close: 100.0,
                open: 100.0,
                volume: 1.0,
            },
        ],
        mt(now - 2.5 / 24.0),
    );

    store.refresh_derived_analytics(mt(now));
    let derived = store.derived_snapshot();

    assert!((derived.candle_deltas.two_hours - 30.0).abs() < 1e-9);
    assert!((derived.candle_deltas.three_hours - 90.0).abs() < 1e-9);
    assert!((derived.candle_deltas.twenty_four_hours - 90.0).abs() < 1e-9);
    assert_eq!(
            derived.candle_volumes.twenty_four_hours, 11.0,
            "candle volumes keep exact 24h semantics; only Delphi long delta fields use h<= bucket windows"
        );
}

#[test]
// parity: MoonBot MarketsU.pas:TMarket.RecalcPumpQ (h<= bucket windows)
fn candle_windows_exclude_exact_old_boundary() {
    let now = 45_000.0;
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });
    store.replace_candles_5m_from_snapshot(
        &[
            Candle5mRow {
                time: mt(now - 15.0 / 1440.0),
                low: 100.0,
                high: 200.0,
                close: 100.0,
                open: 100.0,
                volume: 5.0,
            },
            Candle5mRow {
                time: mt(now - (15.0 * 60.0 - 1.0) / SECONDS_PER_DAY),
                low: 100.0,
                high: 150.0,
                close: 100.0,
                open: 100.0,
                volume: 3.0,
            },
            Candle5mRow {
                time: mt(now),
                low: 100.0,
                high: 100.0,
                close: 100.0,
                open: 100.0,
                volume: 0.0,
            },
        ],
        mt(now),
    );

    store.refresh_derived_analytics(mt(now));
    let derived = store.derived_snapshot();

    assert!((derived.candle_deltas.fifteen_minutes - 50.0).abs() < 1e-9);
    assert_eq!(derived.candle_volumes.fifteen_minutes, 3.0);
}

#[test]
fn candle_derived_long_tail_uses_only_newest_five_hundred_rows() {
    let now = 45_000.0;
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 600,
    });
    let candles = (0..501)
        .map(|idx| {
            let is_excluded_oldest = idx == 0;
            Candle5mRow {
                time: mt(now - (500 - idx) as f64 * 5.0 / 1440.0),
                open: if is_excluded_oldest { 1.0 } else { 100.0 },
                close: 100.0,
                high: 100.0,
                low: if is_excluded_oldest { 1.0 } else { 100.0 },
                volume: 1.0,
            }
        })
        .collect::<Vec<_>>();

    store.replace_candles_5m_from_snapshot(&candles, mt(now));

    let mut retained = Vec::new();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(600, &mut retained);
    assert_eq!(retained.len(), 501);
    assert_eq!(
        store.derived_snapshot().candle_deltas.seventy_two_hours,
        0.0
    );
    assert_eq!(store.last_refresh_work.candle_rows_visited, 500);
}

#[test]
fn derived_refresh_work_is_bounded_by_baskets_and_candle_limit() {
    let now = MoonTime::from_unix_millis(1_800_000_000_000);
    let mut store = MarketHistoryStore::new(MarketHistoryConfig {
        futures_trades_capacity: 4,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 5_000,
        mini_candles_capacity: 0,
        candles_5m_capacity: 1_000,
    });
    store.diag_fill_to_capacity(now, 3_600_000);

    store.trade_analytics_dirty = true;
    store.last_price_analytics_dirty = true;
    store.sealed_candle_analytics_dirty = true;
    store.refresh_derived_analytics(now);

    assert_eq!(
        store.last_refresh_work.trade_buckets_visited,
        crate::state::history::ROLLING_VOLUME_BUCKETS
    );
    assert_eq!(
        store.last_refresh_work.last_price_buckets_visited,
        crate::state::history::ROLLING_PRICE_RANGE_BUCKETS
    );
    assert_eq!(store.last_refresh_work.candle_rows_visited, 500);
    assert!(store.last_refresh_work.published);

    store.refresh_derived_analytics(now);
    assert_eq!(
        store.last_refresh_work,
        derived::DerivedRefreshWork::default()
    );

    store.append_futures_trade(TradeHistoryRow {
        time: MoonTime::from_unix_millis(now.unix_millis() + 1_000),
        price: 101.0,
        qty: 2.0,
    });
    store.refresh_derived_analytics(MoonTime::from_unix_millis(now.unix_millis() + 1_000));
    assert_eq!(
        store.last_refresh_work.trade_buckets_visited,
        crate::state::history::ROLLING_VOLUME_BUCKETS
    );
    assert_eq!(store.last_refresh_work.last_price_buckets_visited, 0);
    assert_eq!(
        store.last_refresh_work.candle_rows_visited, 0,
        "a live trade must overlay the cached candle aggregate without rescanning sealed history"
    );
    assert!(store.last_refresh_work.published);
}

#[test]
#[ignore = "diagnostic CPU benchmark; run with --ignored --nocapture"]
fn derived_refresh_full_rings_cpu_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;

    use crate::client::thread_cpu::ThreadCpuTimer;

    const MAX_CONFIG: MarketHistoryConfig = MarketHistoryConfig {
        futures_trades_capacity: 200_000,
        spot_trades_capacity: 150_000,
        liquidation_capacity: 50_000,
        mm_orders_capacity: 50_000,
        last_price_capacity: 80_000,
        mini_candles_capacity: 50_000,
        candles_5m_capacity: 20_000,
    };
    const REALISTIC_MARKETS: usize = 500;
    const REALISTIC_LAST_PRICES: usize = 7_200;
    const REALISTIC_CANDLES: usize = 500;
    const REALISTIC_TICKS: usize = 3;

    let now = MoonTime::from_unix_millis(1_800_000_000_000);

    let mut max_store = MarketHistoryStore::new(MAX_CONFIG);
    max_store.diag_fill_to_capacity(now, 3_600_000);
    max_store.trade_analytics_dirty = true;
    max_store.last_price_analytics_dirty = true;
    max_store.sealed_candle_analytics_dirty = true;
    let max_cpu = ThreadCpuTimer::start();
    let max_wall = Instant::now();
    max_store.refresh_derived_analytics(now);
    let max_wall = max_wall.elapsed();
    let max_cpu = max_cpu.elapsed();
    black_box(max_store.derived_snapshot());
    eprintln!(
        "DERIVED_CPU max-one-market last_rows={} candle_rows={} wall_us={} thread_cpu_ns={:?} thread_cycles={:?}",
        MAX_CONFIG.last_price_capacity,
        MAX_CONFIG.candles_5m_capacity,
        max_wall.as_micros(),
        max_cpu.time.map(|value| value.as_nanos()),
        max_cpu.cycles
    );

    let realistic_config = MarketHistoryConfig {
        futures_trades_capacity: 1,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: REALISTIC_LAST_PRICES,
        mini_candles_capacity: 0,
        candles_5m_capacity: REALISTIC_CANDLES,
    };
    let names = (0..REALISTIC_MARKETS)
        .map(|idx| format!("PERF{idx:04}USDT"))
        .collect::<Vec<_>>();
    let mut registry = MarketHistoryRegistry::new(realistic_config);
    registry.configure_markets(&names, Some(&TradeStorageScope::All));
    for name in &names {
        registry
            .get_mut(name)
            .expect("configured benchmark market")
            .diag_fill_to_capacity(now, 3_600_000);
    }

    for name in &names {
        registry
            .get_mut(name)
            .expect("configured benchmark market")
            .trade_analytics_dirty = true;
        registry
            .get_mut(name)
            .expect("configured benchmark market")
            .last_price_analytics_dirty = true;
        registry
            .get_mut(name)
            .expect("configured benchmark market")
            .sealed_candle_analytics_dirty = true;
    }
    registry.refresh_derived_analytics(now);

    let realistic_cpu = ThreadCpuTimer::start();
    let realistic_wall = Instant::now();
    for _ in 0..REALISTIC_TICKS {
        for name in &names {
            registry
                .get_mut(name)
                .expect("configured benchmark market")
                .trade_analytics_dirty = true;
            registry
                .get_mut(name)
                .expect("configured benchmark market")
                .last_price_analytics_dirty = true;
            registry
                .get_mut(name)
                .expect("configured benchmark market")
                .sealed_candle_analytics_dirty = true;
        }
        registry.refresh_derived_analytics(now);
    }
    let realistic_wall = realistic_wall.elapsed();
    let realistic_cpu = realistic_cpu.elapsed();
    black_box(registry.get(&names[0]).unwrap().derived_snapshot());
    eprintln!(
        "DERIVED_CPU forced-all markets={} ticks={} last_rows_per_market={} candle_rows_per_market={} wall_us={} wall_us_per_tick={} thread_cpu_ns={:?} thread_cycles={:?}",
        REALISTIC_MARKETS,
        REALISTIC_TICKS,
        REALISTIC_LAST_PRICES,
        REALISTIC_CANDLES,
        realistic_wall.as_micros(),
        realistic_wall.as_micros() / REALISTIC_TICKS as u128,
        realistic_cpu.time.map(|value| value.as_nanos()),
        realistic_cpu.cycles
    );

    let idle_cpu = ThreadCpuTimer::start();
    let idle_wall = Instant::now();
    for _ in 0..REALISTIC_TICKS {
        registry.refresh_derived_analytics(now);
    }
    let idle_wall = idle_wall.elapsed();
    let idle_cpu = idle_cpu.elapsed();
    eprintln!(
        "DERIVED_CPU idle markets={} ticks={} wall_us={} wall_us_per_tick={} thread_cycles={:?}",
        REALISTIC_MARKETS,
        REALISTIC_TICKS,
        idle_wall.as_micros(),
        idle_wall.as_micros() / REALISTIC_TICKS as u128,
        idle_cpu.cycles
    );

    let trade_cpu = ThreadCpuTimer::start();
    let trade_wall = Instant::now();
    for tick in 0..REALISTIC_TICKS {
        let trade_time = MoonTime::from_unix_millis(now.unix_millis() + tick as i64 * 250);
        for market_index in 0..REALISTIC_MARKETS {
            registry
                .get_mut_by_server_index(market_index as u16)
                .expect("configured benchmark market index")
                .append_futures_trade(TradeHistoryRow {
                    time: trade_time,
                    price: 100.0 + tick as f32 * 0.01,
                    qty: 1.0,
                });
        }
        registry.refresh_derived_analytics(trade_time);
    }
    let trade_wall = trade_wall.elapsed();
    let trade_cpu = trade_cpu.elapsed();
    eprintln!(
        "DERIVED_CPU live-trades markets={} ticks={} wall_us={} wall_us_per_tick={} thread_cycles={:?}",
        REALISTIC_MARKETS,
        REALISTIC_TICKS,
        trade_wall.as_micros(),
        trade_wall.as_micros() / REALISTIC_TICKS as u128,
        trade_cpu.cycles
    );

    let last_price_cpu = ThreadCpuTimer::start();
    let last_price_wall = Instant::now();
    for tick in 0..REALISTIC_TICKS {
        let price_time = MoonTime::from_unix_millis(now.unix_millis() + tick as i64 * 250);
        for name in &names {
            registry
                .get_mut(name)
                .expect("configured benchmark market")
                .append_last_price(
                    100.0 + tick as f64 * 0.01,
                    price_time,
                    99.0,
                    101.0,
                    true,
                    false,
                );
        }
        registry.refresh_derived_analytics(price_time);
    }
    let last_price_wall = last_price_wall.elapsed();
    let last_price_cpu = last_price_cpu.elapsed();
    eprintln!(
        "DERIVED_CPU live-last-price markets={} ticks={} wall_us={} wall_us_per_tick={} thread_cycles={:?}",
        REALISTIC_MARKETS,
        REALISTIC_TICKS,
        last_price_wall.as_micros(),
        last_price_wall.as_micros() / REALISTIC_TICKS as u128,
        last_price_cpu.cycles
    );

    for name in &names {
        registry
            .get_mut(name)
            .expect("configured benchmark market")
            .sealed_candle_analytics_dirty = true;
    }
    let candle_cpu = ThreadCpuTimer::start();
    let candle_wall = Instant::now();
    registry.refresh_derived_analytics(now);
    let candle_wall = candle_wall.elapsed();
    let candle_cpu = candle_cpu.elapsed();
    eprintln!(
        "DERIVED_CPU candle-seal markets={} candle_rows_per_market={} wall_us={} thread_cycles={:?}",
        REALISTIC_MARKETS,
        REALISTIC_CANDLES,
        candle_wall.as_micros(),
        candle_cpu.cycles
    );

    const COMPONENT_PASSES: usize = 20;
    let volume_component_wall = Instant::now();
    for _ in 0..COMPONENT_PASSES {
        for name in &names {
            let store = registry.get(name).expect("configured benchmark market");
            black_box(store.rolling_volumes.snapshot(now));
        }
    }
    let volume_component_wall = volume_component_wall.elapsed();

    let price_component_wall = Instant::now();
    for _ in 0..COMPONENT_PASSES {
        for name in &names {
            let store = registry.get(name).expect("configured benchmark market");
            black_box(
                store
                    .rolling_last_price_ranges
                    .snapshot(now, store.eps_profile.eps),
            );
        }
    }
    let price_component_wall = price_component_wall.elapsed();

    let publish_component_wall = Instant::now();
    for _ in 0..COMPONENT_PASSES {
        for name in &names {
            let store = registry.get(name).expect("configured benchmark market");
            store
                .read_handle
                .publish(&store.rolling_volumes, store.derived);
        }
    }
    let publish_component_wall = publish_component_wall.elapsed();
    eprintln!(
        "DERIVED_CPU components markets={} passes={} volume_snapshot_us_per_pass={} last_price_snapshot_us_per_pass={} publish_us_per_pass={}",
        REALISTIC_MARKETS,
        COMPONENT_PASSES,
        volume_component_wall.as_micros() / COMPONENT_PASSES as u128,
        price_component_wall.as_micros() / COMPONENT_PASSES as u128,
        publish_component_wall.as_micros() / COMPONENT_PASSES as u128,
    );
}
