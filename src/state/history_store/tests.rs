use super::*;
use crate::state::history::DerivedDeltaSnapshot;

fn trade(time: f64, price: f32, qty: f32) -> TradeHistoryRow {
    TradeHistoryRow { time, price, qty }
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
fn registry_reconfigure_preserves_existing_store_like_delphi_market_cow() {
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
        .append_futures_trade_like_delphi(trade(45_000.0, 100.0, 1.0));

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
    registry
        .get_mut("BTCUSDT")
        .unwrap()
        .append_last_price_like_delphi(100.0, 45_000.0, 99.0, 101.0, true, false);
    registry
        .get_mut("ETHUSDT")
        .unwrap()
        .append_futures_trade_like_delphi(trade(45_000.0, 10.0, 1.0));

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
            real_time: 45_000.0,
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
        store.append_last_price_like_delphi(10.0, 45_000.0, 9.0, 11.0, false, false),
        None
    );
    assert_eq!(
        store.append_last_price_like_delphi(0.0, 45_000.0, 9.0, 11.0, true, false),
        None
    );
    assert_eq!(
        store.append_last_price_like_delphi(10.0, 45_000.0, 0.0, 0.0, true, false),
        None
    );
    assert_eq!(
        store.append_last_price_like_delphi(10.0, 45_000.0, 9.0, 11.0, true, false),
        Some(0)
    );

    let mut out = Vec::new();
    store.readers().last_prices.unwrap().copy_last(10, &mut out);
    assert_eq!(
        out,
        vec![LastPricePoint {
            current: 10.0,
            real_time: 45_000.0
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

    store.append_last_price_like_delphi(
        100.0,
        now - 50.0 / SECONDS_PER_DAY,
        99.0,
        101.0,
        true,
        false,
    );
    store.append_last_price_like_delphi(130.0, now - 14.0 / 1440.0, 129.0, 131.0, true, false);
    store.append_last_price_like_delphi(170.0, now - 59.0 / 1440.0, 169.0, 171.0, true, false);
    store.append_last_price_like_delphi(250.0, now - 60.0 / 1440.0, 249.0, 251.0, true, false);

    store.refresh_derived_analytics(now);
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
        store.append_futures_trade_like_delphi(trade(sec(10.0), 100.0, 1.0)),
        Some(0)
    );

    assert_eq!(
        store.append_futures_trade_like_delphi(trade(sec(9.0), 90.0, 1.0)),
        Some(1)
    );
    assert_eq!(
        store.append_futures_trade_like_delphi(trade(sec(12.0), 120.0, -2.0)),
        Some(2)
    );
    assert_eq!(
        store.append_futures_trade_like_delphi(trade(sec(11.0), 110.0, 3.0)),
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

    let volumes = store.rolling_volumes_snapshot(sec(12.0));
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

    let fut_time =
        store.append_futures_stream_trade_like_delphi(base, 100, now, 100.0, 1.0, &mut shift);
    let taker = [7u8; 20];
    let (mm_time, mm_seq) = store.append_mm_stream_order_like_delphi(
        base,
        200,
        base - 10.0,
        5.0,
        -2.0,
        Some(taker),
        &mut shift,
    );
    let (spot_time, spot_seq) =
        store.append_spot_stream_trade_like_delphi(base, -300, base - 10.0, 90.0, -1.0, &mut shift);
    assert_eq!(shift.shift_days(), Some(2.0 / 24.0));
    assert_eq!(fut_time, base + 100.0 / 86_400_000.0 + 2.0 / 24.0);
    assert_eq!(mm_time, base + 200.0 / 86_400_000.0 + 2.0 / 24.0);
    assert_eq!(spot_time, base - 300.0 / 86_400_000.0 + 2.0 / 24.0);
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
            color: hl_address_color_like_delphi(taker),
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
        store.append_futures_trade_like_delphi(trade(
            10.0 + i as f64 / 86_400.0,
            100.0 + i as f32,
            1.0,
        ));
    }
    assert_eq!(store.pending_evicted_futures_for_compaction(), 2);
    assert_eq!(store.compact_evicted_futures_like_delphi(20.0), 1);

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
    store.replace_candles_5m_from_snapshot(&[
        Candle5mRow {
            time: now - 10.0 / 1440.0,
            low: 90.0,
            high: 110.0,
            close: 100.0,
            open: 95.0,
            volume: 1_000.0,
        },
        Candle5mRow {
            time: now,
            low: 100.0,
            high: 120.0,
            close: 115.0,
            open: 105.0,
            volume: 2_000.0,
        },
    ]);

    let mut candles = Vec::new();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(candles.len(), 2);

    store.append_futures_trade_like_delphi(trade(now + 1.0 / 86_400.0, 125.0, 2.0));
    candles.clear();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(candles.len(), 2);
    // Снапшот-свечи засилены — трейд их НЕ трогает (эталон: live-свеча отдельно от ring).
    assert_eq!(candles[1].close, 115.0);
    assert_eq!(candles[1].high, 120.0);
    assert_eq!(candles[1].volume, 2_000.0);

    // refresh с временем >= трейда (в проде now всегда >= времени последнего трейда),
    // иначе live-свеча (now+1s) выпала бы из окна дельт.
    store.refresh_derived_analytics(now + 1.0 / 86_400.0);
    let derived = store.derived_snapshot();
    // Трейд ушёл в live-свечу (Delphi `FCandle`), выставлена отдельно от засиленного ring.
    let live = derived.current_candle.expect("live candle from trade");
    assert_eq!(live.close, 125.0);
    assert_eq!(live.high, 125.0);
    assert_eq!(live.volume, 250.0);
    assert!((derived.candle_deltas.fifteen_minutes - 38.8888888889).abs() < 1e-6);
    assert_eq!(derived.candle_volumes.fifteen_minutes, 3_250.0);
    assert_eq!(derived.candle_volumes.one_hour, 3_250.0);
    assert_eq!(derived.trade_deltas.fifteen_minutes, 0.0);
    assert!((derived.deltas.fifteen_minutes - 38.8888888889).abs() < 1e-6);
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
    store.replace_candles_5m_from_snapshot(&[Candle5mRow {
        time: now,
        low: 100.0,
        high: 110.0,
        close: 105.0,
        open: 101.0,
        volume: 1_000.0,
    }]);

    // Первый трейд следующего периода — копится в отдельный live-аккумулятор
    // (Delphi `FCandle`), НЕ кладётся в засиленный ring.
    let t1 = now + 6.0 / 1440.0;
    store.append_futures_trade_like_delphi(trade(t1, 120.0, 2.0));

    let mut candles = Vec::new();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(candles.len(), 1, "снапшот-свеча засилена; live-свеча отдельно, не в ring");
    assert_eq!(candles[0].time, now);
    assert_eq!(candles[0].close, 105.0);
    store.refresh_derived_analytics(t1);
    let live = store
        .derived_snapshot()
        .current_candle
        .expect("live candle accumulating");
    assert_eq!(live.open, 120.0);
    assert_eq!(live.close, 120.0);

    // Второй трейд через >5 мин — текущая свеча засиливается в ring (end-stamped
    // временем seal), начинается новая live-свеча (Delphi Recalc5mCandle roll).
    let t2 = t1 + 6.0 / 1440.0;
    store.append_futures_trade_like_delphi(trade(t2, 130.0, 1.0));
    candles.clear();
    store
        .readers()
        .candles_5m
        .unwrap()
        .copy_last(8, &mut candles);
    assert_eq!(candles.len(), 2, "первая live-свеча засилена и добавлена в ring");
    assert_eq!(candles[0].time, now);
    assert_eq!(candles[1].time, t2, "засиленная свеча штампуется временем seal (конец периода)");
    assert_eq!(candles[1].open, 120.0);
    assert_eq!(candles[1].close, 120.0);
    assert_eq!(candles[1].volume, 240.0);
    store.refresh_derived_analytics(t2);
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
    store.append_futures_trade_like_delphi(trade(now - 10.0 / 86_400.0, 100.0, 2.0));
    store.append_futures_trade_like_delphi(trade(now - 5.0 / 86_400.0, 110.0, -1.0));
    store.refresh_derived_analytics(now);

    let derived = store.derived_snapshot();
    assert_eq!(derived.trade_volumes.one_minute.buy_value, 200.0);
    assert_eq!(derived.trade_volumes.one_minute.sell_value, 110.0);
    assert_eq!(derived.trade_volumes.one_minute.min_price, 100.0);
    assert_eq!(derived.trade_volumes.one_minute.max_price, 110.0);
    assert!((derived.trade_deltas.one_minute - 10.0).abs() < 1e-9);
    assert_eq!(derived.candle_deltas.one_minute, 0.0);
    assert_eq!(derived.candle_volumes.five_minutes, 310.0);
    assert!((derived.deltas.one_minute - 10.0).abs() < 1e-9);
}

#[test]
fn combined_long_deltas_do_not_drop_below_one_hour_like_delphi() {
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
    store.replace_candles_5m_from_snapshot(&[
        Candle5mRow {
            time: now - 2.5 / 24.0,
            low: 100.0,
            high: 130.0,
            close: 100.0,
            open: 100.0,
            volume: 1.0,
        },
        Candle5mRow {
            time: now - 3.0 / 24.0,
            low: 100.0,
            high: 190.0,
            close: 100.0,
            open: 100.0,
            volume: 8.0,
        },
        Candle5mRow {
            time: now - 3.5 / 24.0,
            low: 100.0,
            high: 140.0,
            close: 100.0,
            open: 100.0,
            volume: 2.0,
        },
        Candle5mRow {
            time: now - 24.5 / 24.0,
            low: 100.0,
            high: 150.0,
            close: 100.0,
            open: 100.0,
            volume: 4.0,
        },
        Candle5mRow {
            time: now - 25.0 / 24.0,
            low: 100.0,
            high: 220.0,
            close: 100.0,
            open: 100.0,
            volume: 16.0,
        },
    ]);

    store.refresh_derived_analytics(now);
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
fn candle_windows_exclude_exact_old_boundary_like_delphi() {
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
    store.replace_candles_5m_from_snapshot(&[
        Candle5mRow {
            time: now - 15.0 / 1440.0,
            low: 100.0,
            high: 200.0,
            close: 100.0,
            open: 100.0,
            volume: 5.0,
        },
        Candle5mRow {
            time: now - (15.0 * 60.0 - 1.0) / SECONDS_PER_DAY,
            low: 100.0,
            high: 150.0,
            close: 100.0,
            open: 100.0,
            volume: 3.0,
        },
    ]);

    store.refresh_derived_analytics(now);
    let derived = store.derived_snapshot();

    assert!((derived.candle_deltas.fifteen_minutes - 50.0).abs() < 1e-9);
    assert_eq!(derived.candle_volumes.fifteen_minutes, 3.0);
}
