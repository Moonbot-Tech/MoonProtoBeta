use super::*;
use crate::state::history::DELPHI_MSECS_PER_DAY;
use crate::state::MarketHistoryConfig;
use std::sync::Arc;

fn mt(days: f64) -> MoonTime {
    crate::state::history::moon_time_from_delphi_days(days)
}

#[test]
fn worker_stores_enabled_futures_trades_after_flush() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });
    assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));
    let readers = worker.readers("BTCUSDT").unwrap();
    let futures = readers.futures_trades.unwrap();

    let now_time = 45_000.0 + 1.0 / 24.0 + 1.0 / 86_400.0;
    worker.handle().send_stream_batch(MarketHistoryStreamBatch {
        base_time: 45_000.0,
        now_time,
        sections: vec![MarketHistoryStreamSection {
            market_index: 0,
            kind: MarketHistoryStreamSectionKind::FuturesTrades,
            start: 0,
            len: 1,
        }],
        trade_rows: vec![MarketHistoryTradeInput {
            time_delta_ms: 250,
            price: 100.0,
            qty: 2.0,
        }],
        mm_order_rows: Vec::new(),
    });
    assert!(worker.flush(mt(now_time)));

    let mut out = Vec::new();
    futures.copy_last(8, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].price, 100.0);
    assert_eq!(out[0].qty, 2.0);
    assert_eq!(
        out[0].time,
        mt(45_000.0 + 250.0 / DELPHI_MSECS_PER_DAY + 1.0 / 24.0)
    );

    let volumes = worker
        .rolling_volumes("BTCUSDT", out[0].time)
        .expect("enabled market should expose rolling volumes");
    assert_eq!(volumes.one_minute.buy_value, 200.0);
    assert_eq!(volumes.one_minute.sell_value, 0.0);
    assert_eq!(volumes.five_minutes.trade_count, 1);
    assert!(
        worker.rolling_volumes("ETHUSDT", out[0].time).is_none(),
        "rolling volume reads must not allocate unknown markets"
    );
}

#[test]
fn worker_does_not_create_market_from_stream_batch() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig::default());

    worker.handle().send_stream_batch(MarketHistoryStreamBatch {
        base_time: 45_000.0,
        now_time: 45_000.0,
        sections: vec![MarketHistoryStreamSection {
            market_index: 0,
            kind: MarketHistoryStreamSectionKind::SpotTrades,
            start: 0,
            len: 1,
        }],
        trade_rows: vec![MarketHistoryTradeInput {
            time_delta_ms: 0,
            price: 10.0,
            qty: 1.0,
        }],
        mm_order_rows: Vec::new(),
    });
    assert!(worker.flush(mt(45_000.0)));

    assert!(
        worker.readers("ETHUSDT").is_none(),
        "stream batches must not allocate retained histories for every market"
    );
}

#[test]
fn worker_continues_after_command_panic() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 4,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });
    assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));
    assert!(worker.handle().panic_once_for_test());

    worker
        .handle()
        .send_last_price_batch(MarketHistoryLastPriceBatch {
            now_time: 45_000.25,
            rows: vec![MarketHistoryLastPriceInput {
                market_name: Arc::<str>::from("BTCUSDT"),
                current: 100.5,
                bid: 100.0,
                ask: 101.0,
                mark_price: 100.75,
                mark_price_found: true,
                is_btc_market: true,
                is_base_usdt_market: false,
            }],
        });
    assert!(worker.flush(mt(45_000.25)));

    let last_prices = worker.readers("BTCUSDT").unwrap().last_prices.unwrap();
    let mut out = Vec::new();
    last_prices.copy_last(4, &mut out);
    assert_eq!(
        out.len(),
        1,
        "history worker must log/drop one panicking command and continue"
    );
    assert_eq!(out[0].current, 100.5);
}

#[test]
fn worker_stores_last_price_batch_for_enabled_market() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 4,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });
    assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));
    let readers = worker.readers("BTCUSDT").unwrap();
    let last_prices = readers.last_prices.unwrap();

    worker
        .handle()
        .send_last_price_batch(MarketHistoryLastPriceBatch {
            now_time: 45_000.25,
            rows: vec![MarketHistoryLastPriceInput {
                market_name: Arc::<str>::from("BTCUSDT"),
                current: 100.5,
                bid: 100.0,
                ask: 101.0,
                mark_price: 100.75,
                mark_price_found: true,
                is_btc_market: true,
                is_base_usdt_market: false,
            }],
        });
    assert!(worker.flush(mt(45_000.25)));

    let mut out = Vec::new();
    last_prices.copy_last(4, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].current, 100.5);
    assert_eq!(out[0].time, mt(45_000.25));

    let mark_prices = worker.readers("BTCUSDT").unwrap().mark_prices.unwrap();
    let mut mark_out = Vec::new();
    mark_prices.copy_last(4, &mut mark_out);
    assert_eq!(mark_out.len(), 1);
    assert_eq!(mark_out[0].current, 100.75);
    assert_eq!(mark_out[0].time, mt(45_000.25));
}

#[test]
fn diagnostics_fill_market_history_to_capacity_builds_full_retained_fixture() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 5,
        spot_trades_capacity: 4,
        liquidation_capacity: 3,
        mm_orders_capacity: 4,
        last_price_capacity: 6,
        mini_candles_capacity: 3,
        candles_5m_capacity: 5,
    });
    assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));

    worker
        .handle()
        .send_last_price_batch(MarketHistoryLastPriceBatch {
            now_time: 45_000.25,
            rows: vec![MarketHistoryLastPriceInput {
                market_name: Arc::<str>::from("BTCUSDT"),
                current: 777.0,
                bid: 776.0,
                ask: 778.0,
                mark_price: 778.0,
                mark_price_found: true,
                is_btc_market: true,
                is_base_usdt_market: false,
            }],
        });
    assert!(worker.flush(mt(45_000.25)));

    let now_time = MoonTime::from_unix_millis(1_700_000_000_000);
    assert!(
        worker
            .handle()
            .diag_fill_market_history_to_capacity("BTCUSDT", now_time, 3_600_000,),
        "diagnostics fill should complete through the worker owner"
    );

    let readers = worker.readers("BTCUSDT").unwrap();
    assert_timed_ring_full(readers.futures_trades.as_ref().unwrap(), 5);
    assert_timed_ring_full(readers.spot_trades.as_ref().unwrap(), 4);
    assert_timed_ring_full(readers.liquidations.as_ref().unwrap(), 3);
    assert_timed_ring_full(readers.mm_orders.as_ref().unwrap(), 4);
    assert_ring_full(readers.mm_order_companion.as_ref().unwrap(), 4);
    assert_timed_ring_full(readers.mark_prices.as_ref().unwrap(), 6);
    assert_timed_ring_full(readers.mini_candles.as_ref().unwrap(), 3);
    assert_timed_ring_full(readers.candles_5m.as_ref().unwrap(), 5);

    let last_prices = readers.last_prices.as_ref().unwrap();
    let mut rows = Vec::new();
    let meta = last_prices.copy_last(6, &mut rows);
    assert_eq!(meta.copied, 6);
    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows.last().unwrap().price(),
        777.0,
        "diagnostics fill keeps existing live tail rows"
    );

    let futures_trades = readers.futures_trades.as_ref().unwrap();
    let mut trades = Vec::new();
    futures_trades.copy_last(5, &mut trades);
    assert!(
        trades
            .iter()
            .all(|row| row.price > 760.0 && row.price < 790.0),
        "diagnostics trade fixture should stay near the live market price"
    );

    let mut cursor = last_prices.cursor_from_oldest();
    let drain = last_prices.drain_new_bounded(&mut cursor, 6, &mut rows);
    assert_eq!(drain.copied, 6);
    assert!(drain.caught_up);
    assert_eq!(rows.len(), 6);

    let derived = worker
        .handle()
        .try_derived_snapshot("BTCUSDT", now_time)
        .expect("filled market should publish derived history state");
    assert!(
        derived.deltas.one_hour > 0.0,
        "synthetic retained rows should be visible to derived readers"
    );
}

fn assert_ring_full<T: crate::state::seq_ring::SeqRingRow>(
    reader: &crate::state::seq_ring::SeqRingReader<T>,
    capacity: usize,
) {
    let bounds = reader.bounds();
    assert_eq!(bounds.len, capacity);
    assert_eq!(bounds.capacity, capacity);
    assert_eq!(bounds.oldest_seq, 0);
    assert_eq!(bounds.next_seq, capacity as u64);
}

fn assert_timed_ring_full<T>(reader: &crate::state::seq_ring::SeqRingReader<T>, capacity: usize)
where
    T: crate::state::seq_ring::SeqRingTimedRow,
{
    assert_ring_full(reader, capacity);
    let mut rows = Vec::new();
    reader.copy_last(capacity, &mut rows);
    assert_eq!(rows.len(), capacity);
    assert!(
        rows.windows(2)
            .all(|pair| pair[0].seq_ring_time_ms() <= pair[1].seq_ring_time_ms()),
        "diagnostics fill must preserve chronological row order"
    );
}

#[test]
fn worker_flush_compacts_evicted_futures_to_mini_candles() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 2,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 4,
        candles_5m_capacity: 0,
    });
    assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));
    let readers = worker.readers("BTCUSDT").unwrap();
    let futures = readers.futures_trades.unwrap();
    let mini = readers.mini_candles.unwrap();

    let now_time = 45_000.0 + 1.0 / 24.0;
    worker.handle().send_stream_batch(MarketHistoryStreamBatch {
        base_time: 45_000.0,
        now_time,
        sections: vec![MarketHistoryStreamSection {
            market_index: 0,
            kind: MarketHistoryStreamSectionKind::FuturesTrades,
            start: 0,
            len: 3,
        }],
        trade_rows: vec![
            MarketHistoryTradeInput {
                time_delta_ms: 0,
                price: 100.0,
                qty: 2.0,
            },
            MarketHistoryTradeInput {
                time_delta_ms: 100,
                price: 101.0,
                qty: -3.0,
            },
            MarketHistoryTradeInput {
                time_delta_ms: 200,
                price: 102.0,
                qty: 4.0,
            },
        ],
        mm_order_rows: Vec::new(),
    });
    assert!(worker.flush(mt(now_time)));

    let mut retained = Vec::new();
    futures.copy_last(8, &mut retained);
    assert_eq!(retained.len(), 2);
    assert_eq!(retained[0].price, 101.0);
    assert_eq!(retained[1].price, 102.0);

    let mut compacted = Vec::new();
    mini.copy_last(4, &mut compacted);
    assert_eq!(compacted.len(), 1);
    assert_eq!(compacted[0].cnt, 1);
    assert_eq!(compacted[0].min_price, 100.0);
    assert_eq!(compacted[0].max_price, 100.0);
    assert_eq!(compacted[0].buy_vol, 200.0);
    assert_eq!(compacted[0].sell_vol, 0.0);
}

#[test]
fn worker_applies_candles_snapshot_only_for_configured_scope() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 4,
    });
    assert!(worker.configure_markets(
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        Some(TradeStorageScope::from_markets(["BTCUSDT"]))
    ));
    assert!(worker.apply_candles_snapshot(
        mt(45_000.0),
        vec![
            MarketHistoryCandlesSnapshot {
                market_name: "BTCUSDT".to_string(),
                candles_5m: vec![Candle5mRow {
                    open: 100.0,
                    close: 101.0,
                    high: 102.0,
                    low: 99.0,
                    volume: 10.0,
                    time: mt(45_000.0),
                }],
            },
            MarketHistoryCandlesSnapshot {
                market_name: "ETHUSDT".to_string(),
                candles_5m: vec![Candle5mRow {
                    open: 10.0,
                    close: 11.0,
                    high: 12.0,
                    low: 9.0,
                    volume: 1.0,
                    time: mt(45_000.0),
                }],
            },
        ]
    ));
    assert!(worker.flush(mt(45_000.0)));

    let btc = worker.readers("BTCUSDT").unwrap().candles_5m.unwrap();
    let mut out = Vec::new();
    btc.copy_last(4, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].close, 101.0);
    assert!(worker.readers("ETHUSDT").is_none());
}
