
use super::*;
use crate::state::history::DELPHI_MSECS_PER_DAY;
use crate::state::MarketHistoryConfig;

#[test]
fn worker_stores_enabled_futures_trades_after_flush() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        mm_order_companion_capacity: 0,
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
        sections: vec![MarketHistoryStreamSection::FuturesTrades {
            market_index: 0,
            rows: vec![MarketHistoryTradeInput {
                time_delta_ms: 250,
                price: 100.0,
                qty: 2.0,
            }],
        }],
    });
    assert!(worker.flush(now_time));

    let mut out = Vec::new();
    futures.copy_last(8, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].price, 100.0);
    assert_eq!(out[0].qty, 2.0);
    assert_eq!(
        out[0].time,
        45_000.0 + 250.0 / DELPHI_MSECS_PER_DAY + 1.0 / 24.0
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
        sections: vec![MarketHistoryStreamSection::SpotTrades {
            market_index: 0,
            rows: vec![MarketHistoryTradeInput {
                time_delta_ms: 0,
                price: 10.0,
                qty: 1.0,
            }],
        }],
    });
    assert!(worker.flush(45_000.0));

    assert!(
        worker.readers("ETHUSDT").is_none(),
        "stream batches must not allocate retained histories for every market"
    );
}

#[test]
fn worker_stores_last_price_batch_for_enabled_market() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        mm_order_companion_capacity: 0,
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
                market_name: "BTCUSDT".to_string(),
                current: 100.5,
                bid: 100.0,
                ask: 101.0,
                is_btc_market: true,
                is_base_usdt_market: false,
            }],
        });
    assert!(worker.flush(45_000.25));

    let mut out = Vec::new();
    last_prices.copy_last(4, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].current, 100.5);
    assert_eq!(out[0].real_time, 45_000.25);
}

#[test]
fn worker_flush_compacts_evicted_futures_to_mini_candles() {
    let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
        futures_trades_capacity: 2,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        mm_order_companion_capacity: 0,
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
        sections: vec![MarketHistoryStreamSection::FuturesTrades {
            market_index: 0,
            rows: vec![
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
        }],
    });
    assert!(worker.flush(now_time));

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
        mm_order_companion_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 4,
    });
    assert!(worker.configure_markets(
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        Some(TradeStorageScope::from_markets(["BTCUSDT"]))
    ));
    assert!(worker.apply_candles_snapshot(vec![
        MarketHistoryCandlesSnapshot {
            market_name: "BTCUSDT".to_string(),
            candles_5m: vec![Candle5mRow {
                open_p: 100.0,
                close_p: 101.0,
                max_p: 102.0,
                min_p: 99.0,
                vol: 10.0,
                time: 45_000.0,
            }],
        },
        MarketHistoryCandlesSnapshot {
            market_name: "ETHUSDT".to_string(),
            candles_5m: vec![Candle5mRow {
                open_p: 10.0,
                close_p: 11.0,
                max_p: 12.0,
                min_p: 9.0,
                vol: 1.0,
                time: 45_000.0,
            }],
        },
    ]));
    assert!(worker.flush(45_000.0));

    let btc = worker.readers("BTCUSDT").unwrap().candles_5m.unwrap();
    let mut out = Vec::new();
    btc.copy_last(4, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].close_p, 101.0);
    assert!(worker.readers("ETHUSDT").is_none());
}
