use super::*;
use crate::state::SeqRingWriter;

fn mt(days: f64) -> MoonTime {
    moon_time_from_delphi_days(days)
}

#[test]
fn last_price_point_roundtrips_through_seq_ring() {
    let (mut writer, reader) = SeqRingWriter::<LastPricePoint>::new(4).unwrap();
    writer.push(LastPricePoint {
        current: 123.5,
        time: mt(45_000.25),
    });

    let mut out = Vec::new();
    reader.copy_last(1, &mut out);

    assert_eq!(
        out,
        vec![LastPricePoint {
            current: 123.5,
            time: mt(45_000.25),
        }]
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (per-packet TimeShift)
fn trades_packet_time_shift_is_fixed_by_first_row() {
    let base_time = 45_000.0;
    let now_time = base_time + 3.0 / 24.0 + 10.0 / SECONDS_PER_DAY;
    let mut shift = TradesPacketTimeShift::new();

    let first = shift.shifted_time(base_time, 250, now_time);
    assert_eq!(shift.shift_days(), Some(3.0 / 24.0));
    assert_eq!(
        first,
        mt(base_time + 250.0 / DELPHI_MSECS_PER_DAY + 3.0 / 24.0)
    );

    let second = shift.shifted_time(base_time, -500, base_time - 5.0);
    assert_eq!(
        second,
        mt(base_time - 500.0 / DELPHI_MSECS_PER_DAY + 3.0 / 24.0),
        "later rows reuse the first-row TimeShift even if their own Now delta would differ"
    );
}

#[test]
fn mini_candle_roundtrips_through_seq_ring() {
    let (mut writer, reader) = SeqRingWriter::<MiniCandle>::new(2).unwrap();
    writer.push(MiniCandle {
        time: mt(45_000.0),
        cnt: 7,
        min_price: 10.0,
        max_price: 12.0,
        buy_vol: 100.0,
        sell_vol: 80.0,
    });

    assert_eq!(
        reader.read_at_seq(0),
        Some(MiniCandle {
            time: mt(45_000.0),
            cnt: 7,
            min_price: 10.0,
            max_price: 12.0,
            buy_vol: 100.0,
            sell_vol: 80.0,
        })
    );
}

#[test]
fn trade_history_row_uses_delphi_qty_sign_bit() {
    let buy = TradeHistoryRow {
        time: mt(45_000.0),
        price: 100.0,
        qty: 2.5,
    };
    let sell = TradeHistoryRow {
        time: mt(45_000.1),
        price: 101.0,
        qty: -2.5,
    };
    let negative_zero = TradeHistoryRow {
        time: mt(45_000.2),
        price: 102.0,
        qty: -0.0,
    };

    assert_eq!(buy.quantity(), 2.5);
    assert!(buy.is_buy());
    assert!(!sell.is_buy());
    assert!(!negative_zero.is_buy());
    assert!(!buy.same_direction(sell));
    assert!(sell.same_direction(negative_zero));
    assert_eq!(sell.traded_value(), 252.5);
}

#[test]
fn trade_history_row_roundtrips_through_seq_ring() {
    let (mut writer, reader) = SeqRingWriter::<TradeHistoryRow>::new(2).unwrap();
    writer.push(TradeHistoryRow {
        time: mt(45_000.0),
        price: 100.0,
        qty: 2.5,
    });
    writer.push(TradeHistoryRow {
        time: mt(45_000.25),
        price: 101.0,
        qty: -1.25,
    });

    let mut out = Vec::new();
    reader.copy_from_time(mt(45_000.2), 10, &mut out).unwrap();

    assert_eq!(
        out,
        vec![TradeHistoryRow {
            time: mt(45_000.25),
            price: 101.0,
            qty: -1.25,
        }]
    );
}

#[test]
fn mm_order_history_row_roundtrips_through_seq_ring() {
    let (mut writer, reader) = SeqRingWriter::<MMOrderHistoryRow>::new(2).unwrap();
    writer.push(MMOrderHistoryRow {
        time: mt(45_000.0),
        volume: 50_000.25,
        q: 7.5,
    });
    writer.push(MMOrderHistoryRow {
        time: mt(45_000.5),
        volume: 51_000.5,
        q: 8.25,
    });

    let mut out = Vec::new();
    reader.copy_last(2, &mut out);

    assert_eq!(
        out,
        vec![
            MMOrderHistoryRow {
                time: mt(45_000.0),
                volume: 50_000.25,
                q: 7.5,
            },
            MMOrderHistoryRow {
                time: mt(45_000.5),
                volume: 51_000.5,
                q: 8.25,
            }
        ]
    );
}

#[test]
fn mm_order_companion_data_roundtrips_through_seq_ring() {
    let (mut writer, reader) = SeqRingWriter::<MMOrderCompanionData>::new(2).unwrap();
    let row = MMOrderCompanionData {
        taker: [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ],
        color: 0xAABB_CCDD,
    };
    writer.push(row);

    assert_eq!(reader.read_at_seq(0), Some(row));
}

#[test]
fn hl_address_color_matches_delphi_xor_scale() {
    let mut taker = [0u8; 20];
    for (idx, byte) in taker.iter_mut().enumerate() {
        *byte = idx as u8;
    }
    assert_eq!(hl_address_color(taker), 0xFF62_5360);
}

#[test]
fn hl_address_hex_matches_delphi_display_shape() {
    let mut taker = [0u8; 20];
    for (idx, byte) in taker.iter_mut().enumerate() {
        *byte = idx as u8;
    }
    assert_eq!(
        hl_address_hex(&taker),
        "0x000102030405060708090a0b0c0d0e0f10111213"
    );
}

#[test]
// parity: MoonBot MarketsU.pas:TMarket.ResizeOrdersHistory (UseTradesCompression)
fn compacts_trades_to_mini_candles() {
    let t0 = 45_000.0;
    let rows = [
        TradeHistoryRow {
            time: mt(t0),
            price: 100.0,
            qty: 2.0,
        },
        TradeHistoryRow {
            time: mt(t0 + 4.0 / SECONDS_PER_DAY),
            price: 101.0,
            qty: -3.0,
        },
        TradeHistoryRow {
            time: mt(t0 + 6.0 / SECONDS_PER_DAY),
            price: 102.0,
            qty: 4.0,
        },
    ];

    let mut out = Vec::new();
    compact_trades_to_mini_candles(&rows, MoonTime::ZERO, mt(t0 + 1.0), &mut out);

    assert_eq!(
        out,
        vec![
            MiniCandle {
                time: mt(t0),
                cnt: 2,
                min_price: 100.0,
                max_price: 101.0,
                buy_vol: 200.0,
                sell_vol: 303.0,
            },
            MiniCandle {
                time: mt(t0 + 6.0 / SECONDS_PER_DAY),
                cnt: 1,
                min_price: 102.0,
                max_price: 102.0,
                buy_vol: 408.0,
                sell_vol: 0.0,
            }
        ]
    );
}

#[test]
fn compact_trades_skips_split_group_not_newer_than_existing_mini() {
    let t0 = 45_000.0;
    let rows = [
        TradeHistoryRow {
            time: mt(t0),
            price: 100.0,
            qty: 1.0,
        },
        TradeHistoryRow {
            time: mt(t0 + 6.0 / SECONDS_PER_DAY),
            price: 101.0,
            qty: 1.0,
        },
    ];

    let mut out = Vec::new();
    compact_trades_to_mini_candles(
        &rows,
        mt(t0 + 1.0 / SECONDS_PER_DAY),
        mt(t0 + 1.0),
        &mut out,
    );

    assert_eq!(
        out,
        vec![MiniCandle {
            time: mt(t0 + 6.0 / SECONDS_PER_DAY),
            cnt: 1,
            min_price: 101.0,
            max_price: 101.0,
            buy_vol: 101.0,
            sell_vol: 0.0,
        }]
    );
}

#[test]
fn rolling_trade_volumes_maintain_one_three_five_minute_windows() {
    let now = 45_000.0;
    let mut volumes = RollingTradeVolumes::default();

    volumes.add_trade(TradeHistoryRow {
        time: mt(now - 10.0 / SECONDS_PER_DAY),
        price: 100.0,
        qty: 2.0,
    });
    volumes.add_trade(TradeHistoryRow {
        time: mt(now - 70.0 / SECONDS_PER_DAY),
        price: 200.0,
        qty: -3.0,
    });
    volumes.add_trade(TradeHistoryRow {
        time: mt(now - 200.0 / SECONDS_PER_DAY),
        price: 300.0,
        qty: 4.0,
    });
    volumes.add_trade(TradeHistoryRow {
        time: mt(now - 400.0 / SECONDS_PER_DAY),
        price: 400.0,
        qty: 5.0,
    });

    let snapshot = volumes.snapshot(mt(now));

    assert_eq!(
        snapshot.one_minute,
        TradeVolumeTotals {
            buy_value: 200.0,
            sell_value: 0.0,
            buy_qty: 2.0,
            sell_qty: 0.0,
            trade_count: 1,
            min_price: 100.0,
            max_price: 100.0,
        }
    );
    assert_eq!(snapshot.three_minutes.buy_value, 200.0);
    assert_eq!(snapshot.three_minutes.sell_value, 600.0);
    assert_eq!(snapshot.three_minutes.trade_count, 2);
    assert_eq!(snapshot.three_minutes.min_price, 100.0);
    assert_eq!(snapshot.three_minutes.max_price, 200.0);
    assert_eq!(snapshot.five_minutes.buy_value, 1_400.0);
    assert_eq!(snapshot.five_minutes.sell_value, 600.0);
    assert_eq!(snapshot.five_minutes.trade_count, 3);
    assert_eq!(snapshot.five_minutes.min_price, 100.0);
    assert_eq!(snapshot.five_minutes.max_price, 300.0);
    assert!((snapshot.five_minutes.price_delta_percent() - 200.0).abs() < 1e-9);
}
