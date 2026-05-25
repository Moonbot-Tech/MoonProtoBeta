//! Active-library retained history row types.
//!
//! These rows are the typed payloads stored by [`crate::state::seq_ring`].
//! They intentionally mirror Delphi storage records where the record is a
//! user-visible/history concept rather than only a wire packet.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::state::seq_ring::{SeqRingRow, SeqRingRowSlot, SeqRingTimedRow};

const SECONDS_PER_DAY: f64 = 86_400.0;
const MINI_CANDLE_SPLIT_DAYS: f64 = 5.0 / SECONDS_PER_DAY;

/// Delphi `TTrade`: detailed trade/liquidation row stored in market history.
///
/// Delphi layout is 16 bytes: `Time: TDateTime; Price: Single; Qty: Single`.
/// `Qty` is signed exactly like Delphi: sign bit clear means buy, sign bit set
/// means sell. This intentionally uses sign-bit checks, so `-0.0` has the same
/// machine effect as Delphi's `PCardinal(@Qty)^ and $80000000`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeHistoryRow {
    pub time: f64,
    pub price: f32,
    pub qty: f32,
}

impl TradeHistoryRow {
    pub fn quantity(self) -> f32 {
        self.qty.abs()
    }

    pub fn is_buy(self) -> bool {
        self.qty.to_bits() & 0x8000_0000 == 0
    }

    pub fn same_direction(self, other: Self) -> bool {
        (self.qty.to_bits() ^ other.qty.to_bits()) & 0x8000_0000 == 0
    }

    pub fn traded_value(self) -> f32 {
        self.price * self.quantity()
    }
}

#[derive(Default)]
pub struct TradeHistoryRowSlot {
    time_bits: AtomicU64,
    price_bits: AtomicU32,
    qty_bits: AtomicU32,
}

impl SeqRingRow for TradeHistoryRow {
    type Slot = TradeHistoryRowSlot;
}

impl SeqRingTimedRow for TradeHistoryRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

impl SeqRingRowSlot for TradeHistoryRowSlot {
    type Row = TradeHistoryRow;

    fn store_row(&self, row: Self::Row) {
        self.time_bits.store(row.time.to_bits(), Ordering::Relaxed);
        self.price_bits
            .store(row.price.to_bits(), Ordering::Relaxed);
        self.qty_bits.store(row.qty.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        TradeHistoryRow {
            time: f64::from_bits(self.time_bits.load(Ordering::Relaxed)),
            price: f32::from_bits(self.price_bits.load(Ordering::Relaxed)),
            qty: f32::from_bits(self.qty_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Delphi `TMMOrder`: main market-maker history row.
///
/// Delphi layout is `Time: TDateTime; vol: Double; Q: Double`. Optional taker
/// address and color are companion data in Delphi
/// `TStreamableRingBuffer<TMMOrder, TMMOrderData>` and must be ported as a
/// separate companion layer, not silently folded into this base row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MMOrderHistoryRow {
    pub time: f64,
    pub vol: f64,
    pub q: f64,
}

#[derive(Default)]
pub struct MMOrderHistoryRowSlot {
    time_bits: AtomicU64,
    vol_bits: AtomicU64,
    q_bits: AtomicU64,
}

impl SeqRingRow for MMOrderHistoryRow {
    type Slot = MMOrderHistoryRowSlot;
}

impl SeqRingTimedRow for MMOrderHistoryRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

impl SeqRingRowSlot for MMOrderHistoryRowSlot {
    type Row = MMOrderHistoryRow;

    fn store_row(&self, row: Self::Row) {
        self.time_bits.store(row.time.to_bits(), Ordering::Relaxed);
        self.vol_bits.store(row.vol.to_bits(), Ordering::Relaxed);
        self.q_bits.store(row.q.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        MMOrderHistoryRow {
            time: f64::from_bits(self.time_bits.load(Ordering::Relaxed)),
            vol: f64::from_bits(self.vol_bits.load(Ordering::Relaxed)),
            q: f64::from_bits(self.q_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Delphi `THistoricalPrices` used by `Market.HistoryPrice`.
///
/// Delphi layout is `packed record current: Single; RealTime: TDateTime`.
/// MoonBot draws the brown LastPrice chart line from this history. The source
/// value is `UpdateMarketsList -> pLast = (Bid + Ask) / 2`, not the trades
/// stream last trade price.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LastPricePoint {
    pub current: f32,
    pub real_time: f64,
}

#[derive(Default)]
pub struct LastPricePointSlot {
    current_bits: AtomicU32,
    real_time_bits: AtomicU64,
}

impl SeqRingRow for LastPricePoint {
    type Slot = LastPricePointSlot;
}

impl SeqRingTimedRow for LastPricePoint {
    fn seq_ring_time(&self) -> f64 {
        self.real_time
    }
}

impl SeqRingRowSlot for LastPricePointSlot {
    type Row = LastPricePoint;

    fn store_row(&self, row: Self::Row) {
        self.current_bits
            .store(row.current.to_bits(), Ordering::Relaxed);
        self.real_time_bits
            .store(row.real_time.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        LastPricePoint {
            current: f32::from_bits(self.current_bits.load(Ordering::Relaxed)),
            real_time: f64::from_bits(self.real_time_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Delphi `TMiniCandle` used to compact evicted detailed trades.
///
/// Delphi layout is 24 bytes: `Time: TDateTime; Cnt: Integer; MinPrice,
/// MaxPrice, BuyVol, SellVol: Single`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MiniCandle {
    pub time: f64,
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}

#[derive(Default)]
pub struct MiniCandleSlot {
    time_bits: AtomicU64,
    cnt: AtomicU32,
    min_price_bits: AtomicU32,
    max_price_bits: AtomicU32,
    buy_vol_bits: AtomicU32,
    sell_vol_bits: AtomicU32,
}

impl SeqRingRow for MiniCandle {
    type Slot = MiniCandleSlot;
}

impl SeqRingTimedRow for MiniCandle {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

impl SeqRingRowSlot for MiniCandleSlot {
    type Row = MiniCandle;

    fn store_row(&self, row: Self::Row) {
        self.time_bits.store(row.time.to_bits(), Ordering::Relaxed);
        self.cnt.store(row.cnt as u32, Ordering::Relaxed);
        self.min_price_bits
            .store(row.min_price.to_bits(), Ordering::Relaxed);
        self.max_price_bits
            .store(row.max_price.to_bits(), Ordering::Relaxed);
        self.buy_vol_bits
            .store(row.buy_vol.to_bits(), Ordering::Relaxed);
        self.sell_vol_bits
            .store(row.sell_vol.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        MiniCandle {
            time: f64::from_bits(self.time_bits.load(Ordering::Relaxed)),
            cnt: self.cnt.load(Ordering::Relaxed) as i32,
            min_price: f32::from_bits(self.min_price_bits.load(Ordering::Relaxed)),
            max_price: f32::from_bits(self.max_price_bits.load(Ordering::Relaxed)),
            buy_vol: f32::from_bits(self.buy_vol_bits.load(Ordering::Relaxed)),
            sell_vol: f32::from_bits(self.sell_vol_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Compact detailed trades into Delphi `TMiniCandle` groups.
///
/// This mirrors the `UseTradesCompression` body inside Delphi
/// `TMarket.ResizeOrdersHistory`: the group anchor is the first trade time, a
/// new candle starts when `abs(anchor - row.Time) > 5 / SecsPerDay`, split
/// groups are appended only when newer than `last_mini_time` and older than the
/// resize `now_time`, and the final group only checks `c.Time > last_mini_time`.
pub fn compact_trades_to_mini_candles_like_delphi(
    rows: &[TradeHistoryRow],
    last_mini_time: f64,
    now_time: f64,
    out: &mut Vec<MiniCandle>,
) {
    let Some(first) = rows.first() else {
        return;
    };

    let mut newest_mini_time = last_mini_time;
    let mut anchor_time = first.time;
    let mut candle = empty_mini_candle(anchor_time);

    for row in rows {
        if (anchor_time - row.time).abs() > MINI_CANDLE_SPLIT_DAYS && candle.cnt > 0 {
            if candle.time > newest_mini_time && candle.time < now_time {
                out.push(candle);
                newest_mini_time = candle.time;
            }

            anchor_time = row.time;
            candle = empty_mini_candle(anchor_time);
        }

        if row.is_buy() {
            candle.buy_vol += row.traded_value();
        } else {
            candle.sell_vol += row.traded_value();
        }
        if candle.cnt == 0 {
            candle.min_price = row.price;
        }
        candle.max_price = candle.max_price.max(row.price);
        candle.min_price = candle.min_price.min(row.price);
        candle.cnt += 1;
    }

    if candle.cnt > 0 && candle.time > newest_mini_time {
        out.push(candle);
    }
}

fn empty_mini_candle(time: f64) -> MiniCandle {
    MiniCandle {
        time,
        cnt: 0,
        min_price: 0.0,
        max_price: 0.0,
        buy_vol: 0.0,
        sell_vol: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SeqRingWriter;

    #[test]
    fn last_price_point_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<LastPricePoint>::new(4).unwrap();
        writer.push(LastPricePoint {
            current: 123.5,
            real_time: 45_000.25,
        });

        let mut out = Vec::new();
        reader.copy_last(1, &mut out);

        assert_eq!(
            out,
            vec![LastPricePoint {
                current: 123.5,
                real_time: 45_000.25,
            }]
        );
    }

    #[test]
    fn mini_candle_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<MiniCandle>::new(2).unwrap();
        writer.push(MiniCandle {
            time: 45_000.0,
            cnt: 7,
            min_price: 10.0,
            max_price: 12.0,
            buy_vol: 100.0,
            sell_vol: 80.0,
        });

        assert_eq!(
            reader.read_at_seq(0),
            Some(MiniCandle {
                time: 45_000.0,
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
            time: 45_000.0,
            price: 100.0,
            qty: 2.5,
        };
        let sell = TradeHistoryRow {
            time: 45_000.1,
            price: 101.0,
            qty: -2.5,
        };
        let negative_zero = TradeHistoryRow {
            time: 45_000.2,
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
            time: 45_000.0,
            price: 100.0,
            qty: 2.5,
        });
        writer.push(TradeHistoryRow {
            time: 45_000.25,
            price: 101.0,
            qty: -1.25,
        });

        let mut out = Vec::new();
        reader.copy_from_time(45_000.2, 10, &mut out).unwrap();

        assert_eq!(
            out,
            vec![TradeHistoryRow {
                time: 45_000.25,
                price: 101.0,
                qty: -1.25,
            }]
        );
    }

    #[test]
    fn mm_order_history_row_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<MMOrderHistoryRow>::new(2).unwrap();
        writer.push(MMOrderHistoryRow {
            time: 45_000.0,
            vol: 50_000.25,
            q: 7.5,
        });
        writer.push(MMOrderHistoryRow {
            time: 45_000.5,
            vol: 51_000.5,
            q: 8.25,
        });

        let mut out = Vec::new();
        reader.copy_last(2, &mut out);

        assert_eq!(
            out,
            vec![
                MMOrderHistoryRow {
                    time: 45_000.0,
                    vol: 50_000.25,
                    q: 7.5,
                },
                MMOrderHistoryRow {
                    time: 45_000.5,
                    vol: 51_000.5,
                    q: 8.25,
                }
            ]
        );
    }

    #[test]
    fn compacts_trades_to_mini_candles_like_delphi_resize() {
        let t0 = 45_000.0;
        let rows = [
            TradeHistoryRow {
                time: t0,
                price: 100.0,
                qty: 2.0,
            },
            TradeHistoryRow {
                time: t0 + 4.0 / SECONDS_PER_DAY,
                price: 101.0,
                qty: -3.0,
            },
            TradeHistoryRow {
                time: t0 + 6.0 / SECONDS_PER_DAY,
                price: 102.0,
                qty: 4.0,
            },
        ];

        let mut out = Vec::new();
        compact_trades_to_mini_candles_like_delphi(&rows, 0.0, t0 + 1.0, &mut out);

        assert_eq!(
            out,
            vec![
                MiniCandle {
                    time: t0,
                    cnt: 2,
                    min_price: 100.0,
                    max_price: 101.0,
                    buy_vol: 200.0,
                    sell_vol: 303.0,
                },
                MiniCandle {
                    time: t0 + 6.0 / SECONDS_PER_DAY,
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
                time: t0,
                price: 100.0,
                qty: 1.0,
            },
            TradeHistoryRow {
                time: t0 + 6.0 / SECONDS_PER_DAY,
                price: 101.0,
                qty: 1.0,
            },
        ];

        let mut out = Vec::new();
        compact_trades_to_mini_candles_like_delphi(
            &rows,
            t0 + 1.0 / SECONDS_PER_DAY,
            t0 + 1.0,
            &mut out,
        );

        assert_eq!(
            out,
            vec![MiniCandle {
                time: t0 + 6.0 / SECONDS_PER_DAY,
                cnt: 1,
                min_price: 101.0,
                max_price: 101.0,
                buy_vol: 101.0,
                sell_vol: 0.0,
            }]
        );
    }
}
