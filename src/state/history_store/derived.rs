//! Derived analytics and active 5m candle maintenance.

use crate::state::history::{
    Candle5mRow, CandleVolumeSnapshot, DerivedDeltaSnapshot, LastPricePoint,
    RollingTradeVolumeSnapshot, TradeHistoryRow,
};
use crate::MoonTime;

use super::{MarketHistoryStore, FIVE_MINUTES_MS};

impl MarketHistoryStore {
    pub(crate) fn refresh_derived_analytics(&mut self, now_time: MoonTime) {
        self.seal_current_candle_if_due(now_time);
        let volumes = self.rolling_volumes.snapshot(now_time);
        let trade_deltas = if self.deltas_by_trades {
            trade_deltas_from_rolling_volumes(volumes)
        } else {
            DerivedDeltaSnapshot::default()
        };
        let last_price_deltas = self.last_price_deltas_one_pass(now_time);
        let candle_bucket = candle_delta_bucket(now_time);
        if self.candle_deltas_dirty || self.candle_deltas_bucket != Some(candle_bucket) {
            let (deltas, volumes) = self.candle_derived_one_pass(now_time);
            self.derived.candle_deltas = deltas;
            self.derived.candle_volumes = volumes;
            self.candle_deltas_bucket = Some(candle_bucket);
            self.candle_deltas_dirty = false;
        }

        self.derived.trade_volumes = volumes;
        self.derived.trade_deltas = trade_deltas;
        self.derived.last_price_deltas = last_price_deltas;
        self.derived.deltas =
            combine_deltas(trade_deltas, self.derived.candle_deltas, last_price_deltas);
        // The live (in-progress) candle is exposed separately from the sealed
        // ring so the consumer can draw it as the current bar (Delphi `FCandle`).
        self.derived.current_candle = self.current_candle;
        self.read_handle
            .publish(&self.rolling_volumes, self.derived);
    }

    pub(super) fn update_current_candle_from_trade(&mut self, row: TradeHistoryRow) {
        if row.time == MoonTime::ZERO || row.price <= 0.0 {
            return;
        }
        self.seal_current_candle_if_due(row.time);
        let traded_value = row.traded_value();
        let mut candle = self.current_candle.unwrap_or(Candle5mRow {
            open: row.price,
            close: row.price,
            high: row.price,
            low: row.price,
            volume: 0.0,
            time: row.time,
        });
        candle.close = row.price;
        candle.high = candle.high.max(row.price);
        candle.low = if candle.low <= 0.0 {
            row.price
        } else {
            candle.low.min(row.price)
        };
        candle.volume += traded_value;
        self.current_candle = Some(candle);
        self.candle_deltas_dirty = true;
        // The in-progress candle is a separate accumulator (Delphi `FCandle`),
        // NOT published into the `candles_5m` ring; only sealed (end-stamped)
        // candles go into the ring, see `seal_current_candle_if_due`. This
        // removes the mixing of time conventions (end-stamped snapshot + live
        // start-stamped) within a single ring.
    }

    fn seal_current_candle_if_due(&mut self, now_time: MoonTime) {
        let Some(mut candle) = self.current_candle else {
            return;
        };
        if now_time != MoonTime::ZERO
            && now_time.unix_millis() - candle.time.unix_millis() >= FIVE_MINUTES_MS
        {
            // Delphi `Recalc5mCandle` (MarketsU.pas:9988): the sealed candle is
            // stamped with the seal time (`NowTime` = end of period) and pushed
            // into Deep5m; the in-progress (FCandle) stays separate and starts over.
            candle.time = now_time;
            if let Some(writer) = self.candles_5m.as_mut() {
                writer.push(candle);
            }
            self.current_candle = None;
            self.candle_deltas_dirty = true;
        }
    }

    fn candle_derived_one_pass(
        &self,
        now_time: MoonTime,
    ) -> (DerivedDeltaSnapshot, CandleVolumeSnapshot) {
        let mut acc = CandleDerivedAccumulator::new(now_time, self.eps_profile.eps);
        let mut sealed_count = 0usize;
        let mut newest_sealed_valid = false;
        if let Some(reader) = self.readers.candles_5m.as_ref() {
            reader.with_last(reader.capacity(), |view| {
                view.for_each(|row| {
                    sealed_count += 1;
                    newest_sealed_valid = row.time != MoonTime::ZERO;
                    acc.add(*row);
                });
            });
        }
        if sealed_count < 3 || !newest_sealed_valid {
            return (
                DerivedDeltaSnapshot::default(),
                CandleVolumeSnapshot::default(),
            );
        }
        // The in-progress candle no longer lives in the ring — always add it to the derived values.
        if let Some(candle) = self.current_candle {
            acc.add(candle);
        }
        acc.finish()
    }

    fn last_price_deltas_one_pass(&self, now_time: MoonTime) -> DerivedDeltaSnapshot {
        let mut acc = LastPriceDeltaAccumulator::new(now_time, self.eps_profile.eps);
        if let Some(reader) = self.readers.last_prices.as_ref() {
            reader.with_last(reader.capacity(), |view| {
                view.for_each(|row| acc.add(*row));
            });
        }
        acc.finish()
    }
}

fn delta_percent(min_price: f64, max_price: f64, eps: f64) -> f64 {
    if min_price <= eps || max_price <= eps || max_price < min_price {
        return 0.0;
    }
    (max_price / min_price - 1.0) * 100.0
}

fn trade_deltas_from_rolling_volumes(volumes: RollingTradeVolumeSnapshot) -> DerivedDeltaSnapshot {
    DerivedDeltaSnapshot {
        one_minute: volumes.one_minute.price_delta_percent(),
        five_minutes: volumes.five_minutes.price_delta_percent(),
        ..DerivedDeltaSnapshot::default()
    }
}

pub(super) fn combine_deltas(
    trade_deltas: DerivedDeltaSnapshot,
    candle_deltas: DerivedDeltaSnapshot,
    last_price_deltas: DerivedDeltaSnapshot,
) -> DerivedDeltaSnapshot {
    let one_hour = trade_deltas
        .one_hour
        .max(candle_deltas.one_hour)
        .max(last_price_deltas.one_hour);
    DerivedDeltaSnapshot {
        one_minute: trade_deltas
            .one_minute
            .max(candle_deltas.one_minute)
            .max(last_price_deltas.one_minute),
        five_minutes: trade_deltas
            .five_minutes
            .max(candle_deltas.five_minutes)
            .max(last_price_deltas.five_minutes),
        fifteen_minutes: trade_deltas
            .fifteen_minutes
            .max(candle_deltas.fifteen_minutes)
            .max(last_price_deltas.fifteen_minutes),
        thirty_minutes: trade_deltas
            .thirty_minutes
            .max(candle_deltas.thirty_minutes)
            .max(last_price_deltas.thirty_minutes),
        one_hour,
        two_hours: one_hour.max(
            trade_deltas
                .two_hours
                .max(candle_deltas.two_hours)
                .max(last_price_deltas.two_hours),
        ),
        three_hours: one_hour.max(
            trade_deltas
                .three_hours
                .max(candle_deltas.three_hours)
                .max(last_price_deltas.three_hours),
        ),
        twenty_four_hours: trade_deltas
            .twenty_four_hours
            .max(candle_deltas.twenty_four_hours)
            .max(last_price_deltas.twenty_four_hours)
            .max(one_hour),
        seventy_two_hours: trade_deltas
            .seventy_two_hours
            .max(candle_deltas.seventy_two_hours)
            .max(last_price_deltas.seventy_two_hours),
    }
}

fn candle_delta_bucket(now_time: MoonTime) -> i64 {
    if now_time == MoonTime::ZERO {
        return i64::MIN;
    }
    now_time.unix_millis().div_euclid(FIVE_MINUTES_MS)
}

#[derive(Clone, Copy)]
struct CandleWindow {
    window_ms: i64,
    min_price: f32,
    max_price: f32,
    volume: f64,
    eps: f64,
}

impl CandleWindow {
    fn new(window_seconds: f64, eps: f64) -> Self {
        Self {
            window_ms: (window_seconds * 1_000.0).round() as i64,
            min_price: 0.0,
            max_price: 0.0,
            volume: 0.0,
            eps,
        }
    }

    fn add(&mut self, now_time: MoonTime, candle: Candle5mRow) {
        // Delphi checks are strict on the old boundary:
        // `abs(Now-Time) < 15/MinsInDay`, `h < 72`, `h <= 2` -> age < 3h.
        if candle.time.unix_millis() <= now_time.unix_millis() - self.window_ms
            || candle.time > now_time
        {
            return;
        }
        if candle.low > 0.0 && (self.min_price <= 0.0 || candle.low < self.min_price) {
            self.min_price = candle.low;
        }
        if candle.high > self.max_price {
            self.max_price = candle.high;
        }
        if candle.volume > 0.0 {
            self.volume += f64::from(candle.volume);
        }
    }

    fn finish_delta(self) -> f64 {
        delta_percent(
            f64::from(self.min_price),
            f64::from(self.max_price),
            self.eps,
        )
    }
}

struct CandleDerivedAccumulator {
    now_time: MoonTime,
    five_minutes: CandleWindow,
    fifteen_minutes: CandleWindow,
    thirty_minutes: CandleWindow,
    one_hour: CandleWindow,
    two_hours_volume: CandleWindow,
    three_hours_volume: CandleWindow,
    twenty_four_hours_volume: CandleWindow,
    seventy_two_hours: CandleWindow,
    last2h_delta_like_delphi: CandleWindow,
    last3h_delta_like_delphi: CandleWindow,
    last24h_delta_like_delphi: CandleWindow,
}

impl CandleDerivedAccumulator {
    fn new(now_time: MoonTime, eps: f64) -> Self {
        Self {
            now_time,
            five_minutes: CandleWindow::new(5.0 * 60.0, eps),
            fifteen_minutes: CandleWindow::new(15.0 * 60.0, eps),
            thirty_minutes: CandleWindow::new(30.0 * 60.0, eps),
            one_hour: CandleWindow::new(60.0 * 60.0, eps),
            two_hours_volume: CandleWindow::new(2.0 * 60.0 * 60.0, eps),
            three_hours_volume: CandleWindow::new(3.0 * 60.0 * 60.0, eps),
            twenty_four_hours_volume: CandleWindow::new(24.0 * 60.0 * 60.0, eps),
            seventy_two_hours: CandleWindow::new(72.0 * 60.0 * 60.0, eps),
            last2h_delta_like_delphi: CandleWindow::new(3.0 * 60.0 * 60.0, eps),
            last3h_delta_like_delphi: CandleWindow::new(4.0 * 60.0 * 60.0, eps),
            last24h_delta_like_delphi: CandleWindow::new(25.0 * 60.0 * 60.0, eps),
        }
    }

    fn add(&mut self, candle: Candle5mRow) {
        self.five_minutes.add(self.now_time, candle);
        self.fifteen_minutes.add(self.now_time, candle);
        self.thirty_minutes.add(self.now_time, candle);
        self.one_hour.add(self.now_time, candle);
        self.two_hours_volume.add(self.now_time, candle);
        self.three_hours_volume.add(self.now_time, candle);
        self.twenty_four_hours_volume.add(self.now_time, candle);
        self.seventy_two_hours.add(self.now_time, candle);
        self.last2h_delta_like_delphi.add(self.now_time, candle);
        self.last3h_delta_like_delphi.add(self.now_time, candle);
        self.last24h_delta_like_delphi.add(self.now_time, candle);
    }

    fn finish(self) -> (DerivedDeltaSnapshot, CandleVolumeSnapshot) {
        let one_hour_delta = self.one_hour.finish_delta();
        (
            DerivedDeltaSnapshot {
                five_minutes: self.five_minutes.finish_delta(),
                fifteen_minutes: self.fifteen_minutes.finish_delta(),
                thirty_minutes: self.thirty_minutes.finish_delta(),
                one_hour: one_hour_delta,
                two_hours: one_hour_delta.max(self.last2h_delta_like_delphi.finish_delta()),
                three_hours: one_hour_delta.max(self.last3h_delta_like_delphi.finish_delta()),
                twenty_four_hours: one_hour_delta
                    .max(self.last24h_delta_like_delphi.finish_delta()),
                seventy_two_hours: self.seventy_two_hours.finish_delta(),
                ..DerivedDeltaSnapshot::default()
            },
            CandleVolumeSnapshot {
                five_minutes: self.five_minutes.volume,
                fifteen_minutes: self.fifteen_minutes.volume,
                thirty_minutes: self.thirty_minutes.volume,
                one_hour: self.one_hour.volume,
                two_hours: self.two_hours_volume.volume,
                three_hours: self.three_hours_volume.volume,
                twenty_four_hours: self.twenty_four_hours_volume.volume,
                seventy_two_hours: self.seventy_two_hours.volume,
            },
        )
    }
}

#[derive(Clone, Copy)]
struct LastPriceWindow {
    window_ms: i64,
    min_price: f32,
    max_price: f32,
    eps: f64,
}

impl LastPriceWindow {
    fn new(window_seconds: f64, eps: f64) -> Self {
        Self {
            window_ms: (window_seconds * 1_000.0).round() as i64,
            min_price: 0.0,
            max_price: 0.0,
            eps,
        }
    }

    fn add(&mut self, now_time: MoonTime, row: LastPricePoint) {
        if row.time.unix_millis() <= now_time.unix_millis() - self.window_ms || row.time > now_time
        {
            return;
        }
        if row.current <= 0.0 {
            return;
        }
        if self.min_price <= 0.0 || row.current < self.min_price {
            self.min_price = row.current;
        }
        if row.current > self.max_price {
            self.max_price = row.current;
        }
    }

    fn finish_delta(self) -> f64 {
        delta_percent(
            f64::from(self.min_price),
            f64::from(self.max_price),
            self.eps,
        )
    }
}

struct LastPriceDeltaAccumulator {
    now_time: MoonTime,
    one_minute: LastPriceWindow,
    five_minutes: LastPriceWindow,
    fifteen_minutes: LastPriceWindow,
    thirty_minutes: LastPriceWindow,
    one_hour: LastPriceWindow,
}

impl LastPriceDeltaAccumulator {
    fn new(now_time: MoonTime, eps: f64) -> Self {
        Self {
            now_time,
            one_minute: LastPriceWindow::new(60.0, eps),
            five_minutes: LastPriceWindow::new(5.0 * 60.0, eps),
            fifteen_minutes: LastPriceWindow::new(15.0 * 60.0, eps),
            thirty_minutes: LastPriceWindow::new(30.0 * 60.0, eps),
            one_hour: LastPriceWindow::new(60.0 * 60.0, eps),
        }
    }

    fn add(&mut self, row: LastPricePoint) {
        self.one_minute.add(self.now_time, row);
        self.five_minutes.add(self.now_time, row);
        self.fifteen_minutes.add(self.now_time, row);
        self.thirty_minutes.add(self.now_time, row);
        self.one_hour.add(self.now_time, row);
    }

    fn finish(self) -> DerivedDeltaSnapshot {
        DerivedDeltaSnapshot {
            one_minute: self.one_minute.finish_delta(),
            five_minutes: self.five_minutes.finish_delta(),
            fifteen_minutes: self.fifteen_minutes.finish_delta(),
            thirty_minutes: self.thirty_minutes.finish_delta(),
            one_hour: self.one_hour.finish_delta(),
            ..DerivedDeltaSnapshot::default()
        }
    }
}
