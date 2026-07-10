//! Derived analytics and active 5m candle maintenance.

use crate::state::history::{
    Candle5mRow, CandleVolumeSnapshot, DerivedDeltaSnapshot, RollingTradeVolumeSnapshot,
    TradeHistoryRow,
};
#[cfg(test)]
use crate::state::history::{ROLLING_PRICE_RANGE_BUCKETS, ROLLING_VOLUME_BUCKETS};
use crate::MoonTime;

use super::{MarketHistoryStore, FIVE_MINUTES_MS};

const SHORT_ANALYTICS_BUCKET_MS: i64 = 5_000;
const MAX_DERIVED_CANDLES: usize = 500;
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct DerivedRefreshWork {
    pub(super) trade_buckets_visited: usize,
    pub(super) last_price_buckets_visited: usize,
    pub(super) candle_rows_visited: usize,
    pub(super) published: bool,
}

impl MarketHistoryStore {
    pub(crate) fn refresh_derived_analytics(&mut self, now_time: MoonTime) {
        #[cfg(test)]
        {
            self.last_refresh_work = DerivedRefreshWork::default();
        }

        self.seal_current_candle_if_due(now_time);
        let short_bucket = short_analytics_bucket(now_time);
        let short_bucket_changed = self.short_analytics_bucket != Some(short_bucket);
        let mut changed = false;

        let refresh_trade = self.trade_analytics_dirty
            || (short_bucket_changed
                && self.derived.trade_volumes != RollingTradeVolumeSnapshot::default());
        if refresh_trade {
            let volumes = self.rolling_volumes.snapshot(now_time);
            let trade_deltas = if self.deltas_by_trades {
                trade_deltas_from_rolling_volumes(volumes)
            } else {
                DerivedDeltaSnapshot::default()
            };
            self.derived.trade_volumes = volumes;
            self.derived.trade_deltas = trade_deltas;
            self.trade_analytics_dirty = false;
            changed = true;
            #[cfg(test)]
            {
                self.last_refresh_work.trade_buckets_visited = ROLLING_VOLUME_BUCKETS;
            }
        }

        let refresh_last_price = self.last_price_analytics_dirty
            || (short_bucket_changed
                && self.derived.last_price_deltas != DerivedDeltaSnapshot::default());
        if refresh_last_price {
            self.derived.last_price_deltas = self
                .rolling_last_price_ranges
                .snapshot(now_time, self.eps_profile.eps);
            self.last_price_analytics_dirty = false;
            changed = true;
            #[cfg(test)]
            {
                self.last_refresh_work.last_price_buckets_visited = ROLLING_PRICE_RANGE_BUCKETS;
            }
        }
        self.short_analytics_bucket = Some(short_bucket);

        let candle_bucket = candle_delta_bucket(now_time);
        let candle_bucket_changed = self.sealed_candle_analytics_bucket != Some(candle_bucket);
        let refresh_sealed_candles = self.sealed_candle_analytics_dirty
            || (candle_bucket_changed && self.sealed_candle_derived.is_some());
        if refresh_sealed_candles {
            let (closed, _visited) = self.closed_candle_derived_one_pass(now_time);
            self.sealed_candle_derived = closed;
            self.sealed_candle_analytics_bucket = Some(candle_bucket);
            self.sealed_candle_analytics_dirty = false;
            self.current_candle_analytics_dirty = true;
            #[cfg(test)]
            {
                self.last_refresh_work.candle_rows_visited = _visited;
            }
        } else if candle_bucket_changed {
            self.sealed_candle_analytics_bucket = Some(candle_bucket);
        }

        if self.current_candle_analytics_dirty {
            let (deltas, volumes) = self.candle_derived_with_current(now_time);
            self.derived.candle_deltas = deltas;
            self.derived.candle_volumes = volumes;
            self.derived.current_candle = self.current_candle;
            self.current_candle_analytics_dirty = false;
            changed = true;
        }

        if changed {
            self.derived.deltas = combine_deltas(
                self.derived.trade_deltas,
                self.derived.candle_deltas,
                self.derived.last_price_deltas,
            );
            if self.rolling_volumes_publish_dirty {
                self.read_handle
                    .publish(&self.rolling_volumes, self.derived);
                self.rolling_volumes_publish_dirty = false;
            } else {
                self.read_handle.publish_derived(self.derived);
            }
            #[cfg(test)]
            {
                self.last_refresh_work.published = true;
            }
        }
    }

    pub(super) fn update_current_candle_from_trade(
        &mut self,
        row: TradeHistoryRow,
        traded_value: f32,
    ) {
        if row.time == MoonTime::ZERO || row.price <= 0.0 {
            return;
        }
        self.seal_current_candle_if_due(row.time);
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
        self.current_candle_analytics_dirty = true;
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
            self.sealed_candle_analytics_dirty = true;
            self.current_candle_analytics_dirty = true;
        }
    }

    fn closed_candle_derived_one_pass(
        &self,
        now_time: MoonTime,
    ) -> (Option<CandleDerivedAccumulator>, usize) {
        let mut acc = CandleDerivedAccumulator::new(now_time, self.eps_profile.eps);
        let mut sealed_count = 0usize;
        let mut newest_sealed_valid = false;
        if let Some(reader) = self.readers.candles_5m.as_ref() {
            reader.with_last(reader.capacity().min(MAX_DERIVED_CANDLES), |view| {
                view.for_each(|row| {
                    sealed_count += 1;
                    newest_sealed_valid = row.time != MoonTime::ZERO;
                    acc.add(*row);
                });
            });
        }
        if sealed_count < 3 || !newest_sealed_valid {
            return (None, sealed_count);
        }
        (Some(acc), sealed_count)
    }

    fn candle_derived_with_current(
        &self,
        now_time: MoonTime,
    ) -> (DerivedDeltaSnapshot, CandleVolumeSnapshot) {
        let Some(mut acc) = self.sealed_candle_derived else {
            return (
                DerivedDeltaSnapshot::default(),
                CandleVolumeSnapshot::default(),
            );
        };
        // Closed rows are rebuilt on the 5m boundary. Between boundaries only
        // the evaluation clock moves, so a newer live candle is never rejected
        // as "from the future" while closed-window expiry stays bucket-bounded.
        acc.now_time = now_time;
        if let Some(candle) = self.current_candle {
            acc.add(candle);
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

fn short_analytics_bucket(now_time: MoonTime) -> i64 {
    if now_time == MoonTime::ZERO {
        return i64::MIN;
    }
    now_time.unix_millis().div_euclid(SHORT_ANALYTICS_BUCKET_MS)
}

#[derive(Clone, Copy)]
struct CandleWindow {
    min_price: f32,
    max_price: f32,
    volume: f64,
}

impl CandleWindow {
    fn new() -> Self {
        Self {
            min_price: 0.0,
            max_price: 0.0,
            volume: 0.0,
        }
    }

    fn add_range(&mut self, candle: Candle5mRow) {
        if candle.low > 0.0 && (self.min_price <= 0.0 || candle.low < self.min_price) {
            self.min_price = candle.low;
        }
        if candle.high > self.max_price {
            self.max_price = candle.high;
        }
    }

    fn add_volume(&mut self, candle: Candle5mRow) {
        if candle.volume > 0.0 {
            self.volume += f64::from(candle.volume);
        }
    }

    fn add(&mut self, candle: Candle5mRow) {
        self.add_range(candle);
        self.add_volume(candle);
    }

    fn delta(&self, eps: f64) -> f64 {
        delta_percent(f64::from(self.min_price), f64::from(self.max_price), eps)
    }
}

#[derive(Clone, Copy)]
pub(super) struct CandleDerivedAccumulator {
    now_time: MoonTime,
    eps: f64,
    five_minutes: CandleWindow,
    fifteen_minutes: CandleWindow,
    thirty_minutes: CandleWindow,
    one_hour: CandleWindow,
    two_hours_volume: CandleWindow,
    three_hours: CandleWindow,
    four_hours_delta: CandleWindow,
    twenty_four_hours_volume: CandleWindow,
    twenty_five_hours_delta: CandleWindow,
    seventy_two_hours: CandleWindow,
}

impl CandleDerivedAccumulator {
    fn new(now_time: MoonTime, eps: f64) -> Self {
        Self {
            now_time,
            eps,
            five_minutes: CandleWindow::new(),
            fifteen_minutes: CandleWindow::new(),
            thirty_minutes: CandleWindow::new(),
            one_hour: CandleWindow::new(),
            two_hours_volume: CandleWindow::new(),
            three_hours: CandleWindow::new(),
            four_hours_delta: CandleWindow::new(),
            twenty_four_hours_volume: CandleWindow::new(),
            twenty_five_hours_delta: CandleWindow::new(),
            seventy_two_hours: CandleWindow::new(),
        }
    }

    fn add(&mut self, candle: Candle5mRow) {
        if candle.time > self.now_time {
            return;
        }
        let age_ms = self.now_time.unix_millis() - candle.time.unix_millis();

        // Production-core boundaries are strict on the old edge:
        // `abs(Now-Time) < 15/MinsInDay`, `h < 72`, `h <= 2` -> age < 3h.
        // Compute age once, then update only the range/volume actually consumed
        // by each public field. The 3h window serves both 3h volume and the
        // production-compatible `two_hours` delta, so it is accumulated once.
        if age_ms >= 72 * HOUR_MS {
            return;
        }
        self.seventy_two_hours.add(candle);
        if age_ms >= 25 * HOUR_MS {
            return;
        }
        self.twenty_five_hours_delta.add_range(candle);
        if age_ms >= 24 * HOUR_MS {
            return;
        }
        self.twenty_four_hours_volume.add_volume(candle);
        if age_ms >= 4 * HOUR_MS {
            return;
        }
        self.four_hours_delta.add_range(candle);
        if age_ms >= 3 * HOUR_MS {
            return;
        }
        self.three_hours.add(candle);
        if age_ms >= 2 * HOUR_MS {
            return;
        }
        self.two_hours_volume.add_volume(candle);
        if age_ms >= HOUR_MS {
            return;
        }
        self.one_hour.add(candle);
        if age_ms >= 30 * MINUTE_MS {
            return;
        }
        self.thirty_minutes.add(candle);
        if age_ms >= 15 * MINUTE_MS {
            return;
        }
        self.fifteen_minutes.add(candle);
        if age_ms >= 5 * MINUTE_MS {
            return;
        }
        self.five_minutes.add(candle);
    }

    fn finish(self) -> (DerivedDeltaSnapshot, CandleVolumeSnapshot) {
        let one_hour_delta = self.one_hour.delta(self.eps);
        (
            DerivedDeltaSnapshot {
                five_minutes: self.five_minutes.delta(self.eps),
                fifteen_minutes: self.fifteen_minutes.delta(self.eps),
                thirty_minutes: self.thirty_minutes.delta(self.eps),
                one_hour: one_hour_delta,
                two_hours: one_hour_delta.max(self.three_hours.delta(self.eps)),
                three_hours: one_hour_delta.max(self.four_hours_delta.delta(self.eps)),
                twenty_four_hours: one_hour_delta.max(self.twenty_five_hours_delta.delta(self.eps)),
                seventy_two_hours: self.seventy_two_hours.delta(self.eps),
                ..DerivedDeltaSnapshot::default()
            },
            CandleVolumeSnapshot {
                five_minutes: self.five_minutes.volume,
                fifteen_minutes: self.fifteen_minutes.volume,
                thirty_minutes: self.thirty_minutes.volume,
                one_hour: self.one_hour.volume,
                two_hours: self.two_hours_volume.volume,
                three_hours: self.three_hours.volume,
                twenty_four_hours: self.twenty_four_hours_volume.volume,
                seventy_two_hours: self.seventy_two_hours.volume,
            },
        )
    }
}
