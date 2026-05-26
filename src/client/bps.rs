//! Delphi-style byte-rate counter.

/// O(1) byte-rate counter with about 10 seconds of EMA smoothing.
///
/// This mirrors Delphi `TMoonProtoUDPClient.AddBytesCount` without a heap-backed
/// sliding window.
///
/// Algorithm:
/// - `cur_sec_bytes` accumulates bytes in the current one-second bucket.
/// - Once a second passes, the bucket is folded into the EMA.
/// - `bytes_per_sec()` returns the smoothed bytes-per-second value.
#[derive(Debug, Default)]
pub struct BpsCounter {
    /// Bytes accumulated in the current one-second bucket.
    cur_sec_bytes: u64,
    /// EMA-smoothed value (`10 * average B/s` in steady state).
    ema_10sec: u64,
    /// Timestamp of the current bucket start in milliseconds (`0` means
    /// uninitialized).
    last_sec_ms: i64,
    /// Number of complete seconds accumulated, clamped to 10.
    stat_sec_count: u8,
}

impl BpsCounter {
    /// Create an empty byte-rate counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add bytes observed at a monotonic millisecond timestamp.
    pub fn add(&mut self, bytes: u64, now_ms: i64) {
        // Первый вызов — просто инициализируем bucket.
        if self.last_sec_ms == 0 {
            self.last_sec_ms = now_ms;
        }
        // Прошла секунда? Закрываем bucket в EMA / accumulation.
        if (now_ms - self.last_sec_ms).abs() > 1000 {
            // Ramp-up (audit_delphi_deviation #2): первые 10 секунд — accumulation, далее EMA.
            // Так Delphi `MoonProtoUDPClient.pas:113-138` гарантирует точное среднее
            // с первой секунды (без 10×underestimate).
            if self.stat_sec_count < 10 {
                self.ema_10sec = self.ema_10sec.saturating_add(self.cur_sec_bytes);
                self.stat_sec_count += 1;
            } else {
                // EMA: 90% старого + 10% нового. Формула из Delphi: `ema := ema / 10 * 9 + bucket`.
                self.ema_10sec = (self.ema_10sec / 10) * 9 + self.cur_sec_bytes;
            }
            self.cur_sec_bytes = 0;
            self.last_sec_ms = now_ms;
        }
        self.cur_sec_bytes = self.cur_sec_bytes.saturating_add(bytes);
    }

    /// Return the average bytes per second over the recent smoothing window.
    ///
    /// During the first 10 seconds, this divides by the actual number of closed
    /// buckets instead of by 10, matching Delphi's ramp-up behavior.
    pub fn bytes_per_sec(&self) -> u64 {
        let div = self.stat_sec_count.max(1) as u64;
        self.ema_10sec / div
    }
}

#[cfg(test)]
mod bps_tests {
    use super::*;

    #[test]
    fn bps_counter_empty() {
        let c = BpsCounter::new();
        assert_eq!(c.bytes_per_sec(), 0);
    }

    #[test]
    fn bps_counter_within_second_just_accumulates() {
        let mut c = BpsCounter::new();
        c.add(100, 1000);
        c.add(200, 1500);
        // Не прошла секунда → ema_10sec не обновился → bytes_per_sec = 0.
        assert_eq!(c.bytes_per_sec(), 0);
        // Но bucket собрал 300.
        assert_eq!(c.cur_sec_bytes, 300);
    }

    #[test]
    fn bps_counter_steady_state_converges() {
        let mut c = BpsCounter::new();
        // Эмулируем 100 секунд равномерного потока: 1000 байт/сек.
        // Используем шаг 1100мс между бакетами чтобы условие `> 1000` срабатывало надёжно.
        for sec in 1..101i64 {
            let bucket_start = sec * 1100;
            for _ in 0..10 {
                c.add(100, bucket_start);
            }
        }
        // EMA должна сойтись к ~10000 (= 10 × 1000 byte/sec — формула Delphi).
        // bytes_per_sec возвращает ema/10 = ~1000.
        let bps = c.bytes_per_sec();
        assert!(bps > 850 && bps < 1100, "bps={}, expected ~1000", bps);
    }
}
