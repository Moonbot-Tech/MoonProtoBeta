//! `CheckMissingTradesPackets` resend recovery tick.

use super::*;
use crate::commands::engine_request;

impl TradesState {
    /// Like `tick`, but also returns `BucketClosed` events for recovered/lost
    /// gap buckets.
    ///
    /// The standard `tick` stays compatibility-oriented and returns only resend
    /// payloads.
    pub(crate) fn tick_with_events(
        &mut self,
        rtt_ms: i64,
        now_ms: i64,
    ) -> (Vec<Vec<u8>>, Vec<TradesEvent>) {
        let mut events: Vec<TradesEvent> = Vec::new();
        let payloads = self.tick_impl(rtt_ms, now_ms, &mut events);
        (payloads, events)
    }

    /// Tail tick: check expired buckets and build a resend payload when needed.
    ///
    /// Delphi calls `CheckMissingTradesPackets` only at the tail of a
    /// successfully processed trades packet, behind the external
    /// `LastCheckMissingTime` 100ms throttle. The active library mirrors that:
    /// it calls this after valid live/resend trades packets, not from an
    /// independent timer while the channel is silent.
    ///
    /// Returns `Some(payload)` when the caller should send `TradesResend`.
    /// `rtt_ms` is the current round-trip delay in milliseconds.
    /// Delphi `CheckMissingTradesPackets` MoonProtoEngine.pas:1483-1549.
    #[cfg(test)]
    pub(crate) fn tick(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>> {
        let mut events: Vec<TradesEvent> = Vec::new();
        self.tick_impl(rtt_ms, now_ms, &mut events)
    }

    fn tick_impl(
        &mut self,
        rtt_ms: i64,
        now_ms: i64,
        events: &mut Vec<TradesEvent>,
    ) -> Vec<Vec<u8>> {
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = events;

        // Delphi caller:
        // `If (NowTimeX - LastCheckMissingTime) > 100/MSecsPerDay then begin
        //    CheckMissingTradesPackets;
        //    LastCheckMissingTime := NowTimeX;
        //  end;`
        if now_ms - self.last_check_missing_ms <= 100 {
            return Vec::new();
        }
        self.last_check_missing_ms = now_ms;
        if self.used_buckets == 0 {
            return Vec::new();
        }

        let retry_delay_ms: f64 = rtt_ms.max(250) as f64;
        let min_delay_ms: f64 = 300.0;
        let mut packet_nums: Vec<u16> = Vec::new();

        for b in self.buckets.iter_mut() {
            if !b.active {
                continue;
            }
            let gap_size = b.gap_size();
            let all_recvd = b.recvd.iter().take(gap_size).all(|&r| r);
            // PathDelay = min(1800, max(MinDelay, RetryDelay * (1.2 + retry*0.7)))
            let path_delay_ms: f64 = (retry_delay_ms * (1.2 + b.retry_count as f64 * 0.7))
                .max(min_delay_ms)
                .min(1800.0);

            if all_recvd {
                #[cfg(any(test, feature = "diagnostics"))]
                events.push(TradesEvent::BucketClosed {
                    start: b.start_num,
                    end: b.end_num,
                    all_received: true,
                    retry_count: b.retry_count,
                });
                b.active = false;
                self.used_buckets = self.used_buckets.saturating_sub(1);
                continue;
            }

            if b.retry_count >= MAX_RETRY_COUNT {
                if ((now_ms - b.last_retry_ms).abs() as f64) > path_delay_ms {
                    #[cfg(any(test, feature = "diagnostics"))]
                    events.push(TradesEvent::BucketClosed {
                        start: b.start_num,
                        end: b.end_num,
                        all_received: false,
                        retry_count: b.retry_count,
                    });
                    b.active = false;
                    self.used_buckets = self.used_buckets.saturating_sub(1);
                }
                continue;
            }

            if ((now_ms - b.last_retry_ms).abs() as f64) > path_delay_ms {
                for j in 0..gap_size {
                    if !b.recvd[j] {
                        packet_nums.push(b.start_num.wrapping_add(j as u16));
                    }
                }
                b.retry_count = b.retry_count.saturating_add(1);
                b.last_retry_ms = now_ms;
            }
        }

        // Delphi MoonProtoEngine.pas:1566-1573 (tail of `CheckMissingTradesPackets`):
        // lazy shrink of `recvd` for inactive buckets every 30 minutes — reclaim memory
        // after a one-off large gap. Rust grows `recvd` up to gap_size on a large gap
        // (`create_bucket` / extend) and never shrank it without this. Active buckets in
        // use are left untouched; like Delphi, this runs only when `used_buckets > 0`
        // (early return above), matching `If UsedBuckets = 0 then exit` in the reference.
        const LARGE_RECVD_SHRINK_MS: i64 = 30 * 60 * 1000;
        if now_ms - self.last_large_recvd_ms > LARGE_RECVD_SHRINK_MS {
            for b in self.buckets.iter_mut() {
                if !b.active && b.recvd.len() > DEFAULT_RECVD_SIZE {
                    b.recvd.truncate(DEFAULT_RECVD_SIZE);
                    b.recvd.shrink_to_fit();
                }
            }
            self.last_large_recvd_ms = now_ms;
        }

        if packet_nums.is_empty() {
            return Vec::new();
        }
        #[cfg(any(test, feature = "diagnostics"))]
        events.push(TradesEvent::ResendRequested {
            packet_nums: packet_nums.clone(),
        });
        engine_request::trades_resend_batches(&packet_nums)
    }
}
