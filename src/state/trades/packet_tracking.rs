//! `ProcessTradesStream` packet-number state machine.

use super::*;
use crate::commands::trades_stream::TradesPacket;

impl TradesState {
    /// Create a new gap bucket.
    ///
    /// Delphi source: `CreateGapBucket`, `MoonProtoEngine.pas:1380-1430`.
    fn create_bucket(&mut self, start_num: u16, end_num: u16, now_ms: i64) {
        let gap_size = end_num.wrapping_sub(start_num) as usize + 1;
        let gap_size = gap_size.min(MAX_RECVD_SIZE);

        // Delphi `CreateGapBucket`: `If gapSize > DEFAULT_RECVD_SIZE then
        // LastLargeRecvdTime := NowTimeX`. Record that recvd grew above the default so
        // the lazy shrink (tick) knows when to start counting the 30 minutes from.
        if gap_size > DEFAULT_RECVD_SIZE {
            self.last_large_recvd_ms = now_ms;
        }

        // First look for an empty slot.
        for b in self.buckets.iter_mut() {
            if !b.active {
                b.active = true;
                b.start_num = start_num;
                b.end_num = end_num;
                b.created_ms = now_ms;
                b.last_retry_ms = now_ms;
                b.retry_count = 0;
                b.refund_used = false;
                if b.recvd.len() < gap_size {
                    b.recvd.resize(gap_size, false);
                } else {
                    for r in b.recvd[..gap_size].iter_mut() {
                        *r = false;
                    }
                }
                self.used_buckets += 1;
                return;
            }
        }

        // All slots taken — evict the oldest.
        let oldest_idx = self
            .buckets
            .iter()
            .enumerate()
            .min_by_key(|(_, b)| b.created_ms)
            .map(|(i, _)| i)
            .unwrap_or(0);
        let b = &mut self.buckets[oldest_idx];
        b.start_num = start_num;
        b.end_num = end_num;
        b.created_ms = now_ms;
        b.last_retry_ms = now_ms;
        b.retry_count = 0;
        b.refund_used = false;
        if b.recvd.len() < gap_size {
            b.recvd.resize(gap_size, false);
        } else {
            for r in b.recvd[..gap_size].iter_mut() {
                *r = false;
            }
        }
        // used_buckets does not change (slot was taken, stays taken).
    }

    /// Find a bucket that contains `packet_num`.
    ///
    /// With `want_extend=true`, this also performs Delphi's adjacent-bucket
    /// extension and updates `last_packet_num` inside the method, matching the
    /// Delphi side effect.
    fn find_bucket_for_packet(
        &mut self,
        packet_num: u16,
        want_extend: bool,
        new_gap_start: u16,
        new_gap_end: u16,
    ) -> Option<usize> {
        if self.used_buckets == 0 {
            return None;
        }
        for (i, b) in self.buckets.iter().enumerate() {
            if b.active && is_packet_in_range(packet_num, b.start_num, b.end_num) {
                return Some(i);
            }
        }
        if !want_extend {
            return None;
        }
        for (i, b) in self.buckets.iter_mut().enumerate() {
            if !b.active {
                continue;
            }
            if b.retry_count >= 2 || b.end_num != new_gap_start.wrapping_sub(2) {
                continue;
            }
            let old_size = b.end_num.wrapping_sub(b.start_num) as usize + 1;
            let new_size = new_gap_end.wrapping_sub(b.start_num) as usize + 1;
            if new_size > MAX_RECVD_SIZE {
                continue;
            }
            if b.recvd.len() < new_size {
                b.recvd.resize(new_size, false);
            }
            if old_size < b.recvd.len() {
                b.recvd[old_size] = true;
            }
            if old_size + 1 < new_size {
                for recvd in b.recvd[(old_size + 1)..new_size].iter_mut() {
                    *recvd = false;
                }
            }
            b.end_num = new_gap_end;
            if b.retry_count >= 1 && !b.refund_used {
                b.retry_count = b.retry_count.saturating_sub(1);
                b.refund_used = true;
            }
            self.last_packet_num = packet_num;
            return Some(i);
        }
        None
    }

    /// Process `MPC_TradesStream` packet-number state with packet tracking.
    ///
    /// Low-level callers that still parse owned [`TradesPacket`] get only a
    /// lightweight [`TradesEvent::Applied`] notification; row storage belongs
    /// to the active dispatcher/SeqRing path.
    #[must_use = "TradesEvents must be processed for diagnostics and gap recovery"]
    pub fn on_packet(&mut self, pkt: TradesPacket, now_ms: i64) -> Vec<TradesEvent> {
        let effects = self.on_packet_header(pkt.packet_num, now_ms);
        materialize_packet_effects(effects, pkt)
    }

    /// Packet-number branch of `ProcessTradesStream(TrackPackets=True)`.
    ///
    /// This deliberately takes only `packet_num`. Delphi performs this
    /// bookkeeping before the row-reading loop, so the Rust dispatcher can do
    /// the same and apply decoded sections directly without building an owned
    /// packet for public callbacks.
    pub(crate) fn on_packet_header(&mut self, packet_num: u16, now_ms: i64) -> TradesPacketEffects {
        let mut events = TradesPacketEffects::new();

        // Packet-number tracking is best-effort loss recovery, not anti-replay.
        // Delivery is unordered and unreliable: a delayed/missing packet is
        // re-sent by the server (TradesResendResponse -> on_packet_resend) and
        // applied on arrival; a duplicate is applied too — for a feed, "see it
        // late" beats dropping it or waiting for strict order.
        // Anti-replay is not warranted here:
        //  - thin client: nothing is executed off the feed, so a replay only
        //    affects display — a stale tail is overwritten by the next live
        //    packet, at most leaving cosmetic duplicate rows in the local trade
        //    history; no executable price or account state changes;
        //  - a sustained effect would also require dropping the live feed (an
        //    availability denial — the dominant harm), which a window cannot stop;
        //  - packet_num is a u16 that wraps within seconds on a live stream, and
        //    the feed carries no wider authenticated counter; across the wrap a
        //    replayed number is indistinguishable from a legitimately recurring
        //    one, so a window would either drop real packets or miss the replay.
        // Replay of account-state commands is handled separately by the crypted
        // slider (its msg_num is a non-wrapping u64 inside AEAD).

        // === First packet OR long pause → reset ===
        let pause_detected = self.trades_started
            && self.last_packet_time_ms != 0
            && (now_ms - self.last_packet_time_ms).abs() > TRADES_PAUSE_TIMEOUT_MS;

        if !self.trades_started || pause_detected {
            self.reset_gap_buckets(now_ms);
            self.trades_started = true;
            self.last_packet_num = packet_num;
            self.last_packet_time_ms = now_ms;
            events.push(TradesPacketEffect::Apply);
            return events;
        }

        // === Duplicate ===
        // Delphi `ProcessTradesStream`: the `PacketNum = LastTradesPacketNum` branch
        // only logs a duplicate; after the tracking block the procedure still reads
        // the sections and applies trades. Preserve this: first a diagnostic event,
        // then Apply of the same payload.
        if packet_num == self.last_packet_num {
            self.last_packet_time_ms = now_ms;
            events.push(TradesPacketEffect::Duplicate);
            events.push(TradesPacketEffect::Apply);
            return events;
        }

        // === Sequential: packet_num == last + 1 ===
        if packet_num == self.last_packet_num.wrapping_add(1) {
            self.last_packet_num = packet_num;
            self.last_packet_time_ms = now_ms;
            events.push(TradesPacketEffect::Apply);
            return events;
        }

        // === Out-of-order or Gap ===
        let last = self.last_packet_num;
        // packet_num > last+1 → new gap. Missing range is [last+1 .. packet_num-1].
        let gap_size = packet_num.wrapping_sub(last.wrapping_add(1)) as usize;

        // If packet_num is effectively "ahead" of last (forward gap), create a bucket.
        // Wrap-safe forward detection: packet_num != last && packet_num != last+1.
        // Distinguish a forward gap (small gap_size) from backward (resend matching).

        let new_gap_start = last.wrapping_add(1);
        let new_gap_end = packet_num.wrapping_sub(1);

        // First check whether this is a packet from an existing bucket or an adjacent
        // gap that Delphi `FindBucketForPacket(... WantExtend=True ...)` extends inside
        // the same method.
        if let Some(idx) = self.find_bucket_for_packet(packet_num, true, new_gap_start, new_gap_end)
        {
            let b = &mut self.buckets[idx];
            if is_packet_in_range(packet_num, b.start_num, b.end_num) {
                let recvd_idx = packet_num.wrapping_sub(b.start_num) as usize;
                if recvd_idx < b.recvd.len() {
                    b.recvd[recvd_idx] = true;
                }
                let bucket_range = (b.start_num, b.end_num);
                self.last_packet_time_ms = now_ms;
                events.push(TradesPacketEffect::GapFilled {
                    packet_num,
                    bucket_seq_range: bucket_range,
                });
                events.push(TradesPacketEffect::Apply);
                return events;
            }
            events.push(TradesPacketEffect::GapDetected {
                start: new_gap_start,
                end: new_gap_end,
            });
        }
        if !events.has_gap_detected() {
            // Check the size. Gap too large or buckets overflowed.
            if gap_size > MAX_RECVD_SIZE || self.used_buckets >= MAX_GAP_BUCKETS {
                // Delphi MoonProtoEngine.pas:1649-1658: on overflow it resets buckets,
                // does NOT update LastTradesPacketNum, but the current packet is still
                // applied to markets afterward. The next normal packet restarts tracking.
                //
                // The old "anti-DoS H8" drop+warn was an unauthorized addition: a
                // ServerToken change is already handled via
                // `EventDispatcher.last_known_server_token` BEFORE the packet is applied,
                // so there is no adversarial vector here — only legitimate backpressure
                // from the server (for example after a restart).
                log::warn!(target: "moonproto::trades",
                    "packet_num jump {} -> {} (gap_size={} > MAX_RECVD_SIZE={} or buckets full); resetting gap buckets like Delphi",
                    last, packet_num, gap_size, MAX_RECVD_SIZE);
                self.reset_gap_buckets(now_ms);
                events.push(TradesPacketEffect::Apply);
                return events;
            }

            self.create_bucket(new_gap_start, new_gap_end, now_ms);
            events.push(TradesPacketEffect::GapDetected {
                start: new_gap_start,
                end: new_gap_end,
            });
        }

        self.last_packet_num = packet_num;
        self.last_packet_time_ms = now_ms;
        events.push(TradesPacketEffect::Apply);
        events
    }

    /// Process one packet from `MPC_TradesResendResponse`.
    ///
    /// Resend packets do not advance `last_packet_num`; they only mark received
    /// bits in existing buckets. This mirrors Delphi
    /// `ProcessTradesStream(TrackPackets=False)`.
    pub fn on_packet_resend(&mut self, pkt: TradesPacket) -> Vec<TradesEvent> {
        let effects = self.on_packet_resend_header(pkt.packet_num);
        materialize_packet_effects(effects, pkt)
    }

    /// Packet-number branch of `ProcessTradesStream(TrackPackets=False)`.
    pub(crate) fn on_packet_resend_header(&mut self, packet_num: u16) -> TradesPacketEffects {
        let mut events = TradesPacketEffects::new();
        if let Some(idx) = self.find_bucket_for_packet(packet_num, false, 0, 0) {
            let b = &mut self.buckets[idx];
            let recvd_idx = packet_num.wrapping_sub(b.start_num) as usize;
            if recvd_idx < b.recvd.len() {
                b.recvd[recvd_idx] = true;
            }
            let bucket_range = (b.start_num, b.end_num);
            events.push(TradesPacketEffect::GapFilled {
                packet_num,
                bucket_seq_range: bucket_range,
            });
        } else {
            // Resend arrived for a long-closed bucket. Delphi TrackPackets=False does
            // not mark a bucket, but still parses the sections below and applies the
            // trades; therefore emit a diagnostic OutOfOrder + Apply.
            events.push(TradesPacketEffect::OutOfOrder { packet_num });
        }
        events.push(TradesPacketEffect::Apply);
        events
    }
}
