use super::*;
use crate::commands::trades_stream::TradesPacket;

fn make_pkt(packet_num: u16) -> TradesPacket {
    TradesPacket {
        base_time: 0.0,
        packet_num,
        sections: Vec::new(),
    }
}

#[test]
fn first_packet_starts_state() {
    let mut s = TradesState::new();
    let evs = s.on_packet(make_pkt(100), 1000);
    assert_eq!(evs.len(), 1);
    assert!(matches!(evs[0], TradesEvent::Applied { .. }));
    assert_eq!(s.last_packet_num(), 100);
}

#[test]
fn sequential_packets_applied() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let evs = s.on_packet(make_pkt(101), 1010);
    assert!(matches!(evs[0], TradesEvent::Applied { .. }));
    assert_eq!(s.last_packet_num(), 101);
    assert_eq!(s.used_buckets(), 0);
}

#[test]
fn duplicate_detected() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let evs = s.on_packet(make_pkt(100), 1010);
    assert!(matches!(evs[0], TradesEvent::Duplicate));
    assert!(
        matches!(evs[1], TradesEvent::Applied { .. }),
        "Delphi logs duplicate but still applies the packet payload"
    );
    assert_eq!(
        s.last_packet_num(),
        100,
        "duplicate must not advance tracking state"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.ProcessTradesStream
fn duplicate_refreshes_pause_timer() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(100), 20_000);

    assert_eq!(
            s.last_packet_time_ms, 20_000,
            "Delphi updates LastTradesPacketTime for every TrackPackets=true packet, including duplicates"
        );
}

#[test]
fn gap_creates_bucket() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let evs = s.on_packet(make_pkt(103), 1010); // gap: 101, 102
    let has_gap = evs.iter().any(|e| {
        matches!(
            e,
            TradesEvent::GapDetected {
                start: 101,
                end: 102
            }
        )
    });
    let has_apply = evs.iter().any(|e| matches!(e, TradesEvent::Applied { .. }));
    assert!(has_gap && has_apply);
    assert_eq!(s.used_buckets(), 1);
}

#[test]
fn out_of_order_fills_gap() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(103), 1010); // creates bucket [101, 102]
    let evs = s.on_packet(make_pkt(101), 1020); // fills bucket
    let has_filled = evs.iter().any(|e| {
        matches!(
            e,
            TradesEvent::GapFilled {
                packet_num: 101,
                ..
            }
        )
    });
    assert!(has_filled);
}

#[test]
fn out_of_order_live_packet_refreshes_pause_timer() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(103), 1010); // creates bucket [101, 102]
    let _ = s.on_packet(make_pkt(101), 20_000); // in-bucket live packet

    assert_eq!(
        s.last_packet_time_ms, 20_000,
        "in-bucket TrackPackets=true packets refresh LastTradesPacketTime in Delphi"
    );

    let _ = s.on_packet(make_pkt(104), 45_000);
    assert_eq!(
            s.used_buckets(), 1,
            "packet 104 is only 25s after the in-bucket packet, so the bucket for still-missing 102 must survive"
        );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.ProcessTradesResendBatch
fn late_resend_outside_bucket_is_still_applied() {
    let mut s = TradesState::new();
    let evs = s.on_packet_resend(make_pkt(777));
    assert!(matches!(
        evs[0],
        TradesEvent::OutOfOrder { packet_num: 777 }
    ));
    assert!(
        matches!(evs[1], TradesEvent::Applied { .. }),
        "Delphi TrackPackets=False applies resend payload even when no bucket matches"
    );
    assert_eq!(
        s.last_packet_num(),
        0,
        "resend packets must not advance live tracking"
    );
}

#[test]
fn resend_inside_bucket_marks_gap_and_applies_once() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(103), 1010);
    let evs = s.on_packet_resend(make_pkt(101));
    assert!(matches!(
        evs[0],
        TradesEvent::GapFilled {
            packet_num: 101,
            ..
        }
    ));
    assert!(matches!(evs[1], TradesEvent::Applied { .. }));
    assert_eq!(evs.len(), 2);
    assert_eq!(
        s.last_packet_num(),
        103,
        "resend packets must not advance live tracking"
    );
}

#[test]
fn tick_emits_resend_after_path_delay() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010); // gap [101..104]
                                              // After 500ms with RTT 250 — PathDelay = 250 * 1.2 = 300ms → 500 > 300 → resend.
    let payloads = s.tick(250, 1500);
    assert_eq!(payloads.len(), 1, "expected one resend batch");
    // payload must contain 4 packet_nums (101, 102, 103, 104).
}

#[test]
fn tick_throttles_within_100ms() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010);
    let _ = s.tick(250, 1500);
    // Immediately after — the 100ms throttle is still active.
    let payloads = s.tick(250, 1550);
    assert!(payloads.is_empty());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.CheckMissingTradesPackets
fn tick_updates_last_check_even_without_buckets() {
    let mut s = TradesState::new();

    let payloads = s.tick(250, 1000);

    assert!(payloads.is_empty());
    assert_eq!(
            s.last_check_missing_ms, 1000,
            "Delphi caller writes LastCheckMissingTime after CheckMissingTradesPackets even when UsedBuckets=0"
        );
}

#[test]
fn bucket_closes_after_max_retries() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010);
    // After the third resend the bucket waits one more PathDelay for a reply and only then closes.
    for i in 0..MAX_RETRY_COUNT as i64 {
        let _ = s.tick(250, 1500 + i * 5000);
    }
    assert_eq!(
        s.used_buckets(),
        1,
        "bucket must not close at the same moment it exhausts its retry budget"
    );

    let _ = s.tick(250, 16500);
    assert_eq!(s.used_buckets(), 0);
}

#[test]
fn iter_resend_response_simple() {
    // count=2, 2 packets of 3 bytes each.
    let payload: Vec<u8> = vec![
        2, // count
        3, 0, // sz=3
        0xAA, 0xBB, 0xCC, 3, 0, 0x11, 0x22, 0x33,
    ];
    let packets: Vec<&[u8]> = iter_trades_resend_response(&payload).collect();
    assert_eq!(packets.len(), 2);
    assert_eq!(packets[0], &[0xAA, 0xBB, 0xCC][..]);
    assert_eq!(packets[1], &[0x11, 0x22, 0x33][..]);
}

#[test]
fn iter_resend_response_is_zero_copy() {
    let payload: Vec<u8> = vec![2, 3, 0, 0xAA, 0xBB, 0xCC, 3, 0, 0x11, 0x22, 0x33];
    let packets: Vec<&[u8]> = iter_trades_resend_response(&payload).collect();
    assert_eq!(packets, vec![&payload[3..6], &payload[8..11]]);
}

#[test]
fn iter_resend_response_truncated() {
    // count=2, but the second packet does not fit.
    let payload: Vec<u8> = vec![2, 3, 0, 0xAA, 0xBB, 0xCC, 5, 0, 0x11];
    let packets: Vec<&[u8]> = iter_trades_resend_response(&payload).collect();
    assert_eq!(packets.len(), 1);
}

#[test]
fn consecutive_gaps_extend_existing_bucket() {
    // Scenario: packets 100, [gap 101..104], 105 (sequential!), [gap 106..109], 110.
    // We must end up with ONE extended bucket [101..109], not two.
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010); // gap [101..104] → bucket1
    assert_eq!(s.used_buckets(), 1);
    let _ = s.on_packet(make_pkt(110), 1020); // gap [106..109] → extend bucket1 to [101..109]
                                              // The bucket must extend, not create a second one.
    assert_eq!(s.used_buckets(), 1, "extend must reuse the existing bucket");
    // Find the bucket and check that end_num = 109 and Recvd[4] (= packet 105) = true.
    let bucket = s.buckets.iter().find(|b| b.active).unwrap();
    assert_eq!(bucket.start_num, 101);
    assert_eq!(bucket.end_num, 109);
    assert!(
        bucket.recvd[4],
        "packet 105 (sequential between the gaps) must be marked as received"
    );
    // Resend requests will only ask for [101..104, 106..109] (8 packets).
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.FindBucketForPacket
fn extending_bucket_refunds_one_retry_once() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010);
    let _ = s.tick(250, 1500);

    {
        let bucket = s.buckets.iter().find(|b| b.active).unwrap();
        assert_eq!(bucket.retry_count, 1);
        assert!(!bucket.refund_used);
    }

    let _ = s.on_packet(make_pkt(110), 1600);
    {
        let bucket = s.buckets.iter().find(|b| b.active).unwrap();
        assert_eq!(bucket.start_num, 101);
        assert_eq!(bucket.end_num, 109);
        assert_eq!(bucket.retry_count, 0);
        assert!(bucket.refund_used);
        assert_eq!(
            bucket.last_retry_ms, 1500,
            "Delphi refund does not move LastRetryTime"
        );
    }

    let _ = s.tick(250, 2500);
    let _ = s.on_packet(make_pkt(115), 2600);
    let bucket = s.buckets.iter().find(|b| b.active).unwrap();
    assert_eq!(
        bucket.retry_count, 1,
        "second extend must not refund the same bucket again"
    );
    assert_eq!(bucket.end_num, 114);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.FindBucketForPacket
fn bucket_with_retry_count_two_is_not_extended() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010);
    let _ = s.tick(250, 1500);
    let _ = s.tick(250, 2500);
    assert_eq!(s.buckets.iter().find(|b| b.active).unwrap().retry_count, 2);

    let _ = s.on_packet(make_pkt(110), 2600);

    assert_eq!(
        s.used_buckets(),
        2,
        "RetryCount >= 2 forbids extend; the new gap becomes a fresh bucket"
    );
    assert!(s
        .buckets
        .iter()
        .any(|b| b.active && b.start_num == 101 && b.end_num == 104));
    assert!(s
        .buckets
        .iter()
        .any(|b| b.active && b.start_num == 106 && b.end_num == 109));
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.ProcessTradesStream
fn overflow_gap_resets_buckets_but_applies_packet() {
    // If the gap exceeds MAX_RECVD_SIZE, Delphi resets the buckets but does not
    // discard the current packet.
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(0), 1000);
    let _ = s.on_packet(make_pkt(2900), 1010); // bucket [1..2899]
    assert_eq!(s.used_buckets(), 1);

    // Now a new gap [2901..N] larger than MAX_RECVD_SIZE → reset + Apply.
    let evs = s.on_packet(make_pkt(7000), 1020);
    assert_eq!(s.used_buckets(), 0);
    assert!(evs.iter().any(|e| matches!(
        e,
        TradesEvent::Applied {
            packet_num: 7000,
            ..
        }
    )));
    assert!(!evs
        .iter()
        .any(|e| matches!(e, TradesEvent::GapDetected { .. })));

    // The next packet restarts tracking from scratch, because the reset left
    // trades_started=false as in Delphi ResetGapBuckets.
    let evs = s.on_packet(make_pkt(7001), 1030);
    assert!(evs.iter().any(|e| matches!(
        e,
        TradesEvent::Applied {
            packet_num: 7001,
            ..
        }
    )));
    assert_eq!(s.last_packet_num(), 7001);
}

#[test]
fn max_sized_gap_is_accepted() {
    // gap_size = packet_num - last - 1 (missing range [last+1 .. packet_num-1]).
    // If gap_size == MAX_RECVD_SIZE — the bucket must be created without overflow.
    let mut s = TradesState::new();
    let first = 100u16;
    let next = first.wrapping_add(MAX_RECVD_SIZE as u16 + 1);
    let _ = s.on_packet(make_pkt(first), 1000);

    let evs = s.on_packet(make_pkt(next), 1010);

    assert!(
        evs.iter()
            .any(|e| matches!(e, TradesEvent::GapDetected { start, end }
                if *start == first.wrapping_add(1) && *end == next.wrapping_sub(1))),
        "gap with exactly MAX_RECVD_SIZE missing packets must create a bucket"
    );
    assert_eq!(s.used_buckets(), 1);
}

#[test]
fn pause_resets_buckets() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010); // creates bucket
    assert_eq!(s.used_buckets(), 1);
    // After 31 s — pause.
    let evs = s.on_packet(make_pkt(200), 1000 + 31_000);
    assert_eq!(s.used_buckets(), 0); // reset
    assert!(evs.iter().any(|e| matches!(e, TradesEvent::Applied { .. })));
    assert_eq!(s.last_packet_num(), 200);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.CheckMissingTradesPackets
fn tick_lazily_shrinks_oversized_inactive_recvd_after_30min() {
    // Delphi MoonProtoEngine.pas:1566-1573: every 30 minutes it shrinks `recvd` for
    // inactive buckets that grew above DEFAULT on a one-off large gap, reclaiming memory.
    // Rust grew recvd up to gap_size and never shrank it without this.
    let mut s = TradesState::new();
    // Inactive bucket whose recvd grew above DEFAULT (leftover from a one-off large gap).
    s.buckets[0].active = false;
    s.buckets[0].recvd = vec![false; 1500];
    // Active bucket: tick shrinks only when used_buckets > 0 (like Delphi
    // `If UsedBuckets = 0 then exit`).
    s.buckets[1].active = true;
    s.buckets[1].start_num = 10;
    s.buckets[1].end_num = 11;
    s.used_buckets = 1;
    s.last_large_recvd_ms = 1_000;

    // Earlier than 30 minutes since the last large growth — leave it alone.
    let _ = s.tick(250, 1_000 + 500);
    assert_eq!(
        s.buckets[0].recvd.len(),
        1500,
        "before 30 minutes the oversized recvd is not shrunk"
    );

    // After 30 minutes — the inactive oversized recvd is shrunk to DEFAULT, the active one untouched.
    let _ = s.tick(250, 1_000 + 30 * 60 * 1000 + 1);
    assert_eq!(
        s.buckets[0].recvd.len(),
        DEFAULT_RECVD_SIZE,
        "inactive recvd > DEFAULT shrunk to DEFAULT every 30 min (Delphi LastLargeRecvdTime)"
    );
    assert!(
        s.buckets[1].active,
        "shrink does not close the active bucket"
    );
}
