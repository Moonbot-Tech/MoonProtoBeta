
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
fn duplicate_refreshes_pause_timer_like_delphi() {
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
fn late_resend_outside_bucket_is_still_applied_like_delphi() {
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
                                              // Через 500мс с RTT 250 — PathDelay = 250 * 1.2 = 300мс → 500 > 300 → resend.
    let payloads = s.tick(250, 1500);
    assert_eq!(payloads.len(), 1, "должен быть один батч resend");
    // payload должен содержать 4 packet_nums (101, 102, 103, 104).
}

#[test]
fn tick_throttles_within_100ms() {
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010);
    let _ = s.tick(250, 1500);
    // Сразу же — throttle 100мс ещё активен.
    let payloads = s.tick(250, 1550);
    assert!(payloads.is_empty());
}

#[test]
fn tick_updates_last_check_even_without_buckets_like_delphi_caller() {
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
    // После третьего resend bucket ждёт ещё PathDelay на ответ и только потом закрывается.
    for i in 0..MAX_RETRY_COUNT as i64 {
        let _ = s.tick(250, 1500 + i * 5000);
    }
    assert_eq!(
        s.used_buckets(),
        1,
        "bucket не должен закрываться в тот же момент, когда исчерпал retry budget"
    );

    let _ = s.tick(250, 16500);
    assert_eq!(s.used_buckets(), 0);
}

#[test]
fn parse_resend_response_simple() {
    // count=2, 2 пакета по 3 байта.
    let payload: Vec<u8> = vec![
        2, // count
        3, 0, // sz=3
        0xAA, 0xBB, 0xCC, 3, 0, 0x11, 0x22, 0x33,
    ];
    let packets = parse_trades_resend_response(&payload);
    assert_eq!(packets.len(), 2);
    assert_eq!(packets[0], vec![0xAA, 0xBB, 0xCC]);
    assert_eq!(packets[1], vec![0x11, 0x22, 0x33]);
}

#[test]
fn iter_resend_response_is_zero_copy_and_matches_owned_parser() {
    let payload: Vec<u8> = vec![2, 3, 0, 0xAA, 0xBB, 0xCC, 3, 0, 0x11, 0x22, 0x33];
    let packets: Vec<&[u8]> = iter_trades_resend_response(&payload).collect();
    assert_eq!(packets, vec![&payload[3..6], &payload[8..11]]);
    assert_eq!(
        parse_trades_resend_response(&payload),
        packets
            .iter()
            .map(|packet| packet.to_vec())
            .collect::<Vec<_>>()
    );
}

#[test]
fn parse_resend_response_truncated() {
    // count=2, но второй пакет не помещается.
    let payload: Vec<u8> = vec![2, 3, 0, 0xAA, 0xBB, 0xCC, 5, 0, 0x11];
    let packets = parse_trades_resend_response(&payload);
    assert_eq!(packets.len(), 1);
}

#[test]
fn consecutive_gaps_extend_existing_bucket() {
    // Сценарий: пакеты 100, [gap 101..104], 105 (sequential!), [gap 106..109], 110.
    // Должны получить ОДИН расширенный bucket [101..109], а не два.
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(100), 1000);
    let _ = s.on_packet(make_pkt(105), 1010); // gap [101..104] → bucket1
    assert_eq!(s.used_buckets(), 1);
    let _ = s.on_packet(make_pkt(110), 1020); // gap [106..109] → extend bucket1 до [101..109]
                                              // Bucket должен расшириться, а не создать второй.
    assert_eq!(
        s.used_buckets(),
        1,
        "extend должен переиспользовать существующий bucket"
    );
    // Найдём bucket и проверим что end_num = 109, и Recvd[4] (= packet 105) = true.
    let bucket = s.buckets.iter().find(|b| b.active).unwrap();
    assert_eq!(bucket.start_num, 101);
    assert_eq!(bucket.end_num, 109);
    assert!(
        bucket.recvd[4],
        "packet 105 (sequential между gap'ами) должен быть помечен как received"
    );
    // Запросы resend пойдут только за [101..104, 106..109] (8 packets).
}

#[test]
fn extending_bucket_refunds_one_retry_once_like_delphi() {
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
fn bucket_with_retry_count_two_is_not_extended_like_delphi() {
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
fn overflow_gap_resets_buckets_but_applies_packet_like_delphi() {
    // Если gap превышает MAX_RECVD_SIZE, Delphi сбрасывает buckets, но не
    // выбрасывает текущий пакет.
    let mut s = TradesState::new();
    let _ = s.on_packet(make_pkt(0), 1000);
    let _ = s.on_packet(make_pkt(2900), 1010); // bucket [1..2899]
    assert_eq!(s.used_buckets(), 1);

    // Теперь новый gap [2901..N] больше MAX_RECVD_SIZE → reset + Apply.
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

    // Следующий пакет стартует tracking заново, потому что reset оставил
    // trades_started=false как в Delphi ResetGapBuckets.
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
    // Если gap_size == MAX_RECVD_SIZE — bucket должен создаться без overflow.
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
    // Через 31 сек — пауза.
    let evs = s.on_packet(make_pkt(200), 1000 + 31_000);
    assert_eq!(s.used_buckets(), 0); // reset
    assert!(evs.iter().any(|e| matches!(e, TradesEvent::Applied { .. })));
    assert_eq!(s.last_packet_num(), 200);
}
