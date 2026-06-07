use super::*;
use crate::commands::order_book::OrderLevel;

fn level(rate: f32, quantity: f32) -> OrderLevel {
    OrderLevel { rate, quantity }
}

fn make_pkt(market_idx: u16, book_kind: u8, seq: u16, is_full: bool) -> OrderBookUpdate {
    OrderBookUpdate {
        market_index: market_idx,
        seq,
        is_full,
        book_kind,
        buys: vec![level(100.0, 1.0)],
        sells: vec![level(101.0, 2.0)],
    }
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:ProcessOrderBookPacket (Unit1.pas eps table)
fn eps_profile_changes_diff_quantity_filter() {
    let small_qty = 0.000000005_f32;
    let full = OrderBookUpdate {
        market_index: 30,
        seq: 1,
        is_full: true,
        book_kind: 0,
        buys: vec![],
        sells: vec![],
    };
    let diff = OrderBookUpdate {
        market_index: 30,
        seq: 2,
        is_full: false,
        book_kind: 0,
        buys: vec![level(100.0, small_qty)],
        sells: vec![],
    };

    let mut huobi = OrderBooks::new();
    huobi.set_eps_profile(EpsProfile::HUOBI);
    let _ = huobi.on_packet(full.clone(), 0);
    let _ = huobi.on_packet(diff.clone(), 1);
    assert_eq!(
        huobi.book_by_kind(30, 0).unwrap().buys.len(),
        1,
        "Huobi-class Delphi _eps=1e-12 keeps this non-zero level"
    );

    let mut binance = OrderBooks::new();
    binance.set_eps_profile(EpsProfile::BINANCE);
    let _ = binance.on_packet(full, 0);
    let _ = binance.on_packet(diff, 1);
    assert!(
        binance.book_by_kind(30, 0).unwrap().buys.is_empty(),
        "Binance-class Delphi _eps=1e-8 filters the same tiny quantity"
    );
}

#[test]
fn full_then_inorder_diffs() {
    let mut ob = OrderBooks::new();
    let events = ob.on_packet(make_pkt(1, 0, 10, true), 1000);
    assert!(matches!(
        events[0],
        OrderBookEvent::Apply {
            is_full: true,
            seq: 10,
            ..
        }
    ));

    let events = ob.on_packet(make_pkt(1, 0, 11, false), 1010);
    assert!(matches!(
        events[0],
        OrderBookEvent::Apply {
            is_full: false,
            seq: 11,
            ..
        }
    ));

    let events = ob.on_packet(make_pkt(1, 0, 12, false), 1020);
    assert!(matches!(
        events[0],
        OrderBookEvent::Apply {
            is_full: false,
            seq: 12,
            ..
        }
    ));
}

#[test]
fn snapshot_handle_mutating_one_book_does_not_deep_clone_books_domain() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(1, 0, 10, true), 1000);
    let _ = ob.on_packet(make_pkt(2, 0, 10, true), 1000);

    let snapshot = ob.clone();
    let root_before = ob.arc_ptr();
    let book_before = ob.book_slot_ptr((1, 0)).unwrap();
    let other_before = ob.book_slot_ptr((2, 0)).unwrap();

    let _ = ob.on_packet(make_pkt(1, 0, 11, false), 1010);

    assert_eq!(
        ob.arc_ptr(),
        root_before,
        "orderbook packet must not copy-on-write detach the whole domain while a snapshot is held"
    );
    assert_eq!(snapshot.arc_ptr(), root_before);
    assert_eq!(
        ob.book_slot_ptr((1, 0)),
        Some(book_before),
        "mutating a book updates the shared per-book slot, not a cloned HashMap"
    );
    assert_eq!(ob.book_slot_ptr((2, 0)), Some(other_before));
    assert_eq!(
        snapshot.book_by_kind(1, 0).unwrap().seq,
        11,
        "held snapshot handle sees the same live book object, matching Delphi shared-state semantics"
    );
}

#[test]
fn reset_caches_keep_books_matches_delphi_reset_orderbook_caches() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(1, 0, 10, true), 1000);
    let _ = ob.on_packet(make_pkt(1, 0, 11, false), 1010);

    assert!(ob.book_by_kind(1, 0).is_some());
    assert_eq!(ob.len(), 1);

    ob.reset_caches_keep_books();

    assert!(
        ob.book_by_kind(1, 0).is_some(),
        "Delphi ResetOrderBookCaches resets seq/cache, not visible book levels"
    );
    assert_eq!(ob.len(), 0);

    let events = ob.on_packet(make_pkt(1, 0, 50, false), 2000);
    assert!(
        events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Apply {
                is_full: false,
                seq: 50,
                ..
            }
        )),
        "after seq reset, the next diff is accepted as the new first diff"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:ProcessOrderBookPacket
fn first_diff_without_full_is_applied() {
    // Delphi MoonProtoEngine.pas:2066-2071:
    // If `last_applied_seq = 0` (nothing applied yet) — apply the first Diff
    // without waiting for a Full. Previously we dropped it + requested a Full.
    let mut ob = OrderBooks::new();
    let events = ob.on_packet(make_pkt(2, 0, 5, false), 1000);
    assert!(
        events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Apply {
                is_full: false,
                seq: 5,
                ..
            }
        )),
        "the first Diff with last_applied_seq=0 must apply (Delphi normal-mode)"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
        "RequestFullNeeded is not needed - Delphi does not request Full in this scenario"
    );
}

#[test]
fn gap_caches_then_drains() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(3, 0, 10, true), 1000);
    // Received seq 12 — gap. Put it in the cache.
    let events = ob.on_packet(make_pkt(3, 0, 12, false), 1010);
    assert!(events.iter().any(|e| matches!(
        e,
        OrderBookEvent::Ignored {
            reason: ApplyResult::Cached,
            seq: 12,
            ..
        }
    )));
    // Received seq 11 — apply + drain seq 12.
    let events = ob.on_packet(make_pkt(3, 0, 11, false), 1020);
    let applied_seqs: Vec<u16> = events
        .iter()
        .filter_map(|e| match e {
            OrderBookEvent::Apply { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect();
    assert_eq!(applied_seqs, vec![11, 12]);
}

#[test]
fn stale_diff_rejected() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(4, 0, 20, true), 1000); // Full, expected_seq = 21
    let events = ob.on_packet(make_pkt(4, 0, 19, false), 1010); // seq 19 < 21
    assert!(events.iter().any(|e| matches!(
        e,
        OrderBookEvent::Ignored {
            reason: ApplyResult::Stale,
            ..
        }
    )));
}

#[test]
fn corrupted_throttle() {
    // Throttle RequestFullNeeded after cache.is_expired() triggers corrupted.
    let mut ob = OrderBooks::new();
    // Full + gap → cache.add, not corrupted yet.
    let _ = ob.on_packet(make_pkt(5, 0, 1, true), 10_000);
    let _ = ob.on_packet(make_pkt(5, 0, 10, false), 10_010); // cache_not_empty_since=10010
                                                             // 890ms elapsed — is_expired (>800ms) → corrupted=true → first RequestFullNeeded.
    let events = ob.on_packet(make_pkt(5, 0, 11, false), 10_900);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
        "is_expired (890>800ms) -> corrupted=true -> first RequestFullNeeded"
    );
    // After 100ms in the corrupted branch — must NOT send (throttle 5000ms).
    let events = ob.on_packet(make_pkt(5, 0, 12, false), 11_000);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
        "throttle 5000ms blocks the second RequestFullNeeded"
    );
    // After >5000ms since the first request — the throttle is released.
    let events = ob.on_packet(make_pkt(5, 0, 13, false), 16_001);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
        "after 5000ms the throttle is released"
    );
}

#[test]
fn initial_full_request_throttle_matches_delphi_zero_timestamp() {
    // Delphi `TOrderBookCache.Create` sets FLastFullRequestTime = 0, and
    // TryRequestFull still applies the same <=5000ms throttle against that
    // zero. Rust must not special-case 0 as "never requested".
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(15, 0, 1, true), 0);
    let _ = ob.on_packet(make_pkt(15, 0, 10, false), 10);

    let events = ob.on_packet(make_pkt(15, 0, 11, false), 900);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
        "0..5000ms after process start must be throttled exactly like Delphi"
    );

    let events = ob.on_packet(make_pkt(15, 0, 12, false), 5001);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
        ">5000ms releases the first RequestOrderBookFull"
    );
}

#[test]
fn corrupted_mode_applies_diffs_while_waiting_for_full() {
    // Delphi MoonProtoEngine.pas:2021-2039: in corrupted mode the client
    // applies diffs as-is for a degraded live view, rather than freezing the UI.
    let mut ob = OrderBooks::new();
    // Full + Diff in order, then gap → corrupted.
    let _ = ob.on_packet(make_pkt(6, 0, 10, true), 10_000);
    let _ = ob.on_packet(make_pkt(6, 0, 12, false), 10_010); // gap [11]
                                                             // After 890ms is_expired → corrupted.
    let events = ob.on_packet(make_pkt(6, 0, 13, false), 10_900);
    assert!(events
        .iter()
        .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })));

    // The next Diff in corrupted — must be applied (degraded view).
    let events = ob.on_packet(make_pkt(6, 0, 14, false), 10_910);
    assert!(
        events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Apply {
                is_full: false,
                seq: 14,
                ..
            }
        )),
        "corrupted mode must keep showing the degraded live view"
    );
}

#[test]
fn separate_pairs_independent() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(1, 0, 10, true), 1000); // Futures
    let _ = ob.on_packet(make_pkt(1, 1, 20, true), 1000); // Spot
                                                          // Diff for spot at seq 21 — must be applied.
    let events = ob.on_packet(make_pkt(1, 1, 21, false), 1010);
    assert!(events.iter().any(|e| matches!(
        e,
        OrderBookEvent::Apply {
            is_full: false,
            seq: 21,
            kind: OrderBookKind::Spot,
            ..
        }
    )));
    // Diff for futures at seq 11 — must be applied independently.
    let events = ob.on_packet(make_pkt(1, 0, 11, false), 1010);
    assert!(events.iter().any(|e| matches!(
        e,
        OrderBookEvent::Apply {
            is_full: false,
            seq: 11,
            kind: OrderBookKind::Futures,
            ..
        }
    )));
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:ProcessOrderBookPacket
fn book_seq_zero_overrides_stale_compare() {
    // The Delphi normal-mode condition checks `m.MoonProtoBookSeq = 0` before
    // the stale-drop. So with an initial seq=0 the packet 65535 is still
    // applied, even though CompareSeq(65535, 0) < 0.
    let mut ob = OrderBooks::new();
    let events = ob.on_packet(make_pkt(9, 0, u16::MAX, false), 1000);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::Apply { seq: u16::MAX, .. })),
        "MoonProtoBookSeq=0 must apply the Diff before the stale-check"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:ProcessOrderBookPacket
fn duplicate_gap_packets_are_cached() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(10, 0, 1, true), 1000);
    let _ = ob.on_packet(make_pkt(10, 0, 3, false), 1010);
    let _ = ob.on_packet(make_pkt(10, 0, 3, false), 1020);

    assert_eq!(
        ob.cache_packet_len((10, 0)).unwrap(),
        2,
        "TOrderBookCache.Add inserts duplicate seq packets; stale cleanup happens during drain"
    );
}

#[test]
fn normal_gap_overflow_enters_corrupted_without_clearing_cache() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(11, 0, 1, true), 10_000);

    let mut request_full_seen = false;
    for seq in 3..=67 {
        let events = ob.on_packet(make_pkt(11, 0, seq, false), 10_000 + seq as i64);
        request_full_seen |= events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. }));
    }

    assert!(
        ob.cache_corrupted((11, 0)).unwrap(),
        "Count > BOOK_CACHE_MAX_PACKETS moves the cache into corrupted"
    );
    assert!(
        request_full_seen,
        "TryRequestFull must fire when entering corrupted"
    );
    assert_eq!(
        ob.cache_packet_len((11, 0)).unwrap(),
        65,
        "Delphi normal-mode does not clear the cache on overflow; the 65th gap packet stays in the list"
    );
}

#[test]
fn corrupted_mode_drops_oldest_before_add() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(make_pkt(12, 0, 1, true), 1000);
    for seq in 3..=67 {
        let _ = ob.on_packet(make_pkt(12, 0, seq, false), 1000 + seq as i64);
    }

    let _ = ob.on_packet(make_pkt(12, 0, 68, false), 2000);
    assert_eq!(ob.cache_packet_len((12, 0)).unwrap(), 65);
    assert_eq!(
        ob.cache_front_seq((12, 0)),
        Some(4),
        "in corrupted mode Delphi DropOldest runs before adding the new diff"
    );
}

#[test]
fn full_snapshot_updates_applied_read_model() {
    let mut ob = OrderBooks::new();
    let pkt = OrderBookUpdate {
        market_index: 1,
        seq: 10,
        is_full: true,
        book_kind: 0,
        buys: vec![level(100.0, 1.5), level(99.0, 2.0)],
        sells: vec![level(101.0, 1.25), level(102.0, 3.0)],
    };
    let _ = ob.on_packet(pkt, 1000);

    let book = ob.book(1, OrderBookKind::Futures).unwrap();
    assert_eq!(book.seq, 10);
    assert_eq!(
        book.top().bid,
        Some(OrderBookLevel {
            rate: 100.0,
            quantity: 1.5
        })
    );
    assert_eq!(
        book.top().ask,
        Some(OrderBookLevel {
            rate: 101.0,
            quantity: 1.25
        })
    );
    assert_eq!(book.buys.len(), 2);
    assert_eq!(book.sells.len(), 2);
}

#[test]
fn diff_updates_inserts_and_deletes_applied_levels() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(
        OrderBookUpdate {
            market_index: 2,
            seq: 1,
            is_full: true,
            book_kind: 0,
            buys: vec![level(100.0, 1.0), level(99.0, 1.0)],
            sells: vec![level(101.0, 1.0), level(102.0, 1.0)],
        },
        1000,
    );

    let _ = ob.on_packet(
        OrderBookUpdate {
            market_index: 2,
            seq: 2,
            is_full: false,
            book_kind: 0,
            buys: vec![level(100.0, 2.0), level(98.0, 4.0)],
            sells: vec![level(101.0, 0.0), level(103.0, 3.0)],
        },
        1010,
    );

    let book = ob.book(2, OrderBookKind::Futures).unwrap();
    assert_eq!(
        book.buys,
        vec![
            OrderBookLevel {
                rate: 100.0,
                quantity: 2.0
            },
            OrderBookLevel {
                rate: 99.0,
                quantity: 1.0
            },
            OrderBookLevel {
                rate: 98.0,
                quantity: 4.0
            },
        ]
    );
    assert_eq!(
        book.sells,
        vec![
            OrderBookLevel {
                rate: 102.0,
                quantity: 1.0
            },
            OrderBookLevel {
                rate: 103.0,
                quantity: 3.0
            },
        ]
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:ProcessOrderBookPacket
fn diff_uses_opposite_side_shrink() {
    let mut ob = OrderBooks::new();
    let _ = ob.on_packet(
        OrderBookUpdate {
            market_index: 3,
            seq: 1,
            is_full: true,
            book_kind: 0,
            buys: vec![level(101.0, 1.0), level(99.0, 1.0)],
            sells: vec![level(102.0, 1.0)],
        },
        1000,
    );

    let _ = ob.on_packet(
        OrderBookUpdate {
            market_index: 3,
            seq: 2,
            is_full: false,
            book_kind: 0,
            buys: vec![level(99.5, 2.0)],
            sells: vec![level(100.0, 3.0)],
        },
        1010,
    );

    let book = ob.book(3, OrderBookKind::Futures).unwrap();
    assert_eq!(
        book.buys,
        vec![
            OrderBookLevel {
                rate: 99.5,
                quantity: 2.0
            },
            OrderBookLevel {
                rate: 99.0,
                quantity: 1.0
            },
        ]
    );
    assert_eq!(
        book.sells,
        vec![
            OrderBookLevel {
                rate: 100.0,
                quantity: 3.0
            },
            OrderBookLevel {
                rate: 102.0,
                quantity: 1.0
            },
        ]
    );
}

#[test]
fn diff_does_not_truncate_to_5000_levels_like_current_delphi() {
    let mut book: Vec<OrderBookLevel> = (0..5001)
        .map(|i| OrderBookLevel {
            rate: 10_000.0 - i as f64,
            quantity: 1.0,
        })
        .collect();
    let mut scratch = Vec::new();

    apply_order_book_diff_keep_zero(&mut book, &mut scratch, &[level(4_000.0, 2.0)], &[], true);

    assert_eq!(
        book.len(),
        5002,
        "current Delphi computes n := Min(5000, N) but SetLength/Move use N, so no cap is applied"
    );
    assert_eq!(book[0].rate, 10_000.0);
    assert_eq!(
        book.last().map(|level| level.rate),
        Some(4_000.0),
        "new diff level remains present beyond 5000 entries"
    );
}

#[test]
fn order_book_kind_roundtrip() {
    assert_eq!(OrderBookKind::Futures.as_u8(), 0);
    assert_eq!(OrderBookKind::Spot.as_u8(), 1);
    assert_eq!(OrderBookKind::from_u8(0), Some(OrderBookKind::Futures));
    assert_eq!(OrderBookKind::from_u8(1), Some(OrderBookKind::Spot));
    assert_eq!(OrderBookKind::from_u8(2), None);
    assert_eq!(OrderBookKind::from_u8(255), None);
}
