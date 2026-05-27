use super::*;

fn make_item(name: &str, init_bal: f64) -> BalanceItem {
    BalanceItem {
        market_name: name.to_string(),
        balance_hash: 0,
        initial_balance: init_bal,
        leverage_x: 1,
        ..Default::default()
    }
}

fn upd(cmd_id: u8, epoch: u16, items: Vec<BalanceItem>) -> BalanceUpdate {
    BalanceUpdate {
        cmd_id,
        epoch,
        global_changed: false,
        btc_balance_total: 1.0,
        btc_balance_locked: 0.5,
        btc_balance_full: 0.5,
        special_coin_balance: 0.0,
        items,
    }
}

#[test]
fn full_snapshot_resets_missing_markets() {
    let mut s = BalancesState::new();
    s.apply(upd(
        3,
        1,
        vec![make_item("BTCUSDT", 100.0), make_item("ETHUSDT", 50.0)],
    ));
    assert_eq!(s.len(), 2);
    // Новый snapshot — только BTC. В Delphi ETH market remains but balance
    // fields reset to default.
    s.apply(upd(3, 2, vec![make_item("BTCUSDT", 200.0)]));
    assert_eq!(s.len(), 2);
    assert!(s.get("BTCUSDT").is_some());
    assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 0.0);
    assert_eq!(s.get("ETHUSDT").unwrap().leverage_x, 1);
}

#[test]
fn incremental_merges() {
    let mut s = BalancesState::new();
    s.apply(upd(3, 1, vec![make_item("BTCUSDT", 100.0)]));
    // Incremental добавляет ETH без удаления BTC.
    s.apply(upd(4, 2, vec![make_item("ETHUSDT", 50.0)]));
    assert_eq!(s.len(), 2);
    assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 100.0);
    assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 50.0);
}

#[test]
fn exact_balance_command_is_ignored_like_delphi() {
    let mut s = BalancesState::new();
    s.apply(upd(
        3,
        1,
        vec![make_item("BTCUSDT", 100.0), make_item("ETHUSDT", 50.0)],
    ));
    let mut exact_base = upd(2, 2, vec![make_item("BTCUSDT", 200.0)]);
    exact_base.btc_balance_total = 99.0;

    let ev = s.apply(exact_base);

    assert!(matches!(
        ev,
        BalanceEvent::Ignored {
            cmd_id: 2,
            epoch: 2
        }
    ));
    assert_eq!(s.len(), 2);
    assert_eq!(s.global.btc_balance_total, 1.0);
    assert_eq!(s.last_epoch, 1);
    assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 100.0);
    assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 50.0);
}

#[test]
fn full_snapshot_does_not_use_global_epoch_gate() {
    let mut s = BalancesState::new();
    s.apply(upd(3, 50, vec![make_item("BTCUSDT", 100.0)]));
    let ev = s.apply(upd(3, 45, vec![make_item("BTCUSDT", 200.0)]));
    assert!(matches!(ev, BalanceEvent::SnapshotApplied { .. }));
    assert_eq!(s.last_epoch, 45);
    assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 200.0);
}

#[test]
fn epoch_wrap_accepted() {
    let mut s = BalancesState::new();
    s.apply(upd(3, 65500, vec![]));
    // 65500 → 100: backDist = 65500-100 = 65400 > 100 → accept.
    let ev = s.apply(upd(3, 100, vec![]));
    assert!(matches!(ev, BalanceEvent::SnapshotApplied { .. }));
}

#[test]
fn incremental_global_gated() {
    let mut s = BalancesState::new();
    // First snapshot устанавливает globals.
    s.apply(upd(3, 1, vec![]));
    let initial_btc = s.global.btc_balance_total;

    // Incremental с global_changed=false — globals остаются прежними.
    let mut u = upd(4, 2, vec![]);
    u.btc_balance_total = 999.0; // не применится
    u.global_changed = false;
    s.apply(u);
    assert_eq!(s.global.btc_balance_total, initial_btc);

    // Incremental с global_changed=true — применяется.
    let mut u = upd(4, 3, vec![]);
    u.btc_balance_total = 999.0;
    u.global_changed = true;
    s.apply(u);
    assert_eq!(s.global.btc_balance_total, 999.0);
}

#[test]
fn incremental_epoch_is_checked_per_market() {
    let mut s = BalancesState::new();
    s.apply(upd(4, 10, vec![make_item("BTCUSDT", 100.0)]));
    s.apply(upd(4, 20, vec![make_item("ETHUSDT", 200.0)]));

    let ev = s.apply(upd(
        4,
        15,
        vec![make_item("BTCUSDT", 150.0), make_item("ETHUSDT", 250.0)],
    ));

    assert!(matches!(
        ev,
        BalanceEvent::IncrementalApplied { count: 1, .. }
    ));
    assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 150.0);
    assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 200.0);
}

#[test]
fn incremental_for_new_market_not_rejected_by_other_market_epoch() {
    let mut s = BalancesState::new();
    s.apply(upd(4, 100, vec![make_item("BTCUSDT", 100.0)]));

    let ev = s.apply(upd(4, 90, vec![make_item("ETHUSDT", 90.0)]));

    assert!(matches!(
        ev,
        BalanceEvent::IncrementalApplied { count: 1, .. }
    ));
    assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 90.0);
}

#[test]
fn filtered_apply_ignores_unknown_markets_like_delphi() {
    let mut s = BalancesState::new();

    let ev = s.apply_filtered(
        upd(
            3,
            1,
            vec![make_item("BTCUSDT", 100.0), make_item("UNKNOWNUSDT", 200.0)],
        ),
        |name| name == "BTCUSDT",
    );

    assert!(matches!(
        ev,
        BalanceEvent::SnapshotApplied { count: 1, epoch: 1 }
    ));
    assert!(s.get("BTCUSDT").is_some());
    assert!(s.get("UNKNOWNUSDT").is_none());

    let ev = s.apply_filtered(
        upd(
            4,
            2,
            vec![make_item("ETHUSDT", 300.0), make_item("UNKNOWNUSDT", 400.0)],
        ),
        |name| name == "BTCUSDT" || name == "ETHUSDT",
    );

    assert!(matches!(
        ev,
        BalanceEvent::IncrementalApplied {
            count: 1,
            epoch: 2,
            ..
        }
    ));
    assert!(s.get("ETHUSDT").is_some());
    assert!(s.get("UNKNOWNUSDT").is_none());
}

#[test]
fn full_snapshot_creates_default_for_known_market_without_previous_balance_like_delphi() {
    let mut s = BalancesState::new();
    let known = HashMap::from([("BTCUSDT".to_string(), 0), ("ETHUSDT".to_string(), 1)]);

    let ev = s.apply_with_known_markets(
        upd(3, 10, vec![make_item("BTCUSDT", 100.0)]),
        &known,
        |name| name == "BTCUSDT",
    );

    assert!(matches!(
        ev,
        BalanceEvent::SnapshotApplied {
            count: 1,
            epoch: 10
        }
    ));
    assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 100.0);
    let eth = s
        .get("ETHUSDT")
        .expect("Delphi resets every known market missing from full snapshot");
    assert_eq!(eth.initial_balance, 0.0);
    assert_eq!(eth.leverage_x, 1);
    assert_eq!(eth.position_type, 0);

    let stale = s.apply_with_known_markets(
        upd(4, 0, vec![make_item("ETHUSDT", 55.0)]),
        &known,
        |name| name == "BTCUSDT",
    );
    assert!(matches!(
        stale,
        BalanceEvent::IncrementalApplied { count: 0, .. }
    ));
    assert_eq!(
        s.get("ETHUSDT").unwrap().initial_balance,
        0.0,
        "new default row has Delphi LastBalanceEpoch=0, so duplicate epoch 0 is stale"
    );
}

#[test]
fn recalc_total_pnl_matches_delphi_btc_market_sum() {
    let mut s = BalancesState::new();
    let known = HashMap::from([
        ("BTCUSDT".to_string(), 0),
        ("ETHBTC".to_string(), 1),
        ("ETHUSDT".to_string(), 2),
    ]);
    let mut btc = make_item("BTCUSDT", 0.0);
    btc.total_profit_b = 1.0;
    btc.total_profit_l = 2.0;
    btc.total_profit_s = 3.0;
    let mut eth_btc = make_item("ETHBTC", 0.0);
    eth_btc.total_profit_b = -0.5;
    eth_btc.total_profit_l = 0.25;
    eth_btc.total_profit_s = 1.25;
    let mut eth_usdt = make_item("ETHUSDT", 0.0);
    eth_usdt.total_profit_b = 100.0;

    s.apply_with_known_markets(upd(3, 10, vec![btc, eth_btc, eth_usdt]), &known, |name| {
        name == "BTCUSDT" || name == "ETHBTC"
    });

    assert_eq!(s.get("BTCUSDT").unwrap().total_profit(), 6.0);
    assert_eq!(s.get("ETHBTC").unwrap().total_profit(), 1.0);
    assert_eq!(s.global.total_pnl, 7.0);

    let mut btc_inc = make_item("BTCUSDT", 0.0);
    btc_inc.total_profit_s = 10.0;
    s.apply_with_known_markets(upd(4, 11, vec![btc_inc]), &known, |name| {
        name == "BTCUSDT" || name == "ETHBTC"
    });

    assert_eq!(s.global.total_pnl, 11.0);
}

#[test]
fn incremental_accepts_more_than_former_rust_balance_cap() {
    const FORMER_MAX_BALANCE_MARKETS: usize = 20_000;
    let mut s = BalancesState::new();
    for idx in 0..=FORMER_MAX_BALANCE_MARKETS {
        let name = format!("M{idx}USDT");
        s.apply(upd(4, idx as u16, vec![make_item(&name, idx as f64)]));
    }

    assert_eq!(s.len(), FORMER_MAX_BALANCE_MARKETS + 1);
    assert!(s.get("M20000USDT").is_some());
}

#[test]
fn max_value_zero_preserves_previous_like_delphi() {
    let mut s = BalancesState::new();
    let mut first = make_item("BTCUSDT", 100.0);
    first.max_value = 500.0;
    s.apply(upd(3, 1, vec![first]));

    let second = make_item("BTCUSDT", 200.0);
    s.apply(upd(4, 2, vec![second]));

    let item = s.get("BTCUSDT").unwrap();
    assert_eq!(item.initial_balance, 200.0);
    assert_eq!(item.max_value, 500.0);
}

#[test]
fn max_value_positive_updates_previous() {
    let mut s = BalancesState::new();
    let mut first = make_item("BTCUSDT", 100.0);
    first.max_value = 500.0;
    s.apply(upd(3, 1, vec![first]));

    let mut second = make_item("BTCUSDT", 200.0);
    second.max_value = 600.0;
    s.apply(upd(4, 2, vec![second]));

    assert_eq!(s.get("BTCUSDT").unwrap().max_value, 600.0);
}

#[test]
fn full_snapshot_missing_market_preserves_hash_max_and_epoch_like_delphi() {
    let mut s = BalancesState::new();
    let mut eth = make_item("ETHUSDT", 50.0);
    eth.balance_hash = 77;
    eth.max_value = 500.0;
    s.apply(upd(3, 50, vec![eth]));

    s.apply_filtered(upd(3, 100, vec![make_item("BTCUSDT", 1.0)]), |name| {
        name == "BTCUSDT" || name == "ETHUSDT"
    });

    let eth_after_full = s.get("ETHUSDT").unwrap();
    assert_eq!(eth_after_full.initial_balance, 0.0);
    assert_eq!(eth_after_full.balance_hash, 77);
    assert_eq!(eth_after_full.max_value, 500.0);
    assert_eq!(eth_after_full.leverage_x, 1);

    // Delphi does not update LastBalanceEpoch in the missing-market reset
    // branch, so a stale incremental is still rejected against epoch 50.
    let ev = s.apply(upd(4, 45, vec![make_item("ETHUSDT", 90.0)]));
    assert!(matches!(
        ev,
        BalanceEvent::IncrementalApplied { count: 0, .. }
    ));
    assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 0.0);

    // A fresh incremental with bnMaxValue=0 preserves the previous
    // bnMaxValue, matching `If item.bnMaxValue > _eps then ...`.
    s.apply(upd(4, 60, vec![make_item("ETHUSDT", 90.0)]));
    let eth_after_incremental = s.get("ETHUSDT").unwrap();
    assert_eq!(eth_after_incremental.initial_balance, 90.0);
    assert_eq!(eth_after_incremental.max_value, 500.0);
}
