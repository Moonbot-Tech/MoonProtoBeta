use super::*;
use crate::commands::balance::BalanceUpdate;

fn upd(cmd_id: u8, epoch: u16, global_changed: bool) -> BalanceUpdate {
    BalanceUpdate {
        cmd_id,
        epoch,
        global_changed,
        btc_balance_total: 1.0,
        btc_balance_locked: 0.5,
        btc_balance_full: 1.5,
        special_coin_balance: 42.0,
        items: Vec::new(),
    }
}

// Per-market balance apply (full snapshot, missing-reset, epoch gate, increment)
// is tested at dispatch level in `events::tests` against the live `MarketsState`
// (the single Delphi-parity store). These tests cover the account-level globals
// that `BalancesState` keeps.

#[test]
fn full_snapshot_sets_globals_and_total_pnl() {
    let mut s = BalancesState::new();
    s.apply_global_like_delphi(&upd(3, 1, false), 7.0);
    assert_eq!(s.global().btc_balance_total, 1.0);
    assert_eq!(s.global().btc_balance_locked, 0.5);
    assert_eq!(s.global().special_coin_balance, 42.0);
    assert_eq!(s.global().total_pnl, 7.0);
    assert_eq!(s.last_epoch, 1);
}

#[test]
fn incremental_sets_globals_only_when_changed_but_always_recalcs_pnl() {
    let mut s = BalancesState::new();
    s.apply_global_like_delphi(&upd(3, 1, false), 0.0); // seed globals (btc_total=1.0)

    // global_changed = false: BTC totals kept; total_pnl (recalc) still applied.
    let mut u = upd(4, 2, false);
    u.btc_balance_total = 999.0;
    s.apply_global_like_delphi(&u, 3.0);
    assert_eq!(s.global().btc_balance_total, 1.0); // unchanged
    assert_eq!(s.global().total_pnl, 3.0); // recalc always set
    assert_eq!(s.last_epoch, 2);

    // global_changed = true: BTC totals updated.
    let mut u2 = upd(4, 3, true);
    u2.btc_balance_total = 5.0;
    s.apply_global_like_delphi(&u2, 9.0);
    assert_eq!(s.global().btc_balance_total, 5.0);
    assert_eq!(s.global().total_pnl, 9.0);
}

#[test]
fn exact_balance_command_cmd2_is_ignored_like_delphi() {
    let mut s = BalancesState::new();
    s.apply_global_like_delphi(&upd(3, 1, false), 7.0);
    s.apply_global_like_delphi(&upd(2, 2, true), 999.0); // cmd 2: not applied
    assert_eq!(s.global().total_pnl, 7.0);
    assert_eq!(s.global().btc_balance_total, 1.0);
    assert_eq!(s.last_epoch, 1); // unchanged
}

#[test]
fn clear_resets_globals() {
    let mut s = BalancesState::new();
    s.apply_global_like_delphi(&upd(3, 5, false), 7.0);
    s.clear();
    assert_eq!(s.global().total_pnl, 0.0);
    assert_eq!(s.global().btc_balance_total, 0.0);
    assert_eq!(s.last_epoch, 0);
}
