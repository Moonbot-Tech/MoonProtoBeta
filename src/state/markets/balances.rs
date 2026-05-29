//! Balance/position apply path for live `Market` objects.
//!
//! Delphi does not keep liquidation/position fields in a separate user-facing
//! balance row. `TMoonProtoEngine.ApplyBalanceItem` writes them directly into
//! `TMarket`, and UI code then reads `Market.FLiqPrice`, `Market.LeverageX`,
//! and related fields from that live object.

use crate::commands::balance::{BalanceItem, BalanceUpdate};
use crate::commands::market::PositionType;
use crate::commands::trade::OrderType;
use crate::state::balances::BalanceEvent;
use crate::state::epoch::epoch_is_ok;

use super::{Market, MarketsState};

impl MarketsState {
    pub(crate) fn apply_balance_update_like_delphi(&mut self, upd: &BalanceUpdate) -> BalanceEvent {
        match upd.cmd_id {
            2 => BalanceEvent::Ignored {
                cmd_id: upd.cmd_id,
                epoch: upd.epoch,
            },
            3 => self.apply_balance_snapshot_like_delphi(upd),
            4 => self.apply_balance_increment_like_delphi(upd),
            _ => BalanceEvent::Ignored {
                cmd_id: upd.cmd_id,
                epoch: upd.epoch,
            },
        }
    }

    fn apply_balance_snapshot_like_delphi(&mut self, upd: &BalanceUpdate) -> BalanceEvent {
        use std::collections::HashSet;

        let mut seen = HashSet::with_capacity(upd.items.len());
        let mut count = 0;

        for item in &upd.items {
            let Some(handle) = self.get(&item.market_name) else {
                continue;
            };
            handle.with_mut(|market| {
                apply_balance_item_like_delphi(market, item, upd.epoch, self.eps_profile.eps);
            });
            seen.insert(item.market_name.as_str());
            count += 1;
        }

        for handle in self.markets.iter() {
            let was_seen = handle.with(|market| seen.contains(market.bn_market_name.as_str()));
            if !was_seen {
                handle.with_mut(reset_missing_balance_like_delphi);
            }
        }

        BalanceEvent::SnapshotApplied {
            count,
            epoch: upd.epoch,
        }
    }

    fn apply_balance_increment_like_delphi(&mut self, upd: &BalanceUpdate) -> BalanceEvent {
        let mut count = 0;
        for item in &upd.items {
            let Some(handle) = self.get(&item.market_name) else {
                continue;
            };
            let applied = handle.with_mut(|market| {
                if !epoch_is_ok(market.last_balance_epoch, upd.epoch) {
                    return false;
                }
                apply_balance_item_like_delphi(market, item, upd.epoch, self.eps_profile.eps);
                true
            });
            if applied {
                count += 1;
            }
        }

        BalanceEvent::IncrementalApplied {
            count,
            epoch: upd.epoch,
            global_changed: upd.global_changed,
        }
    }
}

fn apply_balance_item_like_delphi(market: &mut Market, item: &BalanceItem, epoch: u16, eps: f64) {
    market.initial_balance = item.initial_balance;
    market.locked_balance = item.locked_balance;

    market.pos_size = item.pos_size;
    market.pos_price = item.pos_price;
    market.liq_price = item.liq_price;
    market.pos_dir = item.pos_dir;

    market.long_pos_size = item.long_pos_size;
    market.long_pos_price = item.long_pos_price;
    market.long_liq_price = item.long_liq_price;
    market.long_position_type = item.long_position_type;

    market.short_pos_size = item.short_pos_size;
    market.short_pos_price = item.short_pos_price;
    market.short_liq_price = item.short_liq_price;
    market.short_position_type = item.short_position_type;

    market.asset_balance = item.asset_balance;
    market.asset_balance_full = item.asset_balance_full;

    if item.max_value > eps {
        market.bn_max_value = item.max_value;
    }

    market.total_profit_b = item.total_profit_b;
    market.total_profit_l = item.total_profit_l;
    market.total_profit_s = item.total_profit_s;

    market.leverage_x = item.leverage_x;
    market.position_type = item.position_type;

    market.balance_hash = item.balance_hash;
    market.last_balance_epoch = epoch;
}

fn reset_missing_balance_like_delphi(market: &mut Market) {
    market.initial_balance = 0.0;
    market.locked_balance = 0.0;
    market.pos_size = 0.0;
    market.pos_price = 0.0;
    market.liq_price = 0.0;
    market.pos_dir = OrderType::Sell;
    market.long_pos_size = 0.0;
    market.long_pos_price = 0.0;
    market.long_liq_price = 0.0;
    market.long_position_type = PositionType::Cross;
    market.short_pos_size = 0.0;
    market.short_pos_price = 0.0;
    market.short_liq_price = 0.0;
    market.short_position_type = PositionType::Cross;
    market.asset_balance = 0.0;
    market.asset_balance_full = 0.0;
    market.total_profit_b = 0.0;
    market.total_profit_l = 0.0;
    market.total_profit_s = 0.0;
    market.leverage_x = 1;
    market.position_type = PositionType::Cross;
}
