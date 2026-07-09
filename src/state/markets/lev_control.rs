use crate::commands::ui::{lev_control_wildcard_match, parse_lev_control, LevManage};

use super::MarketsState;

impl MarketsState {
    /// Apply leverage-control text to retained markets.
    ///
    /// This is the Active Lib counterpart of
    /// `MarketsTableUnit.pas:ApplyLevConfigString`: the core sends the
    /// `LevManage` snapshot, Active Lib parses its text config once, and UI code
    /// reads ready `market.max_pos_limit()` values without reparsing settings.
    pub(crate) fn apply_lev_manage_to_markets(&self, lev: &LevManage) {
        let parsed = parse_lev_control(&lev.lev_control);
        for handle in self.markets.iter() {
            handle.with_mut(|market| {
                market.max_control_lev = 0;
            });
        }

        for rule in parsed.rules {
            if rule.wildcard {
                for handle in self.markets.iter() {
                    handle.with_mut(|market| {
                        if market.is_btc_market
                            && lev_control_wildcard_match(&market.market_currency, &rule.token)
                        {
                            market.max_control_lev = rule.limit;
                        }
                    });
                }
            } else if let Some(handle) = self.markets.iter().find(|handle| {
                handle.with(|market| {
                    market.is_btc_market && market.market_currency.eq_ignore_ascii_case(&rule.token)
                })
            }) {
                handle.with_mut(|market| {
                    market.max_control_lev = rule.limit;
                });
            }
        }
    }
}
