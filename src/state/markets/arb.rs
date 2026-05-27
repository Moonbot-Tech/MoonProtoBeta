//! Arbitrage compact relay apply path for live `Market` objects.
//!
//! Delphi `ParseArbPayloadCompact` resolves server `mIndex` to `TMarket` and
//! writes `TMarket.ArbSlots` / `TMarket.ArbNow`. The compact packet shape
//! remains available for diagnostics, but the Active Lib state lives on market
//! handles.

use crate::commands::arb::{ArbPayload, ArbPriceItem};
use crate::commands::market::{Market, MarketArbPricePoint, ARB_PRICE_RING_LEN};

use super::MarketsState;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ArbApplySummary {
    pub applied_prices: usize,
    pub applied_isolation_entries: usize,
}

impl MarketsState {
    pub(crate) fn apply_arb_payload_like_delphi(
        &mut self,
        payload: &ArbPayload,
        wanted_platforms: Option<&[bool; 256]>,
        now_time_days: f64,
    ) -> ArbApplySummary {
        let mut summary = ArbApplySummary::default();
        let Some(wanted_platforms) = wanted_platforms else {
            return summary;
        };

        match payload {
            ArbPayload::Price { blocks, .. } => {
                for block in blocks {
                    let Some(local_idx) = self.local_pos_for_server_index(block.market_index)
                    else {
                        continue;
                    };
                    let my_price = self
                        .prices
                        .get(local_idx)
                        .map(|price| price.p_last as f32)
                        .unwrap_or_default();
                    let Some(handle) = self.markets.get(local_idx).cloned() else {
                        continue;
                    };
                    handle.with_mut(|market| {
                        for item in &block.prices {
                            if apply_arb_price_like_delphi(
                                market,
                                item,
                                wanted_platforms,
                                now_time_days,
                                my_price,
                            ) {
                                summary.applied_prices += 1;
                            }
                        }
                    });
                }
            }
            ArbPayload::Isolation { entries, .. } => {
                for entry in entries {
                    let Some(handle) = self.market_by_index(entry.market_index) else {
                        continue;
                    };
                    handle.with_mut(|market| {
                        if market.arb_slots.is_empty() {
                            return;
                        }
                        market
                            .arb_slots
                            .entry(entry.platform_code)
                            .or_default()
                            .isolated_flags_tmp = entry.flags;
                        summary.applied_isolation_entries += 1;
                    });
                }
                self.arb_isol_commit_like_delphi();
            }
        }
        summary
    }

    fn arb_isol_commit_like_delphi(&mut self) {
        for handle in self.markets.iter() {
            handle.with_mut(|market| {
                for slot in market.arb_slots.values_mut() {
                    slot.isolated_flags = slot.isolated_flags_tmp;
                    slot.isolated_flags_tmp = 0;
                }
            });
        }
    }
}

fn apply_arb_price_like_delphi(
    market: &mut Market,
    item: &ArbPriceItem,
    wanted_platforms: &[bool; 256],
    now_time_days: f64,
    my_price: f32,
) -> bool {
    if !wanted_platforms[item.platform_code as usize] {
        return false;
    }

    let slot = market.arb_slots.entry(item.platform_code).or_default();
    slot.enabled = true;
    let head = ((slot.head as usize + 1) % ARB_PRICE_RING_LEN) as u8;
    slot.ring[head as usize] = MarketArbPricePoint {
        price: item.price,
        time: now_time_days,
        my_price,
    };
    slot.head = head;
    slot.now.price = item.price;
    slot.now.time = now_time_days;
    true
}
