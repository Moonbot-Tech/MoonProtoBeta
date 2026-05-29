//! `GetMarketsList` full-list apply path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::commands::candles::current_local_time_shift_minutes;
use crate::commands::market::{
    read_corr_market, read_market_with_local_shift, EngineStreamReader, Market, MarketsListResponse,
};

use super::{MarketHandle, MarketsEvent, MarketsListApplyTiming, MarketsState};

impl MarketsState {
    /// Apply the `emk_GetMarketsList` response.
    ///
    /// Delphi does not replace the whole market universe on a repeated list
    /// response. Existing `TMarket` objects are updated through
    /// `CopyFromMarket`, old live price state is preserved, absent old markets
    /// stay in `Markets`, and CorrMarkets are add/update-only.
    pub fn apply_markets_list(&mut self, resp: MarketsListResponse) -> MarketsEvent {
        let first_create_markets = self.markets.is_empty();
        let new_market_found = self.markets_list_refresh_needed;
        let allow_new_markets = first_create_markets || new_market_found;
        let rebuild_server_indexes = allow_new_markets || !self.indexes_synchronized;
        self.new_markets_pending_price_refresh = 0;
        self.new_markets_added.clear();
        let incoming_count = resp.markets.len();
        let corr_count = resp.corr_markets.len();
        let incoming_server_names = if rebuild_server_indexes {
            resp.markets
                .iter()
                .map(|m| m.bn_market_name.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let old_markets = std::mem::take(&mut self.markets);
        let incoming_by_name = resp
            .markets
            .iter()
            .enumerate()
            .map(|(idx, m)| (m.bn_market_name.clone(), idx))
            .collect::<HashMap<_, _>>();
        let mut consumed = HashMap::with_capacity(resp.markets.len());

        let mut markets = Vec::with_capacity(old_markets.len().max(incoming_count));
        let mut lookup_entries = Vec::with_capacity(old_markets.len().max(incoming_count));

        for handle in old_markets.iter().cloned() {
            let old_name = handle.name_str().to_string();
            if let Some(&incoming_idx) = incoming_by_name.get(&old_name) {
                let incoming = &resp.markets[incoming_idx];
                handle.with_mut(|market| {
                    merge_market_like_delphi_get_markets_list(
                        market,
                        incoming,
                        self.copy_max_leverage_from_markets_list,
                        self.eps_profile.eps,
                    );
                    // Live price (bid/ask/last/mark) is kept on the `Market` itself;
                    // the merge updates only funding_time.
                    market.price.funding_time = market.funding_time;
                    consumed.insert(market.bn_market_name.clone(), true);
                });
            }
            lookup_entries.push((old_name, handle.clone()));
            markets.push(handle);
        }

        for market in resp.markets {
            if consumed.contains_key(&market.bn_market_name) {
                continue;
            }
            if !allow_new_markets {
                continue;
            }
            if new_market_found {
                self.new_markets_pending_price_refresh += 1;
                self.new_markets_added.push(market.bn_market_name.clone());
            }
            let name = market.bn_market_name.clone();
            let handle = MarketHandle::new(market);
            seed_price_funding_from_market(&handle);
            lookup_entries.push((name, handle.clone()));
            markets.push(handle);
        }

        self.markets = Arc::new(markets);
        self.replace_market_lookups_like_delphi_cow(lookup_entries);

        Arc::make_mut(&mut self.token_tags).retain(|name, _| self.by_name.contains_key(name));

        self.bump_markets_version();

        for cm in resp.corr_markets {
            self.apply_one_corr_market_from_list(cm);
        }
        self.check_corr_markets_like_delphi();
        self.check_currency_ref_markets_like_delphi();
        if rebuild_server_indexes {
            self.replace_market_indexes_like_delphi_cow(incoming_server_names);
            self.indexes_synchronized = true;
        }
        self.markets_list_refresh_needed = false;

        MarketsEvent::MarketsListReplaced {
            count: self.markets.len(),
            corr_count,
        }
    }

    /// Active-library direct counterpart of Delphi `GetMarketsList`.
    ///
    /// Delphi applies each market inside the read loop, rebuilds server indexes
    /// after that loop, then applies CorrMarkets. If a CorrMarket read fails,
    /// already-read markets and rebuilt indexes remain.
    pub(crate) fn apply_markets_list_payload_like_delphi(
        &mut self,
        data: &[u8],
        ver: u16,
    ) -> Option<MarketsEvent> {
        self.apply_markets_list_payload_with_local_shift(
            data,
            ver,
            current_local_time_shift_minutes(),
        )
    }

    pub(super) fn apply_markets_list_payload_with_local_shift(
        &mut self,
        data: &[u8],
        ver: u16,
        local_shift_minutes: f64,
    ) -> Option<MarketsEvent> {
        let total_start = Instant::now();
        let first_create_markets = self.markets.is_empty();
        let new_market_found = self.markets_list_refresh_needed;
        let allow_new_markets = first_create_markets || new_market_found;
        let rebuild_server_indexes = allow_new_markets || !self.indexes_synchronized;
        self.new_markets_pending_price_refresh = 0;
        self.new_markets_added.clear();
        let mut r = EngineStreamReader::new(data);
        let count = r.read_count()?;
        let mut incoming_server_names = if rebuild_server_indexes {
            Vec::with_capacity(r.bounded_count_capacity(count, 16))
        } else {
            Vec::new()
        };
        let mut pending_markets: Option<Vec<MarketHandle>> = None;
        let mut pending_handles_by_name: Option<HashMap<String, MarketHandle>> = None;
        let mut any_market_added = false;
        let market_loop_start = Instant::now();
        for _ in 0..count {
            let market = read_market_with_local_shift(&mut r, ver, local_shift_minutes)?;
            if rebuild_server_indexes {
                incoming_server_names.push(market.bn_market_name.clone());
            }
            let market_name = if new_market_found {
                Some(market.bn_market_name.clone())
            } else {
                None
            };
            let added = self.apply_one_market_from_list_payload_batch(
                market,
                allow_new_markets,
                &mut pending_markets,
                &mut pending_handles_by_name,
            );
            if added {
                any_market_added = true;
            }
            if added && new_market_found {
                self.new_markets_pending_price_refresh += 1;
                if let Some(name) = market_name {
                    self.new_markets_added.push(name);
                }
            }
        }
        if let Some(markets) = pending_markets {
            self.markets = Arc::new(markets);
            if let Some(handles_by_name) = pending_handles_by_name {
                self.handles_by_name = Arc::new(handles_by_name);
            }
            if any_market_added {
                self.bump_markets_version();
            }
        }
        let market_loop_ns = elapsed_ns_u64(market_loop_start);

        let index_rebuild_start = Instant::now();
        if rebuild_server_indexes {
            self.replace_market_indexes_like_delphi_cow(incoming_server_names);
            self.indexes_synchronized = true;
        }
        let index_rebuild_ns = elapsed_ns_u64(index_rebuild_start);

        let corr_loop_start = Instant::now();
        let corr_count = r.read_count()?;
        for _ in 0..corr_count {
            let cm = read_corr_market(&mut r)?;
            self.apply_one_corr_market_from_list(cm);
        }
        let corr_loop_ns = elapsed_ns_u64(corr_loop_start);

        let ref_passes_start = Instant::now();
        self.check_corr_markets_like_delphi();
        self.check_currency_ref_markets_like_delphi();
        let ref_passes_ns = elapsed_ns_u64(ref_passes_start);
        self.markets_list_refresh_needed = false;
        self.last_markets_list_timing = Some(MarketsListApplyTiming {
            payload_len: data.len(),
            market_count: count,
            corr_count,
            total_ns: elapsed_ns_u64(total_start),
            market_loop_ns,
            index_rebuild_ns,
            corr_loop_ns,
            ref_passes_ns,
        });

        Some(MarketsEvent::MarketsListReplaced {
            count: self.markets.len(),
            corr_count,
        })
    }

    fn apply_one_market_from_list_payload_batch(
        &mut self,
        market: Market,
        allow_new_markets: bool,
        pending_markets: &mut Option<Vec<MarketHandle>>,
        pending_handles_by_name: &mut Option<HashMap<String, MarketHandle>>,
    ) -> bool {
        if let Some(&idx) = self.by_name.get(&market.bn_market_name) {
            let handle = self.markets.get(idx).cloned().or_else(|| {
                pending_markets
                    .as_ref()
                    .and_then(|markets| markets.get(idx).cloned())
            });
            if let Some(handle) = handle {
                handle.with_mut(|existing| {
                    merge_market_like_delphi_get_markets_list(
                        existing,
                        &market,
                        self.copy_max_leverage_from_markets_list,
                        self.eps_profile.eps,
                    );
                    existing.price.funding_time = market.funding_time;
                });
            }
            return false;
        }

        if !allow_new_markets {
            return false;
        }

        let name = market.bn_market_name.clone();
        let handle = MarketHandle::new(market);
        seed_price_funding_from_market(&handle);
        let markets = pending_markets.get_or_insert_with(|| self.markets.iter().cloned().collect());
        let idx = markets.len();
        markets.push(handle.clone());

        let handles_by_name =
            pending_handles_by_name.get_or_insert_with(|| (*self.handles_by_name).clone());
        handles_by_name.insert(name.clone(), handle.clone());
        Arc::make_mut(&mut self.by_name).insert(name.clone(), idx);
        true
    }

    fn replace_market_lookups_like_delphi_cow(
        &mut self,
        lookup_entries: Vec<(String, MarketHandle)>,
    ) {
        let mut by_name = HashMap::with_capacity(lookup_entries.len());
        let mut handles_by_name = HashMap::with_capacity(lookup_entries.len());
        for (i, (name, handle)) in lookup_entries.into_iter().enumerate() {
            by_name.insert(name.clone(), i);
            handles_by_name.insert(name, handle);
        }
        self.by_name = Arc::new(by_name);
        self.handles_by_name = Arc::new(handles_by_name);
    }

    fn bump_markets_version(&mut self) {
        self.markets_version = self.markets_version.wrapping_add(1);
    }
}

fn merge_market_like_delphi_get_markets_list(
    dst: &mut Market,
    src: &Market,
    copy_max_leverage: bool,
    eps: f64,
) {
    dst.bn_tick_size = src.bn_tick_size;
    dst.bn_step_size = src.bn_step_size;
    dst.bn_min_price = src.bn_min_price;
    dst.bn_max_price = src.bn_max_price;
    dst.bn_min_qty = src.bn_min_qty;
    dst.bn_max_qty = src.bn_max_qty;
    dst.bn_min_notional = src.bn_min_notional;
    if src.bn_max_value > eps {
        dst.bn_max_value = src.bn_max_value;
    }
    dst.bn_iceberg_parts = src.bn_iceberg_parts;
    dst.bn_iceberg = src.bn_iceberg;
    dst.bn_multiplier_down = src.bn_multiplier_down;
    dst.bn_multiplier_up = src.bn_multiplier_up;
    dst.bn_price_precision = src.bn_price_precision;
    dst.bn_quantity_precision = src.bn_quantity_precision;
    dst.status_trading = src.status_trading;
    dst.bn_only_isolated = src.bn_only_isolated;
    dst.bn_margin_table_id = src.bn_margin_table_id;
    dst.bid_multiplier_up = src.bid_multiplier_up;
    dst.bid_multiplier_down = src.bid_multiplier_down;
    dst.ask_multiplier_up = src.ask_multiplier_up;
    dst.ask_multiplier_down = src.ask_multiplier_down;
    if copy_max_leverage {
        dst.max_leverage = src.max_leverage;
    }
    dst.funding_time = src.funding_time;
    dst.volume = src.volume;
}

/// Seed the live `MarketPrice.funding_*` from the market's funding fields when a
/// market first enters the universe. Delphi `market_price_from_market` analogue:
/// bid/ask/mark stay zero until the first `UpdateMarketsList`.
fn seed_price_funding_from_market(handle: &MarketHandle) {
    handle.with_mut(|m| {
        m.price.funding_rate = m.funding_rate;
        m.price.funding_time = m.funding_time;
    });
}

fn elapsed_ns_u64(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
