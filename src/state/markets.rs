//! Markets sync state — snapshot маркетов, поддерживается через Engine API ответы.
//!
//! Источник Delphi: `MarketsU.pas` (TMarket, TCorrMarket) + `MoonProtoEngineServer.pas`.
//!
//! ## Поток обновлений
//! - При запуске клиент шлёт `emk_GetMarketsList` → получает полный список (Markets + CorrMarkets).
//! - Периодически (~2 секунды по Delphi worker cadence) `emk_UpdateMarketsList` → обновление цен/funding.
//! - `emk_GetMarketsIndexes` → имена в порядке индексов (mIndex).
//! - Периодически (~60 секунд + hourly burst) `emk_CheckBinanceTags` → теги монет.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::commands::candles::current_local_time_shift_minutes;
use crate::commands::market::{
    read_corr_market, read_market_with_local_shift, CorrMarket, EngineStreamReader, Market,
    MarketTokenTags, MarketsListResponse, TokenTags,
};
const EPS_MARKET: f64 = 1e-12;

mod accessors;
mod currency;
mod indexes;
mod prices;
mod tags;
mod text;
mod types;

use self::text::same_text_ascii;
pub(crate) use self::types::MarketLastPriceHistoryInput;
pub use self::types::{
    BaseCurrencyPrice, MarketHandle, MarketPrice, MarketTradeState, MarketsEvent,
    MarketsListApplyTiming,
};

#[derive(Debug, Clone, Default)]
pub struct MarketsState {
    /// Маркеты в порядке `mIndex` (как они приходят в `emk_GetMarketsList`).
    ///
    /// Each item is a stable `MarketHandle`, matching Delphi `TMarket` object
    /// references stored in `TMarkets = TSlowSafeList<TMarket>`.
    pub markets: Arc<Vec<MarketHandle>>,
    /// `market_name` → индекс в `markets` (internal fast lookup for parallel arrays).
    pub by_name: HashMap<String, usize>,
    /// COW `market_name` → stable handle lookup exposed by [`Self::get`].
    pub handles_by_name: Arc<HashMap<String, MarketHandle>>,
    /// Корреляционные маркеты (BTC-маркеты для расчётов), key = `bn_market_name`.
    pub corr_markets: HashMap<String, CorrMarket>,
    /// Цены маркетов по `mIndex` (параллельный массив, обновляется prices apply).
    pub prices: Vec<MarketPrice>,
    /// Текущие цены CorrMarkets, key = `bn_market_name`.
    pub corr_prices: HashMap<String, f64>,
    /// Delphi `BaseCurDict`: base currency name -> price/ref state.
    pub base_currency_prices: HashMap<String, BaseCurrencyPrice>,
    /// Delphi `TMarket.refBTCMarket`, represented as market name -> CorrMarket name.
    pub ref_btc_corr_markets: HashMap<String, String>,
    /// Live trade tail state keyed by `bn_market_name`.
    ///
    /// Delphi stores these fields directly on `TMarket`; Rust keeps the wire
    /// market snapshot clean and stores the non-wire live tail here.
    pub trade_states: HashMap<String, MarketTradeState>,
    /// Теги монет, key = `market_name`.
    pub token_tags: HashMap<String, TokenTags>,
    /// Канонический mIndex → имя маркета (из `emk_GetMarketsIndexes`).
    pub market_indexes: Vec<String>,
    /// `true` если последняя пачка `emk_GetMarketsIndexes` была получена для текущего
    /// `PeerAppToken`. При server-restart (`PeerAppToken` сменился) Client сбрасывает в
    /// `false` и отправляет fresh `api_get_markets_indexes()`. До получения ответа
    /// `EventDispatcher` дропает входящие `TradesStream` / `OrderBook` пакеты — они
    /// несут market_idx по новой нумерации, локальные state ещё знают старую.
    ///
    /// Аналог Delphi `MoonProtoEngine.pas:1580 If FLastServerAppToken <> PeerAppToken then exit`.
    pub indexes_synchronized: bool,
    /// Delphi `NewMarketFound` analogue: set when a price row points at a server
    /// market index/name that is not present in the current market list.
    ///
    /// It is intentionally kept true after scheduling `GetMarketsList` and is
    /// cleared only by a successful list apply, matching Delphi's synchronous
    /// `Engine.GetMarketsList()` path.
    pub markets_list_refresh_needed: bool,
    /// Delphi `ES_MaxLevInGetMarkets in EngineProp`: existing markets copy
    /// `MaxLeverage` from `GetMarketsList` only for platforms that set this
    /// support flag. New markets still receive the incoming value because they
    /// are inserted as whole `TMarket` objects.
    copy_max_leverage_from_markets_list: bool,
    /// Count of markets newly added by the last successful `NewMarketFound`
    /// list refresh. Active dispatcher consumes this to request immediate
    /// `UpdateMarketsList`, like Delphi `Engine.NewMarkets.Count > 0`.
    new_markets_pending_price_refresh: usize,
    /// Names of markets inserted by the last successful listing refresh.
    ///
    /// This is emitted by the active dispatcher as a user-facing
    /// `MarketsEvent::NewMarketsAdded` after the market list state is already
    /// updated.
    new_markets_added: Vec<String>,
    /// Monotonic marker for changes to the retained market-name universe.
    ///
    /// Active history storage uses this to avoid cloning all market names on
    /// every packet. Price/tag updates do not change it.
    markets_version: u64,
    server_base_currency_name: Option<String>,
    server_base_currency_code: Option<u8>,
    last_markets_list_timing: Option<MarketsListApplyTiming>,
}

impl MarketsState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Применить ответ `emk_GetMarketsList`.
    ///
    /// Delphi does not replace the whole market universe on a repeated list
    /// response. Existing `TMarket` objects are updated through
    /// `CopyFromMarket`, old live price state is preserved, absent old markets
    /// stay in `Markets`, and CorrMarkets are add/update-only.
    pub fn apply_markets_list(&mut self, resp: MarketsListResponse) -> MarketsEvent {
        let first_create_markets = self.markets.is_empty();
        let new_market_found = self.markets_list_refresh_needed;
        let allow_new_markets = first_create_markets || new_market_found;
        self.new_markets_pending_price_refresh = 0;
        self.new_markets_added.clear();
        let incoming_count = resp.markets.len();
        let corr_count = resp.corr_markets.len();
        let incoming_server_names = if allow_new_markets {
            resp.markets
                .iter()
                .map(|m| m.bn_market_name.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let old_markets = std::mem::take(&mut self.markets);
        let old_prices = std::mem::take(&mut self.prices);
        let incoming_by_name = resp
            .markets
            .iter()
            .enumerate()
            .map(|(idx, m)| (m.bn_market_name.clone(), idx))
            .collect::<HashMap<_, _>>();
        let mut consumed = HashMap::with_capacity(resp.markets.len());

        let mut markets = Vec::with_capacity(old_markets.len().max(incoming_count));
        let mut prices = Vec::with_capacity(old_markets.len().max(incoming_count));
        let mut lookup_entries = Vec::with_capacity(old_markets.len().max(incoming_count));

        for (old_idx, handle) in old_markets.iter().cloned().enumerate() {
            let old_name = handle.with(|market| market.bn_market_name.clone());
            let mut price = old_prices
                .get(old_idx)
                .copied()
                .unwrap_or_else(|| handle.with(market_price_from_market));
            if let Some(&incoming_idx) = incoming_by_name.get(&old_name) {
                let incoming = &resp.markets[incoming_idx];
                handle.with_mut(|market| {
                    merge_market_like_delphi_get_markets_list(
                        market,
                        incoming,
                        self.copy_max_leverage_from_markets_list,
                    );
                    price.funding_time = market.funding_time;
                    consumed.insert(market.bn_market_name.clone(), true);
                });
            }
            lookup_entries.push((old_name, handle.clone()));
            markets.push(handle);
            prices.push(price);
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
            prices.push(market_price_from_market(&market));
            let handle = MarketHandle::new(market);
            lookup_entries.push((name, handle.clone()));
            markets.push(handle);
        }

        self.markets = Arc::new(markets);
        self.replace_market_lookups_like_delphi_cow(lookup_entries);

        self.token_tags
            .retain(|name, _| self.by_name.contains_key(name));

        self.prices = prices;
        self.bump_markets_version();

        for cm in resp.corr_markets {
            self.apply_one_corr_market_from_list(cm);
        }
        self.check_corr_markets_like_delphi();
        self.check_currency_ref_markets_like_delphi();
        if allow_new_markets {
            self.market_indexes = incoming_server_names;
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

    fn apply_markets_list_payload_with_local_shift(
        &mut self,
        data: &[u8],
        ver: u16,
        local_shift_minutes: f64,
    ) -> Option<MarketsEvent> {
        let total_start = Instant::now();
        let first_create_markets = self.markets.is_empty();
        let new_market_found = self.markets_list_refresh_needed;
        let allow_new_markets = first_create_markets || new_market_found;
        self.new_markets_pending_price_refresh = 0;
        self.new_markets_added.clear();
        let mut r = EngineStreamReader::new(data);
        let count = r.read_count()?;
        let mut incoming_server_names = if allow_new_markets {
            Vec::with_capacity(r.bounded_count_capacity(count, 16))
        } else {
            Vec::new()
        };
        let mut pending_markets: Option<Vec<MarketHandle>> = None;
        let mut pending_handles_by_name: Option<HashMap<String, MarketHandle>> = None;
        let mut any_market_added = false;

        let market_loop_start = Instant::now();
        let mut market_read_ns = 0u64;
        let mut market_apply_ns = 0u64;
        for _ in 0..count {
            let read_start = Instant::now();
            let market = read_market_with_local_shift(&mut r, ver, local_shift_minutes)?;
            market_read_ns = market_read_ns.saturating_add(elapsed_ns_u64(read_start));
            if allow_new_markets {
                incoming_server_names.push(market.bn_market_name.clone());
            }
            let market_name = if new_market_found {
                Some(market.bn_market_name.clone())
            } else {
                None
            };
            let apply_start = Instant::now();
            let added = self.apply_one_market_from_list_payload_batch(
                market,
                allow_new_markets,
                &mut pending_markets,
                &mut pending_handles_by_name,
            );
            market_apply_ns = market_apply_ns.saturating_add(elapsed_ns_u64(apply_start));
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
        if allow_new_markets {
            self.market_indexes = incoming_server_names;
            self.indexes_synchronized = true;
        }
        let index_rebuild_ns = elapsed_ns_u64(index_rebuild_start);

        let corr_loop_start = Instant::now();
        let corr_count = r.read_count()?;
        let mut corr_read_ns = 0u64;
        let mut corr_apply_ns = 0u64;
        for _ in 0..corr_count {
            let read_start = Instant::now();
            let cm = read_corr_market(&mut r)?;
            corr_read_ns = corr_read_ns.saturating_add(elapsed_ns_u64(read_start));
            let apply_start = Instant::now();
            self.apply_one_corr_market_from_list(cm);
            corr_apply_ns = corr_apply_ns.saturating_add(elapsed_ns_u64(apply_start));
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
            market_read_ns,
            market_apply_ns,
            index_rebuild_ns,
            corr_loop_ns,
            corr_read_ns,
            corr_apply_ns,
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
                    );
                });
            }
            if let Some(price) = self.prices.get_mut(idx) {
                price.funding_time = market.funding_time;
            }
            return false;
        }

        if !allow_new_markets {
            return false;
        }

        let name = market.bn_market_name.clone();
        let handle = MarketHandle::new(market);
        let markets = pending_markets.get_or_insert_with(|| self.markets.iter().cloned().collect());
        let idx = markets.len();
        markets.push(handle.clone());

        let handles_by_name =
            pending_handles_by_name.get_or_insert_with(|| (*self.handles_by_name).clone());
        handles_by_name.insert(name.clone(), handle.clone());
        self.by_name.insert(name.clone(), idx);
        self.trade_states.entry(name).or_default();
        self.prices.push(handle.with(market_price_from_market));
        true
    }

    fn replace_market_lookups_like_delphi_cow(
        &mut self,
        lookup_entries: Vec<(String, MarketHandle)>,
    ) {
        self.by_name.clear();
        self.by_name.reserve(lookup_entries.len());
        let mut handles_by_name = HashMap::with_capacity(lookup_entries.len());
        for (i, (name, handle)) in lookup_entries.into_iter().enumerate() {
            self.by_name.insert(name.clone(), i);
            self.trade_states.entry(name.clone()).or_default();
            handles_by_name.insert(name, handle);
        }
        self.handles_by_name = Arc::new(handles_by_name);
    }

    /// Apply the Delphi `ProcessTradesStream` live market tail side effects for
    /// one already-known trade row.
    ///
    /// Gap tracking remains in `TradesState`. This mirrors only the bounded
    /// per-market tail fields: futures trades call the `SetLastTradePrices`
    /// tail and update `LastGotAllTrades`; spot trades update only
    /// `LastGotSpotTrades`.
    pub(crate) fn apply_trade_tail_row_like_delphi(
        &mut self,
        market_index: u16,
        is_spot: bool,
        price: f32,
        qty: f32,
        now_ms: i64,
    ) {
        let Some(name) = self.market_name_by_index(market_index).map(str::to_owned) else {
            return;
        };
        if !self.by_name.contains_key(&name) {
            return;
        }
        let state = self.trade_states.entry(name).or_default();
        if is_spot {
            state.apply_spot_trade_like_delphi(now_ms);
        } else {
            state.apply_futures_trade_like_delphi(f64::from(price), f64::from(qty), now_ms);
        }
    }

    pub fn markets_list_refresh_needed(&self) -> bool {
        self.markets_list_refresh_needed
    }

    pub(crate) fn take_new_markets_pending_price_refresh(&mut self) -> usize {
        let count = self.new_markets_pending_price_refresh;
        self.new_markets_pending_price_refresh = 0;
        count
    }

    pub(crate) fn take_new_markets_added(&mut self) -> Vec<String> {
        std::mem::take(&mut self.new_markets_added)
    }

    pub(crate) fn markets_version(&self) -> u64 {
        self.markets_version
    }

    pub fn last_markets_list_apply_timing(&self) -> Option<MarketsListApplyTiming> {
        self.last_markets_list_timing
    }

    fn bump_markets_version(&mut self) {
        self.markets_version = self.markets_version.wrapping_add(1);
    }

    pub(crate) fn set_copy_max_leverage_from_markets_list(&mut self, enabled: bool) {
        self.copy_max_leverage_from_markets_list = enabled;
    }
}

fn merge_market_like_delphi_get_markets_list(
    dst: &mut Market,
    src: &Market,
    copy_max_leverage: bool,
) {
    dst.bn_tick_size = src.bn_tick_size;
    dst.bn_step_size = src.bn_step_size;
    dst.bn_min_price = src.bn_min_price;
    dst.bn_max_price = src.bn_max_price;
    dst.bn_min_qty = src.bn_min_qty;
    dst.bn_max_qty = src.bn_max_qty;
    dst.bn_min_notional = src.bn_min_notional;
    if src.bn_max_value > EPS_MARKET {
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

fn market_price_from_market(m: &Market) -> MarketPrice {
    MarketPrice {
        bid: 0.0,
        ask: 0.0,
        last_bid: 0.0,
        last_ask: 0.0,
        p_last: 0.0,
        min_lot_size: 0.0,
        chart_price_step: 0.0,
        funding_rate: m.funding_rate,
        funding_time: m.funding_time,
        mark_price: 0.0,
        mark_price_found: false,
    }
}

fn elapsed_ns_u64(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests;
