//! Markets sync state — snapshot маркетов, поддерживается через Engine API ответы.
//!
//! Источник Delphi: `MarketsU.pas` (TMarket, TCorrMarket) + `MoonProtoEngineServer.pas`.
//!
//! ## Поток обновлений
//! - При запуске клиент шлёт `emk_GetMarketsList` → получает полный список (Markets + CorrMarkets).
//! - Периодически (~2 секунды по Delphi worker cadence) `emk_UpdateMarketsList` → обновление цен/funding.
//! - `emk_GetMarketsIndexes` → имена в порядке индексов (mIndex).
//! - Периодически (~60 секунд + hourly burst) `emk_CheckBinanceTags` → теги монет.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::commands::candles::current_local_time_shift_minutes;
use crate::commands::market::{
    apply_delphi_local_funding_shift, read_corr_market, read_market_with_local_shift, BaseCurrency,
    CorrMarket, CorrMarketPriceUpdate, EngineStreamReader, Market, MarketPriceUpdate,
    MarketTokenTags, MarketsListResponse, MarketsPricesResponse, TokenTags,
};
const EPS_MARKET: f64 = 1e-12;

mod accessors;
mod types;

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

    fn apply_one_corr_market_from_list(&mut self, cm: CorrMarket) {
        if cm.base_currency_name.is_empty() {
            return;
        }
        self.ensure_base_currency_price(&cm.base_currency_name);
        if let Some(existing) = self.corr_markets.get_mut(&cm.bn_market_name) {
            existing.bn_tick_size = cm.bn_tick_size;
            existing.base_currency_name = cm.base_currency_name;
        } else {
            self.corr_markets.insert(cm.bn_market_name.clone(), cm);
        }
    }

    fn ensure_base_currency_price(&mut self, base_currency: &str) {
        if base_currency.is_empty() || self.base_currency_prices.contains_key(base_currency) {
            return;
        }
        self.base_currency_prices.insert(
            base_currency.to_string(),
            BaseCurrencyPrice::new(base_currency.to_string()),
        );
    }

    pub(crate) fn set_server_base_currency(&mut self, name: Option<&str>, code: Option<u8>) {
        let next_name = name.map(ToOwned::to_owned);
        if self.server_base_currency_name == next_name && self.server_base_currency_code == code {
            return;
        }
        self.server_base_currency_name = next_name;
        self.server_base_currency_code = code;
        self.check_corr_markets_like_delphi();
        self.check_currency_ref_markets_like_delphi();
        self.update_currency_prices_like_delphi();
    }

    fn check_corr_markets_like_delphi(&mut self) {
        if self.server_base_is_btc_like_delphi() {
            return;
        }
        let Some(currency) = self.server_base_currency_name.as_deref() else {
            return;
        };
        if currency.is_empty() {
            return;
        }
        for handle in self.markets.iter() {
            let (market_name, corr_name) = handle.with(|market| {
                (
                    market.bn_market_name.clone(),
                    replace_text_ascii_case_insensitive(&market.bn_market_name, currency, "BTC"),
                )
            });
            if market_name.is_empty() {
                continue;
            }
            if self.corr_markets.contains_key(&corr_name) {
                self.ref_btc_corr_markets
                    .insert(market_name.clone(), corr_name);
            } else {
                self.ref_btc_corr_markets.remove(&market_name);
            }
        }
    }

    fn check_currency_ref_markets_like_delphi(&mut self) {
        // Same final assignments as Delphi nested scans, but indexed first so
        // the protocol tick does not scale as BaseCurDict * CorrDict in Rust.
        let mut usdt_market_by_key = HashMap::new();
        let mut usdt_rev_market_by_key = HashMap::new();
        for handle in self.markets.iter() {
            let (base_currency, bn_market_currency, bn_market_name) = handle.with(|market| {
                (
                    market.base_currency.clone(),
                    market.bn_market_currency.clone(),
                    market.bn_market_name.clone(),
                )
            });
            if same_text_ascii(&base_currency, "USDT") {
                usdt_market_by_key
                    .insert(norm_text_ascii(&bn_market_currency), bn_market_name.clone());
            }
            if same_text_ascii(&bn_market_currency, "USDT") {
                usdt_rev_market_by_key.insert(norm_text_ascii(&base_currency), bn_market_name);
            }
        }

        let mut usdt_corr_market_by_key = HashMap::new();
        let mut usdt_rev_corr_market_by_key = HashMap::new();
        for cm in self.corr_markets.values() {
            if same_text_ascii(&cm.base_currency_name, "USDT") {
                usdt_corr_market_by_key.insert(
                    norm_text_ascii(&cm.bn_market_currency),
                    cm.bn_market_name.clone(),
                );
            }
            if same_text_ascii(&cm.bn_market_currency, "USDT") {
                usdt_rev_corr_market_by_key.insert(
                    norm_text_ascii(&cm.base_currency_name),
                    cm.bn_market_name.clone(),
                );
            }
        }

        let keys = self
            .base_currency_prices
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let norm_key = norm_text_ascii(&key);
            let usdt_market = usdt_market_by_key.get(&norm_key).cloned();
            let usdt_rev_market = usdt_rev_market_by_key.get(&norm_key).cloned();
            let usdt_corr_market = usdt_corr_market_by_key.get(&norm_key).cloned();
            let usdt_rev_corr_market = usdt_rev_corr_market_by_key.get(&norm_key).cloned();

            let Some(bc) = self.base_currency_prices.get_mut(&key) else {
                continue;
            };
            if let Some(name) = usdt_market {
                bc.usdt_market = Some(name);
            }
            if let Some(name) = usdt_rev_market {
                bc.usdt_rev_market = Some(name);
            }
            if let Some(name) = usdt_corr_market {
                bc.usdt_corr_market = Some(name);
            }
            if let Some(name) = usdt_rev_corr_market {
                bc.usdt_rev_corr_market = Some(name);
            }
        }
    }

    fn update_currency_prices_like_delphi(&mut self) {
        let keys = self
            .base_currency_prices
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let next_price = self
                .base_currency_prices
                .get(&key)
                .and_then(|bc| self.next_base_currency_price_like_delphi(bc));
            if let Some(price) = next_price {
                if let Some(bc) = self.base_currency_prices.get_mut(&key) {
                    bc.last_price = price;
                }
            }
        }
    }

    fn next_base_currency_price_like_delphi(&self, bc: &BaseCurrencyPrice) -> Option<f64> {
        if let Some(price) = bc
            .usdt_market
            .as_deref()
            .and_then(|name| self.price(name))
            .map(|p| p.ask)
            .filter(|ask| *ask > EPS_MARKET)
        {
            return Some(price);
        }
        if let Some(price) = bc
            .usdt_rev_market
            .as_deref()
            .and_then(|name| self.price(name))
            .map(|p| p.ask)
            .filter(|ask| *ask > EPS_MARKET)
        {
            return Some(1.0 / price);
        }
        if let Some(price) = bc
            .usdt_corr_market
            .as_deref()
            .and_then(|name| self.corr_prices.get(name))
            .copied()
            .filter(|price| *price > EPS_MARKET)
        {
            return Some(price);
        }
        if let Some(price) = bc
            .usdt_rev_corr_market
            .as_deref()
            .and_then(|name| self.corr_prices.get(name))
            .copied()
            .filter(|price| *price > EPS_MARKET)
        {
            return Some(1.0 / price);
        }
        if same_text_ascii(&bc.base_currency, "USDT") {
            return Some(1.0);
        }
        None
    }

    fn server_base_is_btc_like_delphi(&self) -> bool {
        self.server_base_currency_code == Some(BaseCurrency::BTC.to_byte())
            || self
                .server_base_currency_name
                .as_deref()
                .is_some_and(|name| same_text_ascii(name, "BTC"))
    }

    /// Применить ответ `emk_UpdateMarketsList`.
    /// Обновляет цену рынка, резолвя server `mIndex` через `emk_GetMarketsIndexes`.
    /// Если mapping неизвестен или stale после server restart — запись пропускается.
    pub fn apply_markets_prices(&mut self, resp: MarketsPricesResponse) -> MarketsEvent {
        let count = resp.prices.len();
        for slot in &mut self.prices {
            slot.mark_price_found = false;
        }
        for p in &resp.prices {
            self.apply_one_market_price_update(p, resp.send_funding);
        }
        if resp.send_corr_markets {
            for c in &resp.corr_prices {
                self.apply_one_corr_price_update(c);
            }
        }
        self.update_currency_prices_like_delphi();
        MarketsEvent::PricesUpdated {
            count,
            included_funding: resp.send_funding,
            included_corr: resp.send_corr_markets,
        }
    }

    /// Active-library direct counterpart of Delphi `UpdateMarketsList`.
    ///
    /// Delphi mutates market prices inside the read loop. If a later corr-market
    /// string read raises, already-applied prices remain. The pure parser remains
    /// a low-level command helper; dispatcher uses this method for protocol state.
    pub(crate) fn apply_markets_prices_payload_like_delphi(
        &mut self,
        data: &[u8],
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_collecting_last_price_like_delphi(data, None)
    }

    pub(crate) fn apply_markets_prices_payload_collecting_last_price_like_delphi(
        &mut self,
        data: &[u8],
        last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_with_local_shift(
            data,
            current_local_time_shift_minutes(),
            last_price_rows,
        )
    }

    fn apply_markets_prices_payload_with_local_shift(
        &mut self,
        data: &[u8],
        local_shift_minutes: f64,
        mut last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
    ) -> Option<MarketsEvent> {
        for slot in &mut self.prices {
            slot.mark_price_found = false;
        }

        let mut r = EngineStreamReader::new(data);
        let send_funding = r.read_bool()?;
        let count = r.read_count()?;

        for _ in 0..count {
            let update =
                read_market_price_update_like_delphi(&mut r, send_funding, local_shift_minutes)?;
            if let Some(row) = self.apply_one_market_price_update(&update, send_funding) {
                if let Some(rows) = last_price_rows.as_deref_mut() {
                    rows.push(row);
                }
            }
        }

        let send_corr_markets = r.read_bool()?;
        if send_corr_markets {
            let corr_count = r.read_count()?;
            for _ in 0..corr_count {
                let update = read_corr_price_update_like_delphi(&mut r)?;
                self.apply_one_corr_price_update(&update);
            }
        }

        self.update_currency_prices_like_delphi();
        Some(MarketsEvent::PricesUpdated {
            count,
            included_funding: send_funding,
            included_corr: send_corr_markets,
        })
    }

    /// Build retained LastPrice rows from the current market-price state.
    ///
    /// This is the Active Lib backfill for the common order:
    /// Init `UpdateMarketsList` first, `subscribe_all_trades` later. Delphi has
    /// one always-live `TMarket.HistoryPrice`; Rust creates retained stores only
    /// after the agreed trades-storage opt-in, so the already-known `pLast`
    /// values must be copied once when the storage scope becomes active.
    pub(crate) fn current_last_price_history_rows_like_delphi(
        &self,
    ) -> Vec<MarketLastPriceHistoryInput> {
        let mut rows = Vec::new();
        for (idx, handle) in self.markets.iter().enumerate() {
            let Some(slot) = self.prices.get(idx) else {
                continue;
            };
            let (market_name, is_btc_market, is_base_usdt_market) = handle.with(|market| {
                (
                    market.bn_market_name.clone(),
                    market.is_btc_market,
                    self.market_is_base_usdt_market_like_delphi(market),
                )
            });
            rows.push(MarketLastPriceHistoryInput {
                market_name,
                current: slot.p_last,
                bid: slot.bid,
                ask: slot.ask,
                is_btc_market,
                is_base_usdt_market,
            });
        }
        rows
    }

    fn apply_one_market_price_update(
        &mut self,
        p: &MarketPriceUpdate,
        send_funding: bool,
    ) -> Option<MarketLastPriceHistoryInput> {
        if let Some(idx) = self.local_pos_for_server_index(p.m_index) {
            let handle = self.markets.get(idx).cloned()?;
            let (market_name, is_btc_market, is_base_usdt_market) = handle.with(|market| {
                (
                    market.bn_market_name.clone(),
                    market.is_btc_market,
                    self.market_is_base_usdt_market_like_delphi(market),
                )
            });
            let (bn_step_size, bn_min_qty, bn_min_notional) = handle.with_mut(|market| {
                if send_funding {
                    market.funding_rate = p.funding_rate;
                    market.funding_time = p.funding_time;
                }
                (
                    market.bn_step_size,
                    market.bn_min_qty,
                    market.bn_min_notional,
                )
            });
            let slot = &mut self.prices[idx];
            slot.bid = p.bid;
            slot.ask = p.ask;
            slot.last_bid = slot.bid;
            slot.last_ask = slot.ask;
            slot.p_last = (slot.bid + slot.ask) * 0.5;
            slot.min_lot_size = (bn_step_size.max(bn_min_qty) * slot.p_last).max(bn_min_notional);
            if slot.ask > EPS_MARKET {
                slot.chart_price_step = EPS_MARKET.max(slot.ask / 5000.0);
            }
            if send_funding {
                slot.funding_rate = p.funding_rate;
                slot.funding_time = p.funding_time;
            }
            slot.mark_price = p.mark_price;
            slot.mark_price_found = p.mark_price_found;
            Some(MarketLastPriceHistoryInput {
                market_name,
                current: slot.p_last,
                bid: slot.bid,
                ask: slot.ask,
                is_btc_market,
                is_base_usdt_market,
            })
        } else if self.price_row_points_to_missing_market(p.m_index) {
            self.markets_list_refresh_needed = true;
            None
        } else {
            None
        }
    }

    fn market_is_base_usdt_market_like_delphi(&self, market: &Market) -> bool {
        let market_name = market.bn_market_name.as_str();
        if let Some(base_currency) = self.server_base_currency_name.as_deref() {
            if let Some(base_price) = self.base_currency_price(base_currency) {
                if base_price
                    .usdt_market
                    .as_deref()
                    .is_some_and(|name| same_text_ascii(name, market_name))
                    || base_price
                        .usdt_rev_market
                        .as_deref()
                        .is_some_and(|name| same_text_ascii(name, market_name))
                {
                    return true;
                }
            }
            if !same_text_ascii(base_currency, "USDT")
                && same_text_ascii(&market.bn_market_currency, base_currency)
                && same_text_ascii(&market.base_currency, "USDT")
            {
                return true;
            }
        }

        same_text_ascii(market_name, "BTCUSDT")
            || same_text_ascii(market_name, "BTC_USDT")
            || (market.is_btc_market && same_text_ascii(&market.base_currency, "USDT"))
    }

    fn apply_one_corr_price_update(&mut self, c: &CorrMarketPriceUpdate) {
        if self.corr_markets.contains_key(&c.bn_market_name) {
            self.corr_prices
                .insert(c.bn_market_name.clone(), c.last_price);
        }
    }

    /// Применить ответ `emk_GetMarketsIndexes`.
    /// Помечает `indexes_synchronized = true` — после этого EventDispatcher разблокирует
    /// обработку TradesStream / OrderBook пакетов.
    pub fn apply_markets_indexes(&mut self, names: Vec<String>) -> MarketsEvent {
        let count = names.len();
        self.market_indexes = names;
        self.indexes_synchronized = true;
        MarketsEvent::IndexesUpdated { count }
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

    /// Mark current market indexes as stale after server process restart.
    ///
    /// The old `market_indexes` vector is intentionally kept for diagnostics and for
    /// consumers that need to show the last known mapping, but stream parsing must be
    /// gated until a fresh `emk_GetMarketsIndexes` response arrives.
    pub(crate) fn mark_indexes_stale(&mut self) {
        self.indexes_synchronized = false;
    }

    /// Применить ответ `emk_CheckBinanceTags`.
    ///
    /// Delphi `TMoonProtoEngine.CheckBinanceTags` clears seen state for all
    /// markets, applies tags for markets present in the response, then clears
    /// tags for every market not seen in that response.
    pub fn apply_token_tags(&mut self, items: Vec<MarketTokenTags>) -> MarketsEvent {
        self.token_tags.clear();
        let mut count = 0usize;
        for it in items {
            if self.by_name.contains_key(&it.market_name) {
                self.token_tags.insert(it.market_name, it.tags);
                count += 1;
            }
        }
        MarketsEvent::TokenTagsUpdated { count }
    }

    /// Active-library direct counterpart of Delphi `CheckBinanceTags`.
    ///
    /// Delphi applies tags inside the read loop and clears unseen tags only
    /// after the loop completes. A late string read error therefore leaves
    /// already-read tag updates applied and does not clear old absent tags.
    pub(crate) fn apply_token_tags_payload_like_delphi(
        &mut self,
        data: &[u8],
    ) -> Option<MarketsEvent> {
        let mut r = EngineStreamReader::new(data);
        let count = r.read_count()?;
        let mut seen = HashSet::with_capacity(r.bounded_count_capacity(count, 6));

        for _ in 0..count {
            let market_name = r.read_str()?;
            let tags = TokenTags::from_bits(r.read_int()? as u32);
            if self.by_name.contains_key(&market_name) {
                self.token_tags.insert(market_name.clone(), tags);
                seen.insert(market_name);
            }
        }

        self.token_tags.retain(|name, _| seen.contains(name));
        Some(MarketsEvent::TokenTagsUpdated { count: seen.len() })
    }

    pub(crate) fn has_server_market_index(&self, m_index: u16) -> bool {
        if !self.indexes_synchronized {
            return false;
        }
        self.market_indexes
            .get(m_index as usize)
            .is_some_and(|name| self.by_name.contains_key(name))
    }

    fn local_pos_for_server_index(&self, m_index: u16) -> Option<usize> {
        let server_pos = m_index as usize;
        if self.indexes_synchronized {
            let market_name = self.market_indexes.get(server_pos)?;
            return self.by_name.get(market_name).copied();
        }

        // Cold-start compatibility: before the first explicit indexes response,
        // `GetMarketsList` arrives in server order. Once a mapping exists but is
        // marked stale, direct fallback would silently apply prices to old names.
        if self.market_indexes.is_empty() && server_pos < self.prices.len() {
            Some(server_pos)
        } else {
            None
        }
    }

    fn price_row_points_to_missing_market(&self, m_index: u16) -> bool {
        let server_pos = m_index as usize;
        if self.indexes_synchronized {
            return self
                .market_indexes
                .get(server_pos)
                .is_none_or(|name| !self.by_name.contains_key(name));
        }
        self.market_indexes.is_empty() && server_pos >= self.prices.len()
    }

    pub fn markets_list_refresh_needed(&self) -> bool {
        self.markets_list_refresh_needed
    }

    /// Delphi `AddNewAksPrice` from `GlassUpdated`: keep `ChartPriceStep` fresh
    /// when orderbook updates move the best ask before the next price refresh.
    pub(crate) fn update_chart_price_step_from_server_index(
        &mut self,
        m_index: u16,
        ask: f64,
    ) -> bool {
        if ask <= EPS_MARKET {
            return false;
        }
        let Some(idx) = self.local_pos_for_server_index(m_index) else {
            return false;
        };
        let Some(slot) = self.prices.get_mut(idx) else {
            return false;
        };
        slot.chart_price_step = EPS_MARKET.max(ask / 5000.0);
        true
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

fn read_market_price_update_like_delphi(
    r: &mut EngineStreamReader<'_>,
    send_funding: bool,
    local_shift_minutes: f64,
) -> Option<MarketPriceUpdate> {
    let m_index = r.read_word()?;
    let bid = r.read_double()?;
    let ask = r.read_double()?;
    let (funding_rate, funding_time) = if send_funding {
        (
            r.read_double()?,
            apply_delphi_local_funding_shift(r.read_double()?, local_shift_minutes),
        )
    } else {
        (0.0, 0.0)
    };
    let mark_price = r.read_double()?;
    let mark_price_found = r.read_bool()?;
    Some(MarketPriceUpdate {
        m_index,
        bid,
        ask,
        funding_rate,
        funding_time,
        mark_price,
        mark_price_found,
    })
}

fn read_corr_price_update_like_delphi(
    r: &mut EngineStreamReader<'_>,
) -> Option<CorrMarketPriceUpdate> {
    let bn_market_name = r.read_str()?;
    let last_price = r.read_double()?;
    Some(CorrMarketPriceUpdate {
        bn_market_name,
        last_price,
    })
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

fn same_text_ascii(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn norm_text_ascii(value: &str) -> String {
    value.to_ascii_uppercase()
}

fn replace_text_ascii_case_insensitive(input: &str, from: &str, to: &str) -> String {
    if from.is_empty() {
        return input.to_string();
    }
    let bytes = input.as_bytes();
    let needle = from.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut last = 0usize;
    let mut i = 0usize;
    while i + needle.len() <= bytes.len() {
        let matched = bytes[i..i + needle.len()]
            .iter()
            .zip(needle.iter())
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
        if matched && input.is_char_boundary(i) && input.is_char_boundary(i + needle.len()) {
            out.push_str(&input[last..i]);
            out.push_str(to);
            i += needle.len();
            last = i;
        } else {
            i += 1;
        }
    }
    out.push_str(&input[last..]);
    out
}

#[cfg(test)]
mod tests;
