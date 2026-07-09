//! Market-level retained-history registry.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::*;

#[derive(Default)]
pub(crate) struct MarketHistoryRegistry {
    default_config: MarketHistoryConfig,
    eps_profile: crate::state::eps::EpsProfile,
    deltas_by_trades: bool,
    stores: HashMap<SharedMarketName, MarketHistoryStore>,
    stores_by_index: Vec<Option<SharedMarketName>>,
}

impl MarketHistoryRegistry {
    pub(crate) fn new(default_config: MarketHistoryConfig) -> Self {
        Self {
            default_config,
            eps_profile: crate::state::eps::EpsProfile::default(),
            deltas_by_trades: false,
            stores: HashMap::new(),
            stores_by_index: Vec::new(),
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: crate::state::eps::EpsProfile) {
        if self.eps_profile == eps_profile {
            return;
        }
        self.eps_profile = eps_profile;
        for store in self.stores.values_mut() {
            store.set_eps_profile(eps_profile);
        }
    }

    pub(crate) fn set_deltas_by_trades(&mut self, enabled: bool) {
        if self.deltas_by_trades == enabled {
            return;
        }
        self.deltas_by_trades = enabled;
        for store in self.stores.values_mut() {
            store.set_deltas_by_trades(enabled);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.stores.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn contains_market(&self, market_name: &str) -> bool {
        self.stores.contains_key(market_name)
    }

    #[cfg(test)]
    pub(crate) fn get(&self, market_name: &str) -> Option<&MarketHistoryStore> {
        self.stores.get(market_name)
    }

    pub(crate) fn get_mut(&mut self, market_name: &str) -> Option<&mut MarketHistoryStore> {
        self.stores.get_mut(market_name)
    }

    pub(crate) fn get_mut_by_server_index(
        &mut self,
        market_index: u16,
    ) -> Option<&mut MarketHistoryStore> {
        let market_name = self
            .stores_by_index
            .get(market_index as usize)?
            .as_deref()?;
        self.stores.get_mut(market_name)
    }

    fn insert_configured_market(
        &mut self,
        market_name: SharedMarketName,
    ) -> &mut MarketHistoryStore {
        let deltas_by_trades = self.deltas_by_trades;
        self.stores.entry(market_name).or_insert_with(|| {
            let mut store =
                MarketHistoryStore::new_with_eps_profile(self.default_config, self.eps_profile);
            store.set_deltas_by_trades(deltas_by_trades);
            store
        })
    }

    #[cfg(test)]
    pub(crate) fn configure_markets(
        &mut self,
        market_names: &[String],
        scope: Option<&TradeStorageScope>,
    ) -> usize {
        self.configure_market_index_slot_names(
            market_names.iter().map(|name| Some(name.as_str())),
            scope,
        )
    }

    pub(crate) fn configure_market_index_slots<S>(
        &mut self,
        market_slots: &[Option<S>],
        scope: Option<&TradeStorageScope>,
    ) -> usize
    where
        S: AsRef<str>,
    {
        self.configure_market_index_slot_names(
            market_slots
                .iter()
                .map(|slot| slot.as_ref().map(AsRef::as_ref)),
            scope,
        )
    }

    fn configure_market_index_slot_names<'a, I>(
        &mut self,
        market_slots: I,
        scope: Option<&TradeStorageScope>,
    ) -> usize
    where
        I: IntoIterator<Item = Option<&'a str>>,
    {
        let Some(scope) = scope else {
            self.stores.clear();
            self.stores_by_index.clear();
            return 0;
        };

        let market_slots = market_slots.into_iter();
        let (slot_count, _) = market_slots.size_hint();
        self.stores_by_index.clear();
        self.stores_by_index.reserve(slot_count);
        let mut desired = HashSet::with_capacity(slot_count);
        for slot in market_slots {
            let Some(name) = slot else {
                self.stores_by_index.push(None);
                continue;
            };
            if !scope.contains(name) {
                self.stores_by_index.push(None);
                continue;
            }
            let name = SharedMarketName::from(name);
            self.stores_by_index.push(Some(Arc::clone(&name)));
            desired.insert(name);
        }
        self.stores.retain(|name, _| desired.contains(name));
        for name in desired {
            self.insert_configured_market(name);
        }
        self.stores.len()
    }

    #[cfg(test)]
    pub(crate) fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.stores
            .get(market_name)
            .map(MarketHistoryStore::readers)
    }

    #[cfg(test)]
    pub(crate) fn read_handle(&self, market_name: &str) -> Option<MarketHistoryReadHandle> {
        self.stores
            .get(market_name)
            .map(MarketHistoryStore::read_handle)
    }

    pub(crate) fn read_handles(&self) -> Vec<(Arc<str>, MarketHistoryReadHandle)> {
        self.stores
            .iter()
            .map(|(name, store)| (Arc::clone(name), store.read_handle()))
            .collect()
    }

    // parity: MoonBot MarketsU.pas:TMarket.ResizeOrdersHistory
    pub(crate) fn compact_evicted_futures(&mut self, now_time: MoonTime) -> usize {
        self.stores
            .values_mut()
            .map(|store| store.compact_evicted_futures(now_time))
            .sum()
    }

    pub(crate) fn refresh_derived_analytics(&mut self, now_time: MoonTime) {
        for store in self.stores.values_mut() {
            store.refresh_derived_analytics(now_time);
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn diag_fill_market_history_to_capacity(
        &mut self,
        market_name: &str,
        now_time: MoonTime,
        span_ms: i64,
    ) -> bool {
        let Some(store) = self.stores.get_mut(market_name) else {
            return false;
        };
        store.diag_fill_to_capacity(now_time, span_ms);
        true
    }
}
