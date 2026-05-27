//! Market-level retained-history registry.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::*;

#[derive(Default)]
pub struct MarketHistoryRegistry {
    default_config: MarketHistoryConfig,
    eps_profile: crate::state::eps::EpsProfile,
    stores: HashMap<SharedMarketName, MarketHistoryStore>,
    stores_by_index: Vec<Option<SharedMarketName>>,
}

impl MarketHistoryRegistry {
    pub fn new(default_config: MarketHistoryConfig) -> Self {
        Self {
            default_config,
            eps_profile: crate::state::eps::EpsProfile::default(),
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

    pub fn len(&self) -> usize {
        self.stores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    pub fn contains_market(&self, market_name: &str) -> bool {
        self.stores.contains_key(market_name)
    }

    pub fn get(&self, market_name: &str) -> Option<&MarketHistoryStore> {
        self.stores.get(market_name)
    }

    pub fn get_mut(&mut self, market_name: &str) -> Option<&mut MarketHistoryStore> {
        self.stores.get_mut(market_name)
    }

    pub fn get_mut_by_server_index(
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
        self.stores.entry(market_name).or_insert_with(|| {
            MarketHistoryStore::new_with_eps_profile(self.default_config, self.eps_profile)
        })
    }

    pub fn configure_markets(
        &mut self,
        market_names: &[String],
        scope: Option<&TradeStorageScope>,
    ) -> usize {
        self.configure_market_index_slot_names(
            market_names.iter().map(|name| Some(name.as_str())),
            scope,
        )
    }

    pub fn configure_market_index_slots<S>(
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

    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.stores
            .get(market_name)
            .map(MarketHistoryStore::readers)
    }

    pub fn compact_evicted_futures_like_delphi(&mut self, now_time: f64) -> usize {
        self.stores
            .values_mut()
            .map(|store| store.compact_evicted_futures_like_delphi(now_time))
            .sum()
    }

    pub fn refresh_derived_analytics(&mut self, now_time: f64) {
        for store in self.stores.values_mut() {
            store.refresh_derived_analytics(now_time);
        }
    }
}
