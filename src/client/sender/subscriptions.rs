//! `ClientSender` subscription helpers and reconnect registry updates.
#![allow(dead_code)]

use super::*;

impl ClientSender {
    /// Subscribe to an orderbook stream and remember the intent for reconnect
    /// restore.
    pub(crate) fn subscribe_orderbook(&self, market_name: &str) {
        if let Err(e) = self.try_subscribe_orderbook(market_name) {
            log::warn!(target: "moonproto::client",
                "subscribe_orderbook({market_name}) dropped: {e}");
        }
    }

    /// Unsubscribe from an orderbook stream and update the reconnect registry.
    pub(crate) fn unsubscribe_orderbook(&self, market_name: &str) {
        if let Err(e) = self.try_unsubscribe_orderbook(market_name) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_orderbook({market_name}) dropped: {e}");
        }
    }

    /// Subscribe to several orderbook streams and remember all intents for
    /// reconnect restore.
    ///
    /// This updates the shared reconnect registry immediately, deduplicates
    /// already remembered market names, and appends one batched
    /// `emk_SubscribeOrderBook` request for newly added markets.
    pub(crate) fn subscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_subscribe_orderbooks(market_names) {
            log::warn!(target: "moonproto::client",
                "subscribe_orderbooks dropped: {e}");
        }
    }

    /// Unsubscribe from several orderbook streams and update the reconnect
    /// registry.
    pub(crate) fn unsubscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_unsubscribe_orderbooks(market_names) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_orderbooks dropped: {e}");
        }
    }

    /// Unsubscribe from all orderbook streams remembered by the registry.
    pub(crate) fn unsubscribe_all_orderbooks(&self) {
        if let Err(e) = self.try_unsubscribe_all_orderbooks() {
            log::warn!(target: "moonproto::client",
                "unsubscribe_all_orderbooks dropped: {e}");
        }
    }

    /// Subscribe to the all-trades stream and remember the intent for reconnect
    /// restore.
    pub(crate) fn subscribe_all_trades(&self, want_mm: bool) {
        if let Err(e) = self.try_subscribe_all_trades(want_mm) {
            log::warn!(target: "moonproto::client",
                "subscribe_all_trades(want_mm={want_mm}) dropped: {e}");
        }
    }

    /// Subscribe to the all-trades stream while retaining active-library
    /// history only for the selected markets.
    ///
    /// Empty `market_names` means all markets. The wire command is still
    /// Delphi-compatible `emk_SubscribeAllTrades`; the scope affects only
    /// Active Lib typed events, retained trades/candles, and derived analytics.
    pub(crate) fn subscribe_trades_for<I, S>(&self, want_mm: bool, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_subscribe_trades_for(want_mm, market_names) {
            log::warn!(target: "moonproto::client",
                "subscribe_trades_for(want_mm={want_mm}) dropped: {e}");
        }
    }

    /// Unsubscribe from the all-trades stream and update the reconnect registry.
    pub(crate) fn unsubscribe_all_trades(&self) {
        if let Err(e) = self.try_unsubscribe_all_trades() {
            log::warn!(target: "moonproto::client",
                "unsubscribe_all_trades dropped: {e}");
        }
    }

    /// Subscribe to live TF candles and remember the intent for reconnect
    /// restore.
    pub(crate) fn subscribe_candles<I, S>(
        &self,
        market_names: I,
        kind: crate::commands::candles::DeepHistoryKind,
    ) where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_subscribe_candles(market_names, kind) {
            log::warn!(target: "moonproto::client",
                "subscribe_candles(kind={kind:?}) dropped: {e}");
        }
    }

    /// Unsubscribe from live TF candles and update the reconnect registry.
    pub(crate) fn unsubscribe_candles<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_unsubscribe_candles(market_names) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_candles dropped: {e}");
        }
    }

    /// Fallible orderbook subscription.
    pub(crate) fn try_subscribe_orderbook(&self, market_name: &str) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let market_name = market_name.to_string();
        let newly_added = {
            let mut registry = self.shared.subscription_registry.lock();
            let newly_added = registry.orderbook_subs.insert(market_name.clone());
            self.shared.refresh_subscription_summary(&registry);
            newly_added
        };
        if newly_added && self.domain_ready_for_typed_send() {
            self.try_send_api_request(crate::commands::engine_request::subscribe_order_book(&[
                &market_name,
            ]))?;
        }
        Ok(())
    }

    /// Fallible orderbook unsubscribe.
    pub(crate) fn try_unsubscribe_orderbook(
        &self,
        market_name: &str,
    ) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let market_name = market_name.to_string();
        let removed = {
            let mut registry = self.shared.subscription_registry.lock();
            let removed = registry.orderbook_subs.remove(&market_name);
            self.shared.refresh_subscription_summary(&registry);
            removed
        };
        if removed && self.domain_ready_for_typed_send() {
            self.try_send_api_request(crate::commands::engine_request::unsubscribe_order_book(&[
                &market_name,
            ]))?;
        }
        Ok(())
    }

    /// Fallible batched orderbook subscription.
    pub(crate) fn try_subscribe_orderbooks<I, S>(
        &self,
        market_names: I,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let mut new_names = Vec::new();
        {
            let mut registry = self.shared.subscription_registry.lock();
            for market_name in market_names {
                if registry.orderbook_subs.insert(market_name.clone()) {
                    new_names.push(market_name);
                }
            }
            self.shared.refresh_subscription_summary(&registry);
        }
        if !new_names.is_empty() && self.domain_ready_for_typed_send() {
            let refs: Vec<&str> = new_names.iter().map(String::as_str).collect();
            self.try_send_api_request(crate::commands::engine_request::subscribe_order_book(
                &refs,
            ))?;
        }
        Ok(())
    }

    /// Fallible batched orderbook unsubscribe.
    pub(crate) fn try_unsubscribe_orderbooks<I, S>(
        &self,
        market_names: I,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let mut removed_names = Vec::new();
        {
            let mut registry = self.shared.subscription_registry.lock();
            for market_name in market_names {
                if registry.orderbook_subs.remove(&market_name) {
                    removed_names.push(market_name);
                }
            }
            self.shared.refresh_subscription_summary(&registry);
        }
        if !removed_names.is_empty() && self.domain_ready_for_typed_send() {
            let refs: Vec<&str> = removed_names.iter().map(String::as_str).collect();
            self.try_send_api_request(crate::commands::engine_request::unsubscribe_order_book(
                &refs,
            ))?;
        }
        Ok(())
    }

    /// Fallible all-orderbooks unsubscribe.
    pub(crate) fn try_unsubscribe_all_orderbooks(&self) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let removed_names = {
            let mut registry = self.shared.subscription_registry.lock();
            let removed_names = registry.orderbook_subs.drain().collect::<Vec<_>>();
            self.shared.refresh_subscription_summary(&registry);
            removed_names
        };
        if removed_names.is_empty() || !self.domain_ready_for_typed_send() {
            return Ok(());
        }
        let refs: Vec<&str> = removed_names.iter().map(String::as_str).collect();
        self.try_send_api_request(crate::commands::engine_request::unsubscribe_order_book(
            &refs,
        ))
    }

    /// Fallible all-trades subscription.
    pub(crate) fn try_subscribe_all_trades(&self, want_mm: bool) -> Result<(), SubscribeError> {
        self.try_subscribe_trades_with_scope(want_mm, crate::state::TradeStorageScope::All)
    }

    /// Fallible scoped all-trades subscription.
    pub(crate) fn try_subscribe_trades_for<I, S>(
        &self,
        want_mm: bool,
        market_names: I,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.try_subscribe_trades_with_scope(
            want_mm,
            crate::state::TradeStorageScope::from_markets(market_names),
        )
    }

    fn try_subscribe_trades_with_scope(
        &self,
        want_mm: bool,
        storage_scope: crate::state::TradeStorageScope,
    ) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let wire_changed = {
            let mut registry = self.shared.subscription_registry.lock();
            let new_sub = Some(TradesSubscription { want_mm });
            let wire_changed =
                registry.trades_sub != new_sub || registry.mm_orders_sub != Some(want_mm);
            registry.trades_sub = Some(TradesSubscription { want_mm });
            registry.mm_orders_sub = Some(want_mm);
            registry.trades_storage_scope = storage_scope;
            self.shared.refresh_subscription_summary(&registry);
            wire_changed
        };
        if !wire_changed || !self.domain_ready_for_typed_send() {
            return Ok(());
        }
        self.try_send_api_request(crate::commands::engine_request::subscribe_all_trades(
            want_mm,
        ))
    }

    /// Fallible all-trades unsubscribe.
    pub(crate) fn try_unsubscribe_all_trades(&self) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let had_subscription = {
            let mut registry = self.shared.subscription_registry.lock();
            let had_subscription = registry.trades_sub.take().is_some();
            self.shared.refresh_subscription_summary(&registry);
            had_subscription
        };
        if had_subscription && self.domain_ready_for_typed_send() {
            self.try_send_api_request(crate::commands::engine_request::unsubscribe_all_trades())?;
        }
        Ok(())
    }

    /// Fallible live TF-candles subscription.
    pub(crate) fn try_subscribe_candles<I, S>(
        &self,
        market_names: I,
        kind: crate::commands::candles::DeepHistoryKind,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let mut to_subscribe = Vec::new();
        {
            let mut registry = self.shared.subscription_registry.lock();
            for market_name in market_names {
                if registry.candle_subs.get(&market_name) == Some(&kind) {
                    continue;
                }
                registry.candle_subs.insert(market_name.clone(), kind);
                to_subscribe.push(market_name);
            }
        }
        if !to_subscribe.is_empty() && self.domain_ready_for_typed_send() {
            to_subscribe.sort_unstable();
            let refs: Vec<&str> = to_subscribe.iter().map(String::as_str).collect();
            self.try_send_api_request(crate::commands::candles::subscribe_candles(&refs, kind))?;
        }
        Ok(())
    }

    /// Fallible live TF-candles unsubscribe.
    pub(crate) fn try_unsubscribe_candles<I, S>(
        &self,
        market_names: I,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let mut removed = Vec::new();
        {
            let mut registry = self.shared.subscription_registry.lock();
            for market_name in market_names {
                if registry.candle_subs.remove(&market_name).is_some() {
                    removed.push(market_name);
                }
            }
        }
        if !removed.is_empty() && self.domain_ready_for_typed_send() {
            let refs: Vec<&str> = removed.iter().map(String::as_str).collect();
            self.try_send_api_request(crate::commands::candles::unsubscribe_candles(&refs))?;
        }
        Ok(())
    }
}
