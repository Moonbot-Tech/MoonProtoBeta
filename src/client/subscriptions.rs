use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// =============================================================================
//  Subscription Registry — active library principle
//
//  Stores the consumer's INTENT: which streams are subscribed and with which parameters.
//  The transport handshake does not send this registry before the first Init. After Init
//  (`domain_ready=true`), reconnect inside the same Client session restores the
//  registry itself, so the user does NOT run Init a second time.
//
//  The orderbook key is `market_name` (stable across reindex), not `market_idx`
//  (the latter changes on ServerRestart). Analog of Delphi
//  `MoonProtoEngine.pas:305-360 BookSubbed: TSet<TMarket>`.
// =============================================================================

/// Stored all-trades subscription intent.
///
/// `want_mm` is retained so init and reconnect restore can replay the exact
/// server-side subscription parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradesSubscription {
    /// Whether market-maker order sections should be included in the trades
    /// stream.
    pub want_mm: bool,
}

/// Read-only snapshot of the streams a session currently has subscribed.
///
/// Returned by [`crate::Client::active_subscriptions`] and
/// [`crate::MoonClient::active_subscriptions`]. Because the active library
/// replays these after reconnect, they reflect the session's maintained intent,
/// not just the last packet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveSubscriptions {
    /// Market names with an active orderbook subscription, sorted for stable
    /// display.
    pub orderbooks: Vec<String>,
    /// All-trades subscription intent, or `None` when not subscribed.
    pub all_trades: Option<TradesSubscription>,
    /// Whether market-maker order sections are subscribed in the trades stream.
    pub mm_orders: bool,
}

/// Subscription registry — what the app asked for, what the library must maintain across the session.
///
/// The transport handshake does not send subscriptions itself: the registry is applied only from
/// the init/API layer, so `Fine` stays a Delphi-identical auth block.
#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    pub orderbook_subs: HashSet<String>,
    pub trades_sub: Option<TradesSubscription>,
    pub trades_storage_scope: crate::state::TradeStorageScope,
    /// Last server-side `IsMMOrdersSubscribed` flag.
    ///
    /// Delphi updates it through two paths: `emk_SubscribeAllTrades` with a bool parameter
    /// and a direct `TMMOrdersSubscribeCommand` from UI/strategy state. After reconnect
    /// the new server-side client-state starts at false, so the active library must
    /// reproduce the last known intent in the init/API layer.
    pub mm_orders_sub: Option<bool>,
}

impl SubscriptionRegistry {
    /// Build the public read-model of the currently subscribed streams.
    pub(crate) fn active_subscriptions(&self) -> ActiveSubscriptions {
        let mut orderbooks: Vec<String> = self.orderbook_subs.iter().cloned().collect();
        orderbooks.sort_unstable();
        ActiveSubscriptions {
            orderbooks,
            all_trades: self.trades_sub,
            mm_orders: self.mm_orders_sub.unwrap_or(false),
        }
    }
}

#[derive(Default)]
pub(crate) struct SubscriptionRegistrySummary {
    trades_subscribed: AtomicBool,
    has_orderbook_subs: AtomicBool,
}

impl SubscriptionRegistrySummary {
    pub(crate) fn update_from(&self, registry: &SubscriptionRegistry) {
        self.trades_subscribed
            .store(registry.trades_sub.is_some(), Ordering::Relaxed);
        self.has_orderbook_subs
            .store(!registry.orderbook_subs.is_empty(), Ordering::Relaxed);
    }

    pub(crate) fn trades_subscribed(&self) -> bool {
        self.trades_subscribed.load(Ordering::Relaxed)
    }

    pub(crate) fn has_orderbook_subs(&self) -> bool {
        self.has_orderbook_subs.load(Ordering::Relaxed)
    }
}

pub(crate) fn refresh_subscription_summary(
    summary: &SubscriptionRegistrySummary,
    trades_scope: &parking_lot::RwLock<Option<Arc<crate::state::TradeStorageScope>>>,
    registry: &SubscriptionRegistry,
) {
    summary.update_from(registry);
    let scope = registry
        .trades_sub
        .is_some()
        .then(|| Arc::new(registry.trades_storage_scope.clone()));
    *trades_scope.write() = scope;
}

/// What the single user-level Init requested from the domain layer.
///
/// Invariant: Init is called once per `Client` session lifetime.
/// After that, reconnect does not require a second Init: after a new `Fine` the transport
/// restores only these saved intents and the registry subscriptions.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DomainRestoreIntent {
    pub(crate) fetch_indexes: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingTradesUnsubscribe {
    pub(crate) request_uid: u64,
    pub(crate) sent_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_subscriptions_snapshot_sorts_and_maps_registry() {
        let mut reg = SubscriptionRegistry::default();

        // Empty registry: nothing subscribed.
        let empty = reg.active_subscriptions();
        assert!(empty.orderbooks.is_empty());
        assert_eq!(empty.all_trades, None);
        assert!(!empty.mm_orders);

        // HashSet insertion order is non-deterministic; the snapshot must sort.
        reg.orderbook_subs.insert("ETHUSDT".to_string());
        reg.orderbook_subs.insert("BTCUSDT".to_string());
        reg.trades_sub = Some(TradesSubscription { want_mm: true });
        reg.mm_orders_sub = Some(true);

        let active = reg.active_subscriptions();
        assert_eq!(
            active.orderbooks,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        assert_eq!(active.all_trades, Some(TradesSubscription { want_mm: true }));
        assert!(active.mm_orders);
    }
}

// =============================================================================
