use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// =============================================================================
//  Subscription Registry — active library principle
//
//  Хранит ВОЛЮ потребителя: какие streams подписаны и с какими параметрами.
//  До первого Init transport handshake этот реестр не отправляет. После Init
//  (`domain_ready=true`) reconnect внутри той же Client-сессии сам восстанавливает
//  registry, чтобы пользователь НЕ запускал Init второй раз.
//
//  Ключ orderbook — `market_name` (стабилен через reindex), не `market_idx`
//  (последний меняется при ServerRestart). Аналог Delphi
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

/// Реестр подписок — что app просил, что либа обязана поддерживать на протяжении сессии.
///
/// Transport handshake сам подписки не шлет: registry применяется только из init/API
/// слоя, чтобы `Fine` оставался Delphi-тождественным auth-блоком.
#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    pub orderbook_subs: HashSet<String>,
    pub trades_sub: Option<TradesSubscription>,
    pub trades_storage_scope: crate::state::TradeStorageScope,
    /// Последний серверный флаг `IsMMOrdersSubscribed`.
    ///
    /// Delphi обновляет его двумя путями: `emk_SubscribeAllTrades` с bool-параметром
    /// и прямой `TMMOrdersSubscribeCommand` из UI/strategy state. После reconnect
    /// новый серверный client-state стартует с false, поэтому active library должна
    /// воспроизвести последний известный intent в init/API слое.
    pub mm_orders_sub: Option<bool>,
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

/// Что единственный пользовательский Init заказал у доменного слоя.
///
/// Инвариант: Init вызывается один раз за жизнь `Client`-сессии.
/// После этого reconnect не требует повторного Init: transport после нового
/// `Fine` восстанавливает только эти сохранённые intent'ы и registry-подписки.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DomainRestoreIntent {
    pub(crate) fetch_indexes: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingTradesUnsubscribe {
    pub(crate) request_uid: u64,
    pub(crate) sent_ms: i64,
}

// =============================================================================
