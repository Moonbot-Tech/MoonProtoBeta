use crate::commands::candles::DeepHistoryKind;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
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

/// One maintained live-candle subscription.
///
/// Candle intervals are owned per market by the MoonBot core. A timeframe
/// change for one market does not affect any other subscribed market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveCandleSubscription {
    pub market: String,
    pub kind: DeepHistoryKind,
}

/// Read-only snapshot of the streams a session currently has subscribed.
///
/// Returned by [`crate::MoonClient::active_subscriptions`]. Because the active library
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
    /// Market names with an active live TF-candles subscription, sorted for
    /// stable display.
    pub live_candles: Vec<String>,
    /// Exact per-market live-candle subscriptions, sorted by market name.
    pub live_candle_timeframes: Vec<LiveCandleSubscription>,
    /// Common live-candle interval when every subscribed market uses the same
    /// timeframe. `None` means either no candle subscriptions or mixed
    /// per-market timeframes.
    ///
    /// New code should use [`Self::live_candle_timeframes`].
    pub live_candles_kind: Option<DeepHistoryKind>,
    /// Report DB catch-up intent maintained across hard reconnects.
    pub report_replication: Option<crate::state::ReportSyncRequest>,
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
    pub candle_subs: HashMap<String, DeepHistoryKind>,
    pub report_sync: Option<crate::state::ReportSyncRequest>,
    pub report_open_rows: Arc<[i64]>,
}

impl SubscriptionRegistry {
    /// Build the public read-model of the currently subscribed streams.
    pub(crate) fn active_subscriptions(&self) -> ActiveSubscriptions {
        let mut orderbooks: Vec<String> = self.orderbook_subs.iter().cloned().collect();
        orderbooks.sort_unstable();
        let mut live_candle_timeframes: Vec<LiveCandleSubscription> = self
            .candle_subs
            .iter()
            .map(|(market, &kind)| LiveCandleSubscription {
                market: market.clone(),
                kind,
            })
            .collect();
        live_candle_timeframes.sort_unstable_by(|a, b| a.market.cmp(&b.market));
        let live_candles = live_candle_timeframes
            .iter()
            .map(|sub| sub.market.clone())
            .collect();
        let live_candles_kind = live_candle_timeframes
            .first()
            .map(|first| first.kind)
            .filter(|kind| {
                live_candle_timeframes
                    .iter()
                    .all(|subscription| subscription.kind == *kind)
            });
        ActiveSubscriptions {
            orderbooks,
            all_trades: self.trades_sub,
            mm_orders: self.mm_orders_sub.unwrap_or(false),
            live_candles,
            live_candle_timeframes,
            live_candles_kind,
            report_replication: self.report_sync,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TradeStorageIntent {
    pub(crate) scope: Arc<crate::state::TradeStorageScope>,
    pub(crate) retain_mm_orders: bool,
}

pub(crate) fn refresh_subscription_summary(
    summary: &SubscriptionRegistrySummary,
    storage_intent: &parking_lot::RwLock<Option<TradeStorageIntent>>,
    registry: &SubscriptionRegistry,
) {
    summary.update_from(registry);
    *storage_intent.write() = registry.trades_sub.map(|subscription| TradeStorageIntent {
        scope: Arc::new(registry.trades_storage_scope.clone()),
        retain_mm_orders: registry.mm_orders_sub.unwrap_or(subscription.want_mm),
    });
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

#[derive(Debug, Default)]
pub(crate) struct PendingCandleSubscribes {
    request_uids: HashSet<u64>,
    failed: bool,
}

impl PendingCandleSubscribes {
    pub(crate) fn insert(&mut self, request_uid: u64) {
        if self.request_uids.is_empty() {
            self.failed = false;
        }
        self.request_uids.insert(request_uid);
    }

    pub(crate) fn finish(&mut self, request_uid: u64, success: bool) -> Option<bool> {
        if !self.request_uids.remove(&request_uid) {
            return None;
        }
        self.failed |= !success;
        if !self.request_uids.is_empty() {
            return None;
        }
        let all_succeeded = !self.failed;
        self.failed = false;
        Some(all_succeeded)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.request_uids.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.request_uids.len()
    }

    pub(crate) fn clear(&mut self) {
        self.request_uids.clear();
        self.failed = false;
    }
}

/// Active-library subscription cluster carved out of [`super::Client`].
///
/// Groups what the application subscribed (`subscription_registry` plus its
/// atomic `subscription_summary` mirror and the `subscription_trade_storage_intent`
/// retained-data filter), the Delphi `InitDone` domain gate
/// (`domain_ready` and its `Arc<AtomicBool>` mirror `domain_ready_flag` shared
/// with `ClientSender`), and the saved single-Init restore intent
/// (`domain_restore`). Field names, types, and meaning are unchanged from when
/// they lived directly on `Client`.
pub(crate) struct Subscriptions {
    /// **Active library — subscription registry**: what the app asked to subscribe.
    /// The transport handshake does not send this registry before Init. After Init,
    /// reconnect restores the registry itself via the current keys / market mapping.
    pub(crate) subscription_registry: Arc<Mutex<SubscriptionRegistry>>,
    pub(crate) subscription_summary: Arc<SubscriptionRegistrySummary>,
    pub(crate) subscription_trade_storage_intent:
        Arc<parking_lot::RwLock<Option<TradeStorageIntent>>>,
    /// Delphi `InitDone`: transport auth is already complete, but domain pushes
    /// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`) can only be applied
    /// after the full init bootstrap. Before that, `dispatch_into_active`
    /// drops these channels, like `TMoonProtoNetClient.ClientNewData`.
    pub(crate) domain_ready: bool,
    /// Shared mirror of [`Self::domain_ready`] for `ClientSender`.
    ///
    /// Typed/high-level domain APIs use this gate to record pre-init intent
    /// without putting domain wire commands into send queues before the single
    /// Init pass opens the Delphi `InitDone` gate.
    pub(crate) domain_ready_flag: Arc<AtomicBool>,
    /// Saved intent of the first and only init pass. Needed for post-reconnect
    /// restore without a second Init.
    pub(crate) domain_restore: DomainRestoreIntent,
}

impl Subscriptions {
    pub(crate) fn new_with_registry(
        subscription_registry: Arc<Mutex<SubscriptionRegistry>>,
        subscription_summary: Arc<SubscriptionRegistrySummary>,
        subscription_trade_storage_intent: Arc<parking_lot::RwLock<Option<TradeStorageIntent>>>,
        domain_ready_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            subscription_registry,
            subscription_summary,
            subscription_trade_storage_intent,
            domain_ready: false,
            domain_ready_flag,
            domain_restore: DomainRestoreIntent::default(),
        }
    }
}

/// Post-reconnect index/subscription restore bookkeeping carved out of
/// [`super::Client`].
///
/// Groups the markets-indexes restore state (`tracked_indexes_peer_app_token`,
/// the in-flight fetch guard `indexes_fetch_in_flight`/`indexes_fetch_started_ms`,
/// and the deferred `update_markets_after_indexes`/`restore_orderbooks_after_indexes`
/// flags), the all-trades reconnect clocks/requests
/// (`last_trades_reconnect_check_ms`, `last_trades_subscribe_request_ms`,
/// `pending_trades_unsubscribe`, `pending_trades_resubscribe_after_ms`,
/// `last_trades_tick_ms`), and the orderbook reconnect clocks/requests
/// (`subscribed_book_server_token`, `last_book_reconnect_check_ms`,
/// `last_orderbook_subscribe_request_ms`, `last_orderbook_subscribe_request_uid`,
/// `pending_orderbook_resubscribe_uid`). Field names, types, and meaning are
/// unchanged from when they lived directly on `Client`.
///
/// The three `Arc<Atomic*>` request clocks (`last_trades_subscribe_request_ms`,
/// `last_orderbook_subscribe_request_ms`, `last_orderbook_subscribe_request_uid`)
/// are shared with `ClientSender` via `Arc::clone`; the rest are owned outright.
pub(crate) struct ReconnectRestore {
    /// The previous PeerAppToken that was registered with `MarketsState.indexes_synchronized = true`.
    /// Used in handshake/Ping processing to detect a server restart:
    /// if incoming `peer_app_token != tracked_peer_app_token` — mark the indexes stale.
    /// 0 = no successful synchronization yet (init state).
    pub(crate) tracked_indexes_peer_app_token: u64,

    /// `true` if the init/API layer already sent a markets indexes request and is waiting for the response.
    /// Guards against a storm of repeated explicit requests before a response arrives.
    pub(crate) indexes_fetch_in_flight: bool,

    /// When (`now_ms`) the last `api_get_markets_indexes` was sent. Used for
    /// timeout protection: the UDP response may have been lost — after `INDEXES_FETCH_TIMEOUT_MS`
    /// we reset `indexes_fetch_in_flight = false`. The timeout handler itself does not
    /// resend the request: a new send is allowed only from the init/API layer.
    pub(crate) indexes_fetch_started_ms: i64,

    /// On reconnect restore: as soon as a fresh `GetMarketsIndexes` arrives
    /// successfully, immediately request `UpdateMarketsList`. This reproduces the
    /// Delphi meaning of `TMoonProtoEngine.UpdateMarketsList`: on a new `PeerAppToken`
    /// it first synchronizes indexes, then refreshes prices/funding.
    pub(crate) update_markets_after_indexes: bool,

    /// On reconnect restore: deferred replay of the orderbook registry until a fresh
    /// `GetMarketsIndexes`. Delphi `CheckBookTopics` returns early while
    /// `FLastServerAppToken <> PeerAppToken`; orderbook subscriptions cannot be replayed
    /// before the new server app session's indexes are synchronized.
    pub(crate) restore_orderbooks_after_indexes: bool,

    /// Delphi `TMoonProtoEngine.LastReconnectCheck` for AllTrades reconnect.
    /// `NeedReconnectAllTrades` spends this throttle before it runs the
    /// unsubscribe/sleep/subscribe sequence again.
    pub(crate) last_trades_reconnect_check_ms: i64,

    /// Last queued `emk_SubscribeAllTrades` request, including requests queued
    /// through `ClientSender`. Delphi `SubscribeAllTrades` blocks inside
    /// `SendAndWait` for `FTimeout=12000`, so `NeedReconnectAllTrades` cannot
    /// run while that request is in flight. Rust queues it asynchronously,
    /// therefore this timestamp is part of the machine-effect gate.
    pub(crate) last_trades_subscribe_request_ms: Arc<AtomicI64>,

    /// Delphi `TMoonProtoEngine.FSubscribedBookServerToken`: current
    /// `ServerToken` confirmed by a successful full `BookSubbed` batch subscribe.
    pub(crate) subscribed_book_server_token: u64,

    /// Delphi `TMoonProtoEngine.LastBookReconnectCheck`: 5s throttle for
    /// `NeedResubscribeOrderBooks`.
    pub(crate) last_book_reconnect_check_ms: i64,

    /// Last queued `emk_SubscribeOrderBook` request. Delphi
    /// `DoSubscribeOrderBooks` blocks in `SendAndWait` for `FTimeout=12000`;
    /// Rust queues orderbook subscribes asynchronously, so reconnect retry must
    /// not issue a second batch until the Delphi-equivalent wait window ends or
    /// a response closes it.
    pub(crate) last_orderbook_subscribe_request_ms: Arc<AtomicI64>,
    pub(crate) last_orderbook_subscribe_request_uid: Arc<AtomicU64>,

    /// UID of the last full-registry `emk_SubscribeOrderBook` replay. A success
    /// for this UID, unlike a normal one-market subscribe, is allowed to advance
    /// `subscribed_book_server_token`.
    pub(crate) pending_orderbook_resubscribe_uid: Option<u64>,

    /// Current hard-session token confirmed by a successful candle subscribe.
    pub(crate) subscribed_candle_server_token: u64,
    /// 5s retry throttle for full per-market candle subscription replay.
    pub(crate) last_candle_reconnect_check_ms: i64,
    /// Last queued candle subscribe, shared with `ClientSender`.
    pub(crate) last_candle_subscribe_request_ms: Arc<AtomicI64>,
    /// Candle subscribe batch still inside the Delphi `SendAndWait` window.
    pub(crate) pending_candle_subscribes: Arc<Mutex<PendingCandleSubscribes>>,

    /// ServerToken for which a report page response verified the subscription.
    pub(crate) subscribed_report_server_token: AtomicU64,
    /// ServerToken for which the append-only report schema was validated.
    pub(crate) report_schema_server_token: AtomicU64,
    /// Last report schema request send time.
    pub(crate) last_report_schema_request_ms: AtomicI64,
    /// Last report catch-up request send time.
    pub(crate) last_report_sync_request_ms: AtomicI64,
    /// Current report page request UID; only its matching page may open the
    /// application apply barrier.
    pub(crate) pending_report_sync_uid: AtomicU64,
    /// ServerToken under which `pending_report_sync_uid` was sent.
    pub(crate) pending_report_server_token: AtomicU64,
    /// Request UID of the page currently owned by the application DB writer.
    pub(crate) report_page_waiting_apply_uid: AtomicU64,
    /// Last open-row check send time and hard-session watermark.
    pub(crate) last_report_check_request_ms: AtomicI64,
    pub(crate) pending_report_check_server_token: AtomicU64,
    pub(crate) subscribed_report_check_server_token: AtomicU64,

    /// Delayed `DoSubscribeAllTrades(false)` after Delphi `Sleep(100)` in
    /// `BMarketHistoryWorker.Execute` reconnect branch.
    ///
    /// The sleep starts only after `UnSubscribeAllTrades` has completed its
    /// Delphi `SendAndWait` equivalent. Sending Subscribe after a naked 100ms
    /// timer is wrong on UDP: a retried Unsubscribe can arrive after Subscribe
    /// and leave the server-side client unsubscribed.
    pub(crate) pending_trades_unsubscribe: Option<PendingTradesUnsubscribe>,
    pub(crate) pending_trades_resubscribe_after_ms: Option<i64>,

    /// When `trades_state.tick()` was last called from the active main loop.
    /// Throttle ~100ms — matches the periodicity of Delphi
    /// `MoonProtoEngine.pas:1483 CheckMissingTradesPackets`.
    pub(crate) last_trades_tick_ms: i64,
}

impl ReconnectRestore {
    pub(crate) fn new(
        last_trades_subscribe_request_ms: Arc<AtomicI64>,
        last_orderbook_subscribe_request_ms: Arc<AtomicI64>,
        last_orderbook_subscribe_request_uid: Arc<AtomicU64>,
        last_candle_subscribe_request_ms: Arc<AtomicI64>,
        pending_candle_subscribes: Arc<Mutex<PendingCandleSubscribes>>,
    ) -> Self {
        Self {
            tracked_indexes_peer_app_token: 0,
            indexes_fetch_in_flight: false,
            indexes_fetch_started_ms: 0,
            update_markets_after_indexes: false,
            restore_orderbooks_after_indexes: false,
            last_trades_reconnect_check_ms: super::constants::NEVER_TIME_MS,
            last_trades_subscribe_request_ms,
            subscribed_book_server_token: 0,
            last_book_reconnect_check_ms: super::constants::NEVER_TIME_MS,
            last_orderbook_subscribe_request_ms,
            last_orderbook_subscribe_request_uid,
            pending_orderbook_resubscribe_uid: None,
            subscribed_candle_server_token: 0,
            last_candle_reconnect_check_ms: super::constants::NEVER_TIME_MS,
            last_candle_subscribe_request_ms,
            pending_candle_subscribes,
            subscribed_report_server_token: AtomicU64::new(0),
            report_schema_server_token: AtomicU64::new(0),
            last_report_schema_request_ms: AtomicI64::new(super::constants::NEVER_TIME_MS),
            last_report_sync_request_ms: AtomicI64::new(super::constants::NEVER_TIME_MS),
            pending_report_sync_uid: AtomicU64::new(0),
            pending_report_server_token: AtomicU64::new(0),
            report_page_waiting_apply_uid: AtomicU64::new(0),
            last_report_check_request_ms: AtomicI64::new(super::constants::NEVER_TIME_MS),
            pending_report_check_server_token: AtomicU64::new(0),
            subscribed_report_check_server_token: AtomicU64::new(0),
            pending_trades_unsubscribe: None,
            pending_trades_resubscribe_after_ms: None,
            last_trades_tick_ms: i64::MIN / 2,
        }
    }
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
        assert!(empty.live_candles.is_empty());
        assert!(empty.live_candle_timeframes.is_empty());
        assert_eq!(empty.live_candles_kind, None);

        // HashSet insertion order is non-deterministic; the snapshot must sort.
        reg.orderbook_subs.insert("ETHUSDT".to_string());
        reg.orderbook_subs.insert("BTCUSDT".to_string());
        reg.trades_sub = Some(TradesSubscription { want_mm: true });
        reg.mm_orders_sub = Some(true);
        reg.candle_subs
            .insert("ETHUSDT".to_string(), DeepHistoryKind::Hour4);
        reg.candle_subs
            .insert("BTCUSDT".to_string(), DeepHistoryKind::Hour1);

        let active = reg.active_subscriptions();
        assert_eq!(
            active.orderbooks,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        assert_eq!(
            active.all_trades,
            Some(TradesSubscription { want_mm: true })
        );
        assert!(active.mm_orders);
        assert_eq!(
            active.live_candles,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        assert_eq!(
            active.live_candle_timeframes,
            vec![
                LiveCandleSubscription {
                    market: "BTCUSDT".to_string(),
                    kind: DeepHistoryKind::Hour1,
                },
                LiveCandleSubscription {
                    market: "ETHUSDT".to_string(),
                    kind: DeepHistoryKind::Hour4,
                },
            ]
        );
        assert_eq!(active.live_candles_kind, None);
    }
}

// =============================================================================
