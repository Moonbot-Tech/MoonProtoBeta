//! Typed application events and read-only state on top of raw MoonProto channel
//! payloads.
//!
//! Instead of making applications parse every protocol channel and apply every
//! payload to their own state models, `MoonClient` performs that work inside
//! its owned runtime and publishes events plus immutable snapshots:
//!
//! ```ignore
//! let client = moonproto::MoonClient::connect(cfg, connect)?;
//!
//! for event in client.drain_events() {
//!     match event {
//!         moonproto::Event::Order(order_event) => { /* update order UI */ }
//!         moonproto::Event::OrderBook(book_event) => { /* redraw book */ }
//!         moonproto::Event::Trade(trade_event) => { /* read retained history */ }
//!         _ => {}
//!     }
//! }
//!
//! if let Some(snapshot) = client.snapshot() {
//!     for order in snapshot.orders().iter() {
//!         /* render order row */
//!     }
//! }
//! ```
//!
//! State models (`Orders`, `OrderBooks`, `TradesState`, and the other channel
//! states) are owned by the runtime and exposed through read-only snapshots.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::app_queue::AppQueue;
use crate::commands::engine_api::{
    parse_update_transfer_assets_response, AuthCheckResponse, EngineMethod, EngineResponse,
    ServerInfo,
};
use crate::commands::market::ExchangeCode;
use crate::commands::trade::{OrderType, TradeCtx};
use crate::commands::ui::ClientSettingsCommand;
use crate::protocol::Command;
use crate::state::eps::EpsProfile;
use crate::state::{
    AccountEvent, AccountState, BalanceEvent, BalancesState, Candle5mRow, MarketDerivedSnapshot,
    MarketHistoryCandlesSnapshot, MarketHistoryConfig, MarketHistoryHandle, MarketHistoryReaders,
    MarketHistoryWorker, MarketsEvent, MarketsState, OrderBookEvent, OrderBooks, OrderEvent,
    Orders, RollingTradeVolumeSnapshot, SettingsEvent, SettingsState, StratEvent, StratsState,
    TradeStorageScope, TradesEvent, TradesState, TransferAssetsEvent, TransferAssetsState,
};
use std::ops::{Deref, DerefMut};

mod active;
mod api;
mod balance;
mod history;
mod local_strats;
mod order_book;
mod orders;
mod snapshot;
mod strat;
mod trades;
mod types;
mod ui;

pub(crate) use active::{ActiveAction, ActiveDispatchContext};
#[doc(hidden)]
pub use snapshot::EventDispatcherSnapshot;
pub use snapshot::MoonStateSnapshot;
pub use types::{
    ArbEvent, EngineActionEvent, EngineActionKind, Event, MissingOrderStatusRequest,
    StrategySnapshotReply, WatcherFillEvent, WatcherFillsEvent,
};

fn copy_max_leverage_from_markets_list(info: &ServerInfo) -> bool {
    info.exchange_code == Some(ExchangeCode::FGate)
}

#[derive(Debug)]
pub(crate) struct CowState<T: Clone>(Arc<T>);

impl<T: Clone> CowState<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T: Clone> Clone for CowState<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Clone + Default> Default for CowState<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> Deref for CowState<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Clone> DerefMut for CowState<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

#[cfg(test)]
impl<T: Clone> CowState<T> {
    /// Identity of the backing allocation, for copy-on-write regression tests.
    ///
    /// Two `CowState` values share state iff this pointer is equal. A hot apply
    /// path that keeps the pointer stable while a snapshot clone is alive proves
    /// it did not trigger `Arc::make_mut` (no per-packet container deep clone).
    pub(crate) fn arc_ptr(&self) -> *const T {
        Arc::as_ptr(&self.0)
    }
}

/// State bundle + dispatch logic.
///
/// The dispatcher owns all channel state and exposes it read-only through
/// getters [`Self::orders`], [`Self::order_books`], [`Self::trades_recovery`],
/// [`Self::balances`], [`Self::strats`], [`Self::settings`], [`Self::markets`].
/// Applications should not mutate protocol state directly; state is maintained
/// by [`Self::dispatch`], [`Self::dispatch_into`], and the active action
/// outbox path used by `MoonClient`.
#[doc(hidden)]
pub struct EventDispatcher {
    pub(crate) orders: CowState<Orders>,
    pub(crate) order_books: CowState<OrderBooks>,
    pub(crate) trades: CowState<TradesState>,
    pub(crate) account: CowState<AccountState>,
    pub(crate) balances: CowState<BalancesState>,
    pub(crate) transfer_assets: CowState<TransferAssetsState>,
    pub(crate) coin_card_candles: CowState<crate::state::CoinCardCandlesState>,
    pub(crate) strats: CowState<StratsState>,
    pub(crate) settings: CowState<SettingsState>,
    pub(crate) markets: CowState<MarketsState>,
    /// Session identity from `emk_BaseCheck`/`emk_AuthCheck`, pushed by the active
    /// runtime after Init so the published snapshot carries server/account info.
    /// Delphi keeps these in the engine's BaseCheck/AuthCheck state; multi-server
    /// UI and the account screen read them. Default (all-`None`) before BaseCheck.
    session_server_info: std::sync::Arc<ServerInfo>,
    session_auth_info: Option<std::sync::Arc<AuthCheckResponse>>,
    /// Delphi `cfg.ServerStratEpoch` for snapshots sent by this client.
    /// Do not confuse it with `StratsState::last_server_epoch`, which mirrors
    /// Delphi `cfg.LocalStratEpoch` after receiving a server snapshot.
    local_strategy_epoch: u64,
    /// Last known `ServerToken` — for detecting a hard reconnect.
    /// On a token change, `dispatch_into_active` resets per-token state
    /// (`trades.full_reset()`, `order_books.reset_caches_keep_books()`) before applying the new packet.
    /// Otherwise stale `last_packet_num` / `expected_seq` from the old numbering of the new
    /// session produces spurious `GapDetected` events and a corrupted orderbook display
    /// in the first seconds. Analogous to Delphi `MoonProtoEngine.pas:1586-1591`
    /// (`If FTradesServerToken <> MClient.Client.ServerToken then ResetGapBuckets`) +
    /// `MoonProtoEngine.pas:316-318` (`If NeedResubscribeOrderBooks then ResetOrderBookCaches`).
    /// Init=0 (never connected) → the first non-zero token does not trigger a reset.
    /// See audit_responsibility_hints #1, #2.
    last_known_server_token: u64,
    /// Delphi `Bworks.pas LastAddedNewMarket` analogue for active-lib
    /// `NewMarketFound -> GetMarketsList` auto refresh.
    last_markets_list_refresh_ms: i64,
    /// Delphi `Bworks.pas MustCheckLIstingFromServer`: set by inbound
    /// `TNewMarketNotifyCommand` and bypasses the 30s listing-refresh throttle
    /// for one `GetMarketsList` request.
    force_markets_list_refresh: bool,
    /// Delphi `FTradesServerToken`: updated only when a `TradesStream` packet
    /// reaches the trades parser after the market-index gate. Reconnect restore
    /// uses this to decide whether `SubscribeAllTrades` actually produced a
    /// stream for the current `Client.ServerToken`.
    trades_server_token: u64,
    /// Per-Client `ServerTimeDelta` source. If `Some`, `dispatch_into` for
    /// `Command::Order` reads the delta from here (multi-Client safe). If `None`,
    /// it falls back to the global `SERVER_TIME_DELTA_DAYS` for raw `dispatch_into`
    /// consumers that are not linked.
    ///
    /// Binding: either an explicit call to [`Self::set_server_time_delta_source`] with
    /// `client.server_time_delta_handle()`, or automatically via the active
    /// runtime path.
    server_time_delta_source: Option<Arc<AtomicU64>>,
    /// Optional override for fresh application-owned strategies. Without an
    /// override the dispatcher answers from `strats.snapshot_vec()`.
    strategy_snapshot_provider:
        Option<Box<dyn FnMut(u64) -> Option<StrategySnapshotReply> + Send + 'static>>,
    /// Server asked for local strategies before the live `TStratSchema` was
    /// available. Non-empty typed strategy serialization waits for schema so
    /// Rust does not carry a stale hardcoded `TStrategy` field table.
    pending_strategy_snapshot_request_uid: Option<u64>,
    /// Events produced before publication through the runtime event sink.
    ///
    /// Normal applications receive these through `MoonClient`'s `MoonEventSink`
    /// and read state from published snapshots.
    queued_events: AppQueue<Event>,
    /// Reused hot-path buffer for `OrderBooks::on_packet_into`.
    order_book_events: Vec<OrderBookEvent>,
    /// Optional retained-history writer. The dispatcher only queues typed
    /// batches into this handle; the worker owns `MarketHistoryStore`.
    market_history: Option<MarketHistoryHandle>,
    /// Lazily spawned default retained-history worker.
    ///
    /// Delphi has `BMarketHistoryWorker` as part of the active client. Rust also
    /// allows a custom worker/config via `set_market_history_handle`, but the
    /// default active-lib path must not require an extra hidden call after
    /// `subscribe_all_trades`.
    owned_market_history: Option<MarketHistoryWorker>,
    market_history_auto_enabled: bool,
    /// Active Lib retained-storage scope from `Client::subscribe_*trades*`.
    /// `None` means trades stream is not subscribed and retained trade/candle/
    /// derived state must stay disabled.
    trade_storage_scope: Option<TradeStorageScope>,
    /// Delphi `_eps` / `_epsStep` / `_epsM` profile selected from
    /// `ServerInfo::exchange_code`. Hidden from public API; unknown/missing
    /// exchange falls back to the Huobi-class profile.
    eps_profile: EpsProfile,
    last_market_history_scope: Option<TradeStorageScope>,
    last_market_history_markets_version: Option<u64>,
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self {
            orders: CowState::default(),
            order_books: CowState::default(),
            trades: CowState::default(),
            account: CowState::default(),
            balances: CowState::default(),
            transfer_assets: CowState::default(),
            coin_card_candles: CowState::default(),
            strats: CowState::default(),
            settings: CowState::default(),
            markets: CowState::default(),
            session_server_info: std::sync::Arc::new(ServerInfo::default()),
            session_auth_info: None,
            local_strategy_epoch: 0,
            last_known_server_token: 0,
            last_markets_list_refresh_ms: 0,
            force_markets_list_refresh: false,
            trades_server_token: 0,
            server_time_delta_source: None,
            strategy_snapshot_provider: None,
            pending_strategy_snapshot_request_uid: None,
            queued_events: AppQueue::default(),
            order_book_events: Vec::new(),
            market_history: None,
            owned_market_history: None,
            market_history_auto_enabled: true,
            trade_storage_scope: None,
            eps_profile: EpsProfile::default(),
            last_market_history_scope: None,
            last_market_history_markets_version: None,
        }
    }
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    fn parse_failed(cmd: Command, payload: &[u8]) -> Event {
        Event::ParseFailed {
            cmd,
            len: payload.len(),
            payload: payload.to_vec(),
        }
    }

    fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        if self.eps_profile == eps_profile {
            return;
        }
        self.eps_profile = eps_profile;
        self.orders.set_eps_profile(eps_profile);
        self.order_books.set_eps_profile(eps_profile);
        self.markets.set_eps_profile(eps_profile);
        if let Some(handle) = &self.market_history {
            handle.set_eps_profile(eps_profile);
        }
    }

    /// Push session identity (BaseCheck/AuthCheck) into the dispatcher so the
    /// published snapshot exposes server/account info. The active runtime calls
    /// this once Init completes (and after reconnect re-auth).
    pub(crate) fn set_session_identity(
        &mut self,
        server_info: ServerInfo,
        auth_info: Option<AuthCheckResponse>,
    ) {
        self.session_server_info = std::sync::Arc::new(server_info);
        self.session_auth_info = auth_info.map(std::sync::Arc::new);
    }

    /// Read-only order state, keyed by server order UID.
    ///
    /// It is updated automatically when order-channel payloads are dispatched.
    pub fn orders(&self) -> &Orders {
        &self.orders
    }

    /// Mutable order state for local Delphi-equivalent UI side effects.
    ///
    /// Normal receive updates still go through `dispatch_into_active`; this is
    /// exposed for outgoing actions such as `Client::set_immune`, where Delphi
    /// mutates the local worker before sending a command to the server.
    #[doc(hidden)]
    pub fn orders_mut(&mut self) -> &mut Orders {
        &mut self.orders
    }

    /// Build Delphi `CleanupMissingWorkers` follow-up requests for raw
    /// dispatcher users after `OrderEvent::Snapshot`.
    ///
    /// The active client path consumes the same helper internally and sends the
    /// returned `TOrderStatusRequest`s automatically. Raw `dispatch_into` has no
    /// `Client` handle by design, so the caller must decide whether to send
    /// them through `Client::request_order_status`.
    pub fn missing_order_status_requests_after_snapshot(&self) -> Vec<MissingOrderStatusRequest> {
        self.orders
            .missing_after_snapshot()
            .into_iter()
            .filter_map(|uid| {
                self.orders.get(uid).map(|order| MissingOrderStatusRequest {
                    ctx: order.trade_ctx(),
                    market_name: order.market_name.clone(),
                })
            })
            .collect()
    }

    /// Drain deferred order removals after a reader-decoded batch.
    ///
    /// Delphi queues terminal/order-not-found effects into `BOrderWorker` and
    /// removes the worker from `WCache` later, not inside
    /// `ProcessCommandOrder` itself. The dispatcher mirrors that by letting
    /// terminal orders remain addressable until the caller explicitly flushes
    /// them, then emitting `OrderEvent::Removed` from this step. The active
    /// client path uses `drain_deferred_order_removals_due` so `SellDone`
    /// keeps Delphi's extra 400 ms final-trace window.
    pub fn drain_deferred_order_removals(&mut self, out: &mut Vec<Event>) {
        for uid in self.orders.drain_pending_removals() {
            out.push(Event::Order(OrderEvent::Removed(uid)));
        }
    }

    pub(crate) fn drain_deferred_order_removals_due(&mut self, now_ms: i64, out: &mut Vec<Event>) {
        for uid in self.orders.drain_pending_removals_due(now_ms) {
            out.push(Event::Order(OrderEvent::Removed(uid)));
        }
    }

    /// Read-only orderbook state, including per-market/per-kind recovery
    /// caches and the latest applied books.
    pub fn order_books(&self) -> &OrderBooks {
        &self.order_books
    }

    pub(crate) fn reset_orderbook_caches_keep_books(&mut self) {
        self.order_books.reset_caches_keep_books();
    }

    /// Read-only trades-stream *recovery* state: packet counters, gap buckets,
    /// and resend bookkeeping. This is not the trade rows — read the retained
    /// trade history from the market history readers instead.
    pub fn trades_recovery(&self) -> &TradesState {
        &self.trades
    }

    /// Read-only account-level state such as hedge mode and API-key expiration.
    ///
    /// Delphi updates these from worker paths; Active Lib exposes the retained
    /// state here and lets UI request refreshes asynchronously through
    /// `MoonClient`.
    pub fn account(&self) -> &AccountState {
        &self.account
    }

    pub(crate) fn trades_server_token(&self) -> u64 {
        self.trades_server_token
    }

    /// Read-only balance state for account totals and per-market balances.
    pub fn balances(&self) -> &BalancesState {
        &self.balances
    }

    /// Read-only transferable asset lists by wallet kind.
    ///
    /// These are not market balances. They mirror Delphi `Markets.FAssets` and
    /// are refreshed asynchronously through `MoonClient::refresh_transfer_assets`.
    pub fn transfer_assets(&self) -> &TransferAssetsState {
        &self.transfer_assets
    }

    /// Demand-driven CoinCard candles loaded through
    /// `MoonClient::request_coin_card_candles`.
    ///
    /// This is separate from retained 5m market history.
    pub fn coin_card_candles(&self) -> &crate::state::CoinCardCandlesState {
        &self.coin_card_candles
    }

    /// Apply one async `emk_UpdateTransferAssets` response to Active Lib state.
    pub fn apply_transfer_assets_response(
        &mut self,
        kind: crate::state::ExchangeKind,
        resp: EngineResponse,
    ) -> bool {
        let event = if resp.method != EngineMethod::UpdateTransferAssets {
            TransferAssetsEvent::UpdateFailed {
                kind,
                request_uid: Some(resp.request_uid),
                error: format!("unexpected EngineMethod {:?}", resp.method),
            }
        } else if !resp.success {
            TransferAssetsEvent::UpdateFailed {
                kind,
                request_uid: Some(resp.request_uid),
                error: format!("server error {} {}", resp.error_code, resp.error_msg.trim()),
            }
        } else if let Some(assets) = parse_update_transfer_assets_response(&resp.data) {
            self.transfer_assets
                .apply_update(kind, resp.request_uid, assets)
        } else {
            TransferAssetsEvent::UpdateFailed {
                kind,
                request_uid: Some(resp.request_uid),
                error: format!("parse failed data_len={}", resp.data.len()),
            }
        };
        let changed = matches!(event, TransferAssetsEvent::Updated { .. });
        self.queued_events.extend([Event::TransferAssets(event)]);
        changed
    }

    pub(crate) fn transfer_assets_request_failed(
        &mut self,
        kind: crate::state::ExchangeKind,
        error: impl Into<String>,
    ) {
        self.queued_events
            .extend([Event::TransferAssets(TransferAssetsEvent::UpdateFailed {
                kind,
                request_uid: None,
                error: error.into(),
            })]);
    }

    /// Apply one async `emk_QueryHedgeMode` response to Active Lib account
    /// state.
    pub fn apply_hedge_mode_response(&mut self, resp: EngineResponse) -> bool {
        let event = self.account.apply_hedge_mode_response(resp);
        let changed = matches!(event, AccountEvent::HedgeModeUpdated { .. });
        self.queued_events.extend([Event::Account(event)]);
        changed
    }

    pub(crate) fn hedge_mode_request_failed(
        &mut self,
        request_uid: Option<u64>,
        error: impl Into<String>,
    ) {
        let event = self.account.hedge_mode_request_failed(request_uid, error);
        self.queued_events.extend([Event::Account(event)]);
    }

    /// Apply one async `emk_CheckAPIExpirationTime` response to Active Lib
    /// account state.
    pub fn apply_api_expiration_response(&mut self, resp: EngineResponse) -> bool {
        let event = self.account.apply_api_expiration_response(resp);
        let changed = matches!(event, AccountEvent::ApiExpirationUpdated { .. });
        self.queued_events.extend([Event::Account(event)]);
        changed
    }

    pub(crate) fn api_expiration_request_failed(
        &mut self,
        request_uid: Option<u64>,
        error: impl Into<String>,
    ) {
        let event = self
            .account
            .api_expiration_request_failed(request_uid, error);
        self.queued_events.extend([Event::Account(event)]);
    }

    /// Apply one async `emk_GetCoinCardCandles` response to demand-driven
    /// CoinCard candle state.
    ///
    /// Regular applications should call
    /// `MoonClient::request_coin_card_candles`; the runtime calls this method
    /// after receiving the matching server response.
    pub fn apply_coin_card_candles_response(
        &mut self,
        market: String,
        kind: crate::commands::candles::DeepHistoryKind,
        resp: EngineResponse,
    ) -> bool {
        let event = if resp.method != EngineMethod::GetCoinCardCandles {
            crate::state::CoinCardCandlesEvent::UpdateFailed {
                market,
                kind,
                request_uid: Some(resp.request_uid),
                error: format!("unexpected EngineMethod {:?}", resp.method),
            }
        } else if !resp.success {
            crate::state::CoinCardCandlesEvent::UpdateFailed {
                market,
                kind,
                request_uid: Some(resp.request_uid),
                error: format!("server error {} {}", resp.error_code, resp.error_msg.trim()),
            }
        } else if let Some(candles) =
            crate::commands::candles::parse_coin_card_candles_response(&resp.data)
        {
            self.coin_card_candles
                .apply_update(market, kind, resp.request_uid, candles)
        } else {
            crate::state::CoinCardCandlesEvent::UpdateFailed {
                market,
                kind,
                request_uid: Some(resp.request_uid),
                error: format!("parse failed data_len={}", resp.data.len()),
            }
        };
        let changed = matches!(event, crate::state::CoinCardCandlesEvent::Updated { .. });
        self.queued_events.extend([Event::CoinCardCandles(event)]);
        changed
    }

    pub(crate) fn coin_card_candles_request_failed(
        &mut self,
        market: String,
        kind: crate::commands::candles::DeepHistoryKind,
        request_uid: Option<u64>,
        error: impl Into<String>,
    ) {
        self.queued_events.extend([Event::CoinCardCandles(
            crate::state::CoinCardCandlesEvent::UpdateFailed {
                market,
                kind,
                request_uid,
                error: error.into(),
            },
        )]);
    }

    pub(crate) fn queue_engine_action_response(
        &mut self,
        kind: EngineActionKind,
        resp: EngineResponse,
    ) {
        if resp.success {
            if let EngineActionKind::TransferAsset {
                asset,
                qty,
                from,
                to,
            } = &kind
            {
                let ev =
                    self.transfer_assets
                        .apply_transfer(asset, *qty, *from, *to, resp.request_uid);
                self.queued_events.extend([Event::TransferAssets(ev)]);
            }
        }
        self.queued_events
            .extend([Event::EngineAction(EngineActionEvent {
                kind,
                request_uid: Some(resp.request_uid),
                method: resp.method,
                success: resp.success,
                error_code: resp.error_code,
                error_msg: resp.error_msg,
            })]);
    }

    pub(crate) fn queue_engine_action_disconnected(
        &mut self,
        kind: EngineActionKind,
        request_uid: Option<u64>,
        method: EngineMethod,
        error: impl Into<String>,
    ) {
        self.queued_events
            .extend([Event::EngineAction(EngineActionEvent {
                kind,
                request_uid,
                method,
                success: false,
                error_code: 0,
                error_msg: error.into(),
            })]);
    }

    /// Read-only strategy state and decoded strategy snapshots.
    pub fn strats(&self) -> &StratsState {
        &self.strats
    }

    /// Read-only UI/settings state.
    pub fn settings(&self) -> &SettingsState {
        &self.settings
    }

    /// Seed Delphi `cfg` fallback for old `TClientSettingsCommand` packets.
    ///
    /// Current servers send the full v3 settings snapshot. This matters for
    /// historical/append-only packets: Delphi keeps existing `cfg` values for
    /// missing soft-tail fields, so the active dispatcher needs the same current
    /// settings snapshot before parsing.
    pub fn set_client_settings_fallback(&mut self, fallback: ClientSettingsCommand) {
        self.settings.set_client_settings_fallback(fallback);
    }

    /// Read-only markets state: market catalog, server indexes, prices, and
    /// token tags.
    ///
    /// `markets().indexes_synchronized` gates indexed streams such as
    /// TradesStream and OrderBook after server restarts.
    pub fn markets(&self) -> &MarketsState {
        &self.markets
    }

    /// Events produced by dispatcher state application and not yet published by
    /// the runtime event sink.
    ///
    /// Normal applications use `MoonClient` and never need this queue directly.
    pub fn queued_events(&self) -> &[Event] {
        self.queued_events.as_slice()
    }

    /// Number of currently queued one-shot events.
    pub fn queued_event_count(&self) -> usize {
        self.queued_events.len()
    }

    /// Maximum queued one-shot events observed since dispatcher creation.
    ///
    /// This is diagnostics only. The queue has no fixed capacity and does not
    /// drop old events when this number grows.
    pub fn queued_event_max_count(&self) -> usize {
        self.queued_events.max_len()
    }

    /// Remove and return events accumulated during one-shot waits.
    pub fn take_queued_events(&mut self) -> Vec<Event> {
        self.queued_events.take()
    }

    /// Drop queued one-shot events without processing them.
    pub fn clear_queued_events(&mut self) {
        self.queued_events.clear();
    }

    pub(crate) fn queue_events<I>(&mut self, events: I)
    where
        I: IntoIterator<Item = Event>,
    {
        self.queued_events.extend(events);
    }

    /// Trades-gap recovery tail check.
    ///
    /// It returns serialized `TradesResend` Engine API requests for missing
    /// packet numbers and closes expired gap buckets. Applications do not need
    /// to call this when using [`crate::client::MoonClient`] or the low-level
    /// active runtime path; they call the check after successfully parsed trades
    /// packets.
    ///
    /// Custom loops that bypass the active runtime should call it after a valid
    /// `TradesStream`/`TradesResendResponse` packet, with the current RTT and
    /// monotonic timestamp, then send each returned request through the client.
    pub fn tick_trades(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>> {
        self.trades.tick(rtt_ms, now_ms)
    }

    /// Variant of [`Self::tick_trades`] that also returns tick-generated
    /// [`TradesEvent`] diagnostics.
    pub fn tick_trades_with_events(
        &mut self,
        rtt_ms: i64,
        now_ms: i64,
    ) -> (Vec<Vec<u8>>, Vec<TradesEvent>) {
        self.trades.tick_with_events(rtt_ms, now_ms)
    }

    /// Attach this dispatcher to one client's `ServerTimeDelta` handle.
    ///
    /// After this, order-channel dispatch uses that client's time delta instead
    /// of the process-global raw-dispatch fallback. Custom multi-server
    /// runtimes should attach one dispatcher to the matching client. The
    /// high-level [`crate::client::MoonClient`] path handles this internally.
    ///
    /// ```ignore
    /// let client = Client::new(cfg);
    /// let mut dispatcher = EventDispatcher::new();
    /// dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
    /// ```
    pub fn set_server_time_delta_source(&mut self, handle: Arc<AtomicU64>) {
        self.server_time_delta_source = Some(handle);
    }

    /// Current `ServerTimeDelta` value (days). If a per-Client source is set,
    /// it reads from there; otherwise it falls back to the global.
    fn current_server_time_delta(&self) -> f64 {
        match &self.server_time_delta_source {
            Some(handle) => f64::from_bits(handle.load(Ordering::Relaxed)),
            None => crate::client::get_server_time_delta_global(),
        }
    }

    /// Parse one incoming payload, apply it to the matching state model, and
    /// return the produced typed events.
    ///
    /// Most channels produce zero or one event. OrderBook recovery and balance
    /// batches can produce several events for one payload.
    #[must_use = "Events must be processed or application notifications are lost."]
    pub fn dispatch(&mut self, cmd: Command, payload: &[u8], now_ms: i64) -> Vec<Event> {
        // Convenience wrapper over `dispatch_into`.
        let mut out = Vec::new();
        self.dispatch_into(cmd, payload, now_ms, &mut out);
        out
    }

    /// Zero-allocation dispatch path for performance-sensitive consumers.
    ///
    /// Produced events are pushed into the caller-owned `out` buffer. Reuse the
    /// same vector with `clear()` between packets to avoid per-packet
    /// allocations on high-rate streams.
    ///
    /// This method is the low-level parser path. It does not have a `Client`
    /// reference, so it cannot perform client-backed recovery actions.
    /// Normal applications should use [`crate::client::MoonClient`]. Custom
    /// low-level active runtimes must provide the same active actions: gate
    /// stale indexed streams, send orderbook full requests when recovery needs
    /// them, and request missing order statuses after a fresh order snapshot.
    /// If a raw consumer intentionally uses this method, it should call
    /// [`Self::missing_order_status_requests_after_snapshot`] after
    /// `OrderEvent::Snapshot` and send those requests itself.
    ///
    /// ```ignore
    /// let mut buf = Vec::with_capacity(8);
    /// loop {
    ///     buf.clear();
    ///     dispatcher.dispatch_into(cmd, payload, now_ms, &mut buf);
    ///     for ev in &buf { /* handle */ }
    /// }
    /// ```
    pub fn dispatch_into(
        &mut self,
        cmd: Command,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
    ) {
        self.dispatch_into_with_history(cmd, payload, now_ms, None, out);
    }

    fn dispatch_into_with_history(
        &mut self,
        cmd: Command,
        payload: &[u8],
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        match cmd {
            Command::Order => self.client_new_data_order(payload, now_ms, out),
            Command::OrderBook => self.client_new_data_order_book(payload, now_ms, out),
            Command::TradesStream => {
                self.client_new_data_trades_stream(payload, now_ms, history_now_time_days, out)
            }
            Command::TradesResendResponse => self.client_new_data_trades_resend_response(
                payload,
                now_ms,
                history_now_time_days,
                out,
            ),
            Command::Balance => self.client_new_data_balance(payload, history_now_time_days, out),
            Command::Strat => self.client_new_data_strat(payload, out),
            Command::UI => self.client_new_data_ui(payload, out),
            Command::API => self.client_new_data_api(payload, history_now_time_days, out),
            Command::LogMsg => self.client_new_data_log_msg(payload, out),
            _ => out.push(Event::Raw {
                cmd,
                payload: payload.to_vec(),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
