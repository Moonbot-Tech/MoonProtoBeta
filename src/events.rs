//! Event dispatcher: typed application events and read-only state on top of raw
//! MoonProto channel payloads.
//!
//! Instead of making applications parse every protocol channel and apply every
//! payload to their own state models, `EventDispatcher` performs that work
//! automatically:
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
//! states) are owned by the dispatcher and exposed through read-only getters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::app_queue::AppQueue;
use crate::commands::arb::ArbPayload;
use crate::commands::engine_api::{EngineResponse, ServerInfo};
use crate::commands::trade::{OrderType, TradeCtx};
use crate::commands::ui::ClientSettingsCommand;
use crate::protocol::Command;
use crate::state::{
    BalanceEvent, BalancesState, Candle5mRow, MarketDerivedSnapshot, MarketHistoryCandlesSnapshot,
    MarketHistoryConfig, MarketHistoryHandle, MarketHistoryReaders, MarketHistoryWorker,
    MarketsEvent, MarketsState, OrderBookEvent, OrderBooks, OrderEvent, Orders,
    RollingTradeVolumeSnapshot, SettingsEvent, SettingsState, StratEvent, StratsState,
    TradeStorageScope, TradesEvent, TradesState,
};

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
pub use snapshot::EventDispatcherSnapshot;
pub use types::{
    Event, MissingOrderStatusRequest, StrategySnapshotReply, WatcherFillEvent, WatcherFillsEvent,
};

const DELPHI_PLATFORM_FGATE: u8 = 9;

fn copy_max_leverage_from_markets_list(info: &ServerInfo) -> bool {
    info.exchange_code == Some(DELPHI_PLATFORM_FGATE)
}

/// State bundle + dispatch logic.
///
/// The dispatcher owns all channel state and exposes it read-only through
/// getters [`Self::orders`], [`Self::order_books`], [`Self::trades`],
/// [`Self::balances`], [`Self::strats`], [`Self::settings`], [`Self::markets`].
/// Applications should not mutate protocol state directly; state is maintained
/// by [`Self::dispatch`], [`Self::dispatch_into`], and the active action
/// outbox path used by `MoonClient` and low-level active runtimes.
pub struct EventDispatcher {
    pub(crate) orders: Orders,
    pub(crate) order_books: OrderBooks,
    pub(crate) trades: TradesState,
    pub(crate) balances: BalancesState,
    pub(crate) strats: StratsState,
    pub(crate) settings: SettingsState,
    pub(crate) markets: MarketsState,
    /// Delphi `cfg.ServerStratEpoch` for snapshots sent by this client.
    /// Do not confuse it with `StratsState::last_server_epoch`, which mirrors
    /// Delphi `cfg.LocalStratEpoch` after receiving a server snapshot.
    local_strategy_epoch: u64,
    /// Последний известный `ServerToken` — для детектирования hard reconnect.
    /// При смене токена `dispatch_into_active` сбрасывает per-token state
    /// (`trades.full_reset()`, `order_books.reset_caches_keep_books()`) до применения нового пакета.
    /// Иначе stale `last_packet_num` / `expected_seq` в старой нумерации новой
    /// сессии порождает spurious `GapDetected` события и corrupted orderbook display
    /// в первые секунды. Аналог Delphi `MoonProtoEngine.pas:1586-1591`
    /// (`If FTradesServerToken <> MClient.Client.ServerToken then ResetGapBuckets`) +
    /// `MoonProtoEngine.pas:316-318` (`If NeedResubscribeOrderBooks then ResetOrderBookCaches`).
    /// Init=0 (никогда не подключались) → первый non-zero token не триггерит сброс.
    /// См. audit_responsibility_hints #1, #2.
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
    /// Per-Client `ServerTimeDelta` source. Если `Some` — `dispatch_into` для
    /// `Command::Order` читает delta отсюда (multi-Client safe). Если `None` —
    /// fallback на global `SERVER_TIME_DELTA_DAYS` для raw `dispatch_into`
    /// потребителей без линковки. См. `DEVIATION.md #23`.
    ///
    /// Привязка: либо явный вызов [`Self::set_server_time_delta_source`] с
    /// `client.server_time_delta_handle()`, либо автоматически через active
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
    /// Events produced while a one-shot helper is pumping the client loop.
    ///
    /// One-shot helpers (`run_until_response`, `request_*`) have no callback
    /// argument, so they store produced events here for the application to drain
    /// after the helper returns.
    queued_events: AppQueue<Event>,
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
    last_market_history_scope: Option<TradeStorageScope>,
    last_market_history_markets_version: Option<u64>,
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self {
            orders: Orders::default(),
            order_books: OrderBooks::default(),
            trades: TradesState::default(),
            balances: BalancesState::default(),
            strats: StratsState::default(),
            settings: SettingsState::default(),
            markets: MarketsState::default(),
            local_strategy_epoch: 0,
            last_known_server_token: 0,
            last_markets_list_refresh_ms: 0,
            force_markets_list_refresh: false,
            trades_server_token: 0,
            server_time_delta_source: None,
            strategy_snapshot_provider: None,
            pending_strategy_snapshot_request_uid: None,
            queued_events: AppQueue::default(),
            market_history: None,
            owned_market_history: None,
            market_history_auto_enabled: true,
            trade_storage_scope: None,
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
    /// client path uses `drain_deferred_order_removals_due` so `SelLDone` keeps
    /// Delphi's extra 400 ms final-trace window.
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

    /// Read-only trades-stream state: packet counters, gap buckets, and resend
    /// bookkeeping.
    pub fn trades(&self) -> &TradesState {
        &self.trades
    }

    pub(crate) fn trades_server_token(&self) -> u64 {
        self.trades_server_token
    }

    /// Read-only balance state for account totals and per-market balances.
    pub fn balances(&self) -> &BalancesState {
        &self.balances
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

    /// Events produced by one-shot helpers and not yet drained by the
    /// application.
    ///
    /// Low-level custom runtimes may deliver events to their callback
    /// immediately and skip this queue. The queue is only for helper-driven
    /// waits such as `Client::run_until_response`,
    /// `request_client_settings`, `request_order_snapshot`, and typed
    /// `request_*` Engine API helpers.
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

    /// Текущее значение `ServerTimeDelta` (days). Если установлен per-Client
    /// source — берёт оттуда; иначе fallback на global.
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
        // Convenience-обёртка над `dispatch_into`.
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
            Command::Balance => self.client_new_data_balance(payload, out),
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
