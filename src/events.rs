//! Event dispatcher: typed application events and read-only state on top of raw
//! MoonProto channel payloads.
//!
//! Instead of making applications parse every protocol channel and apply every
//! payload to their own state models, `EventDispatcher` performs that work
//! automatically:
//!
//! ```ignore
//! use moonproto::events::{EventDispatcher, Event};
//! use moonproto::state::{OrderEvent, OrderBookEvent, TradesEvent};
//!
//! let mut dispatcher = EventDispatcher::new();
//! client.on_data(move |cmd, payload| {
//!     for ev in dispatcher.dispatch(cmd, payload, now_ms()) {
//!         match ev {
//!             Event::Order(OrderEvent::Created(uid)) => { /* show new order */ }
//!             Event::OrderBook(OrderBookEvent::Apply { market_index, .. }) => { /* redraw */ }
//!             Event::Trade(TradesEvent::Applied { packet_num, .. }) => {
//!                 /* read new rows from market state / SeqRing */
//!                 let _ = packet_num;
//!             }
//!             _ => {}
//!         }
//!     }
//! });
//! ```
//!
//! State models (`Orders`, `OrderBooks`, `TradesState`, and the other channel
//! states) are owned by the dispatcher and exposed through read-only getters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::app_queue::AppQueue;
use crate::commands::arb::{parse_arb_payload_compact, parse_arb_prices, ArbPayload};
use crate::commands::balance::parse_balance;
use crate::commands::engine_api::{
    parse_base_check_response, parse_engine_response, EngineMethod, EngineResponse, ServerInfo,
};
use crate::commands::market::parse_markets_indexes_response;
use crate::commands::order_book::parse_order_book_packet;
use crate::commands::registry::decode_utf8_delphi;
use crate::commands::strat::StratCommand;
use crate::commands::strategy_schema::StrategySchema;
use crate::commands::strategy_serializer::StrategySnapshot;
use crate::commands::trade::{AllStatuses, OrderType, TradeCommand, TradeCtx};
use crate::commands::ui::{ClientSettingsCommand, UICommand};
use crate::protocol::Command;
use crate::state::markets::MarketLastPriceHistoryInput;
use crate::state::orders::OrderCancelSend;
use crate::state::{
    ApplyResult, BalanceEvent, BalancesState, Candle5mRow, MarketDerivedSnapshot,
    MarketHistoryCandlesSnapshot, MarketHistoryConfig, MarketHistoryHandle,
    MarketHistoryLastPriceBatch, MarketHistoryLastPriceInput, MarketHistoryReaders,
    MarketHistoryWorker, MarketsEvent, MarketsState, OrderBookEvent, OrderBooks, OrderEvent,
    Orders, RollingTradeVolumeSnapshot, SettingsEvent, SettingsState, StratEvent, StratsState,
    TradeStorageScope, TradesEvent, TradesState,
};

mod trades;

/// Fresh strategy snapshot override returned by the application for a server
/// `TStratSnapshotRequest`.
///
/// Normal active-library flow: the application gives strategies to
/// [`EventDispatcher::set_local_strategies`] before init, and the dispatcher
/// uses its owned `StratsState` for the post-init snapshot and request replies.
/// This provider is only an advanced escape hatch for callers that need to
/// rebuild payload bytes themselves.
pub struct StrategySnapshotReply {
    pub server_epoch: u64,
    pub client_max_last_date: u64,
    pub full: bool,
    pub data: Vec<u8>,
}

impl StrategySnapshotReply {
    /// Build a reply from an already serialized `TStrategySerializer` payload.
    ///
    /// Empty `data` is treated as an empty strategy list and normalized to a
    /// valid non-empty serializer payload. This matches Delphi
    /// `TStratSnapshot.CreateFromStrats([])` and prevents a normal provider from
    /// sending malformed `Size=0` snapshot data.
    pub fn from_payload(
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: Vec<u8>,
    ) -> Self {
        let data = if data.is_empty() {
            crate::commands::strategy_serializer::StrategyBatchBuilder::empty_payload()
        } else {
            data
        };
        Self {
            server_epoch,
            client_max_last_date,
            full,
            data,
        }
    }

    /// Build a reply from decoded strategy snapshots.
    ///
    /// This is the provider-side counterpart of Delphi
    /// `TStratSnapshot.CreateFromStrats`: it serializes the current application
    /// strategy list, computes `ClientMaxLastDate`, and marks the packet as a
    /// full snapshot by default. Pass the live `TStratSchema` fetched during
    /// Init; Rust does not carry a static Delphi field/default table.
    pub fn from_strategies(
        server_epoch: u64,
        full: bool,
        schema: &StrategySchema,
        strategies: &[StrategySnapshot],
    ) -> Self {
        let mut builder = crate::commands::strategy_serializer::StrategyBatchBuilder::new(schema);
        let mut client_max_last_date = 0u64;
        for strategy in strategies {
            client_max_last_date = client_max_last_date.max(strategy.last_date);
            builder.write_strategy(strategy);
        }
        Self {
            server_epoch,
            client_max_last_date,
            full,
            data: builder.finalize(),
        }
    }
}

/// Follow-up `TOrderStatusRequest` target produced after a `TAllStatuses`
/// snapshot did not mention a locally tracked Delphi `WCache` worker.
///
/// Active `Client::run_with_dispatcher*` sends these automatically. Raw
/// `EventDispatcher::dispatch_into` users can call
/// [`EventDispatcher::missing_order_status_requests_after_snapshot`] after
/// `OrderEvent::Snapshot` and send the returned requests through
/// `Client::request_order_status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingOrderStatusRequest {
    pub ctx: TradeCtx,
    pub market_name: String,
}

/// One watcher fill after Delphi `ProcessTradesStream` time-shift application.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillEvent {
    /// Delphi `Round(TDateTime * MSecsPerDay)` timestamp used by `TWSFill.Time`.
    pub time_ms: i64,
    /// Shifted Delphi `TDateTime` value for consumers that work in days.
    pub time: f64,
    pub price: f32,
    pub qty: f32,
    pub z_btc: f32,
    pub position: f32,
    /// Raw `TOrderType` ordinal. Unknown values are preserved like Delphi enum bytes.
    pub order_type: OrderType,
    pub is_short: bool,
    pub is_open: bool,
    pub is_taker: bool,
}

/// Typed watcher fills from one `TradesStream` WatcherFills section.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillsEvent {
    pub market_index: u16,
    pub market_name: String,
    pub user: [u8; 20],
    pub fills: Vec<WatcherFillEvent>,
}

/// All typed events emitted by [`EventDispatcher`].
#[derive(Debug)]
pub enum Event {
    /// Order channel event: order creation, update, removal, or snapshot
    /// follow-up.
    Order(OrderEvent),
    /// OrderBook channel: applied updates/low-level cache control events.
    OrderBook(OrderBookEvent),
    /// TradesStream channel event. A packet can produce several
    /// [`TradesEvent`] values, so each sub-event is delivered as a separate
    /// `Event::Trade` instead of a nested vector.
    Trade(TradesEvent),
    /// Typed HyperDex watcher fills. Delphi decodes these inside
    /// `ProcessTradesStream` and calls `ProcessWatcherFillsDetect`; Active Lib
    /// exposes the same domain data instead of dropping the section as opaque
    /// bytes.
    WatcherFills(WatcherFillsEvent),
    /// Balance channel: one event for full/incremental updates (cmd_id_sub 3/4).
    /// The exact base `TBalanceCommand` (cmd_id_sub 2) is parsed but ignored,
    /// matching Delphi `ProcessBalanceCommand`.
    Balance(BalanceEvent),
    /// Arb channel (`MPC_Balance` subcommand 6): compact kernel-to-client
    /// payload.
    Arb { uid: u64, payload: ArbPayload },
    /// Strat channel: snapshot/delete/sell-price update.
    Strat(StratEvent),
    /// UI channel: settings updated, MM subscribe changed, etc.
    Settings(SettingsEvent),
    /// Markets state was updated after an Engine API response.
    Markets(MarketsEvent),
    /// Engine API response that was not consumed by the pending-response
    /// registry.
    EngineResponse(EngineResponse),
    /// Server-side log message (`MPC_LogMsg`): `time:TDateTime + msg:UTF-8 rest`.
    ServerLog { time: f64, msg: String },
    /// Raw payload for channels the dispatcher does not parse.
    Raw { cmd: Command, payload: Vec<u8> },
    /// Payload parsing failed.
    ///
    /// `payload` is cloned only on failure so live diagnostics can dump the
    /// exact bytes that failed to parse without adding work to the normal path.
    ParseFailed {
        cmd: Command,
        len: usize,
        payload: Vec<u8>,
    },
}

pub(crate) struct ActiveDispatchContext {
    pub(crate) peer_app_token: u64,
    pub(crate) market_indexes_current_for_peer: bool,
    pub(crate) server_token: u64,
    pub(crate) subscribed_book_server_token: u64,
    pub(crate) round_trip_delay_ms: i64,
    pub(crate) server_time_delta_source: Arc<AtomicU64>,
    pub(crate) now_time_days: f64,
    pub(crate) domain_ready: bool,
    pub(crate) trades_storage_scope: Option<Arc<crate::state::TradeStorageScope>>,
    pub(crate) copy_max_leverage_from_markets_list: bool,
    pub(crate) server_base_currency_name: Option<String>,
    pub(crate) server_base_currency_code: Option<u8>,
}

impl ActiveDispatchContext {
    pub(crate) fn from_client(client: &crate::client::Client) -> Self {
        Self {
            peer_app_token: client.peer_app_token(),
            market_indexes_current_for_peer: client.market_indexes_current_for_peer(),
            server_token: client.server_token(),
            subscribed_book_server_token: client.subscribed_book_server_token(),
            round_trip_delay_ms: client.round_trip_delay_ms(),
            server_time_delta_source: client.server_time_delta_handle(),
            now_time_days: crate::client::delphi_now_raw(),
            domain_ready: client.is_domain_ready(),
            trades_storage_scope: client.trades_storage_scope_intent(),
            copy_max_leverage_from_markets_list: copy_max_leverage_from_markets_list(
                client.server_info(),
            ),
            server_base_currency_name: client.server_info().base_currency_name.clone(),
            server_base_currency_code: client.server_info().base_currency_code,
        }
    }
}

const DELPHI_PLATFORM_FGATE: u8 = 9;

fn copy_max_leverage_from_markets_list(info: &ServerInfo) -> bool {
    info.exchange_code == Some(DELPHI_PLATFORM_FGATE)
}

pub(crate) enum ActiveAction {
    RequestMarketsList,
    RequestUpdateMarketsList,
    RequestOrderSnapshot,
    RequestStrategySchema,
    RequestOrderBookFull {
        market_index: u16,
        book_kind: u8,
    },
    SendStrategySnapshot {
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: Vec<u8>,
    },
    RequestOrderStatus {
        ctx: TradeCtx,
        market_name: String,
    },
    OrderCancel {
        request: OrderCancelSend,
    },
    TradesResend {
        payload: Vec<u8>,
    },
}

/// State bundle + dispatch logic.
///
/// The dispatcher owns all channel state and exposes it read-only through
/// getters [`Self::orders`], [`Self::order_books`], [`Self::trades`],
/// [`Self::balances`], [`Self::strats`], [`Self::settings`], [`Self::markets`].
/// Applications should not mutate protocol state directly; state is maintained
/// by [`Self::dispatch`], [`Self::dispatch_into`], and the active action
/// outbox path used by `Client::run_with_dispatcher`.
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
    /// `client.server_time_delta_handle()`, либо автоматически через
    /// `Client::run_with_dispatcher`.
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
    /// Long-running `Client::run_with_dispatcher` delivers events directly to its
    /// callback. One-shot helpers (`run_until_response`, `request_*`) have no
    /// callback argument, so they store produced events here for the application
    /// to drain after the helper returns.
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

/// Immutable read-model copy delivered to `run_with_dispatcher_state` callbacks.
///
/// The live [`EventDispatcher`] stays owned by the protocol loop. This snapshot
/// is cloned after dispatcher state is updated, then sent through the
/// application callback queue. User code can block or keep the snapshot without
/// blocking protocol ACK/retry/send progress.
#[derive(Debug, Clone)]
pub struct EventDispatcherSnapshot {
    orders: Orders,
    order_books: OrderBooks,
    trades: TradesState,
    balances: BalancesState,
    strats: StratsState,
    settings: SettingsState,
    markets: MarketsState,
    local_strategy_epoch: u64,
}

impl EventDispatcherSnapshot {
    /// Read-only order state, keyed by server order UID.
    pub fn orders(&self) -> &Orders {
        &self.orders
    }

    /// Read-only orderbook state.
    pub fn order_books(&self) -> &OrderBooks {
        &self.order_books
    }

    /// Read-only trades-stream state.
    pub fn trades(&self) -> &TradesState {
        &self.trades
    }

    /// Read-only balance state.
    pub fn balances(&self) -> &BalancesState {
        &self.balances
    }

    /// Read-only strategy state.
    pub fn strats(&self) -> &StratsState {
        &self.strats
    }

    /// Delphi `cfg.ServerStratEpoch` analogue used by local strategy snapshots.
    pub fn local_strategy_epoch(&self) -> u64 {
        self.local_strategy_epoch
    }

    /// Read one full decoded strategy snapshot from the active-library state.
    pub fn strategy_snapshot(&self, strategy_id: u64) -> Option<&StrategySnapshot> {
        self.strats.snapshot(strategy_id)
    }

    /// Iterate full decoded strategy snapshots in Delphi list order.
    pub fn strategy_snapshots(&self) -> impl Iterator<Item = &StrategySnapshot> {
        self.strats.snapshots()
    }

    /// Clone the current strategy snapshot list in Delphi list order.
    pub fn strategy_snapshot_vec(&self) -> Vec<StrategySnapshot> {
        self.strats.snapshot_vec()
    }

    /// Delphi `TStrategies.GetCheckedDelta` over the active-library strategy list.
    pub fn strategy_checked_delta(&self) -> Vec<crate::commands::strat::StratCheckedItem> {
        self.strats.checked_delta()
    }

    /// Read-only UI/settings state.
    pub fn settings(&self) -> &SettingsState {
        &self.settings
    }

    /// Read-only markets state.
    pub fn markets(&self) -> &MarketsState {
        &self.markets
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

    /// Copy the current read model for application callback delivery.
    ///
    /// This is a read-only snapshot: it intentionally excludes mutable callback
    /// hooks and the one-shot queued-event buffer from the live dispatcher.
    pub fn snapshot(&self) -> EventDispatcherSnapshot {
        EventDispatcherSnapshot {
            orders: self.orders.clone(),
            order_books: self.order_books.clone(),
            trades: self.trades.clone(),
            balances: self.balances.clone(),
            strats: self.strats.clone(),
            settings: self.settings.clone(),
            markets: self.markets.clone(),
            local_strategy_epoch: self.local_strategy_epoch,
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

    /// Set Delphi `cfg.ServerStratEpoch` analogue for local strategy snapshots.
    ///
    /// Use this when loading persisted local strategy state before init. The
    /// value is written into `TStratSnapshot.ServerEpoch` when the dispatcher
    /// answers a server `TStratSnapshotRequest`.
    pub fn set_local_strategy_epoch(&mut self, epoch: u64) {
        self.local_strategy_epoch = epoch;
    }

    pub fn local_strategy_epoch(&self) -> u64 {
        self.local_strategy_epoch
    }

    /// Delphi local strategy edit: `Inc(cfg.ServerStratEpoch)`.
    pub fn mark_local_strategies_changed(&mut self) -> u64 {
        self.local_strategy_epoch = self.local_strategy_epoch.saturating_add(1);
        self.local_strategy_epoch
    }

    /// Replace the library-owned strategy list before init.
    ///
    /// This is the normal active-library path. The dispatcher stores the full
    /// decoded snapshots, feeds the post-init strategy snapshot, answers server
    /// `TStratSnapshotRequest` automatically, and keeps the list current when
    /// server strategy snapshots/deltas arrive.
    pub fn set_local_strategies(&mut self, strategies: &[StrategySnapshot]) {
        self.strats.replace_with_snapshots(strategies);
    }

    /// Upsert one application-owned strategy into the library state.
    pub fn upsert_local_strategy(&mut self, strategy: StrategySnapshot) {
        self.strats.upsert_local_snapshot(strategy);
    }

    /// Change one local strategy checked flag like Delphi `TStrategy.Checked`.
    ///
    /// This does not mark the change acknowledged. The delta stays pending
    /// until a matching `TStratCheckedEcho` or `TStratCheckedSync` arrives from
    /// the server.
    pub fn set_strategy_checked(&mut self, strategy_id: u64, checked: bool) -> bool {
        self.strats.set_checked(strategy_id, checked)
    }

    /// Clear the owned strategy list. The next server request will receive an
    /// empty `TStratSnapshot` unless a provider override supplies one.
    pub fn clear_local_strategies(&mut self) {
        self.strats.replace_with_snapshots(&[]);
    }

    /// Read one full decoded strategy snapshot from the active-library state.
    pub fn strategy_snapshot(&self, strategy_id: u64) -> Option<&StrategySnapshot> {
        self.strats.snapshot(strategy_id)
    }

    /// Iterate full decoded strategy snapshots currently owned by the library.
    pub fn strategy_snapshots(&self) -> impl Iterator<Item = &StrategySnapshot> {
        self.strats.snapshots()
    }

    /// Clone the current strategy snapshot list in Delphi list order.
    pub fn strategy_snapshot_vec(&self) -> Vec<StrategySnapshot> {
        self.strats.snapshot_vec()
    }

    /// Delphi `TStrategies.GetCheckedDelta` over the active-library strategy
    /// list.
    pub fn strategy_checked_delta(&self) -> Vec<crate::commands::strat::StratCheckedItem> {
        self.strats.checked_delta()
    }

    /// Send `TStratCheckedSync.Create(true)` if Delphi checked delta is non-empty.
    ///
    /// Returns the number of delta items queued. The local `PrevChecked` is not
    /// advanced here; Delphi advances it only after server echo/sync.
    pub fn send_strategy_checked_delta(&self, client: &crate::client::Client) -> usize {
        let items = self.strats.checked_delta();
        if items.is_empty() {
            return 0;
        }
        client.strat_checked_sync(&items, true);
        items.len()
    }

    /// Send Delphi `TStratStartStopCommandV2.Create(is_start)`.
    ///
    /// The command is always queued after the client's Init gate is open, even
    /// when the checked delta is empty, because the same packet also carries the
    /// start/stop action.
    pub fn ui_strat_start_stop_v2(&self, client: &crate::client::Client, is_start: bool) -> usize {
        let items = self.strats.checked_delta();
        client.ui_strat_start_stop_v2(is_start, &items);
        items.len()
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
    /// `Client::run_with_dispatcher` delivers events to its callback immediately
    /// and does not use this queue. The queue is only for helper-driven waits
    /// such as `Client::run_until_response`, `request_client_settings`,
    /// `request_order_snapshot`, and typed `request_*` Engine API helpers.
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

    /// Attach a retained-history writer worker.
    ///
    /// The dispatcher does not mutate retained history directly. In active
    /// dispatch mode it only queues typed `TradesStream` batches into this
    /// handle; `MarketHistoryWorker` owns the actual `MarketHistoryStore`s.
    pub fn set_market_history_handle(&mut self, handle: MarketHistoryHandle) {
        self.owned_market_history = None;
        self.market_history_auto_enabled = false;
        self.market_history = Some(handle);
        self.last_market_history_scope = None;
        self.last_market_history_markets_version = None;
        self.sync_market_history_storage();
    }

    /// Disable retained-history batch delivery for this dispatcher.
    pub fn clear_market_history_handle(&mut self) {
        self.market_history = None;
        self.owned_market_history = None;
        self.market_history_auto_enabled = false;
        self.last_market_history_scope = None;
        self.last_market_history_markets_version = None;
    }

    /// Re-enable the default retained-history worker after
    /// [`Self::clear_market_history_handle`] or a custom handle.
    ///
    /// The worker is spawned lazily when trades storage scope is active.
    pub fn enable_default_market_history(&mut self) {
        self.market_history_auto_enabled = true;
        self.ensure_default_market_history_worker();
        self.sync_market_history_storage();
    }

    pub fn market_history_readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.market_history.as_ref()?.readers(market_name)
    }

    pub fn market_history_rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.market_history
            .as_ref()?
            .rolling_volumes(market_name, now_time)
    }

    pub fn market_history_derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
        self.market_history
            .as_ref()?
            .derived_snapshot(market_name, now_time)
    }

    pub fn flush_market_history(&self, now_time: f64) -> bool {
        self.market_history
            .as_ref()
            .is_some_and(|handle| handle.flush(now_time))
    }

    pub fn trade_storage_scope(&self) -> Option<&TradeStorageScope> {
        self.trade_storage_scope.as_ref()
    }

    /// Apply a full `emk_RequestCandlesData` snapshot to retained Active Lib
    /// candle storage. The dispatcher keeps the same trades subscription scope:
    /// if trades storage is disabled or the market is outside
    /// `subscribe_trades_for`, the snapshot row is ignored.
    pub fn apply_candles_snapshot(
        &mut self,
        markets: &[crate::commands::candles::RequestCandlesMarket],
    ) -> bool {
        self.sync_market_history_storage();
        let Some(handle) = &self.market_history else {
            return false;
        };
        let rows = markets
            .iter()
            .filter(|market| self.active_trade_storage_allows_market(&market.market_name))
            .map(|market| MarketHistoryCandlesSnapshot {
                market_name: market.market_name.clone(),
                candles_5m: market
                    .candles_5m
                    .iter()
                    .copied()
                    .map(Candle5mRow::from_deep_price)
                    .collect(),
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return false;
        }
        handle.apply_candles_snapshot(rows)
    }

    fn set_trade_storage_scope(&mut self, scope: Option<&TradeStorageScope>, now_time_days: f64) {
        if self.trade_storage_scope.as_ref() != scope {
            self.trade_storage_scope = scope.cloned();
            self.last_market_history_scope = None;
            self.ensure_default_market_history_worker();
            self.sync_market_history_storage();
            if self.trade_storage_scope.is_some() {
                self.queue_current_last_price_history_like_delphi(now_time_days);
            }
        }
    }

    fn ensure_default_market_history_worker(&mut self) {
        if self.trade_storage_scope.is_none() {
            if self.owned_market_history.is_some() {
                self.market_history = None;
                self.owned_market_history = None;
                self.last_market_history_scope = None;
                self.last_market_history_markets_version = None;
            }
            return;
        }
        if !self.market_history_auto_enabled || self.market_history.is_some() {
            return;
        }
        let worker = MarketHistoryWorker::spawn(MarketHistoryConfig::default());
        self.market_history = Some(worker.handle());
        self.owned_market_history = Some(worker);
        self.last_market_history_scope = None;
        self.last_market_history_markets_version = None;
    }

    fn market_history_market_slots(&self) -> Vec<Option<Arc<str>>> {
        if self.markets.indexes_synchronized && !self.markets.market_indexes.is_empty() {
            return self
                .markets
                .market_indexes
                .iter()
                .map(|name| {
                    self.markets
                        .by_name
                        .contains_key(name.as_str())
                        .then(|| Arc::<str>::from(name.as_str()))
                })
                .collect();
        }
        self.markets
            .markets
            .iter()
            .map(|market| {
                market.with(|market| Some(Arc::<str>::from(market.bn_market_name.as_str())))
            })
            .collect()
    }

    fn sync_market_history_storage(&mut self) {
        self.ensure_default_market_history_worker();
        let Some(handle) = &self.market_history else {
            return;
        };
        let markets_version = self.markets.markets_version();
        if self.last_market_history_scope == self.trade_storage_scope
            && self.last_market_history_markets_version == Some(markets_version)
        {
            return;
        }
        let market_slots = self.market_history_market_slots();
        handle.configure_market_index_slots(market_slots, self.trade_storage_scope.clone());
        self.last_market_history_scope = self.trade_storage_scope.clone();
        self.last_market_history_markets_version = Some(markets_version);
    }

    fn active_trade_storage_allows_market(&self, market_name: &str) -> bool {
        self.trade_storage_scope
            .as_ref()
            .is_some_and(|scope| scope.contains(market_name))
    }

    fn trade_section_visible_to_active_lib(&self, market_name: &str) -> bool {
        self.trade_storage_scope
            .as_ref()
            .map_or(true, |scope| scope.contains(market_name))
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
    /// to call this when using [`crate::client::Client::run_with_dispatcher`],
    /// which calls the check after successfully parsed trades packets.
    ///
    /// Custom loops that bypass `run_with_dispatcher` should call it after a
    /// valid `TradesStream`/`TradesResendResponse` packet, with the current RTT
    /// and monotonic timestamp, then send each returned request through the
    /// client.
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
    /// of the process-global raw-dispatch fallback. Multi-server applications
    /// should attach one dispatcher to the matching client. The usual
    /// `Client::run_with_dispatcher` path links this automatically on first use.
    ///
    /// ```ignore
    /// let client = Client::new(cfg);
    /// let mut dispatcher = EventDispatcher::new();
    /// dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
    /// ```
    pub fn set_server_time_delta_source(&mut self, handle: Arc<AtomicU64>) {
        self.server_time_delta_source = Some(handle);
    }

    /// Register an override provider for fresh strategy snapshots.
    ///
    /// The provider is called with the UID of the incoming
    /// `TStratSnapshotRequest`. The reply itself is sent with a new command UID,
    /// as Delphi creates a fresh `TStratSnapshot` command object for the answer.
    ///
    /// Normal callers should prefer [`Self::set_local_strategies`]. If no
    /// provider is registered, or the provider returns `None`, the dispatcher
    /// sends the current library-owned strategy list. `SnapshotRequested` is
    /// still emitted for UI/diagnostic awareness.
    pub fn set_strategy_snapshot_provider<F>(&mut self, provider: F)
    where
        F: FnMut(u64) -> Option<StrategySnapshotReply> + Send + 'static,
    {
        self.strategy_snapshot_provider = Some(Box::new(provider));
    }

    /// Remove the strategy snapshot provider.
    pub fn clear_strategy_snapshot_provider(&mut self) {
        self.strategy_snapshot_provider = None;
    }

    fn strategy_snapshot_reply(&mut self, request_uid: u64) -> Option<StrategySnapshotReply> {
        self.strategy_snapshot_provider
            .as_mut()
            .and_then(|provider| provider(request_uid))
            .or_else(|| self.local_strategy_snapshot_reply())
    }

    pub(crate) fn local_strategy_snapshot_reply(&mut self) -> Option<StrategySnapshotReply> {
        let cache = self.strats.snapshot_payload_cache()?;
        Some(StrategySnapshotReply::from_payload(
            self.local_strategy_epoch,
            cache.client_max_last_date,
            true,
            cache.data.clone(),
        ))
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
    /// Normal applications should use `Client::run_with_dispatcher`, whose
    /// active dispatch path gates stale indexed streams, sends orderbook full
    /// requests when recovery needs them, and requests missing order statuses
    /// after a fresh order snapshot.
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

    fn client_new_data_order(&mut self, payload: &[u8], now_ms: i64, out: &mut Vec<Event>) {
        match TradeCommand::parse(payload) {
            Some(TradeCommand::AllStatuses(snap)) => self.process_all_statuses(snap, now_ms, out),
            Some(tc) => self.process_command_order(tc, now_ms, out),
            None => out.push(Self::parse_failed(Command::Order, payload)),
        }
    }

    /// Delphi equivalent: `ClientNewData(MPC_Order)` / `TAllStatuses` branch.
    fn process_all_statuses(&mut self, snap: AllStatuses, now_ms: i64, out: &mut Vec<Event>) {
        self.orders.begin_snapshot();
        for status in snap.orders {
            self.process_command_order(TradeCommand::OrderStatus(Box::new(status)), now_ms, out);
        }
        out.push(Event::Order(OrderEvent::Snapshot));
    }

    /// Delphi equivalent: `TMoonProtoNetClient.ProcessCommandOrder`.
    fn process_command_order(&mut self, tc: TradeCommand, now_ms: i64, out: &mut Vec<Event>) {
        if self.drop_new_order_status_without_worker(&tc) {
            return;
        }

        // audit_responsibility A5 / active library: автоматически подхватываем
        // server_time_delta. При наличии per-Client `server_time_delta_source`
        // (multi-Client) — читаем оттуда. Иначе fallback на global для raw
        // dispatch без Client source. Без этого Orders::apply применяет AdjustTime со старым
        // delta=0 — order timestamps сдвинуты на 0.5-2 сек (silent bug).
        // См. DEVIATION #23.
        self.orders
            .set_server_time_delta(self.current_server_time_delta());
        let (apply_result, ev) = self.orders.apply_at(tc, now_ms);
        if apply_result == ApplyResult::Applied {
            out.push(Event::Order(ev));
        }
    }

    pub fn tick_orders(&mut self, now_ms: i64) -> Vec<Event> {
        let mut out = Vec::new();
        self.tick_orders_into(now_ms, &mut out);
        out
    }

    pub(crate) fn tick_orders_into(&mut self, now_ms: i64, out: &mut Vec<Event>) {
        for ev in self.orders.tick_bulk_replace_timeouts(now_ms) {
            out.push(Event::Order(ev));
        }
        self.drain_deferred_order_removals_due(now_ms, out);
    }

    pub(crate) fn tick_orders_active_actions(
        &mut self,
        now_ms: i64,
        out: &mut Vec<Event>,
        actions: &mut Vec<ActiveAction>,
    ) {
        self.tick_orders_into(now_ms, out);
        for request in self.orders.tick_pending_cancel_resends(now_ms) {
            actions.push(ActiveAction::OrderCancel { request });
        }
    }

    /// Delphi `ProcessCommandOrder` first tries `WCache.TryFind(TaskUID)`.
    /// Only an unknown, non-cache `TOrderStatus` may create a worker, and only
    /// when `Cmd.m <> nil` (the market name resolved in local `Markets`).
    fn drop_new_order_status_without_worker(&self, tc: &TradeCommand) -> bool {
        let TradeCommand::OrderStatus(st) = tc else {
            return false;
        };

        let uid = st.epoch_header.market.base.uid;
        if self.orders.get(uid).is_some() {
            return false;
        }

        if st.from_cache {
            return true;
        }

        let market_name = &st.epoch_header.market.market_name;
        if self.markets.get(market_name).is_none() {
            log::warn!(
                target: "moonproto::orders",
                "Drop order <{}>: market not found locally ({})",
                uid,
                market_name
            );
            return true;
        }

        false
    }

    fn client_new_data_order_book(&mut self, payload: &[u8], now_ms: i64, out: &mut Vec<Event>) {
        // Active library: блокируем обработку OrderBook если markets indexes не sync.
        // Соответствует Delphi `MoonProtoEngine.pas:1580 If FLastServerAppToken <>
        // PeerAppToken then exit`. Без этого: потеряем пакеты от первых апдейтов
        // после server restart до получения свежих индексов (market_idx по новой
        // нумерации применился бы к старому by_index → silent data corruption).
        if !self.markets.indexes_synchronized {
            return;
        }
        match parse_order_book_packet(payload) {
            Some(pkt) => {
                if !self.markets.has_server_market_index(pkt.market_index) {
                    return;
                }
                let market_index = pkt.market_index;
                let book_kind = pkt.book_kind;
                let events = self.order_books.on_packet(pkt, now_ms);
                if events
                    .iter()
                    .any(|ev| matches!(ev, OrderBookEvent::Apply { .. }))
                {
                    if let Some(ask) = self
                        .order_books
                        .book_by_kind(market_index, book_kind)
                        .and_then(|book| book.top().ask)
                    {
                        self.markets
                            .update_chart_price_step_from_server_index(market_index, ask.rate);
                    }
                }
                for ev in events {
                    out.push(Event::OrderBook(ev));
                }
            }
            None => out.push(Self::parse_failed(Command::OrderBook, payload)),
        }
    }

    fn client_new_data_balance(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        if payload.len() < 11 {
            out.push(Self::parse_failed(Command::Balance, payload));
            return;
        }
        let sub_cmd_id = payload[0];
        let ver = u16::from_le_bytes([payload[1], payload[2]]);
        if ver > crate::commands::registry::CURRENT_PROTO_CMD_VER {
            return;
        }
        let body = &payload[11..];
        match sub_cmd_id {
            0 | 1 | 2 | 5 => {}
            3 | 4 => match parse_balance(sub_cmd_id, body) {
                Some(upd) => {
                    let ev = self
                        .balances
                        .apply_with_known_markets(upd, &self.markets.by_name);
                    out.push(Event::Balance(ev));
                }
                None => out.push(Self::parse_failed(Command::Balance, payload)),
            },
            6 => match parse_arb_prices(payload) {
                Some(arb) => {
                    if let Some(parsed) = parse_arb_payload_compact(&arb.payload) {
                        let parsed = self.filter_arb_payload_to_known_markets(parsed);
                        out.push(Event::Arb {
                            uid: arb.uid,
                            payload: parsed,
                        });
                    }
                }
                None => out.push(Self::parse_failed(Command::Balance, payload)),
            },
            _ => {}
        }
    }

    fn filter_arb_payload_to_known_markets(&self, payload: ArbPayload) -> ArbPayload {
        match payload {
            ArbPayload::Price {
                version,
                mut blocks,
            } => {
                blocks.retain(|block| self.markets.has_server_market_index(block.market_index));
                ArbPayload::Price { version, blocks }
            }
            ArbPayload::Isolation {
                version,
                mut entries,
            } => {
                entries.retain(|entry| self.markets.has_server_market_index(entry.market_index));
                ArbPayload::Isolation { version, entries }
            }
        }
    }

    fn client_new_data_strat(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        match StratCommand::parse(payload) {
            Some(cmd_v) => self.process_strat_command(cmd_v, out),
            None => out.push(Self::parse_failed(Command::Strat, payload)),
        }
    }

    /// Delphi equivalent: `TMoonProtoNetClient.ProcessStratCommand`.
    fn process_strat_command(&mut self, cmd_v: StratCommand, out: &mut Vec<Event>) {
        match &cmd_v {
            StratCommand::SellPriceUpdate(_)
            | StratCommand::SchemaRequest { .. }
            | StratCommand::Skipped { .. }
            | StratCommand::Unknown { .. } => return,
            _ => {}
        }
        let ev = self.strats.apply(cmd_v);
        // Active library: auto-decode strategy snapshot raw bytes
        // в `StratsState`. Раньше app должен был сам вызывать
        // `strats.apply_snapshot_decoded(raw_data)` — теперь либа
        // делает это сама на SnapshotFull/Partial event'ах.
        match &ev {
            crate::state::StratEvent::SnapshotFull {
                server_epoch,
                raw_data,
            } => {
                if self
                    .strats
                    .apply_snapshot_decoded_with_mode_in_place(raw_data, true)
                    .is_none()
                {
                    log::warn!(
                        target: "moonproto::events",
                        "failed to decode full strategy snapshot payload ({} bytes)",
                        raw_data.len()
                    );
                    return;
                }
                self.strats.last_server_epoch = *server_epoch;
            }
            crate::state::StratEvent::SnapshotPartial {
                server_epoch,
                raw_data,
            } => {
                if self
                    .strats
                    .apply_snapshot_decoded_with_mode_in_place(raw_data, false)
                    .is_none()
                {
                    log::warn!(
                        target: "moonproto::events",
                        "failed to decode partial strategy snapshot payload ({} bytes)",
                        raw_data.len()
                    );
                    return;
                }
                self.strats.last_server_epoch = *server_epoch;
            }
            _ => {}
        }
        out.push(Event::Strat(ev));
    }

    fn client_new_data_ui(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        match UICommand::parse_with_client_settings_fallback(
            payload,
            Some(self.settings.client_settings_parse_fallback()),
        ) {
            Some(UICommand::Skipped { .. } | UICommand::Unknown { .. }) => {}
            Some(UICommand::NewMarketNotify(_)) => {
                self.markets.markets_list_refresh_needed = true;
                self.force_markets_list_refresh = true;
            }
            Some(cmd_v) => {
                if let Some(ev) = self.settings.apply(cmd_v) {
                    out.push(Event::Settings(ev));
                }
            }
            None => out.push(Self::parse_failed(Command::UI, payload)),
        }
    }

    fn client_new_data_api(
        &mut self,
        payload: &[u8],
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        match parse_engine_response(payload) {
            Some(resp) => self.process_api_command(resp, history_now_time_days, out),
            None => out.push(Self::parse_failed(Command::API, payload)),
        }
    }

    /// Active dispatcher counterpart of Delphi `TMoonProtoNetClient.ProcessApiCommand`.
    fn process_api_command(
        &mut self,
        resp: EngineResponse,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        let mut extra_events = Vec::new();
        if resp.success {
            match resp.method {
                EngineMethod::GetMarketsList | EngineMethod::UpdateMarketsList => {
                    if resp.method == EngineMethod::GetMarketsList {
                        if let Some(ev) = self
                            .markets
                            .apply_markets_list_payload_like_delphi(&resp.data, resp.ver)
                        {
                            extra_events.push(Event::Markets(ev));
                            let new_markets = self.markets.take_new_markets_added();
                            if !new_markets.is_empty() {
                                extra_events.push(Event::Markets(MarketsEvent::NewMarketsAdded {
                                    names: new_markets,
                                }));
                            }
                        }
                    } else {
                        let wants_history = self.market_history.is_some()
                            && history_now_time_days.is_some()
                            && self.trade_storage_scope.is_some();
                        let mut last_price_rows = Vec::new();
                        let ev = if wants_history {
                            self.markets
                                .apply_markets_prices_payload_collecting_last_price_like_delphi(
                                    &resp.data,
                                    Some(&mut last_price_rows),
                                )
                        } else {
                            self.markets
                                .apply_markets_prices_payload_like_delphi(&resp.data)
                        };
                        if let Some(ev) = ev {
                            if wants_history {
                                self.queue_last_price_history_like_delphi(
                                    history_now_time_days,
                                    last_price_rows,
                                );
                            }
                            extra_events.push(Event::Markets(ev));
                        }
                    }
                }
                EngineMethod::GetMarketsIndexes => {
                    if let Some(names) = parse_markets_indexes_response(&resp.data) {
                        let ev = self.markets.apply_markets_indexes(names);
                        extra_events.push(Event::Markets(ev));
                    }
                }
                EngineMethod::CheckBinanceTags => {
                    if let Some(ev) = self
                        .markets
                        .apply_token_tags_payload_like_delphi(&resp.data)
                    {
                        extra_events.push(Event::Markets(ev));
                    }
                }
                EngineMethod::BaseCheck => {
                    let info = parse_base_check_response(&resp.data);
                    self.markets.set_copy_max_leverage_from_markets_list(
                        copy_max_leverage_from_markets_list(&info),
                    );
                    self.markets.set_server_base_currency(
                        info.base_currency_name.as_deref(),
                        info.base_currency_code,
                    );
                }
                _ => {}
            }
        }
        out.extend(extra_events);
        out.push(Event::EngineResponse(resp));
    }

    fn queue_last_price_history_like_delphi(
        &self,
        history_now_time_days: Option<f64>,
        rows: Vec<MarketLastPriceHistoryInput>,
    ) {
        let (Some(handle), Some(now_time)) = (&self.market_history, history_now_time_days) else {
            return;
        };
        if rows.is_empty() {
            return;
        }
        let rows: Vec<MarketHistoryLastPriceInput> = rows
            .into_iter()
            .filter(|row| self.active_trade_storage_allows_market(&row.market_name))
            .map(|row| MarketHistoryLastPriceInput {
                market_name: row.market_name,
                current: row.current,
                bid: row.bid,
                ask: row.ask,
                is_btc_market: row.is_btc_market,
                is_base_usdt_market: row.is_base_usdt_market,
            })
            .collect();
        if rows.is_empty() {
            return;
        }
        handle.send_last_price_batch(MarketHistoryLastPriceBatch { now_time, rows });
    }

    fn queue_current_last_price_history_like_delphi(&self, now_time_days: f64) {
        let rows = self.markets.current_last_price_history_rows_like_delphi();
        self.queue_last_price_history_like_delphi(Some(now_time_days), rows);
    }

    fn client_new_data_log_msg(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        if payload.len() < 8 {
            out.push(Self::parse_failed(Command::LogMsg, payload));
            return;
        }
        let time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
        let msg = decode_utf8_delphi(&payload[8..]);
        out.push(Event::ServerLog { time, msg });
    }

    /// Active-library parser step used by `Client::run_with_dispatcher`.
    ///
    /// The reader/main-loop side snapshots the owning `Client` into
    /// [`ActiveDispatchContext`], dispatches the payload, receives protocol
    /// actions into `actions`, then the client applies that outbox to its
    /// Delphi-style send queues. This keeps active dispatch from mutating
    /// `Client` directly and keeps one send path for active auto-actions.
    ///
    /// At most one full-book request is produced per `(market_index, book_kind)`
    /// in one dispatch call, even when a grouped payload contains several
    /// matching control events. Trades gap resend is checked after a successful
    /// trades packet, matching Delphi `ProcessTradesStream`.
    pub(crate) fn dispatch_into_active_actions(
        &mut self,
        cmd: Command,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
        ctx: &ActiveDispatchContext,
        actions: &mut Vec<ActiveAction>,
    ) {
        // Multi-Client safety: lazy-link `server_time_delta_source` к этому Client'у.
        // После первого вызова `dispatch_into_active` все последующие dispatch'и
        // используют Client-specific delta (а не global). Это критично при multi-Client:
        // global перезаписывается последним активным Client'ом, что без линковки давало
        // off-by-50-1000ms timestamps в ордерах других Client'ов. См. DEVIATION #23.
        if self.server_time_delta_source.is_none() {
            self.server_time_delta_source = Some(Arc::clone(&ctx.server_time_delta_source));
        }
        self.markets
            .set_copy_max_leverage_from_markets_list(ctx.copy_max_leverage_from_markets_list);
        self.markets.set_server_base_currency(
            ctx.server_base_currency_name.as_deref(),
            ctx.server_base_currency_code,
        );
        self.set_trade_storage_scope(ctx.trades_storage_scope.as_deref(), ctx.now_time_days);

        if matches!(cmd, Command::TradesStream | Command::TradesResendResponse)
            && ctx.trades_storage_scope.is_none()
        {
            log::warn!(target: "moonproto::events",
                "unexpected {:?} received without all-trades subscription; active packet dropped", cmd);
            return;
        }

        // Server restart / PeerAppToken change: Delphi gates stream parsing with
        // `FLastServerAppToken <> PeerAppToken` until `GetMarketsIndexes` succeeds.
        // Keep the same behavioral guard here. Otherwise old `indexes_synchronized`
        // from the previous server process would let fresh TradesStream/OrderBook
        // packets be decoded through stale market indexes.
        if ctx.peer_app_token != 0 && !ctx.market_indexes_current_for_peer {
            self.markets.mark_indexes_stale();
        }

        // Hard reconnect detection: при смене ServerToken вся per-session state
        // (trades.last_packet_num, order_books.*.expected_seq) устарела — сервер
        // начинает нумерацию заново. Сбрасываем ДО применения нового пакета.
        // Init last_known=0; первый non-zero token (после первого Fine) — не triggers
        // (последующие пакеты будут с тем же token, full_reset не нужен). Сброс
        // срабатывает только на ИЗМЕНЕНИИ token'а между установившейся сессией и
        // новой (hard reconnect через `WantNewHello` или server restart с новым ST).
        let current_token = ctx.server_token;
        if current_token != 0
            && self.last_known_server_token != 0
            && self.last_known_server_token != current_token
        {
            self.trades.full_reset_at(now_ms);
            self.order_books.reset_caches_keep_books();
            log::info!(target: "moonproto::events",
                "ServerToken changed ({:#x} -> {:#x}) — trades reset + orderbook caches reset",
                self.last_known_server_token, current_token);
        }
        self.last_known_server_token = current_token;

        if is_pre_init_domain_command(cmd) && !ctx.domain_ready {
            log::debug!(target: "moonproto::events",
                "domain command {:?} skipped before init completion", cmd);
            return;
        }

        // Delphi `TMoonProtoEngine.ProcessOrderBookPacket` exits before
        // decompression unless the current `Client.ServerToken` is confirmed by
        // a successful `DoSubscribeOrderBooks` batch:
        // `If MClient.Client.ServerToken <> FSubscribedBookServerToken then exit`.
        if cmd == Command::OrderBook
            && (ctx.server_token == 0 || ctx.server_token != ctx.subscribed_book_server_token)
        {
            return;
        }

        // Delphi `ProcessTradesStream`: after the PeerAppToken/index gate, but
        // before packet parsing, a changed ServerToken resets gap buckets and
        // stores `FTradesServerToken`. This is separate from generic hard-reset
        // bookkeeping above because reconnect retry checks whether the stream
        // itself has resumed for the new token.
        if cmd == Command::TradesStream
            && self.markets.indexes_synchronized
            && current_token != 0
            && self.trades_server_token != current_token
        {
            self.trades.full_reset_at(now_ms);
            self.trades_server_token = current_token;
        }

        let start_len = out.len();
        self.dispatch_into_with_history(cmd, payload, now_ms, Some(ctx.now_time_days), out);
        self.sync_market_history_storage();
        if self.force_markets_list_refresh
            || (self.markets.markets_list_refresh_needed()
                && (self.last_markets_list_refresh_ms == 0
                    || (now_ms - self.last_markets_list_refresh_ms).abs() > 30_000))
        {
            self.force_markets_list_refresh = false;
            self.last_markets_list_refresh_ms = now_ms;
            actions.push(ActiveAction::RequestMarketsList);
        }
        let new_markets_need_price_refresh =
            self.markets.take_new_markets_pending_price_refresh() > 0;
        // now_ms прокинут в dispatch_into для state.on_packet(now_ms).
        // Delphi `ProcessTradesStream` в конце вызывает `CheckMissingTradesPackets`;
        // значит recovery resend — последействие успешного trades-пакета, а не
        // независимый timer в тишине канала.
        let processed_trades_packet =
            matches!(cmd, Command::TradesStream | Command::TradesResendResponse)
                && out[start_len..]
                    .iter()
                    .any(|ev| matches!(ev, Event::Trade(TradesEvent::Applied { .. })));
        // Auto-action 1: OrderBookEvent::RequestFullNeeded → send_api_request (sync, no pending).
        // Dedup через маленький Vec без heap при пустом наборе: Grouped-payload может содержать несколько
        // RequestFullNeeded для одной и той же книги (corruption detection +
        // последующий update в одном datagram'е). Шлём один запрос на пару.
        let mut to_request_full: Vec<(u16, u8)> = Vec::new();
        // Auto-action 2: StratEvent::SnapshotRequested → шлём fresh snapshot
        // из library-owned StratsState (или provider override). Delphi
        // `MoonProtoClient.pas:ProcessStratCommand` пересобирает ответ через
        // `TStratSnapshot.CreateFromStrats(Strats)`.
        let mut snapshot_requested_uid: Option<u64> = None;
        let mut strategy_schema_applied = false;
        let mut new_markets_added = false;
        // Auto-action 3: OrderEvent::Snapshot → CleanupMissingWorkers.
        // Delphi after TAllStatuses increments CurrentSnapshotFlag, applies all
        // statuses, then requests exact status for workers absent from the fresh
        // snapshot. The application must not know about snapshot flags.
        let mut order_snapshot_applied = false;
        let mut idx = start_len;
        while idx < out.len() {
            let remove_event = match &out[idx] {
                Event::OrderBook(OrderBookEvent::RequestFullNeeded {
                    market_index,
                    book_kind,
                }) => {
                    let key = (*market_index, *book_kind);
                    if !to_request_full.contains(&key) {
                        to_request_full.push(key);
                    }
                    true
                }
                Event::Order(OrderEvent::Snapshot) => {
                    order_snapshot_applied = true;
                    false
                }
                Event::Strat(crate::state::StratEvent::SnapshotRequested { uid }) => {
                    snapshot_requested_uid = Some(*uid);
                    false
                }
                Event::Strat(crate::state::StratEvent::SchemaApplied { .. }) => {
                    strategy_schema_applied = true;
                    false
                }
                Event::Markets(MarketsEvent::NewMarketsAdded { .. }) => {
                    new_markets_added = true;
                    false
                }
                _ => false,
            };
            if remove_event {
                out.remove(idx);
            } else {
                idx += 1;
            }
        }
        if new_markets_added {
            actions.push(ActiveAction::RequestOrderSnapshot);
        }
        if new_markets_need_price_refresh {
            actions.push(ActiveAction::RequestUpdateMarketsList);
        }
        for (mi, bk) in to_request_full {
            // Fire-and-forget — response придёт обычным OrderBook-пакетом (is_full=true)
            // через тот же dispatcher. Регистрировать pending API receiver не нужно.
            actions.push(ActiveAction::RequestOrderBookFull {
                market_index: mi,
                book_kind: bk,
            });
        }
        if let Some(uid) = snapshot_requested_uid {
            if let Some(snapshot) = self.strategy_snapshot_reply(uid) {
                actions.push(ActiveAction::SendStrategySnapshot {
                    server_epoch: snapshot.server_epoch,
                    client_max_last_date: snapshot.client_max_last_date,
                    full: snapshot.full,
                    data: snapshot.data,
                });
            } else {
                self.pending_strategy_snapshot_request_uid = Some(uid);
                actions.push(ActiveAction::RequestStrategySchema);
            }
            // Событие всё равно эмиттится в `out` для UI/диагностики.
        }
        if strategy_schema_applied {
            if let Some(uid) = self.pending_strategy_snapshot_request_uid.take() {
                if let Some(snapshot) = self.strategy_snapshot_reply(uid) {
                    actions.push(ActiveAction::SendStrategySnapshot {
                        server_epoch: snapshot.server_epoch,
                        client_max_last_date: snapshot.client_max_last_date,
                        full: snapshot.full,
                        data: snapshot.data,
                    });
                } else {
                    self.pending_strategy_snapshot_request_uid = Some(uid);
                }
            }
        }
        if order_snapshot_applied {
            self.cleanup_missing_workers(actions);
        }
        if processed_trades_packet {
            let (payloads, tick_events) = self
                .trades
                .tick_with_events(ctx.round_trip_delay_ms, now_ms);
            out.extend(tick_events.into_iter().map(Event::Trade));
            for payload in payloads {
                actions.push(ActiveAction::TradesResend { payload });
            }
        }
    }

    /// Delphi equivalent: `TMoonProtoNetClient.CleanupMissingWorkers`.
    fn cleanup_missing_workers(&self, actions: &mut Vec<ActiveAction>) {
        for request in self.missing_order_status_requests_after_snapshot() {
            actions.push(ActiveAction::RequestOrderStatus {
                ctx: request.ctx,
                market_name: request.market_name,
            });
        }
    }
}

fn is_pre_init_domain_command(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::Order
            | Command::Strat
            | Command::Balance
            | Command::TradesStream
            | Command::TradesResendResponse
            | Command::OrderBook
            | Command::UI
    )
}

#[cfg(test)]
mod tests;
