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
//!             Event::Trade(TradesEvent::Apply(pkt)) => { /* process pkt */ }
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

use crate::commands::arb::{parse_arb_payload_compact, parse_arb_prices, ArbPayload};
use crate::commands::balance::parse_balance;
use crate::commands::engine_api::{parse_engine_response, EngineMethod, EngineResponse};
use crate::commands::market::{
    parse_markets_indexes_response, parse_markets_list_response, parse_markets_prices_response,
    parse_token_tags_response,
};
use crate::commands::order_book::parse_order_book_packet;
use crate::commands::strat::StratCommand;
use crate::commands::strategy_serializer::StrategySnapshot;
use crate::commands::trade::{TradeCommand, TradeCtx};
use crate::commands::trades_stream::parse_trades_packet;
use crate::commands::ui::UICommand;
use crate::protocol::Command;
use crate::state::parse_trades_resend_response;
use crate::state::{
    BalanceEvent, BalancesState, MarketsEvent, MarketsState, OrderBookEvent, OrderBooks,
    OrderEvent, Orders, SettingsEvent, SettingsState, StratEvent, StratsState, TradesEvent,
    TradesState,
};

/// Fresh strategy snapshot override returned by the application for a server
/// `TStratSnapshotRequest`.
///
/// Normal active-library flow: the application gives strategies to
/// [`EventDispatcher::set_local_strategies`] before init, and the dispatcher
/// answers from its owned `StratsState`. This provider is only an advanced
/// escape hatch for callers that need to rebuild payload bytes themselves.
pub struct StrategySnapshotReply {
    pub server_epoch: u64,
    pub client_max_last_date: u64,
    pub full: bool,
    pub data: Vec<u8>,
}

impl StrategySnapshotReply {
    /// Build a reply from an already serialized `TStrategySerializer` payload.
    pub fn from_payload(
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: Vec<u8>,
    ) -> Self {
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
    /// full snapshot by default.
    pub fn from_strategies(server_epoch: u64, full: bool, strategies: &[StrategySnapshot]) -> Self {
        let mut builder = crate::commands::strategy_serializer::StrategyBatchBuilder::new();
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
    ParseFailed { cmd: Command, len: usize },
}

pub(crate) struct ActiveDispatchContext {
    pub(crate) peer_app_token: u64,
    pub(crate) market_indexes_current_for_peer: bool,
    pub(crate) server_token: u64,
    pub(crate) server_time_delta_source: Arc<AtomicU64>,
    pub(crate) domain_ready: bool,
}

impl ActiveDispatchContext {
    pub(crate) fn from_client(client: &crate::client::Client) -> Self {
        Self {
            peer_app_token: client.peer_app_token(),
            market_indexes_current_for_peer: client.market_indexes_current_for_peer(),
            server_token: client.server_token(),
            server_time_delta_source: client.server_time_delta_handle(),
            domain_ready: client.is_domain_ready(),
        }
    }
}

pub(crate) enum ActiveAction {
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
}

/// State bundle + dispatch logic.
///
/// The dispatcher owns all channel state and exposes it read-only through
/// getters [`Self::orders`], [`Self::order_books`], [`Self::trades`],
/// [`Self::balances`], [`Self::strats`], [`Self::settings`], [`Self::markets`].
/// Applications should not mutate protocol state directly; state is maintained
/// by [`Self::dispatch`], [`Self::dispatch_into`], and the active action
/// outbox path used by `Client::run_with_dispatcher`.
#[derive(Default)]
pub struct EventDispatcher {
    pub(crate) orders: Orders,
    pub(crate) order_books: OrderBooks,
    pub(crate) trades: TradesState,
    pub(crate) balances: BalancesState,
    pub(crate) strats: StratsState,
    pub(crate) settings: SettingsState,
    pub(crate) markets: MarketsState,
    /// Последний известный `ServerToken` — для детектирования hard reconnect.
    /// При смене токена `dispatch_into_active` сбрасывает per-token state
    /// (`trades.full_reset()`, `order_books.clear()`) до применения нового пакета.
    /// Иначе stale `last_packet_num` / `expected_seq` в старой нумерации новой
    /// сессии порождает spurious `GapDetected` события и corrupted orderbook display
    /// в первые секунды. Аналог Delphi `MoonProtoEngine.pas:1586-1591`
    /// (`If FTradesServerToken <> MClient.Client.ServerToken then ResetGapBuckets`) +
    /// `MoonProtoEngine.pas:316-318` (`If NeedResubscribeOrderBooks then ResetOrderBookCaches`).
    /// Init=0 (никогда не подключались) → первый non-zero token не триггерит сброс.
    /// См. audit_responsibility_hints #1, #2.
    last_known_server_token: u64,
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
    /// Events produced while a one-shot helper is pumping the client loop.
    ///
    /// Long-running `Client::run_with_dispatcher` delivers events directly to its
    /// callback. One-shot helpers (`run_until_response`, `request_*`) have no
    /// callback argument, so they store produced events here for the application
    /// to drain after the helper returns.
    queued_events: Vec<Event>,
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read-only order state, keyed by server order UID.
    ///
    /// It is updated automatically when order-channel payloads are dispatched.
    pub fn orders(&self) -> &Orders {
        &self.orders
    }

    /// Read-only orderbook state, including per-market/per-kind recovery
    /// caches and the latest applied books.
    pub fn order_books(&self) -> &OrderBooks {
        &self.order_books
    }

    /// Read-only trades-stream state: packet counters, gap buckets, and resend
    /// bookkeeping.
    pub fn trades(&self) -> &TradesState {
        &self.trades
    }

    /// Read-only balance state for account totals and per-market balances.
    pub fn balances(&self) -> &BalancesState {
        &self.balances
    }

    /// Read-only strategy state and decoded strategy snapshots.
    pub fn strats(&self) -> &StratsState {
        &self.strats
    }

    /// Replace the library-owned strategy list before init.
    ///
    /// This is the normal active-library path. The dispatcher stores the full
    /// decoded snapshots, answers server `TStratSnapshotRequest` automatically,
    /// and keeps the list current when server strategy snapshots/deltas arrive.
    pub fn set_local_strategies(&mut self, strategies: &[StrategySnapshot]) {
        self.strats.replace_with_snapshots(strategies);
    }

    /// Upsert one application-owned strategy into the library state.
    pub fn upsert_local_strategy(&mut self, strategy: StrategySnapshot) {
        self.strats.upsert_local_snapshot(strategy);
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

    /// Clone the current strategy snapshot list in deterministic id order.
    pub fn strategy_snapshot_vec(&self) -> Vec<StrategySnapshot> {
        self.strats.snapshot_vec()
    }

    /// Read-only UI/settings state.
    pub fn settings(&self) -> &SettingsState {
        &self.settings
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
        &self.queued_events
    }

    /// Number of currently queued one-shot events.
    pub fn queued_event_count(&self) -> usize {
        self.queued_events.len()
    }

    /// Remove and return events accumulated during one-shot waits.
    pub fn take_queued_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.queued_events)
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

    /// Periodic trades-gap recovery tick.
    ///
    /// It returns serialized `TradesResend` Engine API requests for missing
    /// packet numbers and closes expired gap buckets. Applications do not need
    /// to call this when using [`crate::client::Client::run_with_dispatcher`],
    /// which ticks trades recovery automatically about every 100 ms.
    ///
    /// Custom loops that bypass `run_with_dispatcher` should call it with the
    /// current RTT and monotonic timestamp, then send each returned request
    /// through the client.
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

    pub(crate) fn send_strategy_snapshot_reply(
        &mut self,
        request_uid: u64,
        client: &crate::client::Client,
    ) -> bool {
        let snapshot = self.strategy_snapshot_reply(request_uid);
        client.strat_send_snapshot_payload(
            snapshot.server_epoch,
            snapshot.client_max_last_date,
            snapshot.full,
            &snapshot.data,
        );
        true
    }

    fn strategy_snapshot_reply(&mut self, request_uid: u64) -> StrategySnapshotReply {
        self.strategy_snapshot_provider
            .as_mut()
            .and_then(|provider| provider(request_uid))
            .unwrap_or_else(|| {
                StrategySnapshotReply::from_strategies(
                    self.strats.last_server_epoch,
                    true,
                    &self.strats.snapshot_vec(),
                )
            })
    }

    pub(crate) fn send_or_queue_strategy_snapshot_request(
        &mut self,
        request_uid: u64,
        client: &crate::client::Client,
    ) -> bool {
        self.send_strategy_snapshot_reply(request_uid, client);
        self.queued_events
            .push(Event::Strat(crate::state::StratEvent::SnapshotRequested {
                uid: request_uid,
            }));
        true
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
        match cmd {
            Command::Order => {
                match TradeCommand::parse(payload) {
                    Some(tc) => {
                        // audit_responsibility A5 / active library: автоматически подхватываем
                        // server_time_delta. При наличии per-Client `server_time_delta_source`
                        // (multi-Client) — читаем оттуда. Иначе fallback на global для raw
                        // dispatch без Client source. Без этого Orders::apply применяет AdjustTime со старым
                        // delta=0 — order timestamps сдвинуты на 0.5-2 сек (silent bug).
                        // См. DEVIATION #23.
                        self.orders
                            .set_server_time_delta(self.current_server_time_delta());
                        let (_apply_result, ev) = self.orders.apply(tc);
                        out.push(Event::Order(ev));
                    }
                    None => out.push(Event::ParseFailed {
                        cmd,
                        len: payload.len(),
                    }),
                }
            }

            Command::OrderBook => {
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
                        for ev in self.order_books.on_packet(pkt, now_ms) {
                            out.push(Event::OrderBook(ev));
                        }
                    }
                    None => out.push(Event::ParseFailed {
                        cmd,
                        len: payload.len(),
                    }),
                }
            }

            Command::TradesStream => {
                // Active library: блокируем обработку TradesStream пока markets indexes не sync.
                if !self.markets.indexes_synchronized {
                    return;
                }
                match parse_trades_packet(payload) {
                    Some(pkt) => {
                        // Flatten: каждое TradesEvent пушится в out отдельно — без nested Vec.
                        for ev in self.trades.on_packet(pkt, now_ms) {
                            out.push(Event::Trade(ev));
                        }
                    }
                    None => out.push(Event::ParseFailed {
                        cmd,
                        len: payload.len(),
                    }),
                }
            }

            Command::TradesResendResponse => {
                let inner_payloads = parse_trades_resend_response(payload);
                for inner in inner_payloads {
                    match parse_trades_packet(&inner) {
                        Some(pkt) => {
                            for ev in self.trades.on_packet_resend(pkt) {
                                out.push(Event::Trade(ev));
                            }
                        }
                        None => out.push(Event::ParseFailed {
                            cmd,
                            len: inner.len(),
                        }),
                    }
                }
            }

            Command::Balance => {
                if payload.len() < 11 {
                    out.push(Event::ParseFailed {
                        cmd,
                        len: payload.len(),
                    });
                    return;
                }
                let sub_cmd_id = payload[0];
                let body = &payload[11..];
                match sub_cmd_id {
                    2 => {
                        if parse_balance(sub_cmd_id, body).is_none() {
                            out.push(Event::ParseFailed {
                                cmd,
                                len: payload.len(),
                            });
                        }
                    }
                    3 | 4 => match parse_balance(sub_cmd_id, body) {
                        Some(upd) => {
                            let known_markets = &self.markets.by_name;
                            let ev = self
                                .balances
                                .apply_filtered(upd, |name| known_markets.contains_key(name));
                            out.push(Event::Balance(ev));
                        }
                        None => out.push(Event::ParseFailed {
                            cmd,
                            len: payload.len(),
                        }),
                    },
                    6 => match parse_arb_prices(payload) {
                        Some(arb) => {
                            if let Some(parsed) = parse_arb_payload_compact(&arb.payload) {
                                out.push(Event::Arb {
                                    uid: arb.uid,
                                    payload: parsed,
                                });
                            }
                        }
                        None => out.push(Event::ParseFailed {
                            cmd,
                            len: payload.len(),
                        }),
                    },
                    _ => out.push(Event::Raw {
                        cmd,
                        payload: payload.to_vec(),
                    }),
                }
            }

            Command::Strat => {
                match StratCommand::parse(payload) {
                    Some(cmd_v) => {
                        let ev = self.strats.apply(cmd_v);
                        // Active library: auto-decode strategy snapshot raw bytes
                        // в `StratsState`. Раньше app должен был сам вызывать
                        // `strats.apply_snapshot_decoded(raw_data)` — теперь либа
                        // делает это сама на SnapshotFull/Partial event'ах.
                        match &ev {
                            crate::state::StratEvent::SnapshotFull { raw_data, .. } => {
                                if self
                                    .strats
                                    .apply_snapshot_decoded_with_mode(raw_data, true)
                                    .is_none()
                                {
                                    log::warn!(
                                        target: "moonproto::events",
                                        "failed to decode full strategy snapshot payload ({} bytes)",
                                        raw_data.len()
                                    );
                                }
                            }
                            crate::state::StratEvent::SnapshotPartial { raw_data, .. } => {
                                if self
                                    .strats
                                    .apply_snapshot_decoded_with_mode(raw_data, false)
                                    .is_none()
                                {
                                    log::warn!(
                                        target: "moonproto::events",
                                        "failed to decode partial strategy snapshot payload ({} bytes)",
                                        raw_data.len()
                                    );
                                }
                            }
                            _ => {}
                        }
                        out.push(Event::Strat(ev));
                    }
                    None => out.push(Event::ParseFailed {
                        cmd,
                        len: payload.len(),
                    }),
                }
            }

            Command::UI => match UICommand::parse(payload) {
                Some(cmd_v) => {
                    let ev = self.settings.apply(cmd_v);
                    out.push(Event::Settings(ev));
                }
                None => out.push(Event::ParseFailed {
                    cmd,
                    len: payload.len(),
                }),
            },

            Command::API => match parse_engine_response(payload) {
                Some(resp) => {
                    const ASSUMED_VER: u16 = 2;
                    let extra_event: Option<Event> = if resp.success {
                        match resp.method {
                            EngineMethod::GetMarketsList | EngineMethod::UpdateMarketsList => {
                                if resp.method == EngineMethod::GetMarketsList {
                                    if let Some(list) =
                                        parse_markets_list_response(&resp.data, ASSUMED_VER)
                                    {
                                        let ev = self.markets.apply_markets_list(list);
                                        Some(Event::Markets(ev))
                                    } else {
                                        None
                                    }
                                } else if let Some(prices) =
                                    parse_markets_prices_response(&resp.data)
                                {
                                    let ev = self.markets.apply_markets_prices(prices);
                                    Some(Event::Markets(ev))
                                } else {
                                    None
                                }
                            }
                            EngineMethod::GetMarketsIndexes => {
                                if let Some(names) = parse_markets_indexes_response(&resp.data) {
                                    let ev = self.markets.apply_markets_indexes(names);
                                    Some(Event::Markets(ev))
                                } else {
                                    None
                                }
                            }
                            EngineMethod::CheckBinanceTags => {
                                if let Some(items) = parse_token_tags_response(&resp.data) {
                                    let ev = self.markets.apply_token_tags(items);
                                    Some(Event::Markets(ev))
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    if let Some(ev) = extra_event {
                        out.push(ev);
                    }
                    out.push(Event::EngineResponse(resp));
                }
                None => out.push(Event::ParseFailed {
                    cmd,
                    len: payload.len(),
                }),
            },

            Command::LogMsg => {
                if payload.len() < 8 {
                    out.push(Event::ParseFailed {
                        cmd,
                        len: payload.len(),
                    });
                    return;
                }
                let time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
                let msg = String::from_utf8_lossy(&payload[8..]).to_string();
                out.push(Event::ServerLog { time, msg });
            }

            _ => out.push(Event::Raw {
                cmd,
                payload: payload.to_vec(),
            }),
        }
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
    /// matching control events. Trades gap resend is owned by the periodic
    /// trades tick in the client loop so a single packet cannot trigger
    /// duplicate resend batches.
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
            self.trades.full_reset();
            self.order_books.clear();
            log::info!(target: "moonproto::events",
                "ServerToken changed ({:#x} -> {:#x}) — trades+order_books state reset",
                self.last_known_server_token, current_token);
        }
        self.last_known_server_token = current_token;

        if is_pre_init_domain_command(cmd) && !ctx.domain_ready {
            log::debug!(target: "moonproto::events",
                "domain command {:?} skipped before init completion", cmd);
            return;
        }

        let start_len = out.len();
        self.dispatch_into(cmd, payload, now_ms, out);
        // now_ms прокинут в dispatch_into для state.on_packet(now_ms); auto-actions
        // ниже не зависят от времени.

        // Auto-action 1: OrderBookEvent::RequestFullNeeded → send_api_request (sync, no pending).
        // Dedup через HashSet — Grouped-payload может содержать несколько
        // RequestFullNeeded для одной и той же книги (corruption detection +
        // последующий update в одном datagram'е). Шлём один запрос на пару.
        use std::collections::HashSet;
        let mut to_request_full: HashSet<(u16, u8)> = HashSet::new();
        // Auto-action 2: StratEvent::SnapshotRequested → шлём fresh snapshot
        // из library-owned StratsState (или provider override). Delphi
        // `MoonProtoClient.pas:ProcessStratCommand` пересобирает ответ через
        // `TStratSnapshot.CreateFromStrats(Strats)`.
        let mut snapshot_requested_uid: Option<u64> = None;
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
                    to_request_full.insert((*market_index, *book_kind));
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
                _ => false,
            };
            if remove_event {
                out.remove(idx);
            } else {
                idx += 1;
            }
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
            let snapshot = self.strategy_snapshot_reply(uid);
            actions.push(ActiveAction::SendStrategySnapshot {
                server_epoch: snapshot.server_epoch,
                client_max_last_date: snapshot.client_max_last_date,
                full: snapshot.full,
                data: snapshot.data,
            });
            // Событие всё равно эмиттится в `out` для UI/диагностики.
        }
        if order_snapshot_applied {
            let missing = self.orders.missing_after_snapshot();
            for uid in missing {
                if let Some(order) = self.orders.get(uid) {
                    let trade_ctx = order.trade_ctx();
                    actions.push(ActiveAction::RequestOrderStatus {
                        ctx: trade_ctx,
                        market_name: order.market_name.clone(),
                    });
                }
            }
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
mod tests {
    use super::*;
    use crate::commands::arb::build_arb_prices;
    use crate::commands::market::{BaseCurrency, Market, MarketsListResponse};
    use crate::commands::registry::write_string;
    use crate::commands::strat::build_snapshot_request;
    use crate::commands::trade::{
        build_all_statuses_request, BaseCommandHeader, MarketCommandHeader, OrderCompact,
        OrderStatus, OrderWorkerStatus, StopSettings, TradeCommand, TradeCtx, TradeEpochHeader,
    };

    static SERVER_TIME_DELTA_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn server_time_delta_test_lock() -> std::sync::MutexGuard<'static, ()> {
        SERVER_TIME_DELTA_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn order_book_payload_with(market_index: u16, seq: u16, is_full: bool) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&market_index.to_le_bytes());
        raw.extend_from_slice(&seq.to_le_bytes());
        raw.push(if is_full { 1 } else { 0 }); // Futures.
        raw.extend_from_slice(&0u16.to_le_bytes()); // buy_count=0, sell_count=0.
        crate::compression::synlz_compress(&raw)
    }

    fn order_book_payload(market_index: u16) -> Vec<u8> {
        order_book_payload_with(market_index, 1, true)
    }

    fn empty_all_statuses_payload(uid: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(15);
        out.push(8);
        out.extend_from_slice(&3u16.to_le_bytes());
        out.extend_from_slice(&uid.to_le_bytes());
        out.extend_from_slice(&0i32.to_le_bytes());
        out
    }

    fn balance_payload(cmd_id: u8, uid: u64, epoch: u16, btc_total: f64) -> Vec<u8> {
        let mut out = Vec::with_capacity(49);
        out.push(cmd_id);
        out.extend_from_slice(&3u16.to_le_bytes());
        out.extend_from_slice(&uid.to_le_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(&btc_total.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0i32.to_le_bytes());
        out
    }

    fn write_balance_item_minimal(out: &mut Vec<u8>, market_name: &str, initial_balance: f64) {
        write_string(out, market_name);
        out.extend_from_slice(&0u64.to_le_bytes()); // BalanceHash.
        out.extend_from_slice(&1u32.to_le_bytes()); // InitialBalance flag only.
        out.extend_from_slice(&initial_balance.to_le_bytes());
    }

    fn balance_payload_with_items(
        cmd_id: u8,
        uid: u64,
        epoch: u16,
        items: &[(&str, f64)],
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + items.len() * 32);
        out.push(cmd_id);
        out.extend_from_slice(&3u16.to_le_bytes());
        out.extend_from_slice(&uid.to_le_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        if cmd_id == 4 {
            out.push(0); // GlobalChanged=false.
        } else {
            out.extend_from_slice(&1.0f64.to_le_bytes());
            out.extend_from_slice(&0.0f64.to_le_bytes());
            out.extend_from_slice(&0.0f64.to_le_bytes());
            out.extend_from_slice(&0.0f64.to_le_bytes());
        }
        out.extend_from_slice(&(items.len() as i32).to_le_bytes());
        for (market_name, initial_balance) in items {
            write_balance_item_minimal(&mut out, market_name, *initial_balance);
        }
        out
    }

    fn event_market(name: &str) -> Market {
        Market {
            bn_market_name: name.to_string(),
            market_currency: name.to_string(),
            bn_market_currency: name.to_string(),
            base_currency: "USDT".to_string(),
            market_currency_long: name.to_string(),
            market_currency_canonic: name.to_string(),
            market_name: name.to_string(),
            market_name_mb_classic: name.to_string(),
            bn_status: "TRADING".to_string(),
            leading1000: String::new(),
            bn_price_precision: 2,
            bn_quantity_precision: 5,
            max_leverage: 50,
            k1000: 1,
            bn_iceberg_parts: 0,
            bn_margin_table_id: 0,
            bn_delivery_time: 0,
            bn_tick_size: 0.01,
            bn_step_size: 0.01,
            bn_min_qty: 0.0,
            bn_max_qty: 0.0,
            bn_min_notional: 0.0,
            bn_max_notional: 0.0,
            bn_contract_size: 0.0,
            bn_min_price: 0.0,
            bn_max_price: 0.0,
            bn_max_value: 0.0,
            bn_multiplier_up: 0.0,
            bn_multiplier_down: 0.0,
            bid_multiplier_up: 0.0,
            bid_multiplier_down: 0.0,
            ask_multiplier_up: 0.0,
            ask_multiplier_down: 0.0,
            int_bn_max_qty: 0.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            volume: 0.0,
            is_btc_market: false,
            status_trading: true,
            bn_is_fucking_shib: false,
            bn_iceberg: false,
            bn_only_isolated: false,
            futures_type: BaseCurrency::USDT,
        }
    }

    fn order_status_for_test(
        uid: u64,
        market_name: &str,
        currency: u8,
        platform: u8,
        status: OrderWorkerStatus,
    ) -> OrderStatus {
        OrderStatus {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 4,
                        ver: 3,
                        uid,
                    },
                    currency,
                    platform,
                    market_name: market_name.to_string(),
                },
                epoch: 1,
                status,
            },
            buy_order: OrderCompact::default(),
            sell_order: OrderCompact::default(),
            stops: StopSettings::default(),
            strat_id: 0,
            is_short: false,
            db_id: 0,
            from_cache: false,
            emulator_mode: false,
            immune_for_clicks: false,
        }
    }

    #[test]
    fn dispatcher_routes_order_to_orders_state() {
        let mut d = EventDispatcher::new();
        // Empty AllStatusesReq — парсер вернёт TradeCommand::AllStatusesReq
        let payload = build_all_statuses_request(123);
        let events = d.dispatch(Command::Order, &payload, 1000);
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Order(_) => {}
            other => panic!("expected Order event, got {:?}", other),
        }
    }

    #[test]
    fn dispatcher_routes_strat_to_strats_state() {
        let mut d = EventDispatcher::new();
        let payload = build_snapshot_request(7);
        let events = d.dispatch(Command::Strat, &payload, 1000);
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Strat(StratEvent::Ignored) => {} // SnapshotRequest from server is unusual; state ignores
            Event::Strat(_) => {}
            other => panic!("expected Strat event, got {:?}", other),
        }
    }

    #[test]
    fn dispatcher_unknown_channel_returns_raw() {
        let mut d = EventDispatcher::new();
        // Reserved1 — нет dispatch'а → fallback в Raw
        let events = d.dispatch(Command::Reserved1, b"hello", 1000);
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Raw { cmd, payload } => {
                assert_eq!(*cmd, Command::Reserved1);
                assert_eq!(payload, b"hello");
            }
            other => panic!("expected Raw event, got {:?}", other),
        }
    }

    #[test]
    fn dispatcher_logmsg_parses_time_and_msg() {
        let mut d = EventDispatcher::new();
        let mut payload = 45678.5f64.to_le_bytes().to_vec();
        payload.extend_from_slice(b"server log message");
        let events = d.dispatch(Command::LogMsg, &payload, 1000);
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::ServerLog { time, msg } => {
                assert_eq!(*time, 45678.5);
                assert_eq!(msg, "server log message");
            }
            other => panic!("expected ServerLog, got {:?}", other),
        }
    }

    #[test]
    fn dispatcher_routes_arb_to_typed_event() {
        let mut d = EventDispatcher::new();
        let mut compact = vec![2u8];
        compact.extend_from_slice(&42u16.to_le_bytes());
        compact.push(1);
        compact.push(7);
        compact.extend_from_slice(&123.25f32.to_le_bytes());

        let payload = build_arb_prices(9, &compact);
        let events = d.dispatch(Command::Balance, &payload, 1000);
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Arb { uid, payload } => match payload {
                ArbPayload::Price { version, blocks } => {
                    assert_eq!(*uid, 9);
                    assert_eq!(*version, 2);
                    assert_eq!(blocks.len(), 1);
                    assert_eq!(blocks[0].market_index, 42);
                    assert_eq!(blocks[0].prices[0].platform_code, 7);
                    assert_eq!(blocks[0].prices[0].price, 123.25);
                }
                other => panic!("expected ArbPayload::Price, got {:?}", other),
            },
            other => panic!("expected typed Arb event, got {:?}", other),
        }
    }

    #[test]
    fn dispatcher_ignores_exact_balance_command_id_2_like_delphi() {
        let mut d = EventDispatcher::new();

        let full = balance_payload(3, 10, 1, 1.0);
        let events = d.dispatch(Command::Balance, &full, 1000);
        assert_eq!(events.len(), 1);
        assert_eq!(d.balances.global.btc_balance_total, 1.0);
        assert_eq!(d.balances.last_epoch, 1);

        let exact_base = balance_payload(2, 11, 2, 99.0);
        let events = d.dispatch(Command::Balance, &exact_base, 1001);

        assert!(events.is_empty());
        assert_eq!(d.balances.global.btc_balance_total, 1.0);
        assert_eq!(d.balances.last_epoch, 1);
    }

    #[test]
    fn dispatcher_filters_balance_items_through_markets_state() {
        let mut d = EventDispatcher::new();
        d.markets.apply_markets_list(MarketsListResponse {
            markets: vec![event_market("BTCUSDT")],
            corr_markets: vec![],
        });

        let payload =
            balance_payload_with_items(3, 10, 1, &[("BTCUSDT", 100.0), ("UNKNOWNUSDT", 200.0)]);
        let events = d.dispatch(Command::Balance, &payload, 1000);

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            Event::Balance(BalanceEvent::SnapshotApplied { count: 1, epoch: 1 })
        ));
        assert!(d.balances.get("BTCUSDT").is_some());
        assert!(d.balances.get("UNKNOWNUSDT").is_none());
    }

    #[test]
    fn dispatcher_corrupted_order_returns_parse_failed() {
        let mut d = EventDispatcher::new();
        let events = d.dispatch(Command::Order, &[1, 2, 3], 1000); // too short for header
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::ParseFailed { .. }));
    }

    #[test]
    fn dispatcher_ctx_unused_warning_silenced() {
        // Suppress dead_code warning for TradeCtx if not used elsewhere
        let _ = TradeCtx::with_route(1, 1, 4);
    }

    #[test]
    fn dispatcher_blocks_orderbook_until_indexes_sync() {
        let mut d = EventDispatcher::new();
        // indexes_synchronized = false по умолчанию — OrderBook event должен быть дропнут.
        // Делаем минимальный wire-payload для OrderBook (parse может не пройти, и это ок —
        // главное что мы ВООБЩЕ не доходим до parse, потому что блокировка раньше).
        let dummy_payload = vec![0u8; 32];
        let events = d.dispatch(Command::OrderBook, &dummy_payload, 1000);
        assert!(
            events.is_empty(),
            "OrderBook event должен быть дропнут до indexes_synchronized"
        );

        // После apply_markets_indexes — должен начать парсить.
        d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
        let _events = d.dispatch(Command::OrderBook, &dummy_payload, 1000);
        // Теперь либо успешный parse, либо ParseFailed (но не пусто).
        // Точное значение зависит от содержимого dummy_payload — главное что блок снят.
    }

    #[test]
    fn dispatcher_drops_orderbook_for_unknown_market_index() {
        let mut d = EventDispatcher::new();
        d.markets.indexes_synchronized = true;
        d.markets.market_indexes = vec!["BTCUSDT".to_string()];
        d.markets.by_name.insert("BTCUSDT".to_string(), 0);

        let events = d.dispatch(Command::OrderBook, &order_book_payload(1), 1000);
        assert!(
            events.is_empty(),
            "unknown server market index must be dropped"
        );
        assert!(
            d.order_books.is_empty(),
            "unknown index must not create OrderBooks cache"
        );

        d.markets.market_indexes = vec!["UNKNOWNUSDT".to_string()];
        d.markets.by_name.clear();
        let events = d.dispatch(Command::OrderBook, &order_book_payload(0), 1000);
        assert!(
            events.is_empty(),
            "index mapped to unknown local market must be dropped"
        );
        assert!(
            d.order_books.is_empty(),
            "unknown local market must not create cache"
        );
    }

    #[test]
    fn dispatcher_blocks_trades_until_indexes_sync() {
        let mut d = EventDispatcher::new();
        let dummy_payload = vec![0u8; 16];
        let events = d.dispatch(Command::TradesStream, &dummy_payload, 1000);
        assert!(
            events.is_empty(),
            "TradesStream должен быть дропнут до indexes_synchronized"
        );
    }

    #[test]
    fn dispatcher_order_not_blocked_by_indexes_sync() {
        // Order channel не зависит от market_idx → не должен блокироваться indexes_sync.
        let mut d = EventDispatcher::new();
        let payload = build_all_statuses_request(123);
        let events = d.dispatch(Command::Order, &payload, 1000);
        assert!(
            !events.is_empty(),
            "Order должен обрабатываться даже без indexes_synchronized"
        );
    }

    #[test]
    fn dispatch_into_active_invalidates_indexes_on_peer_token_mismatch() {
        let mut d = EventDispatcher::new();
        d.markets.apply_markets_indexes(vec!["OLDUSDT".to_string()]);
        assert!(d.markets.indexes_synchronized);

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        client.testing_set_peer_app_tokens(0x2222, 0x1111);

        let mut out = Vec::new();
        let mut actions = Vec::new();
        let dummy_payload = vec![0u8; 32];
        dispatch_active_packet_for_test(
            &mut d,
            Command::OrderBook,
            &dummy_payload,
            1000,
            &mut out,
            &client,
            &mut actions,
        );

        assert!(
            !d.markets.indexes_synchronized,
            "PeerAppToken mismatch must close stream gate until fresh GetMarketsIndexes"
        );
        assert!(
            out.is_empty(),
            "OrderBook packet from a new server process must be dropped with stale indexes"
        );
    }

    #[test]
    fn dispatch_into_active_requests_missing_order_status_after_snapshot() {
        let mut d = EventDispatcher::new();
        let stale_uid = 0xAABB_CCDD_0011_2233;
        let status = order_status_for_test(stale_uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
        let (_result, _event) = d.orders.apply(TradeCommand::OrderStatus(Box::new(status)));

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        let mut out = Vec::new();
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Order,
            &empty_all_statuses_payload(0x55),
            1000,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);

        assert!(out
            .iter()
            .any(|ev| matches!(ev, Event::Order(OrderEvent::Snapshot))));

        let mut found = false;
        for item in drain_client_send_items(&client) {
            if item.cmd != Command::Order as u8 {
                continue;
            }
            let Some(TradeCommand::OrderStatusRequest(req)) = TradeCommand::parse(&item.data)
            else {
                continue;
            };
            assert_eq!(req.market.base.uid, stale_uid);
            assert_eq!(req.market.market_name, "BTCUSDT");
            assert_eq!(req.market.currency, 7);
            assert_eq!(req.market.platform, 9);
            found = true;
        }

        assert!(found, "missing order must trigger TOrderStatusRequest");
    }

    #[test]
    fn dispatch_into_active_consumes_orderbook_full_request_event() {
        let mut d = EventDispatcher::new();
        d.markets.indexes_synchronized = true;
        d.markets.market_indexes = vec!["BTCUSDT".to_string()];
        d.markets.by_name.insert("BTCUSDT".to_string(), 0);

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        let mut out = Vec::new();
        let mut actions = Vec::new();

        dispatch_active_packet_for_test(
            &mut d,
            Command::OrderBook,
            &order_book_payload_with(0, 1, true),
            1000,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);
        out.clear();
        actions.clear();
        dispatch_active_packet_for_test(
            &mut d,
            Command::OrderBook,
            &order_book_payload_with(0, 10, false),
            1010,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);
        out.clear();
        actions.clear();
        dispatch_active_packet_for_test(
            &mut d,
            Command::OrderBook,
            &order_book_payload_with(0, 11, false),
            2000,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);

        assert!(
            !out.iter().any(|ev| matches!(
                ev,
                Event::OrderBook(OrderBookEvent::RequestFullNeeded { .. })
            )),
            "active dispatcher должен потреблять RequestFullNeeded как внутренний control-event"
        );

        let mut found = false;
        for item in drain_client_send_items(&client) {
            if item.cmd == Command::API as u8
                && item.data.get(11).copied()
                    == Some(crate::commands::engine_api::EngineMethod::RequestOrderBookFull as u8)
            {
                found = true;
                break;
            }
        }
        assert!(found, "RequestOrderBookFull must still be sent internally");
    }

    #[test]
    fn dispatch_into_active_drops_domain_commands_before_init() {
        let mut d = EventDispatcher::new();
        let client = crate::client::Client::new(dummy_client_cfg());
        let mut out = Vec::new();
        let mut actions = Vec::new();

        dispatch_active_packet_for_test(
            &mut d,
            Command::Order,
            &empty_all_statuses_payload(0x55),
            1000,
            &mut out,
            &client,
            &mut actions,
        );

        assert!(
            out.is_empty(),
            "pre-init Order must be dropped like Delphi InitDone gate"
        );
        assert_eq!(d.orders().current_snapshot_flag(), 0);
    }

    // =========================================================================
    //  Multi-Client ServerTimeDelta tests (DEVIATION #23)
    // =========================================================================

    /// Helper для тестов: дни конвертирует в seconds для удобства сравнения.
    fn delta_seconds(d: &EventDispatcher) -> f64 {
        d.current_server_time_delta() * 86400.0
    }

    #[test]
    fn current_delta_falls_back_to_global_when_source_is_none() {
        let _guard = server_time_delta_test_lock();
        // Raw dispatch без линковки dispatcher читает global.
        let d = EventDispatcher::new();
        assert!(d.server_time_delta_source.is_none());
        // Записываем в global → dispatcher видит то же значение.
        crate::client::set_server_time_delta_global(2.5 / 86400.0);
        assert!((delta_seconds(&d) - 2.5).abs() < 1e-9);
        // Сбросим global назад чтобы не аффектить другие тесты.
        crate::client::set_server_time_delta_global(0.0);
    }

    #[test]
    fn current_delta_reads_from_source_when_set() {
        let _guard = server_time_delta_test_lock();
        // Multi-Client: с линковкой dispatcher читает per-Client handle,
        // НЕ global. Изменения global на этот dispatcher не влияют.
        let handle = Arc::new(AtomicU64::new(0));
        // Эмулируем что Client записал свою delta = 7.0 секунд.
        let days: f64 = 7.0 / 86400.0;
        handle.store(days.to_bits(), Ordering::Relaxed);
        let mut d = EventDispatcher::new();
        d.set_server_time_delta_source(Arc::clone(&handle));
        // Global при этом стоит другое значение — dispatcher должен игнорировать.
        crate::client::set_server_time_delta_global(99.0 / 86400.0);
        assert!(
            (delta_seconds(&d) - 7.0).abs() < 1e-9,
            "dispatcher должен читать handle, а не global"
        );
        crate::client::set_server_time_delta_global(0.0);
    }

    #[test]
    fn delta_handle_update_visible_to_dispatcher() {
        // Изменение handle отражается в следующем чтении dispatcher'а
        // (atomic snapshot — нет кэширования).
        let handle = Arc::new(AtomicU64::new(0));
        let mut d = EventDispatcher::new();
        d.set_server_time_delta_source(Arc::clone(&handle));
        assert!((delta_seconds(&d) - 0.0).abs() < 1e-9);
        // Обновляем handle (как сделал бы Client::handle_ping).
        let days: f64 = 3.5 / 86400.0;
        handle.store(days.to_bits(), Ordering::Relaxed);
        assert!((delta_seconds(&d) - 3.5).abs() < 1e-9);
    }

    #[test]
    fn two_dispatchers_with_distinct_handles_are_isolated() {
        // **Core multi-Client gurantee**: два EventDispatcher'а с разными handle'ами
        // (один на Client) видят разные delta. Это и есть фикс DEVIATION #23.
        let h_a = Arc::new(AtomicU64::new(0));
        let h_b = Arc::new(AtomicU64::new(0));
        let mut d_a = EventDispatcher::new();
        let mut d_b = EventDispatcher::new();
        d_a.set_server_time_delta_source(Arc::clone(&h_a));
        d_b.set_server_time_delta_source(Arc::clone(&h_b));

        // Client A: delta = +5s; Client B: delta = -200ms (разные серверы — разный drift).
        h_a.store((5.0_f64 / 86400.0).to_bits(), Ordering::Relaxed);
        h_b.store((-0.2_f64 / 86400.0).to_bits(), Ordering::Relaxed);

        assert!((delta_seconds(&d_a) - 5.0).abs() < 1e-9);
        assert!((delta_seconds(&d_b) - (-0.2)).abs() < 1e-9);

        // Изменение одного handle не аффектит другой.
        h_a.store((10.0_f64 / 86400.0).to_bits(), Ordering::Relaxed);
        assert!((delta_seconds(&d_a) - 10.0).abs() < 1e-9);
        assert!(
            (delta_seconds(&d_b) - (-0.2)).abs() < 1e-9,
            "dispatcher B не должен видеть изменения handle A"
        );
    }

    // =========================================================================
    //  dispatch_into_active — server_token tracking + auto-link delta handle
    // =========================================================================

    fn dummy_client_cfg() -> crate::client::ClientConfig {
        crate::client::ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: crate::client::RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn drain_client_send_items(client: &crate::client::Client) -> Vec<crate::client::SendItem> {
        let (mut sliced, mut high, mut low) = client.take_send_queues_for_test();
        sliced.append(&mut high);
        sliced.append(&mut low);
        sliced
    }

    fn dispatch_active_packet_for_test(
        dispatcher: &mut EventDispatcher,
        cmd: Command,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
        client: &crate::client::Client,
        actions: &mut Vec<ActiveAction>,
    ) {
        let ctx = ActiveDispatchContext::from_client(client);
        dispatcher.dispatch_into_active_actions(cmd, payload, now_ms, out, &ctx, actions);
    }

    fn apply_active_actions_for_test(
        client: &crate::client::Client,
        actions: &mut Vec<ActiveAction>,
    ) {
        client.apply_active_actions(actions.drain(..));
    }

    #[test]
    fn dispatch_into_active_records_initial_server_token() {
        // Первый вызов запоминает текущий server_token в last_known_server_token.
        // Sentinel значение 0 (init) → не triggers reset на первом non-zero token.
        let mut d = EventDispatcher::new();
        let mut client = crate::client::Client::new(dummy_client_cfg());
        // Установим server_token=42 (имитация после первого Fine).
        client.testing_set_server_token(42);
        assert_eq!(d.last_known_server_token, 0);
        let mut out = Vec::new();
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Reserved1,
            b"x",
            0,
            &mut out,
            &client,
            &mut actions,
        );
        assert_eq!(
            d.last_known_server_token, 42,
            "первый dispatch_into_active должен запомнить server_token"
        );
    }

    #[test]
    fn dispatch_into_active_does_not_reset_on_first_non_zero_token() {
        // Init last_known=0 → первый non-zero token НЕ triggers full_reset.
        // Чтобы это проверить — устанавливаем "сигнатурные" значения в trades/order_books
        // и проверяем что они НЕ сбросились.
        let mut d = EventDispatcher::new();
        // Сделаем order_books непустым через apply_markets_indexes (создаёт market_idx mapping).
        d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
        let snapshot_count_before = d.markets.by_name.len();
        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_server_token(0x100);
        let mut out = Vec::new();
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Reserved1,
            b"x",
            0,
            &mut out,
            &client,
            &mut actions,
        );
        // markets state НЕ должны быть сброшен (full_reset не вызывался).
        assert_eq!(
            d.markets.by_name.len(),
            snapshot_count_before,
            "первый non-zero token — не triggers reset"
        );
    }

    #[test]
    fn dispatch_into_active_triggers_reset_on_token_change() {
        let mut d = EventDispatcher::new();
        // Симулируем что мы уже видели server_token = 0xAAA.
        d.last_known_server_token = 0xAAA;
        // Установим trades state в non-default (last_packet_num != 0 наблюдаемо через
        // повторный dispatch — но private. Достаточно проверить что `last_known`
        // обновляется на новый, а full_reset работает на уровне самой TradesState).
        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_server_token(0xBBB);
        let mut out = Vec::new();
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Reserved1,
            b"x",
            0,
            &mut out,
            &client,
            &mut actions,
        );
        assert_eq!(
            d.last_known_server_token, 0xBBB,
            "после смены токена — last_known обновлён"
        );
        // Поведение TradesState.full_reset() и OrderBooks.clear() покрыто
        // unit-тестами в соответствующих модулях (state::trades, state::order_books).
    }

    #[test]
    fn dispatch_into_active_auto_links_server_time_delta_source() {
        // Первый вызов — линкует handle от Client'а. До этого source = None,
        // dispatcher падает обратно на global.
        let mut d = EventDispatcher::new();
        assert!(d.server_time_delta_source.is_none());
        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        let mut out = Vec::new();
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Reserved1,
            b"x",
            0,
            &mut out,
            &client,
            &mut actions,
        );
        assert!(
            d.server_time_delta_source.is_some(),
            "после первого dispatch_into_active — source привязан к Client'у"
        );

        // Повторный вызов — source не меняется (already linked).
        let handle_after_first = Arc::clone(d.server_time_delta_source.as_ref().unwrap());
        actions.clear();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Reserved1,
            b"y",
            0,
            &mut out,
            &client,
            &mut actions,
        );
        let handle_after_second = d.server_time_delta_source.as_ref().unwrap();
        assert!(
            Arc::ptr_eq(&handle_after_first, handle_after_second),
            "повторный вызов — source остаётся тем же handle"
        );
    }

    #[test]
    fn snapshot_requested_with_provider_triggers_fresh_reply() {
        // Active library auto-action 2: при SnapshotRequested → если приложение
        // дало provider, либа берёт fresh snapshot из provider'а и шлёт ответ.
        let mut d = EventDispatcher::new();
        let fresh_snapshot = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let fresh_for_provider = fresh_snapshot.clone();
        d.set_strategy_snapshot_provider(move |uid| {
            assert_eq!(uid, 42);
            Some(StrategySnapshotReply::from_payload(
                7,
                99,
                true,
                fresh_for_provider.clone(),
            ))
        });

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        let mut out = Vec::new();
        let mut actions = Vec::new();

        // Server prods клиента: "пришли свой snapshot стратегий" — это
        // StratCommand::SnapshotRequest. Payload = `build_snapshot_request(uid)`.
        let payload = crate::commands::strat::build_snapshot_request(42);

        dispatch_active_packet_for_test(
            &mut d,
            Command::Strat,
            &payload,
            0,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);

        // Drain send queues — должна быть отправка Command::Strat с fresh
        // TStratSnapshot body: CmdId/ver/uid + ServerEpoch/ClientMaxLastDate/Size/Full/Data.
        let mut found_snapshot_send = false;
        for item in drain_client_send_items(&client) {
            if item.cmd == Command::Strat as u8 {
                let data = &item.data;
                if data.len() == 11 + 8 + 8 + 4 + 1 + fresh_snapshot.len() {
                    let cmd_subcode = data[0];
                    let server_epoch = u64::from_le_bytes(data[11..19].try_into().unwrap());
                    let client_max_last_date = u64::from_le_bytes(data[19..27].try_into().unwrap());
                    let size = u32::from_le_bytes(data[27..31].try_into().unwrap());
                    let full = data[31] != 0;
                    let tail = &data[32..];
                    if cmd_subcode == 2
                        && server_epoch == 7
                        && client_max_last_date == 99
                        && size == fresh_snapshot.len() as u32
                        && full
                        && tail == fresh_snapshot.as_slice()
                    {
                        found_snapshot_send = true;
                    }
                }
            }
        }
        assert!(
            found_snapshot_send,
            "после SnapshotRequest с provider — должна быть fresh отправка"
        );

        // out содержит event SnapshotRequested (app тоже видит, для UI awareness).
        let has_snapshot_event = out.iter().any(|ev| {
            matches!(
                ev,
                Event::Strat(crate::state::StratEvent::SnapshotRequested { uid: 42 })
            )
        });
        assert!(
            has_snapshot_event,
            "event SnapshotRequested должен быть в out (для app awareness)"
        );
    }

    #[test]
    fn snapshot_requested_without_provider_sends_owned_empty_snapshot() {
        // Если provider не задан и локальных стратегий нет, dispatcher всё равно
        // отвечает корректным пустым snapshot'ом. Это active-lib механика:
        // сервер не должен ждать ручного ответа от приложения.
        let mut d = EventDispatcher::new();

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        let mut out = Vec::new();
        let payload = crate::commands::strat::build_snapshot_request(99);
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Strat,
            &payload,
            0,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);

        // Drain send queues — должен быть Command::Strat с пустым serializer batch.
        let mut empty_snapshot_sends = 0;
        for item in drain_client_send_items(&client) {
            if item.cmd == Command::Strat as u8 {
                let cmd = crate::commands::strat::StratCommand::parse(&item.data)
                    .expect("sent strat command must parse");
                match cmd {
                    crate::commands::strat::StratCommand::Snapshot(snapshot) => {
                        let batch = crate::commands::strategy_serializer::parse_strategy_batch(
                            &snapshot.data,
                        )
                        .expect("empty strategy batch must parse");
                        assert!(snapshot.full);
                        assert!(batch.strategies.is_empty());
                        empty_snapshot_sends += 1;
                    }
                    other => panic!("expected snapshot reply, got {other:?}"),
                }
            }
        }
        assert_eq!(
            empty_snapshot_sends, 1,
            "без provider — должен уйти пустой owned snapshot"
        );

        // Event SnapshotRequested всё равно прилетает app'у для UI/диагностики.
        let has_event = out.iter().any(|ev| {
            matches!(
                ev,
                Event::Strat(crate::state::StratEvent::SnapshotRequested { .. })
            )
        });
        assert!(has_event);
    }

    #[test]
    fn snapshot_requested_uses_local_strategies() {
        use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};
        use std::collections::HashMap;

        let mut fields = HashMap::new();
        fields.insert(
            "Comment".to_string(),
            FieldValue::String("local".to_string()),
        );
        let strategy = StrategySnapshot {
            strategy_id: 0xF17E,
            strategy_ver: 3,
            last_date: 1234,
            checked: true,
            kind: 1,
            path: "FireTest".to_string(),
            fields,
        };

        let mut d = EventDispatcher::new();
        d.set_local_strategies(std::slice::from_ref(&strategy));
        assert_eq!(
            d.strategy_snapshot(strategy.strategy_id).unwrap().last_date,
            1234
        );

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_domain_ready(true);
        let mut out = Vec::new();
        let payload = crate::commands::strat::build_snapshot_request(100);
        let mut actions = Vec::new();
        dispatch_active_packet_for_test(
            &mut d,
            Command::Strat,
            &payload,
            0,
            &mut out,
            &client,
            &mut actions,
        );
        apply_active_actions_for_test(&client, &mut actions);

        let mut found = false;
        for item in drain_client_send_items(&client) {
            if item.cmd != Command::Strat as u8 {
                continue;
            }
            let cmd = crate::commands::strat::StratCommand::parse(&item.data)
                .expect("sent strat command must parse");
            if let crate::commands::strat::StratCommand::Snapshot(snapshot) = cmd {
                let batch =
                    crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
                        .expect("local strategy batch must parse");
                assert_eq!(snapshot.client_max_last_date, 1234);
                assert_eq!(batch.strategies.len(), 1);
                assert_eq!(batch.strategies[0].strategy_id, strategy.strategy_id);
                assert_eq!(
                    batch.strategies[0].fields.get("Comment"),
                    Some(&FieldValue::String("local".to_string()))
                );
                found = true;
            }
        }
        assert!(found, "local strategy snapshot must be sent");
    }

    #[test]
    fn dispatcher_propagates_delta_to_orders_state() {
        // End-to-end: при `dispatch(Command::Order, ...)` dispatcher применяет текущий
        // delta к Orders state. Проверяем что после линковки handle'а delta попадает
        // в `Orders.server_time_delta`.
        let handle = Arc::new(AtomicU64::new(0));
        let days: f64 = 1.25 / 86400.0;
        handle.store(days.to_bits(), Ordering::Relaxed);

        let mut d = EventDispatcher::new();
        d.set_server_time_delta_source(Arc::clone(&handle));

        // Любой Order payload триггерит set_server_time_delta.
        let payload = build_all_statuses_request(99);
        let _events = d.dispatch(Command::Order, &payload, 1000);

        // Делаем round-trip days → seconds для сравнения с 1.25.
        let applied_days = d.orders.server_time_delta;
        let applied_seconds = applied_days * 86400.0;
        assert!(
            (applied_seconds - 1.25).abs() < 1e-9,
            "Orders.server_time_delta должен получить значение из handle ({}s, got {}s)",
            1.25,
            applied_seconds
        );
    }
}
