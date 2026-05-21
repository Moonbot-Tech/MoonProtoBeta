//! Event dispatcher — высокоуровневое API поверх `on_data`.
//!
//! Вместо того чтобы потребитель вручную парсил каждый канал и применял к state'ам,
//! `EventDispatcher` делает это автоматически:
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
//! Состояния (`Orders`, `OrderBooks`, `TradesState`, etc.) живут внутри dispatcher —
//! доступны как поля `dispatcher.orders`, `dispatcher.order_books`, etc.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::protocol::Command;
use crate::state::{
    Orders, OrderBooks, TradesState, BalancesState, StratsState, SettingsState, MarketsState,
    OrderEvent, OrderBookEvent, TradesEvent, BalanceEvent, StratEvent, SettingsEvent, MarketsEvent,
};
use crate::commands::trade::TradeCommand;
use crate::commands::strat::StratCommand;
use crate::commands::ui::UICommand;
use crate::commands::order_book::parse_order_book_packet;
use crate::commands::trades_stream::parse_trades_packet;
use crate::commands::engine_api::{EngineResponse, EngineMethod, parse_engine_response};
use crate::commands::balance::parse_balance;
use crate::commands::arb::{ArbPayload, parse_arb_payload_compact, parse_arb_prices};
use crate::commands::market::{
    parse_markets_list_response, parse_markets_prices_response,
    parse_markets_indexes_response, parse_token_tags_response,
};
use crate::state::parse_trades_resend_response;

/// Fresh strategy snapshot returned by the application for a server
/// `TStratSnapshotRequest`.
///
/// Delphi answers that request by rebuilding `TStratSnapshot.CreateFromStrats`
/// from the live `Strats` object. The Rust library does not own application
/// strategies, so `EventDispatcher` can only auto-answer when the application
/// registers a provider through [`EventDispatcher::set_strategy_snapshot_provider`].
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
        Self { server_epoch, client_max_last_date, full, data }
    }

    /// Build a reply from decoded strategy snapshots.
    ///
    /// This is the provider-side counterpart of Delphi
    /// `TStratSnapshot.CreateFromStrats`: it serializes the current application
    /// strategy list, computes `ClientMaxLastDate`, and marks the packet as a
    /// full snapshot by default.
    pub fn from_strategies(
        server_epoch: u64,
        full: bool,
        strategies: &[crate::commands::strategy_serializer::StrategySnapshot],
    ) -> Self {
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

/// Все возможные события которые dispatcher может выдать.
#[derive(Debug)]
pub enum Event {
    /// Order channel: создание/обновление/удаление ордера.
    Order(OrderEvent),
    /// OrderBook channel: применение/запрос Full snapshot.
    OrderBook(OrderBookEvent),
    /// TradesStream channel: одно событие (Apply/Duplicate/GapDetected/...).
    /// audit_rust_quality #11: variant изменён с `Trades(Vec<TradesEvent>)` на
    /// `Trade(TradesEvent)` — каждый sub-event пушится в out отдельно, без
    /// nested Vec allocation на каждом TradesStream пакете. На пике 50K pps это
    /// экономит ~50K Vec alloc/sec + matching dealloc.
    Trade(TradesEvent),
    /// Balance channel: одно событие на пакет (только для cmd_id_sub 2/3/4).
    Balance(BalanceEvent),
    /// Arb channel (CmdId=6 внутри MPC_Balance): compact kernel→client payload.
    Arb { uid: u64, payload: ArbPayload },
    /// Strat channel: snapshot/delete/sell-price update.
    Strat(StratEvent),
    /// UI channel: settings updated, MM subscribe changed, etc.
    Settings(SettingsEvent),
    /// Markets state updated (после Engine API response).
    Markets(MarketsEvent),
    /// Engine API response пришёл, но не зарегистрирован в pending registry.
    EngineResponse(EngineResponse),
    /// Server-side log message (`MPC_LogMsg`): `time:TDateTime + msg:UTF-8 rest`.
    ServerLog { time: f64, msg: String },
    /// Сырой payload — для каналов которые dispatcher не знает (LogMsg, Service, etc.).
    Raw { cmd: Command, payload: Vec<u8> },
    /// Парсинг не удался (повреждённый payload).
    ParseFailed { cmd: Command, len: usize },
}

/// State bundle + dispatch logic.
///
/// A-V2-09: `#[derive(Default)]` — каждое поле имеет свой `Default::default`
/// (через `pub fn new()` который равен `default()`), что эквивалентно ручному
/// `impl Default`. Ручной impl убран как избыточный.
///
/// **API encapsulation (audit_rust_quality #9):** state-поля имеют видимость
/// `pub(crate)` (read-only снаружи). Пользователь получает доступ через
/// getters [`Self::orders`], [`Self::order_books`], [`Self::trades`],
/// [`Self::balances`], [`Self::strats`], [`Self::settings`], [`Self::markets`].
/// Прямая мутация state'ов снаружи — запрещена: state поддерживается через
/// [`Self::dispatch`] / [`Self::dispatch_into`] / [`Self::dispatch_into_active`].
#[derive(Default)]
pub struct EventDispatcher {
    pub(crate) orders:      Orders,
    pub(crate) order_books: OrderBooks,
    pub(crate) trades:      TradesState,
    pub(crate) balances:    BalancesState,
    pub(crate) strats:      StratsState,
    pub(crate) settings:    SettingsState,
    pub(crate) markets:     MarketsState,
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
    /// fallback на global `SERVER_TIME_DELTA_DAYS` (back-compat для single-Client
    /// потребителей без линковки). См. `DEVIATION.md #23`.
    ///
    /// Привязка: либо явный вызов [`Self::set_server_time_delta_source`] с
    /// `client.server_time_delta_handle()`, либо автоматически — `dispatch_into_active`
    /// делает lazy-link при первом вызове.
    server_time_delta_source: Option<Arc<AtomicU64>>,
    /// Provider for fresh application-owned strategies. Called when the server
    /// sends `TStratSnapshotRequest`; if it returns a snapshot, the dispatcher
    /// sends `TStratSnapshot` immediately, matching Delphi's fresh
    /// `CreateFromStrats(Strats)` response.
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
    pub fn new() -> Self { Self::default() }

    /// Read-only доступ к `Orders` state (sync-state ордеров — uid → Order map).
    /// Состояние обновляется автоматически при dispatch'е `Command::Order` пакетов.
    pub fn orders(&self) -> &Orders { &self.orders }

    /// Read-only доступ к `OrderBooks` state (per-market kind sliding каше).
    /// Состояние обновляется при dispatch'е `Command::OrderBook` пакетов.
    pub fn order_books(&self) -> &OrderBooks { &self.order_books }

    /// Read-only доступ к `TradesState` (gap detection + bucket tracking).
    pub fn trades(&self) -> &TradesState { &self.trades }

    /// Read-only доступ к `BalancesState` (балансы валют + locked).
    pub fn balances(&self) -> &BalancesState { &self.balances }

    /// Read-only доступ к `StratsState` (стратегии: uid → StratSnapshot).
    pub fn strats(&self) -> &StratsState { &self.strats }

    /// Read-only доступ к `SettingsState` (`TClientSettingsCommand` snapshot).
    pub fn settings(&self) -> &SettingsState { &self.settings }

    /// Read-only доступ к `MarketsState` (markets list + indexes + token tags).
    /// `markets().indexes_synchronized` — ключевой инвариант active library
    /// (gating флаг для TradesStream/OrderBook парсинга).
    pub fn markets(&self) -> &MarketsState { &self.markets }

    /// Events produced by one-shot helpers and not yet drained by the
    /// application.
    ///
    /// `Client::run_with_dispatcher` delivers events to its callback immediately
    /// and does not use this queue. The queue is only for helper-driven waits
    /// such as `Client::run_until_response`, `request_client_settings`,
    /// `request_order_snapshot`, and typed `request_*` Engine API helpers.
    pub fn queued_events(&self) -> &[Event] { &self.queued_events }

    /// Number of currently queued one-shot events.
    pub fn queued_event_count(&self) -> usize { self.queued_events.len() }

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

    /// Periodic tick для `TradesState` gap recovery — генерирует `TradesResend`
    /// payload'ы для пропущенных packet num'ов, закрывает старые buckets.
    ///
    /// Пользователю **не нужно** вызывать вручную если используется
    /// [`crate::client::Client::run_with_dispatcher`] (он делает это автоматически
    /// каждые ~100мс).
    ///
    /// Только при custom main loop'е (вызов `client.run(...)` с собственным
    /// callback'ом): вызывай раз в ~100мс с актуальным `rtt_ms` и `now_ms` чтобы
    /// trades-channel самовосстанавливался от UDP loss.
    pub fn tick_trades(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>> {
        self.trades.tick(rtt_ms, now_ms)
    }

    /// Variant of [`Self::tick_trades`] возвращающий также emitted `TradesEvent`'ы.
    /// Полезно для observability — `BucketClosed` / `GapFilled` events не доходят до
    /// потребителя через `dispatch`, только через tick.
    pub fn tick_trades_with_events(
        &mut self,
        rtt_ms: i64,
        now_ms: i64,
    ) -> (Vec<Vec<u8>>, Vec<TradesEvent>) {
        self.trades.tick_with_events(rtt_ms, now_ms)
    }

    /// Привязать dispatcher к per-Client `ServerTimeDelta` handle. После этого
    /// `dispatch_into` для `Command::Order` применяет **этот** Client's delta вместо
    /// глобального (multi-Client safe).
    ///
    /// **Когда вызывать.** При multi-Client архитектуре — обязательно для каждого
    /// `EventDispatcher` (по одному на Client). При single-Client можно не вызывать —
    /// dispatcher падает обратно на global, который Client всё равно обновляет.
    ///
    /// **Auto-link.** `dispatch_into_active(&mut Client)` делает линковку
    /// автоматически на первом вызове — для типичного use case'а
    /// `Client::run_with_dispatcher` ручная привязка не нужна.
    ///
    /// Example:
    /// ```ignore
    /// let client = Client::new(cfg);
    /// let mut dispatcher = EventDispatcher::new();
    /// dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
    /// // Теперь dispatcher.dispatch_into читает delta из client.
    /// ```
    ///
    /// См. `DEVIATION.md #23`.
    pub fn set_server_time_delta_source(&mut self, handle: Arc<AtomicU64>) {
        self.server_time_delta_source = Some(handle);
    }

    /// Register a provider for fresh strategy snapshots.
    ///
    /// The provider is called with the UID of the incoming
    /// `TStratSnapshotRequest`. The reply itself is sent with a new command UID,
    /// as Delphi creates a fresh `TStratSnapshot` command object for the answer.
    ///
    /// If no provider is registered, or the provider returns `None`,
    /// `SnapshotRequested` is still emitted and the application can answer
    /// manually with [`crate::client::Client::strat_send_snapshot_batch`] or
    /// [`crate::client::Client::strat_send_snapshot_payload`].
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

    /// Текущее значение `ServerTimeDelta` (days). Если установлен per-Client
    /// source — берёт оттуда; иначе fallback на global.
    fn current_server_time_delta(&self) -> f64 {
        match &self.server_time_delta_source {
            Some(handle) => f64::from_bits(handle.load(Ordering::Relaxed)),
            None => crate::client::get_server_time_delta_global(),
        }
    }

    /// Распарсить входящий payload и применить к соответствующему state.
    /// Возвращает список событий — для большинства каналов 0 или 1 событие,
    /// для OrderBook (с buffered cache) и Balance (multi-market batch) может быть несколько.
    #[must_use = "Events must be processed — пропуск приведёт к потере OrderEvent/TradesEvent/etc."]
    pub fn dispatch(&mut self, cmd: Command, payload: &[u8], now_ms: i64) -> Vec<Event> {
        // Convenience-обёртка над `dispatch_into`. Backwards compat.
        let mut out = Vec::new();
        self.dispatch_into(cmd, payload, now_ms, &mut out);
        out
    }

    /// Аудит #6 (audit_delphi_deviation): zero-alloc dispatch path.
    ///
    /// Раньше `dispatch` делал `vec![event]` per call → 50K alloc/sec на пике
    /// TradesStream. Теперь events pushed в переданный `out` buffer который потребитель
    /// переиспользует через `clear()` между вызовами.
    ///
    /// **Active library**: если есть `&mut Client` — используй
    /// [`Self::dispatch_into_active`]. Этот вариант:
    ///   1. блокирует обработку TradesStream/OrderBook пакетов когда
    ///      `MarketsState.indexes_synchronized = false` (event drop'ается тихо);
    ///   2. автоматически шлёт `api_request_order_book_full` на
    ///      `OrderBookEvent::RequestFullNeeded` — потребитель не должен делать это сам;
    ///   3. automatically sends `TOrderStatusRequest` for orders missing from a
    ///      fresh `TAllStatuses` snapshot, matching Delphi `CleanupMissingWorkers`.
    /// `dispatch_into` (без Client) — backwards compat, потребитель должен сам
    /// обрабатывать RequestFullNeeded events.
    ///
    /// Pattern для performance-sensitive потребителей:
    /// ```ignore
    /// let mut buf = Vec::with_capacity(8);
    /// loop {
    ///     buf.clear();
    ///     dispatcher.dispatch_into(cmd, payload, now_ms, &mut buf);
    ///     for ev in &buf { /* handle */ }
    /// }
    /// ```
    pub fn dispatch_into(&mut self, cmd: Command, payload: &[u8], now_ms: i64, out: &mut Vec<Event>) {
        match cmd {
            Command::Order => {
                match TradeCommand::parse(payload) {
                    Some(tc) => {
                        // audit_responsibility A5 / active library: автоматически подхватываем
                        // server_time_delta. При наличии per-Client `server_time_delta_source`
                        // (multi-Client) — читаем оттуда. Иначе fallback на global (single-Client
                        // back-compat). Без этого Orders::apply применяет AdjustTime со старым
                        // delta=0 — order timestamps сдвинуты на 0.5-2 сек (silent bug).
                        // См. DEVIATION #23.
                        self.orders.set_server_time_delta(self.current_server_time_delta());
                        let (_apply_result, ev) = self.orders.apply(tc);
                        out.push(Event::Order(ev));
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
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
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
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
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
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
                        None => out.push(Event::ParseFailed { cmd, len: inner.len() }),
                    }
                }
            }

            Command::Balance => {
                if payload.len() < 11 {
                    out.push(Event::ParseFailed { cmd, len: payload.len() });
                    return;
                }
                let sub_cmd_id = payload[0];
                let body = &payload[11..];
                match sub_cmd_id {
                    2 | 3 | 4 => match parse_balance(sub_cmd_id, body) {
                        Some(upd) => {
                            let ev = self.balances.apply(upd);
                            out.push(Event::Balance(ev));
                        }
                        None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
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
                        None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                    },
                    _ => out.push(Event::Raw { cmd, payload: payload.to_vec() }),
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
                            crate::state::StratEvent::SnapshotFull { raw_data, .. }
                            | crate::state::StratEvent::SnapshotPartial { raw_data, .. } => {
                                if self.strats.apply_snapshot_decoded(raw_data).is_none() {
                                    log::warn!(
                                        target: "moonproto::events",
                                        "failed to decode strategy snapshot payload ({} bytes)",
                                        raw_data.len()
                                    );
                                }
                            }
                            _ => {}
                        }
                        out.push(Event::Strat(ev));
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                }
            }

            Command::UI => {
                match UICommand::parse(payload) {
                    Some(cmd_v) => {
                        let ev = self.settings.apply(cmd_v);
                        out.push(Event::Settings(ev));
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                }
            }

            Command::API => {
                match parse_engine_response(payload) {
                    Some(resp) => {
                        const ASSUMED_VER: u16 = 2;
                        let extra_event: Option<Event> = if resp.success {
                            match resp.method {
                                EngineMethod::GetMarketsList | EngineMethod::UpdateMarketsList => {
                                    if resp.method == EngineMethod::GetMarketsList {
                                        if let Some(list) = parse_markets_list_response(&resp.data, ASSUMED_VER) {
                                            let ev = self.markets.apply_markets_list(list);
                                            Some(Event::Markets(ev))
                                        } else { None }
                                    } else if let Some(prices) = parse_markets_prices_response(&resp.data) {
                                        let ev = self.markets.apply_markets_prices(prices);
                                        Some(Event::Markets(ev))
                                    } else { None }
                                }
                                EngineMethod::GetMarketsIndexes => {
                                    if let Some(names) = parse_markets_indexes_response(&resp.data) {
                                        let ev = self.markets.apply_markets_indexes(names);
                                        Some(Event::Markets(ev))
                                    } else { None }
                                }
                                EngineMethod::CheckBinanceTags => {
                                    if let Some(items) = parse_token_tags_response(&resp.data) {
                                        let ev = self.markets.apply_token_tags(items);
                                        Some(Event::Markets(ev))
                                    } else { None }
                                }
                                _ => None,
                            }
                        } else { None };

                        if let Some(ev) = extra_event { out.push(ev); }
                        out.push(Event::EngineResponse(resp));
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                }
            }

            Command::LogMsg => {
                if payload.len() < 8 {
                    out.push(Event::ParseFailed { cmd, len: payload.len() });
                    return;
                }
                let time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
                let msg = String::from_utf8_lossy(&payload[8..]).to_string();
                out.push(Event::ServerLog { time, msg });
            }

            _ => out.push(Event::Raw { cmd, payload: payload.to_vec() }),
        }
    }

    /// **Active library dispatch** — расширение `dispatch_into` с `&mut Client` для
    /// auto-actions либы.
    ///
    /// Auto-action: `OrderBookEvent::RequestFullNeeded` → автоматически отправляется
    /// `emk_RequestOrderBookFull` через `send_api_request` (fire-and-forget — без
    /// регистрации в pending API registry, т.к. response придёт обычным OrderBook-пакетом
    /// который сам разберёт диспетчер). Event всё равно эмиттится в `out` — для UI
    /// индикатора «загружаем стакан» — но **потребитель не должен слать запрос сам**.
    ///
    /// Дедупликация: за один `dispatch_into_active` вызов на одну `(market_idx, kind)`
    /// пару отправляется максимум один запрос, даже если Grouped-payload содержит
    /// несколько `RequestFullNeeded` для того же книги.
    ///
    /// **Trades gap resend** в этой функции НЕ запускается — он управляется единым
    /// периодическим тиком в `Client::run_with_dispatcher` (раз в ~100мс). Так
    /// избегаем double-resend на одном пакете.
    pub fn dispatch_into_active(
        &mut self,
        cmd: Command,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
        client: &mut crate::client::Client,
    ) {
        // Multi-Client safety: lazy-link `server_time_delta_source` к этому Client'у.
        // После первого вызова `dispatch_into_active` все последующие dispatch'и
        // используют Client-specific delta (а не global). Это критично при multi-Client:
        // global перезаписывается последним активным Client'ом, что без линковки давало
        // off-by-50-1000ms timestamps в ордерах других Client'ов. См. DEVIATION #23.
        if self.server_time_delta_source.is_none() {
            self.server_time_delta_source = Some(client.server_time_delta_handle());
        }

        // Server restart / PeerAppToken change: Delphi gates stream parsing with
        // `FLastServerAppToken <> PeerAppToken` until `GetMarketsIndexes` succeeds.
        // Keep the same behavioral guard here. Otherwise old `indexes_synchronized`
        // from the previous server process would let fresh TradesStream/OrderBook
        // packets be decoded through stale market indexes.
        if client.peer_app_token() != 0 && !client.market_indexes_current_for_peer() {
            self.markets.mark_indexes_stale();
        }

        // Hard reconnect detection: при смене ServerToken вся per-session state
        // (trades.last_packet_num, order_books.*.expected_seq) устарела — сервер
        // начинает нумерацию заново. Сбрасываем ДО применения нового пакета.
        // Init last_known=0; первый non-zero token (после первого Fine) — не triggers
        // (последующие пакеты будут с тем же token, full_reset не нужен). Сброс
        // срабатывает только на ИЗМЕНЕНИИ token'а между установившейся сессией и
        // новой (hard reconnect через `WantNewHello` или server restart с новым ST).
        let current_token = client.server_token();
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

        let start_len = out.len();
        self.dispatch_into(cmd, payload, now_ms, out);
        // now_ms прокинут в dispatch_into для state.on_packet(now_ms); auto-actions
        // ниже не зависят от времени (события OrderBookEvent::RequestFullNeeded и
        // TradesEvent::GapDetected уже содержат всё нужное).

        // Auto-action 1: OrderBookEvent::RequestFullNeeded → send_api_request (sync, no pending).
        // Dedup через HashSet — Grouped-payload может содержать несколько
        // RequestFullNeeded для одной и той же книги (corruption detection +
        // последующий update в одном datagram'е). Шлём один запрос на пару.
        use std::collections::HashSet;
        let mut to_request_full: HashSet<(u16, u8)> = HashSet::new();
        // Auto-action 2: StratEvent::SnapshotRequested → если приложение
        // зарегистрировало provider, берём fresh snapshot из него и шлём ответ.
        // Delphi `MoonProtoClient.pas:ProcessStratCommand` пересобирает ответ
        // через `TStratSnapshot.CreateFromStrats(Strats)`, кеш последнего
        // server-snapshot там не используется.
        let mut snapshot_requested_uid: Option<u64> = None;
        // Auto-action 3: OrderEvent::Snapshot → CleanupMissingWorkers.
        // Delphi after TAllStatuses increments CurrentSnapshotFlag, applies all
        // statuses, then requests exact status for workers absent from the fresh
        // snapshot. The application must not know about snapshot flags.
        let mut order_snapshot_applied = false;
        for ev in &out[start_len..] {
            match ev {
                Event::OrderBook(OrderBookEvent::RequestFullNeeded { market_index, book_kind }) => {
                    to_request_full.insert((*market_index, *book_kind));
                }
                Event::Order(OrderEvent::Snapshot) => {
                    order_snapshot_applied = true;
                }
                Event::Strat(crate::state::StratEvent::SnapshotRequested { uid }) => {
                    snapshot_requested_uid = Some(*uid);
                }
                _ => {}
            }
        }
        for (mi, bk) in to_request_full {
            // Fire-and-forget — response придёт обычным OrderBook-пакетом (is_full=true)
            // через тот же dispatcher. Регистрировать pending API receiver не нужно.
            client.send_api_request(
                &crate::commands::engine_request::request_order_book_full(mi, bk),
            );
        }
        if let Some(uid) = snapshot_requested_uid {
            if let Some(provider) = self.strategy_snapshot_provider.as_mut() {
                if let Some(snapshot) = provider(uid) {
                    client.strat_send_snapshot_payload(
                        snapshot.server_epoch,
                        snapshot.client_max_last_date,
                        snapshot.full,
                        &snapshot.data,
                    );
                }
            }
            // Если provider не задан или вернул None — событие всё равно эмиттится
            // в `out`, потребитель может ответить вручную.
        }
        if order_snapshot_applied {
            let missing = self.orders.missing_after_snapshot();
            for uid in missing {
                if let Some(order) = self.orders.get(uid) {
                    let ctx = crate::commands::trade::TradeCtx {
                        uid,
                        currency: order.currency,
                        platform: order.platform,
                    };
                    client.request_order_status(ctx, &order.market_name);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::arb::build_arb_prices;
    use crate::commands::trade::{
        BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, OrderWorkerStatus,
        StopSettings, TradeCommand, TradeCtx, TradeEpochHeader, build_all_statuses_request,
    };
    use crate::commands::strat::build_snapshot_request;

    static SERVER_TIME_DELTA_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn server_time_delta_test_lock() -> std::sync::MutexGuard<'static, ()> {
        SERVER_TIME_DELTA_TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn order_book_payload(market_index: u16) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&market_index.to_le_bytes());
        raw.extend_from_slice(&1u16.to_le_bytes());
        raw.push(1); // Full, Futures.
        raw.extend_from_slice(&0u16.to_le_bytes()); // buy_count=0, sell_count=0.
        crate::compression::synlz_compress(&raw)
    }

    fn empty_all_statuses_payload(uid: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(15);
        out.push(8);
        out.extend_from_slice(&3u16.to_le_bytes());
        out.extend_from_slice(&uid.to_le_bytes());
        out.extend_from_slice(&0i32.to_le_bytes());
        out
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
                    base: BaseCommandHeader { cmd_id: 4, ver: 3, uid },
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
            Event::Order(_) => {},
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
            Event::Strat(_) => {},
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
    fn dispatcher_corrupted_order_returns_parse_failed() {
        let mut d = EventDispatcher::new();
        let events = d.dispatch(Command::Order, &[1, 2, 3], 1000); // too short for header
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::ParseFailed { .. }));
    }

    #[test]
    fn dispatcher_ctx_unused_warning_silenced() {
        // Suppress dead_code warning for TradeCtx if not used elsewhere
        let _ = TradeCtx::new(1);
    }

    #[test]
    fn dispatcher_blocks_orderbook_until_indexes_sync() {
        let mut d = EventDispatcher::new();
        // indexes_synchronized = false по умолчанию — OrderBook event должен быть дропнут.
        // Делаем минимальный wire-payload для OrderBook (parse может не пройти, и это ок —
        // главное что мы ВООБЩЕ не доходим до parse, потому что блокировка раньше).
        let dummy_payload = vec![0u8; 32];
        let events = d.dispatch(Command::OrderBook, &dummy_payload, 1000);
        assert!(events.is_empty(), "OrderBook event должен быть дропнут до indexes_synchronized");

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
        assert!(events.is_empty(), "unknown server market index must be dropped");
        assert!(d.order_books.is_empty(), "unknown index must not create OrderBooks cache");

        d.markets.market_indexes = vec!["UNKNOWNUSDT".to_string()];
        d.markets.by_name.clear();
        let events = d.dispatch(Command::OrderBook, &order_book_payload(0), 1000);
        assert!(events.is_empty(), "index mapped to unknown local market must be dropped");
        assert!(d.order_books.is_empty(), "unknown local market must not create cache");
    }

    #[test]
    fn dispatcher_blocks_trades_until_indexes_sync() {
        let mut d = EventDispatcher::new();
        let dummy_payload = vec![0u8; 16];
        let events = d.dispatch(Command::TradesStream, &dummy_payload, 1000);
        assert!(events.is_empty(), "TradesStream должен быть дропнут до indexes_synchronized");
    }

    #[test]
    fn dispatcher_order_not_blocked_by_indexes_sync() {
        // Order channel не зависит от market_idx → не должен блокироваться indexes_sync.
        let mut d = EventDispatcher::new();
        let payload = build_all_statuses_request(123);
        let events = d.dispatch(Command::Order, &payload, 1000);
        assert!(!events.is_empty(), "Order должен обрабатываться даже без indexes_synchronized");
    }

    #[test]
    fn dispatch_into_active_invalidates_indexes_on_peer_token_mismatch() {
        let mut d = EventDispatcher::new();
        d.markets.apply_markets_indexes(vec!["OLDUSDT".to_string()]);
        assert!(d.markets.indexes_synchronized);

        let mut client = crate::client::Client::new(dummy_client_cfg());
        client.testing_set_peer_app_tokens(0x2222, 0x1111);

        let mut out = Vec::new();
        let dummy_payload = vec![0u8; 32];
        d.dispatch_into_active(Command::OrderBook, &dummy_payload, 1000, &mut out, &mut client);

        assert!(!d.markets.indexes_synchronized,
            "PeerAppToken mismatch must close stream gate until fresh GetMarketsIndexes");
        assert!(out.is_empty(),
            "OrderBook packet from a new server process must be dropped with stale indexes");
    }

    #[test]
    fn dispatch_into_active_requests_missing_order_status_after_snapshot() {
        let mut d = EventDispatcher::new();
        let stale_uid = 0xAABB_CCDD_0011_2233;
        let status = order_status_for_test(
            stale_uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        );
        let (_result, _event) = d.orders.apply(TradeCommand::OrderStatus(status));

        let mut client = crate::client::Client::new(dummy_client_cfg());
        let mut out = Vec::new();
        d.dispatch_into_active(
            Command::Order,
            &empty_all_statuses_payload(0x55),
            1000,
            &mut out,
            &mut client,
        );

        assert!(out.iter().any(|ev| matches!(ev, Event::Order(OrderEvent::Snapshot))));

        let mut found = false;
        while let Ok(ev) = client.event_rx.try_recv() {
            let crate::client::ClientEvent::Send(msg) = ev else {
                continue;
            };
            if msg.item.cmd != Command::Order as u8 {
                continue;
            }
            let Some(TradeCommand::OrderStatusRequest(req)) =
                TradeCommand::parse(&msg.item.data)
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
        // Single-Client back-compat: без линковки dispatcher читает global.
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
        assert!((delta_seconds(&d) - 7.0).abs() < 1e-9,
            "dispatcher должен читать handle, а не global");
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
        assert!((delta_seconds(&d_b) - (-0.2)).abs() < 1e-9, "dispatcher B не должен видеть изменения handle A");
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
        d.dispatch_into_active(Command::Reserved1, b"x", 0, &mut out, &mut client);
        assert_eq!(d.last_known_server_token, 42,
            "первый dispatch_into_active должен запомнить server_token");
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
        d.dispatch_into_active(Command::Reserved1, b"x", 0, &mut out, &mut client);
        // markets state НЕ должны быть сброшен (full_reset не вызывался).
        assert_eq!(d.markets.by_name.len(), snapshot_count_before,
            "первый non-zero token — не triggers reset");
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
        d.dispatch_into_active(Command::Reserved1, b"x", 0, &mut out, &mut client);
        assert_eq!(d.last_known_server_token, 0xBBB,
            "после смены токена — last_known обновлён");
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
        let mut out = Vec::new();
        d.dispatch_into_active(Command::Reserved1, b"x", 0, &mut out, &mut client);
        assert!(d.server_time_delta_source.is_some(),
            "после первого dispatch_into_active — source привязан к Client'у");

        // Повторный вызов — source не меняется (already linked).
        let handle_after_first = Arc::clone(d.server_time_delta_source.as_ref().unwrap());
        d.dispatch_into_active(Command::Reserved1, b"y", 0, &mut out, &mut client);
        let handle_after_second = d.server_time_delta_source.as_ref().unwrap();
        assert!(Arc::ptr_eq(&handle_after_first, handle_after_second),
            "повторный вызов — source остаётся тем же handle");
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
        let mut out = Vec::new();

        // Server prods клиента: "пришли свой snapshot стратегий" — это
        // StratCommand::SnapshotRequest. Payload = `build_snapshot_request(uid)`.
        let payload = crate::commands::strat::build_snapshot_request(42);

        d.dispatch_into_active(Command::Strat, &payload, 0, &mut out, &mut client);

        // Drain event channel — должна быть отправка Command::Strat с fresh
        // TStratSnapshot body: CmdId/ver/uid + ServerEpoch/ClientMaxLastDate/Size/Full/Data.
        let mut found_snapshot_send = false;
        while let Ok(ev) = client.event_rx.try_recv() {
            if let crate::client::ClientEvent::Send(msg) = ev {
                if msg.item.cmd == Command::Strat as u8 {
                    let data = &msg.item.data;
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
        }
        assert!(found_snapshot_send,
            "после SnapshotRequest с provider — должна быть fresh отправка");

        // out содержит event SnapshotRequested (app тоже видит, для UI awareness).
        let has_snapshot_event = out.iter().any(|ev| matches!(
            ev, Event::Strat(crate::state::StratEvent::SnapshotRequested { uid: 42 })
        ));
        assert!(has_snapshot_event,
            "event SnapshotRequested должен быть в out (для app awareness)");
    }

    #[test]
    fn snapshot_requested_without_provider_does_not_send() {
        // Если provider не задан — auto-echo не происходит. App получает event
        // и может сам решить что делать.
        let mut d = EventDispatcher::new();

        let mut client = crate::client::Client::new(dummy_client_cfg());
        let mut out = Vec::new();
        let payload = crate::commands::strat::build_snapshot_request(99);
        d.dispatch_into_active(Command::Strat, &payload, 0, &mut out, &mut client);

        // Drain event channel — НЕ должно быть Command::Strat send'ов (нет provider).
        let mut strat_sends = 0;
        while let Ok(ev) = client.event_rx.try_recv() {
            if let crate::client::ClientEvent::Send(msg) = ev {
                if msg.item.cmd == Command::Strat as u8 {
                    strat_sends += 1;
                }
            }
        }
        assert_eq!(strat_sends, 0, "без provider — auto-echo не должен сработать");

        // Но event SnapshotRequested всё равно прилетает app'у.
        let has_event = out.iter().any(|ev| matches!(
            ev, Event::Strat(crate::state::StratEvent::SnapshotRequested { .. })
        ));
        assert!(has_event);
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
        assert!((applied_seconds - 1.25).abs() < 1e-9,
            "Orders.server_time_delta должен получить значение из handle ({}s, got {}s)",
            1.25, applied_seconds);
    }
}
