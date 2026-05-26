//! Active-library action routing.
//!
//! This is the Rust counterpart of Delphi receive-side domain effects that
//! immediately schedule follow-up protocol commands: full orderbook refresh,
//! missing order statuses, strategy snapshot replies, and trades gap resend.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use super::{copy_max_leverage_from_markets_list, Event, EventDispatcher};
use crate::commands::trade::TradeCtx;
use crate::protocol::Command;
use crate::state::orders::OrderCancelSend;
use crate::state::{MarketsEvent, OrderBookEvent, OrderEvent, TradesEvent};

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

impl EventDispatcher {
    /// Active-library parser step used by `MoonClient` and custom active runtimes.
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
        // (trades.last_packet_num, order_books.*.expected_seq) устарела - сервер
        // начинает нумерацию заново. Сбрасываем ДО применения нового пакета.
        // Init last_known=0; первый non-zero token (после первого Fine) - не triggers
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
                "ServerToken changed ({:#x} -> {:#x}) - trades reset + orderbook caches reset",
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
        // значит recovery resend - последействие успешного trades-пакета, а не
        // независимый timer в тишине канала.
        let processed_trades_packet =
            matches!(cmd, Command::TradesStream | Command::TradesResendResponse)
                && out[start_len..]
                    .iter()
                    .any(|ev| matches!(ev, Event::Trade(TradesEvent::Applied { .. })));
        // Auto-action 1: OrderBookEvent::RequestFullNeeded -> send_api_request (sync, no pending).
        // Dedup через маленький Vec без heap при пустом наборе: Grouped-payload может содержать несколько
        // RequestFullNeeded для одной и той же книги (corruption detection +
        // последующий update в одном datagram'е). Шлём один запрос на пару.
        let mut to_request_full: Vec<(u16, u8)> = Vec::new();
        // Auto-action 2: StratEvent::SnapshotRequested -> шлём fresh snapshot
        // из library-owned StratsState (или provider override). Delphi
        // `MoonProtoClient.pas:ProcessStratCommand` пересобирает ответ через
        // `TStratSnapshot.CreateFromStrats(Strats)`.
        let mut snapshot_requested_uid: Option<u64> = None;
        let mut strategy_schema_applied = false;
        let mut new_markets_added = false;
        // Auto-action 3: OrderEvent::Snapshot -> CleanupMissingWorkers.
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
            // Fire-and-forget - response придёт обычным OrderBook-пакетом (is_full=true)
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
