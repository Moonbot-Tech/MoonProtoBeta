//! Active-library action routing.
//!
//! This is the Rust counterpart of Delphi receive-side domain effects that
//! immediately schedule follow-up protocol commands: full orderbook refresh,
//! missing order statuses, strategy snapshot replies, and trades gap resend.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use super::{copy_max_leverage_from_markets_list, Event, EventDispatcher};
use crate::commands::market::BaseCurrency;
use crate::protocol::Command;
use crate::state::eps::EpsProfile;
#[cfg(any(test, feature = "diagnostics"))]
use crate::state::OrderBookEvent;
use crate::state::{MarketsEvent, OrderBookControl, TradesEvent};

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
    pub(crate) eps_profile: EpsProfile,
    pub(crate) server_base_currency_name: Option<Arc<str>>,
    pub(crate) server_base_currency_code: Option<BaseCurrency>,
    pub(crate) exchange_code: crate::commands::market::ExchangeCode,
    pub(crate) kernel_health: crate::state::KernelHealth,
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
            eps_profile: EpsProfile::from_exchange_code(client.server_info().exchange_code),
            server_base_currency_name: client.server_base_currency_name_arc(),
            server_base_currency_code: client.server_info().base_currency_code,
            exchange_code: client
                .server_info()
                .exchange_code
                .unwrap_or(crate::commands::market::ExchangeCode::None),
            kernel_health: client.kernel_health(),
        }
    }
}

pub(crate) enum ActiveAction {
    RequestMarketsList,
    RequestUpdateMarketsList,
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
        order_id: u64,
        exact_rev: u64,
    },
    TradesResend {
        payload: Vec<u8>,
    },
    ReportSync {
        request_uid: u64,
        request: crate::state::ReportSyncRequest,
    },
    ReportPageReceived {
        request_uid: u64,
        server_token: u64,
    },
    ReportOpenRowsCheck {
        rec_ids: std::sync::Arc<[i64]>,
    },
    ReportSchemaReceived {
        server_token: u64,
    },
    ReportOpenRowsCheckCompleted {
        server_token: u64,
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
        #[cfg(test)]
        if self.panic_next_active_dispatch {
            self.panic_next_active_dispatch = false;
            panic!("synthetic active dispatch panic");
        }

        // Multi-Client safety: lazy-link `server_time_delta_source` to this Client.
        // After the first `dispatch_into_active` call, all subsequent dispatches
        // use the Client-specific delta (not the global). This is critical for multi-Client:
        // the global is overwritten by the last active Client, which without linking gave
        // off-by-50-1000ms timestamps in other Clients' orders.
        if self.server_time_delta_source.is_none() {
            self.server_time_delta_source = Some(Arc::clone(&ctx.server_time_delta_source));
        }
        self.set_eps_profile(ctx.eps_profile);
        self.markets
            .set_copy_max_leverage_from_markets_list(ctx.copy_max_leverage_from_markets_list);
        self.markets.set_server_base_currency(
            ctx.server_base_currency_name.as_deref(),
            ctx.server_base_currency_code,
        );
        self.orders.set_route(
            ctx.server_base_currency_code
                .unwrap_or(BaseCurrency::UNKNOWN),
            ctx.exchange_code,
        );
        if cmd == Command::Ping && self.kernel_health != ctx.kernel_health {
            self.kernel_health = ctx.kernel_health;
            out.push(Event::KernelHealth(ctx.kernel_health));
        }
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

        // Hard reconnect detection: on a ServerToken change all per-session state
        // (trades.last_packet_num, order_books.*.expected_seq) is stale - the server
        // restarts numbering from scratch. Reset BEFORE applying the new packet.
        // Init last_known=0; the first non-zero token (after the first Fine) does not trigger
        // (subsequent packets carry the same token, full_reset is not needed). The reset
        // fires only on a CHANGE of the token between an established session and
        // a new one (hard reconnect via `WantNewHello` or a server restart with a new ST).
        let current_token = ctx.server_token;
        if current_token != 0
            && self.last_known_server_token != 0
            && self.last_known_server_token != current_token
        {
            self.trades.full_reset_at(now_ms);
            self.order_books.reset_caches_keep_books();
            // A hard session creates a fresh server-side client and SrvConnect
            // sends the authoritative news ring again. Drop the previous local
            // ring first; live frames that overtake the sliced history are then
            // merged after that history by NewsState::apply_history.
            self.news.clear_for_hard_session();
            log::info!(target: "moonproto::events",
                "ServerToken changed ({:#x} -> {:#x}) - trades/orderbook/news session state reset",
                self.last_known_server_token, current_token);
        }
        self.last_known_server_token = current_token;
        if ctx.peer_app_token != 0
            && self.last_known_peer_app_token != 0
            && self.last_known_peer_app_token != ctx.peer_app_token
        {
            self.news.clear_for_new_world();
        }
        self.last_known_peer_app_token = ctx.peer_app_token;

        if is_pre_init_domain_command(cmd)
            && !ctx.domain_ready
            && !is_pre_init_state_payload(cmd, payload)
        {
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
        for repair in self.order_repairs.drain(..) {
            actions.push(ActiveAction::RequestOrderStatus {
                order_id: repair.order_id,
                exact_rev: repair.exact_rev,
            });
        }
        for control in self.report_controls.drain(..) {
            match control {
                crate::state::ReportControl::SendSync {
                    request_uid,
                    request,
                } => {
                    actions.push(ActiveAction::ReportSync {
                        request_uid,
                        request,
                    });
                }
                crate::state::ReportControl::PageReceived { request_uid } => {
                    actions.push(ActiveAction::ReportPageReceived {
                        request_uid,
                        server_token: ctx.server_token,
                    });
                }
                crate::state::ReportControl::SendOpenRowsCheck { rec_ids } => {
                    actions.push(ActiveAction::ReportOpenRowsCheck { rec_ids });
                }
                crate::state::ReportControl::SchemaReceived => {
                    actions.push(ActiveAction::ReportSchemaReceived {
                        server_token: ctx.server_token,
                    });
                }
                crate::state::ReportControl::OpenRowsCheckCompleted => {
                    actions.push(ActiveAction::ReportOpenRowsCheckCompleted {
                        server_token: ctx.server_token,
                    });
                }
            }
        }
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
        // now_ms is passed through dispatch_into for state.on_packet(now_ms).
        // Delphi `ProcessTradesStream` calls `CheckMissingTradesPackets` at the end;
        // so recovery resend is an after-effect of a successful trades packet, not an
        // independent timer running in a silent channel.
        let processed_trades_packet =
            matches!(cmd, Command::TradesStream | Command::TradesResendResponse)
                && out[start_len..]
                    .iter()
                    .any(|ev| matches!(ev, Event::Trade(TradesEvent::Applied { .. })));
        // Auto-action 1: OrderBookControl::RequestFullNeeded -> send_api_request (sync, no pending).
        // Dedup via a small Vec with no heap when the set is empty: a grouped payload can contain several
        // RequestFullNeeded for the same book (corruption detection +
        // a subsequent update in one datagram). We send one request per pair.
        let mut to_request_full: Vec<(u16, u8)> = Vec::new();
        for control in self.order_book_controls.drain(..) {
            match control {
                OrderBookControl::RequestFullNeeded { market_index, kind } => {
                    let key = (market_index, kind.as_u8());
                    if !to_request_full.contains(&key) {
                        to_request_full.push(key);
                    }
                }
            }
        }
        // Auto-action 2: internal TStratSnapshotRequest controls -> remember/send
        // a fresh snapshot from the library-owned StratsState (or provider
        // override).
        // If the request arrived before `domain_ready`, do not open the whole MPC_Strat
        // pre-init: only set a latch and reply post-init, once the schema/state
        // are ready. This preserves the obligation to reply without competing with
        // BaseCheck/AuthCheck and without a Rust-only early Strat-domain flow.
        let snapshot_requested_uids = self
            .strategy_snapshot_request_uids
            .drain(..)
            .collect::<Vec<_>>();
        let mut strategy_schema_applied = false;
        let mut new_markets_added = false;
        let mut idx = start_len;
        while idx < out.len() {
            let remove_event = match &out[idx] {
                #[cfg(any(test, feature = "diagnostics"))]
                Event::OrderBook(OrderBookEvent::RequestFullNeeded { .. }) => true,
                #[cfg(any(test, feature = "diagnostics"))]
                Event::OrderBook(OrderBookEvent::Ignored { .. }) => {
                    // Active Lib UI path only publishes applied state changes.
                    // Ignored orderbook packets are protocol diagnostics; low-level
                    // EventDispatcher users can still observe them directly.
                    true
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
            self.rescan_parked_orders(now_ms, out);
        }
        if new_markets_need_price_refresh {
            actions.push(ActiveAction::RequestUpdateMarketsList);
        }
        for (mi, bk) in to_request_full {
            // Fire-and-forget - the response arrives as a normal OrderBook packet (is_full=true)
            // through the same dispatcher. No need to register a pending API receiver.
            actions.push(ActiveAction::RequestOrderBookFull {
                market_index: mi,
                book_kind: bk,
            });
        }
        for uid in snapshot_requested_uids {
            if ctx.domain_ready {
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
            } else {
                self.pending_strategy_snapshot_request_uid = Some(uid);
            }
        }
        if strategy_schema_applied && ctx.domain_ready {
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

fn is_pre_init_state_payload(cmd: Command, payload: &[u8]) -> bool {
    matches!(
        cmd,
        Command::Strat
            if crate::commands::strat::is_schema_payload(payload)
                || crate::commands::strat::is_snapshot_request_payload(payload)
                || crate::commands::strat::is_runtime_state_payload(payload)
    ) || matches!(
        cmd,
        Command::UI
            if crate::commands::ui::is_runtime_state_payload(payload)
                || crate::commands::ui::is_kernel_license_state_payload(payload)
                || crate::commands::ui::is_news_payload(payload)
    )
}
