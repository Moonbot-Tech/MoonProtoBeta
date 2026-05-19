//! Event dispatcher — высокоуровневое API поверх `on_data`.
//!
//! Вместо того чтобы потребитель вручную парсил каждый канал и применял к state'ам,
//! `EventDispatcher` делает это автоматически:
//!
//! ```ignore
//! use moonproto::events::{EventDispatcher, Event};
//!
//! let mut dispatcher = EventDispatcher::new();
//! client.on_data(move |cmd, payload| {
//!     for ev in dispatcher.dispatch(cmd, payload, now_ms()) {
//!         match ev {
//!             Event::Order(OrderEvent::Created(o)) => { /* show new order */ }
//!             Event::OrderBook { market_idx, book_kind, .. } => { /* redraw */ }
//!             Event::Trades(TradesEvent::Sequential) => { /* process pkt */ }
//!             _ => {}
//!         }
//!     }
//! });
//! ```
//!
//! Состояния (`Orders`, `OrderBooks`, `TradesState`, etc.) живут внутри dispatcher —
//! доступны как поля `dispatcher.orders`, `dispatcher.order_books`, etc.

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
use crate::commands::arb::parse_arb_prices;
use crate::commands::market::{
    parse_markets_list_response, parse_markets_prices_response,
    parse_markets_indexes_response, parse_token_tags_response,
};
use crate::state::parse_trades_resend_response;

/// Все возможные события которые dispatcher может выдать.
#[derive(Debug)]
pub enum Event {
    /// Order channel: создание/обновление/удаление ордера.
    Order(OrderEvent),
    /// OrderBook channel: применение/запрос Full snapshot.
    OrderBook(OrderBookEvent),
    /// TradesStream channel: пакет принят, дубликат, gap, etc. (может быть несколько при drain'е cache).
    Trades(Vec<TradesEvent>),
    /// Balance channel: одно событие на пакет (только для cmd_id_sub 2/3/4).
    Balance(BalanceEvent),
    /// Arb channel (CmdId=6 внутри MPC_Balance): raw payload — структурный декодер ParseArbPayloadCompact ещё не портирован.
    Arb { uid: u64, payload: Vec<u8> },
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
#[derive(Default)]
pub struct EventDispatcher {
    pub orders:      Orders,
    pub order_books: OrderBooks,
    pub trades:      TradesState,
    pub balances:    BalancesState,
    pub strats:      StratsState,
    pub settings:    SettingsState,
    pub markets:     MarketsState,
}

impl EventDispatcher {
    pub fn new() -> Self { Self::default() }

    /// Распарсить входящий payload и применить к соответствующему state.
    /// Возвращает список событий — для большинства каналов 0 или 1 событие,
    /// для OrderBook (с buffered cache) и Balance (multi-market batch) может быть несколько.
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
                        // server_time_delta из глобального atomic (Client пишет туда в
                        // handle_ping). Без этого Orders::apply применяет AdjustTime со старым
                        // delta=0 — order timestamps сдвинуты на 0.5-2 сек (silent bug).
                        self.orders.set_server_time_delta(crate::client::get_server_time_delta_global());
                        let (_apply_result, ev) = self.orders.apply(tc);
                        out.push(Event::Order(ev));
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                }
            }

            Command::OrderBook => {
                match parse_order_book_packet(payload) {
                    Some(pkt) => {
                        for ev in self.order_books.on_packet(pkt, now_ms) {
                            out.push(Event::OrderBook(ev));
                        }
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                }
            }

            Command::TradesStream => {
                match parse_trades_packet(payload) {
                    Some(pkt) => {
                        let evs = self.trades.on_packet(pkt, now_ms);
                        out.push(Event::Trades(evs));
                    }
                    None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                }
            }

            Command::TradesResendResponse => {
                let inner_payloads = parse_trades_resend_response(payload);
                for inner in inner_payloads {
                    match parse_trades_packet(&inner) {
                        Some(pkt) => {
                            let evs = self.trades.on_packet_resend(pkt);
                            out.push(Event::Trades(evs));
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
                        Some(arb) => out.push(Event::Arb { uid: arb.uid, payload: arb.payload }),
                        None => out.push(Event::ParseFailed { cmd, len: payload.len() }),
                    },
                    _ => out.push(Event::Raw { cmd, payload: payload.to_vec() }),
                }
            }

            Command::Strat => {
                match StratCommand::parse(payload) {
                    Some(cmd_v) => {
                        let ev = self.strats.apply(cmd_v);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::trade::{TradeCtx, build_all_statuses_request};
    use crate::commands::strat::build_snapshot_request;

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
}
