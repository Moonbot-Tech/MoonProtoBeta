//! Active `MPC_OrderBook` dispatch.
//!
//! Keeps the Delphi order-book receive block together: market-index gate,
//! packet parse/apply, chart price-step side effect, then typed events.

use super::{Event, EventDispatcher};
use crate::commands::order_book::parse_order_book_packet;
use crate::protocol::Command;
use crate::state::OrderBookEvent;

impl EventDispatcher {
    pub(super) fn client_new_data_order_book(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
    ) {
        // Active library: блокируем обработку OrderBook если markets indexes не sync.
        // Соответствует Delphi `MoonProtoEngine.pas:1580 If FLastServerAppToken <>
        // PeerAppToken then exit`. Без этого: потеряем пакеты от первых апдейтов
        // после server restart до получения свежих индексов (market_idx по новой
        // нумерации применился бы к старому by_index -> silent data corruption).
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
                let market_name = self
                    .markets
                    .market_name_by_index(market_index)
                    .map(str::to_owned);
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
                for mut ev in events {
                    if let OrderBookEvent::Apply {
                        market_name: name, ..
                    } = &mut ev
                    {
                        *name = market_name.clone();
                    }
                    out.push(Event::OrderBook(ev));
                }
            }
            None => out.push(Self::parse_failed(Command::OrderBook, payload)),
        }
    }
}
