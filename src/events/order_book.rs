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
        // Active library: block OrderBook processing if the market indexes are not synced.
        // Matches Delphi `MoonProtoEngine.pas:1580 If FLastServerAppToken <>
        // PeerAppToken then exit`. Without this: we would lose packets from the first updates
        // after a server restart until fresh indexes arrive (a market_idx in the new
        // numbering would be applied to the old by_index -> silent data corruption).
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
                    .market_by_index(market_index)
                    .map(|handle| handle.name_arc());
                self.order_book_events.clear();
                self.order_book_controls.clear();
                self.order_books.on_packet_into(
                    pkt,
                    now_ms,
                    &mut self.order_book_events,
                    &mut self.order_book_controls,
                );
                if self
                    .order_book_events
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
                for mut ev in self.order_book_events.drain(..) {
                    match &mut ev {
                        OrderBookEvent::Apply {
                            market_name: name, ..
                        } => {
                            *name = market_name.clone();
                        }
                        #[cfg(any(test, feature = "diagnostics"))]
                        _ => {}
                    }
                    out.push(Event::OrderBook(ev));
                }
            }
            None => Self::push_parse_failed(out, Command::OrderBook, payload),
        }
    }
}
