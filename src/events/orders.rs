//! Active canonical order dispatch for protocol v4.

use super::{ClosedSellOrderReportEvent, Event, EventDispatcher};
use crate::commands::trade::TradeCommand;
use crate::protocol::Command;

impl EventDispatcher {
    pub(super) fn client_new_data_order(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
    ) {
        match TradeCommand::parse(payload) {
            Some(TradeCommand::ClosedSellOrderReport(report)) => {
                out.push(Event::ClosedSellOrderReport(ClosedSellOrderReportEvent {
                    db_id: report.db_id,
                    sql: report.sql,
                }));
            }
            Some(TradeCommand::ReportRowUpsert(report)) => {
                let mut events = Vec::new();
                if self.reports.apply_live_upsert(
                    report.rec_id,
                    &report.row,
                    &mut events,
                    &mut self.report_controls,
                ) {
                    out.extend(events.into_iter().map(Event::Report));
                } else {
                    Self::push_parse_failed(out, Command::Order, payload);
                }
            }
            Some(TradeCommand::ReportRowDelete(report)) => {
                let mut events = Vec::new();
                if self.reports.apply_live_delete(
                    report.rec_id,
                    &mut events,
                    &mut self.report_controls,
                ) {
                    out.extend(events.into_iter().map(Event::Report));
                } else {
                    Self::push_parse_failed(out, Command::Order, payload);
                }
            }
            Some(TradeCommand::ReportSyncPage(report)) => {
                let mut events = Vec::new();
                if self
                    .reports
                    .apply_sync_page(report, &mut events, &mut self.report_controls)
                {
                    out.extend(events.into_iter().map(Event::Report));
                } else {
                    Self::push_parse_failed(out, Command::Order, payload);
                }
            }
            Some(TradeCommand::ReportSchema(report)) => {
                let mut events = Vec::new();
                if self
                    .reports
                    .apply_schema(report, &mut events, &mut self.report_controls)
                {
                    out.extend(events.into_iter().map(Event::Report));
                } else {
                    Self::push_parse_failed(out, Command::Order, payload);
                }
            }
            Some(command) => self.process_command_order(command, now_ms, out),
            None => Self::push_parse_failed(out, Command::Order, payload),
        }
    }

    pub(super) fn process_command_order(
        &mut self,
        command: TradeCommand,
        now_ms: i64,
        out: &mut Vec<Event>,
    ) {
        let server_time_delta = self.current_server_time_delta();
        let markets = &self.markets;
        let market_exists = |name: &str| markets.get(name).is_some();
        let mut events = Vec::new();
        self.orders.apply_protocol(
            command,
            now_ms,
            self.last_known_server_token,
            self.last_known_peer_app_token,
            server_time_delta,
            &market_exists,
            &mut events,
            &mut self.order_repairs,
        );
        out.extend(events.into_iter().map(Event::Order));
    }

    pub(super) fn rescan_parked_orders(&mut self, now_ms: i64, out: &mut Vec<Event>) {
        let markets = &self.markets;
        let market_exists = |name: &str| markets.get(name).is_some();
        let mut events = Vec::new();
        self.orders
            .rescan_parked(now_ms, &market_exists, &mut events);
        out.extend(events.into_iter().map(Event::Order));
    }

    pub(crate) fn tick_orders_into(&mut self, now_ms: i64, out: &mut Vec<Event>) {
        for event in self.orders.tick_bulk_replace_timeouts(now_ms) {
            out.push(Event::Order(event));
        }
        for event in self.orders.tick_order_trace_line_shrink(now_ms) {
            out.push(Event::Order(event));
        }
        self.drain_deferred_order_removals_due(now_ms, out);
    }

    pub(crate) fn tick_orders_active_actions(
        &mut self,
        now_ms: i64,
        out: &mut Vec<Event>,
        _actions: &mut Vec<super::ActiveAction>,
    ) {
        if self.orders.has_due_tick_work(now_ms) {
            self.tick_orders_into(now_ms, out);
        }
    }
}
