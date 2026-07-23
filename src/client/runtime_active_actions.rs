use super::*;

impl Client {
    pub(crate) fn apply_active_actions<I>(&self, actions: I)
    where
        I: IntoIterator<Item = crate::events::ActiveAction>,
    {
        if !self.domain_ready_for_typed_send() {
            return;
        }
        for action in actions {
            match action {
                crate::events::ActiveAction::RequestMarketsList => {
                    self.send_api_request(&crate::commands::engine_request::get_markets_list());
                }
                crate::events::ActiveAction::RequestUpdateMarketsList => {
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                crate::events::ActiveAction::RequestStrategySchema => {
                    self.strat_schema_request();
                }
                crate::events::ActiveAction::RequestOrderBookFull {
                    market_index,
                    book_kind,
                } => {
                    self.send_api_request(
                        &crate::commands::engine_request::request_order_book_full(
                            market_index,
                            book_kind,
                        ),
                    );
                }
                crate::events::ActiveAction::SendStrategySnapshot {
                    server_epoch,
                    client_max_last_date,
                    full,
                    data,
                } => {
                    self.strat_send_snapshot_payload(
                        server_epoch,
                        client_max_last_date,
                        full,
                        &data,
                    );
                }
                crate::events::ActiveAction::RequestOrderStatus {
                    order_id,
                    exact_rev,
                } => {
                    self.request_order_status(order_id, exact_rev);
                }
                crate::events::ActiveAction::TradesResend { payload } => {
                    self.send_api_request(&payload);
                }
                crate::events::ActiveAction::ReportSync {
                    request_uid,
                    request,
                } => {
                    self.send_report_sync_at(request_uid, request, self.now_ms());
                }
                crate::events::ActiveAction::ReportPageReceived {
                    request_uid,
                    server_token,
                } => {
                    self.record_report_page_received(request_uid, server_token);
                }
                crate::events::ActiveAction::ReportOpenRowsCheck { rec_ids } => {
                    self.send_report_open_rows_check_at(&rec_ids, self.now_ms());
                }
                crate::events::ActiveAction::ReportSchemaReceived { server_token } => {
                    self.record_report_schema_received(server_token);
                }
                crate::events::ActiveAction::ReportOpenRowsCheckCompleted { server_token } => {
                    self.complete_report_open_rows_check(server_token);
                }
                crate::events::ActiveAction::CandleTimeframeChanged { market_name, kind } => {
                    let unsubscribe = {
                        let mut registry = self.subscriptions.subscription_registry.lock();
                        match kind {
                            Some(kind) => {
                                registry.candle_subs.insert(market_name.clone(), kind);
                                false
                            }
                            None => registry.candle_subs.remove(&market_name).is_some(),
                        }
                    };
                    if unsubscribe {
                        self.send_api_request(&crate::commands::candles::unsubscribe_candles(&[
                            &market_name,
                        ]));
                    }
                }
            }
        }
    }
}
