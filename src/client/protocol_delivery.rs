use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    pub(crate) fn apply_recv_side_effects(&mut self, recv_bytes: u64, timestamp_ms: i64) {
        self.client.connected = true;
        if self.client.auth_status == AuthStatus::Base {
            self.client.auth_status = AuthStatus::Connected;
        }
        self.client.total_recv += recv_bytes;
        self.client.track_recv(recv_bytes, timestamp_ms);
        self.client.last_online = timestamp_ms;
    }

    pub(crate) fn drain_post_receive_delivery(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        self.drain_deferred_order_removals_due(cur_tm, mode);
    }

    pub(crate) fn drain_deferred_order_removals_due(
        &mut self,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        match mode {
            #[cfg(test)]
            RunMode::Dispatcher {
                dispatcher,
                on_event,
                event_buf,
                ..
            } => {
                event_buf.clear();
                dispatcher.drain_deferred_order_removals_due(cur_tm, event_buf);
                on_event.drain_events(
                    event_buf,
                    dispatcher,
                    &self.client.protocol_metrics,
                    None,
                    u8::MAX,
                    0,
                );
            }
            RunMode::DispatcherWorker { tx, .. } => {
                let _ = tx.send(DispatcherWorkItem::DrainDeferredOrderRemovals { now_ms: cur_tm });
            }
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => {}
        }
    }

    pub(crate) fn client_new_data(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        let command = Command::from_byte(cmd);
        if is_domain_push_command(command) && !self.client.domain_ready {
            log::debug!(target: "moonproto::client",
                "domain command {:?} skipped before InitDone/domain_ready", command);
            return;
        }
        if is_trades_stream_command(command) && !self.client.has_trades_subscription_intent() {
            log::warn!(target: "moonproto::client",
                "unexpected {:?} received without all-trades subscription; packet dropped", command);
            return;
        }

        match mode {
            #[cfg(test)]
            RunMode::Callback { on_data } => {
                let mut sink = DispatchSink::Callback(on_data);
                self.client.client_new_data_decoded(
                    cmd,
                    payload,
                    api_pending_consumed_by_reader,
                    candles_chunk_consumed_by_reader,
                    &mut sink,
                );
            }
            RunMode::CallbackQueue { app_tx } => {
                let mut sink = DispatchSink::CallbackQueue(app_tx);
                self.client.client_new_data_decoded(
                    cmd,
                    payload,
                    api_pending_consumed_by_reader,
                    candles_chunk_consumed_by_reader,
                    &mut sink,
                );
            }
            #[cfg(test)]
            RunMode::Dispatcher {
                dispatcher,
                on_event,
                event_buf,
                payload_buf,
                active_actions_buf,
            } => {
                payload_buf.clear();
                let authorized_before = self.client.authorized;
                {
                    let mut sink = DispatchSink::Buffer(payload_buf);
                    self.client.client_new_data_decoded(
                        cmd,
                        payload,
                        api_pending_consumed_by_reader,
                        candles_chunk_consumed_by_reader,
                        &mut sink,
                    );
                }
                if !authorized_before && !self.client.authorized {
                    payload_buf.clear();
                    return;
                }
                for (c, p) in payload_buf.drain(..) {
                    event_buf.clear();
                    active_actions_buf.clear();
                    let ctx = crate::events::ActiveDispatchContext::from_client(self.client);
                    let active_dispatch_start = Instant::now();
                    dispatcher.dispatch_into_active_actions(
                        c,
                        &p,
                        cur_tm,
                        event_buf,
                        &ctx,
                        active_actions_buf,
                    );
                    let event_count = event_buf.len();
                    let action_count = active_actions_buf.len();
                    self.client
                        .apply_active_actions(active_actions_buf.drain(..));
                    self.client.protocol_metrics.record_active_dispatch_labeled(
                        active_dispatch_start.elapsed(),
                        c.to_byte(),
                        metric_api_method(c, &p),
                        p.len(),
                        event_count,
                        action_count,
                    );
                    on_event.drain_events(
                        event_buf,
                        dispatcher,
                        &self.client.protocol_metrics,
                        Some(c),
                        metric_api_method(c, &p),
                        p.len(),
                    );
                }
            }
            RunMode::DispatcherWorker { tx, payload_buf } => {
                payload_buf.clear();
                let authorized_before = self.client.authorized;
                if command == Command::API {
                    if !candles_chunk_consumed_by_reader {
                        self.client.process_api_bookkeeping_light(&payload);
                        payload_buf.push((Command::API, payload));
                    }
                } else {
                    let mut sink = DispatchSink::Buffer(payload_buf);
                    self.client.client_new_data_decoded(
                        cmd,
                        payload,
                        api_pending_consumed_by_reader,
                        candles_chunk_consumed_by_reader,
                        &mut sink,
                    );
                }
                if !authorized_before && !self.client.authorized {
                    payload_buf.clear();
                    return;
                }
                for (c, p) in payload_buf.drain(..) {
                    let enqueue_start = Instant::now();
                    let payload_len = p.len();
                    let api_method = metric_api_method(c, &p);
                    let work = DispatcherWorkItem::Data {
                        cmd: c,
                        payload: p,
                        now_ms: cur_tm,
                        ctx: crate::events::ActiveDispatchContext::from_client(self.client),
                    };
                    let _ = tx.send(work);
                    self.client.protocol_metrics.record_app_enqueue_labeled(
                        enqueue_start.elapsed(),
                        c.to_byte(),
                        api_method,
                        payload_len,
                        1,
                        4,
                    );
                }
            }
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => {}
        }
    }
}
