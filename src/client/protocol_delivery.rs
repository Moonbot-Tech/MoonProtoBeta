use super::protocol_core::ProtocolCore;
use super::*;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(msg) = payload.downcast_ref::<&'static str>() {
        (*msg).to_owned()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

impl ProtocolCore<'_> {
    pub(crate) fn apply_recv_side_effects(&mut self, recv_bytes: u64, timestamp_ms: i64) {
        self.client.connected = true;
        if self.client.auth_status == AuthStatus::Base {
            self.client.auth_status = AuthStatus::Connected;
        }
        self.client.metrics.total_recv += recv_bytes;
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
        mode.event_buf.clear();
        mode.dispatcher
            .drain_deferred_order_removals_due(cur_tm, &mut mode.event_buf);
        mode.drain_events(&self.client.metrics.protocol_metrics, None, u8::MAX, 0);
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
        if is_domain_push_command(command)
            && !self.client.subscriptions.domain_ready
            && !incoming_allowed_before_domain_ready(command, &payload)
        {
            if trace_io_enabled() {
                eprintln!(
                    "[mp-dispatch-drop] t={} cmd={:?} raw={} payload_len={} payload_hash={:016X} reason=domain_not_ready",
                    trace_elapsed_ms(),
                    command,
                    cmd,
                    payload.len(),
                    fnv1a64(&payload)
                );
            }
            log::debug!(target: "moonproto::client",
                "domain command {:?} skipped before InitDone/domain_ready", command);
            return;
        }
        if is_trades_stream_command(command) && !self.client.has_trades_subscription_intent() {
            if trace_io_enabled() {
                eprintln!(
                    "[mp-dispatch-drop] t={} cmd={:?} raw={} payload_len={} payload_hash={:016X} reason=trades_without_subscription",
                    trace_elapsed_ms(),
                    command,
                    cmd,
                    payload.len(),
                    fnv1a64(&payload)
                );
            }
            log::warn!(target: "moonproto::client",
                "unexpected {:?} received without all-trades subscription; packet dropped", command);
            return;
        }

        mode.payload_buf.clear();
        let authorized_before = self.client.authorized;
        let decode_result = catch_unwind(AssertUnwindSafe(|| {
            self.client.client_new_data_decoded(
                cmd,
                payload,
                api_pending_consumed_by_reader,
                candles_chunk_consumed_by_reader,
                &mut mode.payload_buf,
            );
        }));
        if let Err(panic_payload) = decode_result {
            mode.payload_buf.clear();
            mode.event_buf.clear();
            mode.active_actions_buf.clear();
            log::error!(target: "moonproto::runtime",
                "moonproto active decode panicked; dropping {:?} payload and continuing: {}",
                command,
                panic_payload_message(panic_payload.as_ref()));
            return;
        }
        if !authorized_before && !self.client.authorized {
            mode.payload_buf.clear();
            return;
        }
        let mut decoded_payloads = std::mem::take(&mut mode.payload_buf);
        for (c, p) in decoded_payloads.drain(..) {
            let dispatch_result = catch_unwind(AssertUnwindSafe(|| {
                mode.event_buf.clear();
                mode.active_actions_buf.clear();
                let ctx = crate::events::ActiveDispatchContext::from_client(self.client);
                #[cfg(any(test, feature = "diagnostics"))]
                let active_dispatch_start = Instant::now();
                mode.dispatcher.dispatch_into_active_actions(
                    c,
                    &p,
                    cur_tm,
                    &mut mode.event_buf,
                    &ctx,
                    &mut mode.active_actions_buf,
                );
                #[cfg(any(test, feature = "diagnostics"))]
                let event_count = mode.event_buf.len();
                #[cfg(any(test, feature = "diagnostics"))]
                let action_count = mode.active_actions_buf.len();
                self.client
                    .apply_active_actions(mode.active_actions_buf.drain(..));
                #[cfg(any(test, feature = "diagnostics"))]
                self.client
                    .metrics
                    .protocol_metrics
                    .record_active_dispatch_labeled(
                        active_dispatch_start.elapsed(),
                        c.to_byte(),
                        metric_api_method(c, &p),
                        p.len(),
                        event_count,
                        action_count,
                    );
                mode.drain_events(
                    &self.client.metrics.protocol_metrics,
                    Some(c),
                    metric_api_method(c, &p),
                    p.len(),
                );
            }));
            if let Err(panic_payload) = dispatch_result {
                mode.event_buf.clear();
                mode.active_actions_buf.clear();
                log::error!(target: "moonproto::runtime",
                    "moonproto active dispatch panicked; dropping {:?} payload_len={} and continuing: {}",
                    c,
                    p.len(),
                    panic_payload_message(panic_payload.as_ref()));
            }
        }
        mode.payload_buf = decoded_payloads;
    }
}
