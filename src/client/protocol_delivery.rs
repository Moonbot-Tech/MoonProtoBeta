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

#[inline]
fn is_strat_runtime_state_payload(command: Command, payload: &[u8]) -> bool {
    command == Command::Strat && crate::commands::strat::is_runtime_state_payload(payload)
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
        let trace_runtime_state =
            trace_io_enabled() && is_strat_runtime_state_payload(command, &payload);
        let trace_runtime_payload_hash = if trace_runtime_state {
            fnv1a64(&payload)
        } else {
            0
        };
        let trace_runtime_payload_head = if trace_runtime_state {
            trace_head(&payload, 16)
        } else {
            String::new()
        };
        if trace_runtime_state {
            eprintln!(
                "[mp-runtime-state] t={} stage=enter raw={} payload_len={} payload_hash={:016X} payload_head={} authorized={} auth_status={:?} domain_ready={}",
                trace_elapsed_ms(),
                cmd,
                payload.len(),
                trace_runtime_payload_hash,
                trace_runtime_payload_head,
                self.client.authorized,
                self.client.auth_status,
                self.client.subscriptions.domain_ready
            );
        }
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
        #[cfg(any(test, feature = "diagnostics"))]
        let active_decode_start = Instant::now();
        let decode_result = catch_unwind(AssertUnwindSafe(|| {
            self.client.client_new_data_decoded(
                cmd,
                payload,
                api_pending_consumed_by_reader,
                candles_chunk_consumed_by_reader,
                &mut mode.payload_buf,
            );
        }));
        #[cfg(any(test, feature = "diagnostics"))]
        self.client
            .metrics
            .protocol_metrics
            .record_profile_phase_labeled(
                ProfilePhase::ActiveDecode,
                active_decode_start.elapsed(),
                cmd,
                u8::MAX,
                mode.payload_buf.iter().map(|(_, p)| p.len()).sum::<usize>(),
            );
        if let Err(panic_payload) = decode_result {
            if trace_runtime_state {
                eprintln!(
                    "[mp-runtime-state] t={} stage=decode_panic raw={} payload_hash={:016X}",
                    trace_elapsed_ms(),
                    cmd,
                    trace_runtime_payload_hash
                );
            }
            mode.payload_buf.clear();
            mode.event_buf.clear();
            mode.active_actions_buf.clear();
            log::error!(target: "moonproto::runtime",
                "moonproto active decode panicked; dropping {:?} payload and continuing: {}",
                command,
                panic_payload_message(panic_payload.as_ref()));
            return;
        }
        if trace_runtime_state {
            let decoded = mode
                .payload_buf
                .iter()
                .map(|(c, p)| format!("{:?}/len={}/head={}", c, p.len(), trace_head(p, 12)))
                .collect::<Vec<_>>()
                .join(",");
            eprintln!(
                "[mp-runtime-state] t={} stage=decoded raw={} payload_hash={:016X} authorized_before={} authorized_after={} payload_buf_len={} decoded=[{}]",
                trace_elapsed_ms(),
                cmd,
                trace_runtime_payload_hash,
                authorized_before,
                self.client.authorized,
                mode.payload_buf.len(),
                decoded
            );
        }
        if !authorized_before && !self.client.authorized {
            if trace_runtime_state {
                eprintln!(
                    "[mp-runtime-state] t={} stage=drop_after_decode reason=not_authorized raw={} payload_hash={:016X} payload_buf_len={}",
                    trace_elapsed_ms(),
                    cmd,
                    trace_runtime_payload_hash,
                    mode.payload_buf.len()
                );
            }
            mode.payload_buf.clear();
            return;
        }
        let mut decoded_payloads = std::mem::take(&mut mode.payload_buf);
        for (c, p) in decoded_payloads.drain(..) {
            let trace_runtime_dispatch =
                trace_io_enabled() && is_strat_runtime_state_payload(c, &p);
            let dispatch_result = catch_unwind(AssertUnwindSafe(|| {
                mode.event_buf.clear();
                mode.active_actions_buf.clear();
                let ctx = crate::events::ActiveDispatchContext::from_client(self.client);
                #[cfg(any(test, feature = "diagnostics"))]
                let active_dispatch_start = Instant::now();
                #[cfg(any(test, feature = "diagnostics"))]
                let active_dispatch_inner_start = Instant::now();
                mode.dispatcher.dispatch_into_active_actions(
                    c,
                    &p,
                    cur_tm,
                    &mut mode.event_buf,
                    &ctx,
                    &mut mode.active_actions_buf,
                );
                #[cfg(any(test, feature = "diagnostics"))]
                self.client
                    .metrics
                    .protocol_metrics
                    .record_profile_phase_labeled(
                        ProfilePhase::ActiveDispatch,
                        active_dispatch_inner_start.elapsed(),
                        c.to_byte(),
                        metric_api_method(c, &p),
                        p.len(),
                    );
                if trace_runtime_dispatch {
                    eprintln!(
                        "[mp-runtime-state] t={} stage=dispatch_decoded payload_hash={:016X} event_count={} action_count={}",
                        trace_elapsed_ms(),
                        fnv1a64(&p),
                        mode.event_buf.len(),
                        mode.active_actions_buf.len()
                    );
                }
                #[cfg(any(test, feature = "diagnostics"))]
                let event_count = mode.event_buf.len();
                #[cfg(any(test, feature = "diagnostics"))]
                let action_count = mode.active_actions_buf.len();
                #[cfg(any(test, feature = "diagnostics"))]
                let active_actions_start = Instant::now();
                self.client
                    .apply_active_actions(mode.active_actions_buf.drain(..));
                #[cfg(any(test, feature = "diagnostics"))]
                self.client
                    .metrics
                    .protocol_metrics
                    .record_profile_phase_labeled(
                        ProfilePhase::ActiveActions,
                        active_actions_start.elapsed(),
                        c.to_byte(),
                        metric_api_method(c, &p),
                        p.len(),
                    );
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
                #[cfg(any(test, feature = "diagnostics"))]
                let drain_events_start = Instant::now();
                mode.drain_events(
                    &self.client.metrics.protocol_metrics,
                    Some(c),
                    metric_api_method(c, &p),
                    p.len(),
                );
                #[cfg(any(test, feature = "diagnostics"))]
                self.client
                    .metrics
                    .protocol_metrics
                    .record_profile_phase_labeled(
                        ProfilePhase::DrainEvents,
                        drain_events_start.elapsed(),
                        c.to_byte(),
                        metric_api_method(c, &p),
                        p.len(),
                    );
                if trace_runtime_dispatch {
                    eprintln!(
                        "[mp-runtime-state] t={} stage=dispatch_done payload_hash={:016X}",
                        trace_elapsed_ms(),
                        fnv1a64(&p)
                    );
                }
            }));
            if let Err(panic_payload) = dispatch_result {
                if trace_runtime_dispatch {
                    eprintln!(
                        "[mp-runtime-state] t={} stage=dispatch_panic payload_hash={:016X}",
                        trace_elapsed_ms(),
                        fnv1a64(&p)
                    );
                }
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
