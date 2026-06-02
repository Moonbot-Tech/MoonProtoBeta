use super::protocol_core::ProtocolCore;
use super::*;

struct RecvPhaseOutcome {
    drained_any: bool,
    #[cfg(test)]
    deadline_reached: bool,
}

impl ProtocolCore<'_> {
    pub(crate) fn recv_one_phase(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) -> bool {
        self.recv_phase_limited(cur_tm, None, 1, mode).drained_any
    }

    #[cfg(test)]
    pub(crate) fn recv_drain_phase(
        &mut self,
        cur_tm: i64,
        run_deadline: Instant,
        mode: &mut RunMode<'_>,
    ) -> bool {
        self.recv_phase_limited(cur_tm, Some(run_deadline), usize::MAX, mode)
            .deadline_reached
    }

    fn recv_phase_limited(
        &mut self,
        cur_tm: i64,
        run_deadline: Option<Instant>,
        max_datagrams: usize,
        mode: &mut RunMode<'_>,
    ) -> RecvPhaseOutcome {
        let mut buf = [0u8; 65535];
        let mut drained_any = false;
        let mut deadline_reached = false;
        let mut datagrams = 0usize;
        loop {
            if max_datagrams == 0 || datagrams >= max_datagrams {
                break;
            }
            if run_deadline.is_some_and(|deadline| Instant::now() >= deadline)
                || self.client.shutdown_requested()
            {
                deadline_reached = true;
                break;
            }
            // The datagram source address from recv_from is intentionally not
            // validated. Authenticity comes from the keyed MAC (+ AEAD for
            // sensitive commands); a source IP is attacker-spoofable, so filtering
            // it adds no security. A connected socket would instead surface ICMP
            // ECONNREFUSED churn and silently drop replies whose source !=
            // destination (multi-socket servers, NAT, asymmetric routing,
            // VPN/tunnel egress), breaking legitimate users behind non-trivial
            // networks. Off-path junk is rejected by the MAC; an address filter
            // would not stop a spoofing flooder anyway.
            let recv_result = {
                let Some(sock) = self.client.transport.socket.as_ref() else {
                    break;
                };
                sock.recv_from(&mut buf)
            };

            match recv_result {
                Ok((n, _)) => {
                    drained_any = true;
                    datagrams += 1;
                    let continue_recv = self.process_datagram(&buf[..n], n as u64, mode);
                    self.drain_post_receive_delivery(cur_tm, mode);
                    if !continue_recv {
                        break;
                    }
                    if run_deadline.is_some_and(|deadline| Instant::now() >= deadline)
                        || self.client.shutdown_requested()
                    {
                        deadline_reached = true;
                        break;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(e) => {
                    log::warn!(target: "moonproto::reader",
                        "recv_from error: {} ({:?})", e, e.kind());
                    break;
                }
            }
        }

        if drained_any {
            self.rearm_recv_poller();
        }
        if deadline_reached && trace_io_enabled() {
            eprintln!(
                "[mp-recv-yield] t={} reason=run_deadline drained_any={}",
                trace_elapsed_ms(),
                drained_any
            );
        }
        RecvPhaseOutcome {
            drained_any,
            #[cfg(test)]
            deadline_reached,
        }
    }

    pub(crate) fn rearm_recv_poller(&mut self) {
        let (Some(poller), Some(sock)) = (
            self.client.transport.recv_poller.as_ref(),
            self.client.transport.socket.as_ref(),
        ) else {
            return;
        };
        if let Err(e) = poller.modify(sock, PollEvent::readable(1)) {
            log::warn!(target: "moonproto::reader", "UDP poller rearm failed: {e}");
            self.client.transport.recv_poller = None;
        }
    }

    pub(crate) fn process_datagram(
        &mut self,
        datagram: &[u8],
        recv_bytes: u64,
        mode: &mut RunMode<'_>,
    ) -> bool {
        #[cfg(any(test, feature = "diagnostics"))]
        let protocol_metrics = Arc::clone(&self.client.metrics.protocol_metrics);
        #[cfg(any(test, feature = "diagnostics"))]
        protocol_metrics.record_recv_packet();
        #[cfg(any(test, feature = "diagnostics"))]
        let protocol_start = Instant::now();
        let mut protocol_wait = Duration::ZERO;
        #[cfg(any(test, feature = "diagnostics"))]
        let mut metric_cmd = u8::MAX;
        #[cfg(any(test, feature = "diagnostics"))]
        let mut metric_payload_len = datagram.len();

        let continue_recv = if let Some((hdr, payload)) =
            crate::transport::transport_unpack_with_mac(
                &self.client.transport.mac_ctx,
                datagram,
                self.client.cfg.transport_mode.to_byte(),
            ) {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                metric_cmd = Command::from_byte(hdr.cmd).to_byte();
                metric_payload_len = payload.len();
            }

            if trace_io_enabled() {
                eprintln!(
                    "[mp-io-rx] t={} cmd={:?} raw={} packet_len={} payload_len={} packet_hash={:016X} packet_head={} payload_hash={:016X} payload_head={}",
                    trace_elapsed_ms(),
                    Command::from_byte(hdr.cmd),
                    hdr.cmd,
                    datagram.len(),
                    payload.len(),
                    fnv1a64(datagram),
                    trace_head(datagram, 16),
                    fnv1a64(&payload),
                    trace_head(&payload, 16)
                );
            }

            let timestamp_ms = self.client.now_ms();
            if Command::from_byte(hdr.cmd) == Command::WantNewHello {
                let total_recv_after = self
                    .client
                    .metrics
                    .total_recv_shared
                    .load(Ordering::Relaxed);
                self.route_command(
                    hdr.cmd,
                    &payload,
                    recv_bytes,
                    total_recv_after,
                    timestamp_ms,
                    mode,
                    &mut protocol_wait,
                )
            } else {
                self.apply_recv_side_effects(recv_bytes, timestamp_ms);
                let total_recv_after = self
                    .client
                    .metrics
                    .total_recv_shared
                    .fetch_add(recv_bytes, Ordering::Relaxed)
                    + recv_bytes;

                #[cfg(any(test, feature = "diagnostics"))]
                {
                    if let Some(decision) = err_emu_drop_decision(hdr.cmd) {
                        self.client
                            .metrics
                            .err_emu_diagnostics
                            .lock()
                            .record_packet(hdr.cmd, &payload, decision);
                        if decision.dropped {
                            Self::on_err_emu_drop(hdr.cmd, &payload);
                            true
                        } else {
                            self.route_command(
                                hdr.cmd,
                                &payload,
                                recv_bytes,
                                total_recv_after,
                                timestamp_ms,
                                mode,
                                &mut protocol_wait,
                            )
                        }
                    } else {
                        self.route_command(
                            hdr.cmd,
                            &payload,
                            recv_bytes,
                            total_recv_after,
                            timestamp_ms,
                            mode,
                            &mut protocol_wait,
                        )
                    }
                }
                #[cfg(not(any(test, feature = "diagnostics")))]
                {
                    self.route_command(
                        hdr.cmd,
                        &payload,
                        recv_bytes,
                        total_recv_after,
                        timestamp_ms,
                        mode,
                        &mut protocol_wait,
                    )
                }
            }
        } else {
            if trace_io_enabled() {
                eprintln!(
                    "[mp-io-rx-invalid] t={} packet_len={} packet_hash={:016X} packet_head={}",
                    trace_elapsed_ms(),
                    datagram.len(),
                    fnv1a64(datagram),
                    trace_head(datagram, 16)
                );
            }
            true
        };

        #[cfg(any(test, feature = "diagnostics"))]
        if protocol_wait > Duration::ZERO {
            protocol_metrics.record_reader_protocol_wait_labeled(
                protocol_wait,
                metric_cmd,
                metric_payload_len,
            );
        }
        #[cfg(any(test, feature = "diagnostics"))]
        protocol_metrics.record_reader_protocol_labeled(
            protocol_start.elapsed().saturating_sub(protocol_wait),
            metric_cmd,
            metric_payload_len,
        );
        continue_recv
    }

    // Routes one decoded wire command byte to its service/handshake handler,
    // falling through to `dispatch_packet_commands` for data commands.
    // (Named `route_command` to stay distinct from the runtime-loop
    // `handle_command`, which dispatches app-level `RuntimeCommand`s.)
    pub(crate) fn route_command(
        &mut self,
        raw_cmd: u8,
        payload: &[u8],
        recv_bytes: u64,
        total_recv_after: u64,
        timestamp_ms: i64,
        mode: &mut RunMode<'_>,
        protocol_wait: &mut Duration,
    ) -> bool {
        if Command::from_byte(raw_cmd) == Command::WantNewHello {
            self.on_handshake_control(Command::WantNewHello, payload, recv_bytes, timestamp_ms);
            return true;
        }

        self.client
            .transport
            .recv_slicer
            .set_last_online(timestamp_ms);
        self.client.transport.recv_slicer.do_cleanup();

        match Command::from_byte(raw_cmd) {
            Command::Ping => {
                // Delphi UDPRead treats Ping as an established-session packet:
                // LastOnline/recv counters are already updated, but before
                // AuthDone the Ping body must not update RTT/PMTU, TmpSlider
                // ACK state, NeedConnect, or emit a Ping response.
                if !self.client.authorized {
                    return true;
                }
                self.on_new_ping(payload, recv_bytes, total_recv_after, timestamp_ms, mode);
            }
            Command::WrongHello | Command::WantNewHello => {
                self.on_handshake_control(
                    Command::from_byte(raw_cmd),
                    payload,
                    recv_bytes,
                    timestamp_ms,
                );
            }
            Command::WhoAreYou => {
                *protocol_wait += self.on_who_are_you(payload, recv_bytes, timestamp_ms);
            }
            Command::Fine => {
                self.on_fine(payload, recv_bytes, timestamp_ms);
            }
            Command::SizeTest => {
                self.on_new_size_test(payload);
            }
            Command::ProbeMTU => {
                self.on_new_probe_mtu(payload);
            }
            Command::SlicedACK => {
                self.on_new_sliced_ack(payload);
            }
            Command::Sliced => {
                if !self.on_new_sliced(payload, recv_bytes, timestamp_ms, mode) {
                    return false;
                }
            }
            _ => {
                self.dispatch_packet_commands(
                    raw_cmd,
                    payload,
                    recv_bytes,
                    timestamp_ms,
                    false,
                    mode,
                );
            }
        }

        true
    }

    // parity: MoonBot MoonProtoCommon.pas:DataRead — splits a Grouped container
    // into its sub-commands (cmd byte + u16 len + payload) and dispatches each;
    // a non-Grouped command is dispatched directly as the single command.
    pub(crate) fn dispatch_packet_commands(
        &mut self,
        raw_cmd: u8,
        payload: &[u8],
        recv_bytes: u64,
        timestamp_ms: i64,
        apply_recv_effects_first: bool,
        mode: &mut RunMode<'_>,
    ) {
        if Command::from_byte(raw_cmd & 0x7F) != Command::Grouped {
            self.dispatch_command(
                raw_cmd,
                payload,
                recv_bytes,
                timestamp_ms,
                apply_recv_effects_first,
                None,
                mode,
            );
            return;
        }
        if raw_cmd & COMPRESSED_FLAG != 0 {
            if apply_recv_effects_first {
                self.apply_recv_side_effects(recv_bytes, timestamp_ms);
            }
            return;
        }

        let mut pos = 0;
        let mut emitted = false;
        while pos + 3 <= payload.len() {
            let sub_cmd = payload[pos];
            pos += 1;
            let sz = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
            pos += 2;
            if pos + sz > payload.len() {
                break;
            }
            self.dispatch_command(
                sub_cmd,
                &payload[pos..pos + sz],
                recv_bytes,
                timestamp_ms,
                apply_recv_effects_first && !emitted,
                None,
                mode,
            );
            emitted = true;
            pos += sz;
        }

        if !emitted && apply_recv_effects_first {
            self.apply_recv_side_effects(recv_bytes, timestamp_ms);
        }
    }

    // parity: MoonBot MoonProtoCommon.pas:DataReadInt — per-single-command
    // decode (Crypted/compressed/auth gates) then dispatch; borrowed payload.
    pub(crate) fn dispatch_command(
        &mut self,
        raw_cmd: u8,
        payload: &[u8],
        recv_bytes: u64,
        timestamp_ms: i64,
        apply_recv_effects: bool,
        sliced_stats: Option<ReaderSlicedStats>,
        mode: &mut RunMode<'_>,
    ) {
        if apply_recv_effects {
            self.apply_recv_side_effects(recv_bytes, timestamp_ms);
        }
        if self.should_drop_crypted_before_auth(raw_cmd, payload.len()) {
            if let Some(stats) = sliced_stats {
                self.apply_reader_sliced_stats(stats);
            }
            return;
        }
        let decoded = Client::decode_command_payload_shared(
            &mut self.client.recv.data_read_state,
            raw_cmd,
            payload,
        );
        let Some((cmd, payload)) = decoded else {
            if let Some(stats) = sliced_stats {
                self.apply_reader_sliced_stats(stats);
            }
            return;
        };
        self.finish_decoded_command(cmd, payload, timestamp_ms, sliced_stats, mode);
    }

    // parity: MoonBot MoonProtoCommon.pas:DataReadInt — same per-command decode
    // and dispatch as `dispatch_command`, but takes ownership of the payload
    // bytes (sliced reassembly already owns a Vec, so no extra copy).
    pub(crate) fn dispatch_command_owned(
        &mut self,
        raw_cmd: u8,
        payload: Vec<u8>,
        recv_bytes: u64,
        timestamp_ms: i64,
        apply_recv_effects: bool,
        sliced_stats: Option<ReaderSlicedStats>,
        mode: &mut RunMode<'_>,
    ) {
        if apply_recv_effects {
            self.apply_recv_side_effects(recv_bytes, timestamp_ms);
        }
        if self.should_drop_crypted_before_auth(raw_cmd, payload.len()) {
            if let Some(stats) = sliced_stats {
                self.apply_reader_sliced_stats(stats);
            }
            return;
        }
        let Some((cmd, payload)) = Client::decode_command_payload_owned(
            &mut self.client.recv.data_read_state,
            raw_cmd,
            payload,
        ) else {
            return;
        };
        self.finish_decoded_command(cmd, payload, timestamp_ms, sliced_stats, mode);
    }

    fn should_drop_crypted_before_auth(&self, raw_cmd: u8, payload_len: usize) -> bool {
        if self.client.authorized || Command::from_byte(raw_cmd) != Command::Crypted {
            return false;
        }
        if trace_io_enabled() {
            eprintln!(
                "[mp-dispatch-drop] t={} cmd=Crypted raw={} payload_len={} reason=not_authorized_crypted",
                trace_elapsed_ms(),
                raw_cmd,
                payload_len
            );
        }
        true
    }

    fn finish_decoded_command(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        timestamp_ms: i64,
        sliced_stats: Option<ReaderSlicedStats>,
        mode: &mut RunMode<'_>,
    ) {
        match Command::from_byte(cmd) {
            Command::NeedHelloAgain => {
                self.apply_need_hello_again(timestamp_ms);
                return;
            }
            Command::SessionClose => {
                return;
            }
            _ => {}
        }

        let api_pending_consumed = Client::dispatch_api_pending(
            self.client.pending_api.api_pending.as_ref(),
            cmd,
            &payload,
        );
        let candles_chunk_consumed = Client::dispatch_candles_chunk(
            &mut self.client.pending_api,
            cmd,
            &payload,
            timestamp_ms,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        if let Some(stats) = sliced_stats.as_ref() {
            self.client
                .metrics
                .err_emu_diagnostics
                .lock()
                .record_sliced_complete(stats.datagram_num, stats.blocks_count, cmd, &payload);
        }
        if trace_io_enabled() {
            let cmd_kind = Command::from_byte(cmd);
            let api = if cmd_kind == Command::API {
                Client::engine_response_meta_from_payload(&payload)
                    .map(|meta| {
                        format!(
                            " api_uid={} api_method={:?} api_success={}",
                            meta.request_uid, meta.method, meta.success
                        )
                    })
                    .unwrap_or_else(|| " api=malformed".to_string())
            } else {
                String::new()
            };
            let strat = if cmd_kind == Command::Strat {
                let strat_cmd = payload.first().copied();
                let strat_uid = payload
                    .get(3..11)
                    .and_then(|uid| uid.try_into().ok())
                    .map(u64::from_le_bytes);
                format!(" strat_cmd={strat_cmd:?} strat_uid={strat_uid:?}")
            } else {
                String::new()
            };
            let ui = if cmd_kind == Command::UI {
                format!(" ui_cmd={:?}", payload.first().copied())
            } else {
                String::new()
            };
            let sliced = sliced_stats
                .as_ref()
                .map(|stats| {
                    format!(
                        " sliced_d={} sliced_blocks={}",
                        stats.datagram_num, stats.blocks_count
                    )
                })
                .unwrap_or_default();
            eprintln!(
                "[mp-dataread] t={} cmd={:?} raw={} payload_len={} payload_hash={:016X} payload_head={} api_pending_consumed={} candles_chunk_consumed={}{}{}{}{}",
                trace_elapsed_ms(),
                cmd_kind,
                cmd,
                payload.len(),
                fnv1a64(&payload),
                trace_head(&payload, 16),
                api_pending_consumed,
                candles_chunk_consumed,
                sliced,
                api,
                strat,
                ui
            );
        }
        if let Some(stats) = sliced_stats {
            self.apply_reader_sliced_stats(stats);
        }
        self.client_new_data(
            cmd,
            payload,
            api_pending_consumed,
            candles_chunk_consumed,
            timestamp_ms,
            mode,
        );
    }
}
