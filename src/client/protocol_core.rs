use super::*;

pub(crate) struct ProtocolCore<'client> {
    pub(crate) client: &'client mut Client,
}

impl ProtocolCore<'_> {
    pub(crate) fn run(&mut self, duration: Duration, mode: &mut RunMode<'_>) {
        let run_start = Instant::now();
        let protocol_metrics = Arc::clone(&self.client.protocol_metrics);

        loop {
            let _tick_timer = protocol_metrics.writer_tick_timer();
            if run_start.elapsed() >= duration {
                break;
            }
            let cur_tm = self.client.now_ms();

            self.writer_tick_prologue(cur_tm);

            if self.ensure_socket_bound(cur_tm) {
                self.recv_drain_phase(cur_tm, mode);

                let cpu_start = Instant::now();
                self.drain_app_commands(cur_tm, mode);
                self.send_maintenance_phase(cur_tm, mode, &protocol_metrics);
                protocol_metrics.record_writer_cpu(cpu_start.elapsed());
                self.wait_5ms();
            } else {
                let cpu_start = Instant::now();
                protocol_metrics.record_writer_cpu(cpu_start.elapsed());
                // Сокет ещё не привязан — короткая пауза перед повторной попыткой bind.
                thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            }
        }
    }

    pub(crate) fn writer_tick_prologue(&mut self, cur_tm: i64) {
        // Emit lifecycle events on auth_status transitions.
        self.client.check_lifecycle_transition();

        // ActualSleepTime EMA (matches UDPClient.pas:725-734)
        if self.client.prev_cycle_tm != 0 {
            let raw = (cur_tm - self.client.prev_cycle_tm).abs();
            if raw > 0 && raw < 100 {
                if self.client.actual_sleep_time <= 0.0 {
                    self.client.actual_sleep_time = raw as f64;
                } else {
                    self.client.actual_sleep_time =
                        self.client.actual_sleep_time * 0.7 + raw as f64 * 0.3;
                }
            }
        }
        self.client.prev_cycle_tm = cur_tm;
    }

    pub(crate) fn ensure_socket_bound(&mut self, cur_tm: i64) -> bool {
        if self.client.socket.is_none() && self.client.need_connect {
            self.client.bind_socket(cur_tm);
        }
        if self.client.socket.is_some() && self.client.recv_poller.is_none() {
            self.client.register_recv_poller();
        }
        self.client.socket.is_some()
    }

    pub(crate) fn recv_drain_phase(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        let mut buf = [0u8; 65535];
        let mut drained_any = false;
        loop {
            let recv_result = {
                let Some(sock) = self.client.socket.as_ref() else {
                    break;
                };
                sock.recv_from(&mut buf)
            };

            match recv_result {
                Ok((n, _)) => {
                    drained_any = true;
                    let continue_recv = self.process_datagram_inline(&buf[..n], n as u64, mode);
                    self.drain_post_receive_delivery(cur_tm, mode);
                    if !continue_recv {
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
    }

    pub(crate) fn rearm_recv_poller(&mut self) {
        let (Some(poller), Some(sock)) = (
            self.client.recv_poller.as_ref(),
            self.client.socket.as_ref(),
        ) else {
            return;
        };
        if let Err(e) = poller.modify(sock, PollEvent::readable(1)) {
            log::warn!(target: "moonproto::reader", "UDP poller rearm failed: {e}");
            self.client.recv_poller = None;
        }
    }

    pub(crate) fn process_datagram_inline(
        &mut self,
        datagram: &[u8],
        recv_bytes: u64,
        mode: &mut RunMode<'_>,
    ) -> bool {
        let protocol_metrics = Arc::clone(&self.client.protocol_metrics);
        protocol_metrics.record_recv_packet();
        let protocol_start = Instant::now();
        let mut metric_cmd = u8::MAX;
        let mut metric_payload_len = datagram.len();

        let continue_recv = if let Some((hdr, payload)) =
            moonproto_transport::transport_unpack_with_mac(
                &self.client.mac_ctx,
                &self.client.cfg.mac_key,
                datagram,
                self.client.cfg.mask_ver,
            ) {
            metric_cmd = Command::from_byte(hdr.cmd).to_byte();
            metric_payload_len = payload.len();

            if trace_io_enabled() {
                eprintln!(
                    "[mp-io-rx] cmd={:?} raw={} packet_len={} payload_len={}",
                    Command::from_byte(hdr.cmd),
                    hdr.cmd,
                    datagram.len(),
                    payload.len()
                );
            }

            let timestamp_ms = self.client.now_ms();
            self.apply_recv_side_effects(recv_bytes, timestamp_ms);
            let total_recv_after = self
                .client
                .total_recv_shared
                .fetch_add(recv_bytes, Ordering::Relaxed)
                + recv_bytes;

            if let Some(decision) = err_emu_drop_decision(hdr.cmd) {
                self.client
                    .err_emu_diagnostics
                    .lock()
                    .unwrap()
                    .record_packet(hdr.cmd, &payload, decision);
                if decision.dropped {
                    Self::on_err_emu_drop_inline(hdr.cmd, &payload);
                    true
                } else {
                    self.handle_command_inline(
                        hdr.cmd,
                        &payload,
                        recv_bytes,
                        total_recv_after,
                        timestamp_ms,
                        mode,
                    )
                }
            } else {
                self.handle_command_inline(
                    hdr.cmd,
                    &payload,
                    recv_bytes,
                    total_recv_after,
                    timestamp_ms,
                    mode,
                )
            }
        } else {
            true
        };

        protocol_metrics.record_reader_protocol_labeled(
            protocol_start.elapsed(),
            metric_cmd,
            metric_payload_len,
        );
        continue_recv
    }

    pub(crate) fn handle_command_inline(
        &mut self,
        raw_cmd: u8,
        payload: &[u8],
        recv_bytes: u64,
        total_recv_after: u64,
        timestamp_ms: i64,
        mode: &mut RunMode<'_>,
    ) -> bool {
        self.client.recv_slicer.set_last_online(timestamp_ms);
        self.client.recv_slicer.do_cleanup();

        match Command::from_byte(raw_cmd) {
            Command::Ping => {
                self.on_new_ping_inline(payload, recv_bytes, total_recv_after, timestamp_ms, mode);
            }
            Command::WrongHello | Command::WantNewHello | Command::NeedHelloAgain => {
                self.on_handshake_control_inline(
                    Command::from_byte(raw_cmd),
                    recv_bytes,
                    timestamp_ms,
                );
            }
            Command::WhoAreYou => {
                self.on_who_are_you_inline(payload, recv_bytes, timestamp_ms);
            }
            Command::Fine => {
                self.on_fine_inline(payload, recv_bytes, timestamp_ms);
            }
            Command::SizeTest => {
                self.on_new_size_test_inline(payload);
            }
            Command::ProbeMTU => {
                self.on_new_probe_mtu_inline(payload);
            }
            Command::SlicedACK => {
                self.on_new_sliced_ack_inline(payload);
            }
            Command::Sliced => {
                if !self.on_new_sliced_inline(payload, recv_bytes, timestamp_ms, mode) {
                    return false;
                }
            }
            _ => {
                self.data_read_inline(raw_cmd, payload, recv_bytes, timestamp_ms, false, mode);
            }
        }

        true
    }

    pub(crate) fn on_err_emu_drop_inline(raw_cmd: u8, payload: &[u8]) {
        if trace_io_enabled() {
            eprintln!(
                "[mp-io-drop-err-emu] cmd={:?} raw={} payload_len={}",
                Command::from_byte(raw_cmd),
                raw_cmd,
                payload.len()
            );
        }
        if slicing::trace_enabled() && Command::from_byte(raw_cmd) == Command::Sliced {
            if let Some(sh) = slicing::SliceHeader::from_bytes(payload) {
                eprintln!(
                    "[slice-rx-drop-err-emu] d={} b={}/{} len={}",
                    sh.datagram_num,
                    sh.block_num,
                    sh.max_block_num,
                    payload.len()
                );
            } else {
                eprintln!("[slice-rx-drop-err-emu] malformed len={}", payload.len());
            }
        }
    }

    pub(crate) fn data_read_inline(
        &mut self,
        raw_cmd: u8,
        payload: &[u8],
        recv_bytes: u64,
        timestamp_ms: i64,
        apply_recv_effects_first: bool,
        mode: &mut RunMode<'_>,
    ) {
        if Command::from_byte(raw_cmd) != Command::Grouped {
            self.data_read_int_inline(
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
            self.data_read_int_inline(
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

    pub(crate) fn data_read_int_inline(
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
        let decoded = Client::decode_data_read_int_payload_shared(
            &mut self.client.data_read_state,
            raw_cmd,
            payload,
        );
        let (cmd, payload) = decoded
            .map(|(cmd, payload)| (cmd, Some(payload)))
            .unwrap_or((raw_cmd, None));
        let dispatch_api_pending_in_reader = !matches!(mode, RunMode::DispatcherWorker { .. });
        let api_pending_consumed = payload.as_deref().is_some_and(|payload| {
            dispatch_api_pending_in_reader
                && Client::dispatch_api_pending_inline(
                    self.client.api_pending.as_ref(),
                    cmd,
                    payload,
                )
        });
        let candles_chunk_consumed = payload.as_deref().is_some_and(|payload| {
            Client::dispatch_candles_chunk_inline(
                &mut self.client.pending_candles,
                cmd,
                payload,
                timestamp_ms,
            )
        });
        if let (Some(stats), Some(payload)) = (sliced_stats.as_ref(), payload.as_deref()) {
            self.client
                .err_emu_diagnostics
                .lock()
                .unwrap()
                .record_sliced_complete(stats.datagram_num, stats.blocks_count, cmd, payload);
        }
        if let Some(stats) = sliced_stats {
            self.apply_reader_sliced_stats(stats);
        }
        if let Some(payload) = payload {
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

    pub(crate) fn data_read_int_owned_inline(
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
        let Some((cmd, payload)) = Client::decode_data_read_int_payload_owned(
            &mut self.client.data_read_state,
            raw_cmd,
            payload,
        ) else {
            return;
        };
        let api_pending_consumed = !matches!(mode, RunMode::DispatcherWorker { .. })
            && Client::dispatch_api_pending_inline(self.client.api_pending.as_ref(), cmd, &payload);
        let candles_chunk_consumed = Client::dispatch_candles_chunk_inline(
            &mut self.client.pending_candles,
            cmd,
            &payload,
            timestamp_ms,
        );
        if let Some(stats) = sliced_stats.as_ref() {
            self.client
                .err_emu_diagnostics
                .lock()
                .unwrap()
                .record_sliced_complete(stats.datagram_num, stats.blocks_count, cmd, &payload);
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

    pub(crate) fn on_new_size_test_inline(&mut self, payload: &[u8]) {
        if let Some(ack) = Client::build_size_ack_payload(&mut self.client.data_read_state, payload)
        {
            if let Some(sock) = self.client.socket.as_ref() {
                set_dont_fragment_for_socket(sock, true);
            }
            self.send_command(Command::SizeAck, &ack);
            if let Some(sock) = self.client.socket.as_ref() {
                set_dont_fragment_for_socket(sock, false);
            }
        }
    }

    pub(crate) fn on_new_probe_mtu_inline(&mut self, payload: &[u8]) {
        if let Some(ack) = Client::build_probe_mtu_ack_payload(payload) {
            if let Some(sock) = self.client.socket.as_ref() {
                set_dont_fragment_for_socket(sock, true);
            }
            self.send_command(Command::ProbeMTUAck, &ack);
            if let Some(sock) = self.client.socket.as_ref() {
                set_dont_fragment_for_socket(sock, false);
            }
        }
    }

    pub(crate) fn on_handshake_control_inline(
        &mut self,
        cmd: Command,
        recv_bytes: u64,
        timestamp_ms: i64,
    ) {
        if matches!(cmd, Command::WrongHello | Command::WantNewHello) {
            self.client.waiting_hello = false;
        }
        if cmd == Command::WantNewHello {
            self.client.data_read_state.reset();
            self.client.send_lock.lock().unwrap().reset_tmp_slider();
            self.client.used_sliced_limit = false;
            self.client.crypt_msg_counter.store(0, Ordering::Relaxed);
            self.client.total_sent.store(0, Ordering::Relaxed);
            self.client.recvd_slider = Slider::new();
            self.client.recv_slicer = slicing::SlicingReceiver::new();
            self.client.total_recv_shared.store(0, Ordering::Relaxed);
        }
        let _ = recv_bytes;
        match cmd {
            Command::WrongHello => self.apply_wrong_hello(),
            Command::WantNewHello => self.apply_want_new_hello(),
            Command::NeedHelloAgain => self.apply_need_hello_again(timestamp_ms),
            _ => {}
        }
    }

    pub(crate) fn on_who_are_you_inline(
        &mut self,
        payload: &[u8],
        recv_bytes: u64,
        timestamp_ms: i64,
    ) {
        self.client.waiting_hello = false;
        if let Some(hello) = Client::decode_handshake_hello(
            &self.client.cfg.master_key,
            self.client.cfg.client_id,
            payload,
        ) {
            let encrypted = self.apply_who_are_you_hello_and_build_imfriend(hello);
            self.send_command(Command::ImFriend, &encrypted);
            self.send_command(Command::ImFriend, &encrypted);
            let _ = recv_bytes;
        } else {
            let _ = (recv_bytes, timestamp_ms);
        }
    }

    pub(crate) fn on_fine_inline(&mut self, payload: &[u8], recv_bytes: u64, timestamp_ms: i64) {
        self.client.waiting_hello = false;
        if Client::decode_handshake_hello(
            &self.client.cfg.master_key,
            self.client.cfg.client_id,
            payload,
        )
        .is_some()
        {
            let _ = recv_bytes;
            self.apply_fine_auth_done();
        } else {
            let _ = (recv_bytes, timestamp_ms);
        }
    }

    pub(crate) fn on_new_sliced_inline(
        &mut self,
        payload: &[u8],
        recv_bytes: u64,
        timestamp_ms: i64,
        mode: &mut RunMode<'_>,
    ) -> bool {
        let (assembled, ack) = self.client.recv_slicer.on_new_sliced(payload);

        if slicing::trace_enabled() {
            if let Some(hdr) = slicing::SliceHeader::from_bytes(payload) {
                let mut flags = [0u8; 32];
                flags.copy_from_slice(&ack[..32]);
                let total = hdr.max_block_num as usize + 1;
                eprintln!(
                    "[slice-ack-tx] d={} after_b={}/{} acked={}/{} missing={}",
                    u16::from_le_bytes([ack[32], ack[33]]),
                    hdr.block_num,
                    hdr.max_block_num,
                    slicing::acked_count(&flags, total),
                    total,
                    slicing::missing_preview(&flags, total)
                );
            }
        }
        let partial_sliced = assembled.is_none();
        self.send_command(Command::SlicedACK, &ack);
        if partial_sliced {
            for duplicate_idx in 0..diagnostic_duplicate_sliced_acks() {
                if slicing::trace_enabled() {
                    eprintln!(
                        "[slice-ack-tx-duplicate] d={} duplicate_idx={}",
                        u16::from_le_bytes([ack[32], ack[33]]),
                        duplicate_idx + 1
                    );
                }
                self.send_command(Command::SlicedACK, &ack);
            }
        }

        if let Some((datagram_num, cmd, payload, dup_count, blocks_count)) = assembled {
            self.data_read_int_owned_inline(
                cmd,
                payload,
                recv_bytes,
                timestamp_ms,
                false,
                Some(ReaderSlicedStats {
                    datagram_num,
                    dup_count,
                    blocks_count,
                }),
                mode,
            );
            self.client.recv_slicer.receiving.remove(&datagram_num);
        }

        true
    }

    pub(crate) fn on_new_sliced_ack_inline(&mut self, payload: &[u8]) {
        Client::push_sliced_ack_payload(&self.client.send_lock, payload);
    }

    pub(crate) fn on_new_ping_inline(
        &mut self,
        payload: &[u8],
        recv_bytes: u64,
        total_recv_after: u64,
        timestamp_ms: i64,
        mode: &mut RunMode<'_>,
    ) {
        let raw_now_dt = delphi_now_raw();
        let corrected_now_dt = delphi_now();
        if let Some(response) = self.client.apply_ping_and_build_response(
            payload,
            raw_now_dt,
            corrected_now_dt,
            self.client.total_sent.load(Ordering::Relaxed),
            total_recv_after,
        ) {
            self.send_command(Command::Ping, &response);
            self.client_new_data(
                Command::Ping.to_byte(),
                payload.to_vec(),
                false,
                false,
                timestamp_ms,
                mode,
            );
            let _ = recv_bytes;
        } else {
            let _ = (recv_bytes, timestamp_ms);
        }
    }

    pub(crate) fn drain_app_commands(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        self.drain_post_receive_delivery(cur_tm, mode);
    }

    pub(crate) fn wait_5ms(&mut self) {
        // Delphi writer sleeps a fixed short tick when there is no outgoing
        // work. In the single-owner Rust loop this wait is also the UDP
        // readable wait; the next loop drains the socket before send phase.
        if !self.client.send_lock.lock().unwrap().is_empty() {
            return;
        }
        let timeout = Some(Duration::from_millis(DEFAULT_SLEEP_MS));
        let Some(poller) = self.client.recv_poller.as_ref() else {
            thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            return;
        };
        self.client.recv_events.clear();
        match poller.wait(&mut self.client.recv_events, timeout) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                log::warn!(target: "moonproto::reader",
                    "UDP poller wait failed: {e}; falling back to sleep for this tick");
                thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            }
        }
    }

    pub(crate) fn send_maintenance_phase(
        &mut self,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
        protocol_metrics: &ProtocolMetrics,
    ) {
        let send_phase_start = Instant::now();
        self.transport_writer_maintenance_tick(cur_tm);
        protocol_metrics.record_send_phase(send_phase_start.elapsed());

        // Active library: all-trades reconnect sequence lives on the
        // writer tick. Gap recovery itself is checked only after
        // successful trades packets, like Delphi `ProcessTradesStream`.
        self.periodic_trades_reconnect_tick(cur_tm, mode);
        self.periodic_orderbook_reconnect_tick(cur_tm, mode);
        self.periodic_orders_tick(cur_tm, mode);

        self.transport_reconnect_tail_tick(cur_tm);
    }

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

    pub(crate) fn apply_reader_sliced_stats(&mut self, stats: ReaderSlicedStats) {
        let dup_pct = stats.dup_count as f64 / stats.blocks_count.max(1) as f64 * 100.0;
        if self.client.avg_dup_count == 0.0 {
            self.client.avg_dup_count = dup_pct;
        } else {
            self.client.avg_dup_count = (self.client.avg_dup_count * 9.0 + dup_pct) * 0.1;
        }
    }

    pub(crate) fn apply_wrong_hello(&mut self) {
        self.client.auth_status = AuthStatus::Connected;
    }

    pub(crate) fn apply_want_new_hello(&mut self) {
        self.client.full_reset();
        self.client.last_sent_hello = NEVER_SENT_MS;
        self.client.auth_status = AuthStatus::Connected;
        self.client.authorized = false;
        self.client.need_connect = true;
        self.client.soft_reconnect = false;
    }

    pub(crate) fn apply_need_hello_again(&mut self, timestamp_ms: i64) {
        if (timestamp_ms - self.client.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS {
            self.client.last_need_hello_again = timestamp_ms;
            if !self.client.waiting_hello {
                self.client.waiting_hello_start = timestamp_ms;
            }
            self.client.waiting_hello = true;
            self.client.last_sent_hello = NEVER_SENT_MS;
        }
    }

    pub(crate) fn apply_who_are_you_hello_and_build_imfriend(
        &mut self,
        mut hello: handshake::Hello,
    ) -> Vec<u8> {
        self.client.server_token = hello.server_token;
        let prev_app_token = self.client.peer_app_token;
        self.client.peer_app_token = hello.app_token;
        if prev_app_token != 0 && prev_app_token != hello.app_token {
            self.client.indexes_fetch_in_flight = false;
            self.client.tracked_indexes_peer_app_token = 0;
            self.client.fire_lifecycle(LifecycleEvent::ServerRestart);
        }

        self.client.client_token = self.client.client_token.wrapping_add(1);
        hello.mix_ts = self.client.client_token;
        hello.app_token = self.client.app_token;
        hello.timestamp = delphi_now();
        let packed = hello.to_bytes_packed();

        let (encode_key, decode_key) =
            crypto::generate_sub_keys(&self.client.cfg.master_key, self.client.server_token);
        self.client.encode_key = encode_key;
        self.client.decode_key = decode_key;
        let encode_cipher = crate::crypto::cipher_from_key(&self.client.encode_key);
        self.client.encode_cipher = Some(encode_cipher.clone());
        self.client
            .data_read_state
            .set_decode_cipher(crate::crypto::cipher_from_key(&self.client.decode_key));

        let aad = self.client.cfg.client_id.to_le_bytes();
        crypto::encrypt_with_cipher(&encode_cipher, &packed, &aad)
    }

    pub(crate) fn apply_fine_auth_done(&mut self) {
        let restore_after_reconnect = self.client.domain_ready && self.client.was_ever_connected;
        self.client.need_connect = false;
        self.client.auth_status = AuthStatus::AuthDone;
        self.client.authorized = true;
        if restore_after_reconnect {
            self.client.restore_domain_after_reconnect();
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

    pub(crate) fn transport_writer_maintenance_tick(&mut self, cur_tm: i64) {
        self.copy_send_ack_and_check_sening_data(cur_tm);

        // Timeout protection для init/API markets-index request marker.
        self.check_indexes_fetch_timeout(cur_tm);

        // F6/F7: periodic refresh prices + tags (опционально через ClientConfig.refresh).
        // Шлём только если auth_status == AuthDone (сервер примет запрос только в этой
        // фазе; до неё запрос потеряется впустую).
        if matches!(self.client.auth_status, AuthStatus::AuthDone) && self.client.domain_ready {
            self.tick_periodic_refresh(cur_tm);
        }
    }

    /// F6/F7: проверка пора ли слать periodic refresh-команды.
    /// Вызывается из writer loop каждый тик (~5мс), но реальная отправка происходит
    /// только когда прошёл `update_markets_every` / `check_tags_every` от последнего раза.
    ///
    /// Fire-and-forget: используем `send_api_request` без регистрации в pending registry —
    /// EventDispatcher автоматически применяет ответ к MarketsState когда он придёт.
    pub(crate) fn tick_periodic_refresh(&mut self, cur_tm: i64) {
        let hour_slot = if self.client.cfg.refresh.check_tags_every.is_some() {
            current_utc_hour_slot()
        } else {
            self.client.check_tags_hour_slot
        };
        self.tick_periodic_refresh_at(cur_tm, hour_slot);
    }

    pub(crate) fn tick_periodic_refresh_at(&mut self, cur_tm: i64, hour_slot: i64) {
        if self.client.domain_ready
            && self.client.domain_restore_needs_indexes()
            && self.client.peer_app_token != 0
            && !self.client.market_indexes_current_for_peer()
            && !self.client.indexes_fetch_in_flight
        {
            self.client.send_markets_indexes_restore_request(cur_tm);
        }

        if let Some(interval) = self.client.cfg.refresh.update_markets_every {
            let interval_ms = interval.as_millis() as i64;
            if (cur_tm - self.client.last_update_markets_ms) >= interval_ms {
                self.client
                    .send_api_request(&crate::commands::engine_request::update_markets_list());
                self.client.last_update_markets_ms = cur_tm;
            }
        }
        if let Some(interval) = self.client.cfg.refresh.check_tags_every {
            if self.client.check_tags_hour_slot == i64::MIN {
                self.client.check_tags_hour_slot = hour_slot;
            } else if hour_slot != self.client.check_tags_hour_slot {
                self.client.check_tags_hour_slot = hour_slot;
                self.client.check_tags_burst_sent = 0;
                self.client.last_check_tags_burst_ms = i64::MIN / 2;
            }

            let interval_ms = interval.as_millis() as i64;
            let burst_due = self.client.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT
                && (cur_tm - self.client.last_check_tags_burst_ms) >= CHECK_TAGS_BURST_SPACING_MS;
            let interval_due = (cur_tm - self.client.last_check_tags_ms) >= interval_ms;

            if burst_due || interval_due {
                self.client
                    .send_api_request(&crate::commands::engine_request::check_binance_tags());
                self.client.last_check_tags_ms = cur_tm;
                if self.client.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT {
                    self.client.check_tags_burst_sent += 1;
                    self.client.last_check_tags_burst_ms = cur_tm;
                }
            }
        }
    }

    /// Periodic timeout cleanup/retry for an in-flight markets-index restore marker.
    /// UDP-ответ может потеряться — без этой проверки `indexes_fetch_in_flight = true`
    /// остался бы навсегда. До Init запрос НЕ отправляется; после Init reconnect
    /// restore имеет право повторить `GetMarketsIndexes`, потому что пользовательский
    /// intent уже был задан единственным init-проходом.
    pub(crate) fn check_indexes_fetch_timeout(&mut self, now_ms: i64) {
        const INDEXES_FETCH_TIMEOUT_MS: i64 = 12_000;
        if self.client.indexes_fetch_in_flight
            && now_ms - self.client.indexes_fetch_started_ms > INDEXES_FETCH_TIMEOUT_MS
        {
            self.client.indexes_fetch_in_flight = false;
            if self.client.domain_ready
                && self.client.domain_restore_needs_indexes()
                && self.client.peer_app_token != 0
                && !self.client.market_indexes_current_for_peer()
            {
                self.client.send_markets_indexes_restore_request(now_ms);
            }
        }
    }

    /// Periodic all-trades reconnect sequence (только в Dispatcher mode).
    /// Trades gap recovery is not here: Delphi calls `CheckMissingTradesPackets`
    /// from the tail of `ProcessTradesStream`, and Rust mirrors that in
    /// `EventDispatcher::dispatch_into_active_actions`.
    pub(crate) fn periodic_trades_reconnect_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        if cur_tm - self.client.last_trades_tick_ms < 100 {
            return;
        }
        self.client.last_trades_tick_ms = cur_tm;
        let trades_server_token = match mode {
            #[cfg(test)]
            RunMode::Dispatcher { dispatcher, .. } => dispatcher.trades_server_token(),
            RunMode::DispatcherWorker { .. } => self
                .client
                .dispatcher_trades_server_token
                .load(Ordering::Relaxed),
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => return,
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => return,
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => return,
        };
        self.client
            .tick_trades_reconnect_sequence(cur_tm, trades_server_token);
    }

    pub(crate) fn send_worker_item(mode: &RunMode<'_>, item: DispatcherWorkItem) {
        if let RunMode::DispatcherWorker { tx, .. } = mode {
            let _ = tx.send(item);
        }
    }

    pub(crate) fn periodic_orderbook_reconnect_tick(
        &mut self,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        match mode {
            #[cfg(test)]
            RunMode::Dispatcher { dispatcher, .. } => {
                if self.client.tick_orderbook_reconnect_sequence(cur_tm) {
                    dispatcher.reset_orderbook_caches_keep_books();
                }
            }
            RunMode::DispatcherWorker { .. } => {
                if self.client.tick_orderbook_reconnect_sequence(cur_tm) {
                    Self::send_worker_item(mode, DispatcherWorkItem::ResetOrderbookCachesKeepBooks);
                }
            }
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => {}
        }
    }

    pub(crate) fn periodic_orders_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        match mode {
            #[cfg(test)]
            RunMode::Dispatcher {
                dispatcher,
                on_event,
                event_buf,
                active_actions_buf,
                ..
            } => {
                event_buf.clear();
                active_actions_buf.clear();
                dispatcher.tick_orders_active_actions(cur_tm, event_buf, active_actions_buf);
                self.client
                    .apply_active_actions(active_actions_buf.drain(..));
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
                let _ = tx.send(DispatcherWorkItem::TickOrders { now_ms: cur_tm });
            }
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => {}
        }
    }

    pub(crate) fn transport_reconnect_tail_tick(&mut self, cur_tm: i64) {
        // Reconnect logic
        self.check_hello_send(cur_tm);
        self.check_offline_reconnect(cur_tm);
        self.check_reconnect_timeout(cur_tm);
        self.check_dead_zone(cur_tm);

        if self.client.force_disconnect {
            self.do_force_disconnect();
        }
    }
}
