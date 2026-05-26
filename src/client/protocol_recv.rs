use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
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
            crate::transport::transport_unpack_with_mac(
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
}
