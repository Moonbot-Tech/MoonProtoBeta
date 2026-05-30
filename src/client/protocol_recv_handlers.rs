use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    pub(crate) fn on_err_emu_drop_inline(raw_cmd: u8, payload: &[u8]) {
        if trace_io_enabled() {
            eprintln!(
                "[mp-io-drop-err-emu] t={} cmd={:?} raw={} payload_len={} payload_hash={:016X} payload_head={}",
                trace_elapsed_ms(),
                Command::from_byte(raw_cmd),
                raw_cmd,
                payload.len(),
                fnv1a64(payload),
                trace_head(payload, 16)
            );
        }
        if slicing::trace_enabled() && Command::from_byte(raw_cmd) == Command::Sliced {
            if let Some(sh) = slicing::SliceHeader::from_bytes(payload) {
                eprintln!(
                    "[slice-rx-drop-err-emu] t={} d={} b={}/{} len={}",
                    trace_elapsed_ms(),
                    sh.datagram_num,
                    sh.block_num,
                    sh.max_block_num,
                    payload.len()
                );
            } else {
                eprintln!(
                    "[slice-rx-drop-err-emu] t={} malformed len={}",
                    trace_elapsed_ms(),
                    payload.len()
                );
            }
        }
    }

    pub(crate) fn on_new_size_test_inline(&mut self, payload: &[u8]) {
        if let Some(ack) = Client::build_size_ack_payload(&mut self.client.data_read_state, payload)
        {
            // Delphi `SendSizeAck`: pad the ack to the tested size and send it
            // with DontFragment. If the OS rejects it as too large, that is the
            // negative PMTU signal; this service packet must not be sliced.
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
            // Same PMTU rule as SizeAck: ProbeMTUAck is intentionally padded to
            // the tested size and sent with DF. EMSGSIZE means "probe failed".
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
        if cmd == Command::WantNewHello && !self.client.should_accept_want_new_hello() {
            let _ = (recv_bytes, timestamp_ms);
            return;
        }
        if matches!(cmd, Command::WrongHello | Command::WantNewHello) {
            self.client.clear_hello_wait_state();
        }
        if cmd == Command::WantNewHello {
            self.client.data_read_state.reset();
            self.client.send_lock.lock().unwrap().reset_tmp_slider();
            self.client.used_sliced_limit = false;
            self.client.crypt_msg_counter.store(0, Ordering::Relaxed);
            self.client.metrics.total_sent.store(0, Ordering::Relaxed);
            self.client.recvd_slider = Slider::new();
            self.client.recv_slicer = slicing::SlicingReceiver::new();
            self.client
                .metrics
                .total_recv_shared
                .store(0, Ordering::Relaxed);
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
    ) -> Duration {
        if !self.client.hello_wait_state.allows_who_are_you() {
            let _ = (payload, recv_bytes, timestamp_ms);
            return Duration::ZERO;
        }
        if let Some(hello) = Client::decode_handshake_hello(
            &self.client.cfg.master_key,
            self.client.cfg.client_id,
            Command::WhoAreYou.to_byte(),
            payload,
        ) {
            let encrypted = self.apply_who_are_you_hello_and_build_imfriend(hello);
            self.client
                .start_hello_wait(HelloWaitState::PrimaryImFriendSent, timestamp_ms);
            self.send_command(Command::ImFriend, &encrypted);
            // Delphi blocks inside the WhoAreYou reader handler here, and so do
            // we, on purpose. Besides duplicate-loss protection (ImFriend is sent,
            // paused 32 ms, then resent), the block is load-bearing for ordering:
            // it stops the client from processing Fine and firing post-Fine Engine
            // API traffic during this window, so that traffic cannot overtake the
            // server-side FClients insertion that happens after MPC_Fine.
            //
            // sverka #14 A2 suggested replacing this with a non-blocking scheduled
            // resend; we deliberately do NOT. The 32 ms block is rare (handshake
            // only; Ping cadence is far longer, so the single-owner thread being
            // deaf for 32 ms is benign), and converting it to async would let other
            // packets process mid-window and break the ordering guarantee above.
            let protocol_wait = Duration::from_millis(DELPHI_IMFRIEND_RESEND_PAUSE_MS);
            thread::sleep(protocol_wait);
            self.send_command(Command::ImFriend, &encrypted);
            let _ = recv_bytes;
            protocol_wait
        } else {
            let _ = (recv_bytes, timestamp_ms);
            Duration::ZERO
        }
    }

    pub(crate) fn on_fine_inline(&mut self, payload: &[u8], recv_bytes: u64, timestamp_ms: i64) {
        if !self.client.hello_wait_state.allows_fine() {
            let _ = (payload, recv_bytes, timestamp_ms);
            return;
        }
        if Client::decode_handshake_hello(
            &self.client.cfg.master_key,
            self.client.cfg.client_id,
            Command::Fine.to_byte(),
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
            self.client.metrics.total_sent.load(Ordering::Relaxed),
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
