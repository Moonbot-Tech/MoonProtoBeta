use super::*;

impl Client {
    pub(crate) fn parse_sliced_ack_payload(payload: &[u8]) -> Option<SlicedAck> {
        // Delphi OnNewSlicedACK reads Flags(32 bytes) + DatagramNum(2 bytes)
        // from the command payload after the transport header.
        let (flags, datagram_num) = slicing::parse_ack_bytes(payload)?;
        Some(SlicedAck {
            flags,
            datagram_num,
        })
    }

    pub(crate) fn push_sliced_ack_payload(send_lock: &Arc<Mutex<SendLockState>>, payload: &[u8]) {
        if let Some(ack) = Self::parse_sliced_ack_payload(payload) {
            send_lock.lock().unwrap().push_sliced_ack(ack);
        }
    }

    pub(crate) fn decode_handshake_hello(
        master_key: &MoonKey,
        client_id: u64,
        cmd: u8,
        payload: &[u8],
    ) -> Option<handshake::Hello> {
        // AAD = {client_id, cmd}: the inbound handshake command (WhoAreYou or
        // Fine) is bound into the GCM tag, so a relabelled header fails decode.
        let aad = handshake::handshake_aad(client_id, cmd);
        let decrypted = crypto::decrypt(master_key, payload, &aad)?;
        handshake::Hello::from_bytes(&decrypted)
    }

    pub(crate) fn build_size_ack_payload(
        data_read_state: &mut DataReadState,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        let size_test = control::SizeTestData::read(payload)?;
        let size = size_test.size;
        if (size as usize) < 6 {
            return None;
        }
        let series = data_read_state.update_data_size_ack_series_num(size_test.series_num);
        Some(control::SizeTestData::ack_bytes(size, series))
    }

    pub(crate) fn build_probe_mtu_ack_payload(payload: &[u8]) -> Option<Vec<u8>> {
        let probe = control::ProbeMtu::read(payload)?;
        if (probe.test_size as usize) < control::PROBE_MTU_ACK_SIZE {
            return None;
        }
        Some(probe.ack_bytes())
    }

    pub(crate) fn apply_ping_and_build_response(
        &mut self,
        payload: &[u8],
        raw_now_dt: f64,
        corrected_now_dt: f64,
        total_sent_before_ping: u64,
        total_recv_after_packet: u64,
    ) -> Option<Vec<u8>> {
        let ping = control::PingFrame::read(payload)?;

        // UDPRead Ping branch: update transport ping fields before DataRead.
        let rs = ping.rs();
        const COMFORTABLE_RS: f64 = 0.92;
        const CRITICAL_RS: f64 = 0.85;
        const MIN_RATE: i32 = 256 * 1024;
        const MAX_RATE: i32 = 8 * 1024 * 1024;
        self.round_trip_delay = ping.trip_delay as i64;
        self.actual_pmtu = ping.pmtu;
        self.overheat = ping.overheat;
        self.rs = rs;
        // A server can start sending Ping after it created its side of the
        // client, even if the final MPC_Fine was lost on the way back. Ping
        // proves the peer is alive, but it does not complete authorization.
        // Keep the connect loop alive until AuthDone, otherwise a single lost
        // Fine can leave the client permanently Connected-but-not-authorized.
        if self.auth_status == AuthStatus::AuthDone {
            self.need_connect = false;
        }
        if self.used_sliced_limit {
            let new_rate = if rs > COMFORTABLE_RS {
                let increase = (self.can_send_rate as f64 * 0.03).round() as i32;
                self.can_send_rate + increase.max(32 * 1024)
            } else if rs < CRITICAL_RS {
                (self.can_send_rate as f64 * 0.85).round() as i32
            } else {
                let drift = (rs - COMFORTABLE_RS) / COMFORTABLE_RS;
                (self.can_send_rate as f64 * (1.0 + drift * 0.05)).round() as i32
            };
            self.can_send_rate = new_rate.clamp(MIN_RATE, MAX_RATE);
            self.used_sliced_limit = false;
        }

        // DataReadInt(MPC_Ping): write server ACK bitmap into TmpSlider.
        self.send_lock
            .lock()
            .unwrap()
            .apply_ping_ack_bitmap(payload);

        // ClientNewData(MPC_Ping): update wall-clock deltas before SendPing.
        self.ping_count = self.ping_count.wrapping_add(1);
        self.global_timing_orders = ping.global_timing_orders;
        let initial_time = ping.initial_time;
        let server_time = ping.time;
        let server_time_delta = initial_time - raw_now_dt;
        self.server_time_delta = server_time_delta;
        self.server_time_delta_handle.store(
            server_time_delta.to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
        set_server_time_delta_global(server_time_delta);
        self.net_lag_ping = ((corrected_now_dt - server_time) * 86400000.0).abs() as i64;

        // SendPing(var APing): mutate the same Ping struct, then append our ACK half.
        let (ack_start, ack_words) = self.data_read_state.build_ack_half();
        let mut response = ping.response_bytes(
            corrected_now_dt,
            total_sent_before_ping,
            total_recv_after_packet,
            ack_start,
        );
        for word in &ack_words {
            response.extend_from_slice(&word.to_le_bytes());
        }

        Some(response)
    }

    #[cfg(test)]
    pub(crate) fn on_new_sliced_ack(&self, payload: &[u8]) {
        Self::push_sliced_ack_payload(&self.send_lock, payload);
    }

    pub(crate) fn apply_sliced_ack(&mut self, ack: SlicedAck, _now_ms: i64) {
        // Matches TMoonProtoClient.ApplyACK (MoonProtoIntStruct.pas:1200-1218):
        // find first matching Sending datagram, apply, maybe remove, then stop.
        let mut completed_ratio = None;
        let mut completed_idx = None;
        if let Some(idx) = self
            .sending
            .iter()
            .position(|s| s.datagram_num == ack.datagram_num)
        {
            let s = &mut self.sending[idx];
            // Merge ACK flags (set union, like Delphi Flags := Flags + ACK.Flags).
            // If no new flag appears, Delphi `ApplyACK` exits before touching
            // the piece list; keep the same no-op machine effect.
            let mut changed = false;
            for (dst, src) in s.ack_flags.iter_mut().zip(ack.flags) {
                let before = *dst;
                *dst |= src;
                changed |= before != *dst;
            }
            if changed {
                // Delphi server/client fix: ACK progress proves the peer is
                // alive, so the datagram retry budget starts over.
                s.retry_count = 0;
                let complete = (0..s.blocks_count).all(|block| s.is_block_acked(block));
                if complete {
                    if s.blocks_count > 0 {
                        completed_ratio =
                            Some((s.sent_count as f64 / s.blocks_count as f64 - 1.0) * 100.0);
                    }
                    if trace_io_enabled() {
                        eprintln!(
                            "[mp-sliced-ack] t={} d={} acked={}/{} complete=true sent_count={}",
                            trace_elapsed_ms(),
                            s.datagram_num,
                            s.blocks_count,
                            s.blocks_count,
                            s.sent_count
                        );
                    }
                    completed_idx = Some(idx);
                } else {
                    // Current Delphi keeps the retry clocks of remaining holes:
                    // ACK-progress only removes ACKed pieces and resets FRetryCount.
                    // Rust keeps arrays indexed by block number, so recompute the
                    // datagram clock from unACKed blocks instead of zeroing them.
                    s.refresh_last_checked_from_unacked(_now_ms);
                    if trace_io_enabled() {
                        let acked = (0..s.blocks_count)
                            .filter(|&block| s.is_block_acked(block))
                            .count();
                        eprintln!(
                            "[mp-sliced-ack] t={} d={} acked={}/{} complete=false last_checked={}",
                            trace_elapsed_ms(),
                            s.datagram_num,
                            acked,
                            s.blocks_count,
                            s.last_checked
                        );
                    }
                }
            }
        } else if trace_io_enabled() {
            eprintln!(
                "[mp-sliced-ack-miss] t={} d={} no_matching_sending=true",
                trace_elapsed_ms(),
                ack.datagram_num
            );
        }

        if let Some(idx) = completed_idx {
            self.sending.remove(idx);
        }

        if let Some(ratio) = completed_ratio {
            self.avg_over_heat = if self.avg_over_heat == 0.0 {
                ratio
            } else {
                (self.avg_over_heat * 9.0 + ratio) * 0.1
            };
        }
    }

    /// S1 (эталон `MoonProtoCommon.pas` DataReadInt): a non-crypted command in
    /// `MoonProtoSensitiveCmds` must be dropped on the client, except `MPC_API`
    /// (a server API response is the only legitimate plaintext sensitive command;
    /// its method is further checked by [`Self::drop_plaintext_api`]). On the
    /// server every sensitive command including API must be crypted, but the Rust
    /// port is a client, so the API exception always applies here.
    fn drop_plaintext_sensitive(real_cmd: u8) -> bool {
        matches!(
            Command::from_byte(real_cmd),
            Command::Order | Command::Strat | Command::UI | Command::Balance
        )
    }

    /// S1 part 2 (эталон `MoonProtoClient.pas` ClientNewData `MPC_API` branch):
    /// the only legitimate plaintext `MPC_API` is an engine *response* whose
    /// method is in `UnencryptedMethods` — the reference server sends
    /// `GetMarketsList` / `UpdateMarketsList` / `RequestCandlesData` unencrypted
    /// (public market lists, and candle data that is already zlib-compressed) and
    /// crypts everything else. Anything else over plaintext is dropped:
    /// - a response with a sensitive method → forged balance/order-status spoof;
    /// - a request or an unparseable payload → never sent plaintext by the server.
    ///
    /// This nets out exactly like the эталон, where the only plaintext API that
    /// ever reaches client state is an `UnencryptedMethods` response: the
    /// `ClientNewData` gate drops sensitive-method responses, and
    /// `ProcessApiCommand` is a no-op for a non-response
    /// (`if cmd.ClassType = TEngineResponse`), so a plaintext request changes
    /// nothing. Part 1 deliberately lets `MPC_API` past the sensitive gate, so
    /// this method check is what blocks injection over the authenticity-only MAC.
    fn drop_plaintext_api(payload: &[u8]) -> bool {
        match Self::engine_response_meta_from_payload(payload) {
            Some(meta) => !matches!(
                meta.method,
                EngineMethod::GetMarketsList
                    | EngineMethod::UpdateMarketsList
                    | EngineMethod::RequestCandlesData
            ),
            None => true,
        }
    }

    pub(crate) fn decode_data_read_int_payload_shared(
        data_read_state: &mut DataReadState,
        raw_cmd: u8,
        data: &[u8],
    ) -> Option<(u8, Vec<u8>)> {
        // Keep the borrowed/decompressed split explicit until the final owner
        // handoff. The current dispatcher path still returns `Vec<u8>` because
        // worker delivery owns payload bytes; removing that final copy belongs
        // to the planned hot-path delivery optimization.
        use std::borrow::Cow;
        let mut cmd = raw_cmd;
        let mut was_crypted = false;
        let mut payload: Cow<'_, [u8]> = Cow::Borrowed(data);

        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            // B-V2-03: use the cached cipher instead of the key. Before handshake
            // (cipher = None) there should be no Crypted packets at all — but we
            // guard with an early return.
            let DataReadState {
                decode_cipher,
                slider,
                ..
            } = data_read_state;
            let decode_cipher = decode_cipher.as_ref()?;
            let decrypted = crypted::decrypt_command(decode_cipher, &payload, slider);
            if let Some((inner_cmd, decrypted, _want_ack)) = decrypted {
                cmd = inner_cmd;
                payload = Cow::Owned(decrypted);
                was_crypted = true;
            } else {
                return None;
            }
        }

        // S1: drop plaintext sensitive commands early (before decompression),
        // matching the эталон DataReadInt security gate.
        if !was_crypted && Self::drop_plaintext_sensitive(cmd & 0x7F) {
            return None;
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = Cow::Owned(decompressed);
            }
        }

        // S1 part 2: drop a plaintext API response whose method is not in
        // UnencryptedMethods. Runs after decompression, like the эталон
        // ClientNewData MPC_API guard that parses the already-decompressed stream.
        if !was_crypted
            && cmd & 0x7F == Command::API.to_byte()
            && Self::drop_plaintext_api(&payload)
        {
            return None;
        }

        // MPC_Ping is handled in the reader Ping path. Its server ACK bitmap follows the
        // Delphi TmpSlider -> RecvdSlider -> ApplyRegularHLAck path, not this
        // generic delivery branch.
        Some((cmd, payload.into_owned()))
    }

    pub(crate) fn decode_data_read_int_payload_owned(
        data_read_state: &mut DataReadState,
        raw_cmd: u8,
        data: Vec<u8>,
    ) -> Option<(u8, Vec<u8>)> {
        let mut cmd = raw_cmd;
        let mut was_crypted = false;
        let mut payload = data;

        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            let DataReadState {
                decode_cipher,
                slider,
                ..
            } = data_read_state;
            let decode_cipher = decode_cipher.as_ref()?;
            let decrypted = crypted::decrypt_command(decode_cipher, &payload, slider);
            if let Some((inner_cmd, decrypted, _want_ack)) = decrypted {
                cmd = inner_cmd;
                payload = decrypted;
                was_crypted = true;
            } else {
                return None;
            }
        }

        // S1: drop plaintext sensitive commands early, like the эталон DataReadInt gate.
        if !was_crypted && Self::drop_plaintext_sensitive(cmd & 0x7F) {
            return None;
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = decompressed;
            }
        }

        // S1 part 2: drop a plaintext API response whose method is not in
        // UnencryptedMethods (after decompression, like the эталон MPC_API guard).
        if !was_crypted
            && cmd & 0x7F == Command::API.to_byte()
            && Self::drop_plaintext_api(&payload)
        {
            return None;
        }

        Some((cmd, payload))
    }
}
