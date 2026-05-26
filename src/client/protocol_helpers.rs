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
        payload: &[u8],
    ) -> Option<handshake::Hello> {
        let aad = client_id.to_le_bytes();
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
                            "[mp-sliced-ack] d={} acked={}/{} complete=true sent_count={}",
                            s.datagram_num, s.blocks_count, s.blocks_count, s.sent_count
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
                            "[mp-sliced-ack] d={} acked={}/{} complete=false last_checked={}",
                            s.datagram_num, acked, s.blocks_count, s.last_checked
                        );
                    }
                }
            }
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

    pub(crate) fn decode_data_read_int_payload_shared(
        data_read_state: &mut DataReadState,
        raw_cmd: u8,
        data: &[u8],
    ) -> Option<(u8, Vec<u8>)> {
        // B-V2-01 fix: используем Cow вместо безусловного data.to_vec(). Большинство
        // пакетов не Crypted и не Compressed (Ping, handshake, Sliced-блоки) — для них
        // payload остаётся borrowed (zero alloc). Crypted и Compressed создают Owned
        // только когда реально нужны. На пике TradesStream это устраняет 50K alloc'ов/сек.
        use std::borrow::Cow;
        let mut cmd = raw_cmd;
        let mut payload: Cow<'_, [u8]> = Cow::Borrowed(data);

        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            // B-V2-03: используем кэшированный cipher вместо ключа. До handshake
            // (cipher = None) Crypted-пакетов и быть не должно — но защищаемся return.
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
            } else {
                return None;
            }
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = Cow::Owned(decompressed);
            }
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
            } else {
                return None;
            }
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = decompressed;
            }
        }

        Some((cmd, payload))
    }

    pub(crate) fn engine_response_request_uid_from_payload(payload: &[u8]) -> Option<u64> {
        // Engine response payload includes 11-byte TBaseCommand header, then
        // RequestUID. This is enough to cheaply check ApiPending without
        // inflating a full response in the receive phase.
        let uid = payload.get(11..19)?;
        Some(u64::from_le_bytes(uid.try_into().unwrap()))
    }

    pub(crate) fn engine_response_meta_from_payload(payload: &[u8]) -> Option<EngineResponseMeta> {
        if payload.len() < 11 {
            return None;
        }
        let mut pos = 11usize;
        let request_uid = u64::from_le_bytes(payload.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let method = EngineMethod::from_byte(*payload.get(pos)?);
        pos += 1;
        let success = *payload.get(pos)? != 0;
        pos += 1;
        // ErrorCode.
        payload.get(pos..pos + 4)?;
        pos += 4;
        // ErrorMsg string, length-prefixed UTF-8. Skip only; no allocation.
        let len = u16::from_le_bytes(payload.get(pos..pos + 2)?.try_into().ok()?) as usize;
        pos += 2;
        payload.get(pos..pos + len)?;
        Some(EngineResponseMeta {
            request_uid,
            method,
            success,
        })
    }

    pub(crate) fn engine_response_method_from_payload(payload: &[u8]) -> Option<EngineMethod> {
        payload.get(19).copied().map(EngineMethod::from_byte)
    }

    pub(crate) fn apply_engine_response_client_bookkeeping(&mut self, resp: &EngineResponse) {
        // Active library: auto-clear indexes_fetch_in_flight на ответе
        // GetMarketsIndexes (любой — даже неуспешный, чтобы не зависнуть навсегда).
        if resp.method == EngineMethod::GetMarketsIndexes {
            self.indexes_fetch_in_flight = false;
            let indexes_payload_ok = resp.success
                && crate::commands::market::parse_markets_indexes_response(&resp.data).is_some();
            if indexes_payload_ok {
                // Запоминаем что для текущего PeerAppToken индексы получены.
                self.tracked_indexes_peer_app_token = self.peer_app_token;
                if self.update_markets_after_indexes {
                    self.update_markets_after_indexes = false;
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                if self.restore_orderbooks_after_indexes {
                    self.restore_orderbooks_after_indexes = false;
                    self.restore_orderbook_subscriptions_from_registry();
                }
            }
        }

        // Delphi `DoSubscribeOrderBooks`: только успешный ответ подтверждает
        // текущий `ServerToken`. Для reconnect batch это полный `BookSubbed`
        // replay; обычная точечная подписка может выставить token только в
        // initial state, как Delphi `FSubscribedBookServerToken = 0`.
        if resp.method == EngineMethod::SubscribeOrderBook {
            let is_reconnect_batch =
                self.pending_orderbook_resubscribe_uid == Some(resp.request_uid);
            if resp.success && (self.subscribed_book_server_token == 0 || is_reconnect_batch) {
                self.subscribed_book_server_token = self.server_token;
            }
            self.close_orderbook_subscribe_wait_if_matches(resp.request_uid);
            if is_reconnect_batch {
                self.pending_orderbook_resubscribe_uid = None;
            }
        }

        // Delphi `TMoonProtoEngine.SubscribeAllTrades`: successful
        // `emk_SubscribeAllTrades` refreshes `LastReconnectCheck`.
        // Until the first TradesStream packet updates `FTradesServerToken`,
        // this 5s gate prevents immediate unsubscribe/resubscribe churn.
        if resp.method == EngineMethod::SubscribeAllTrades && resp.success {
            let now_ms = self.now_ms();
            self.last_trades_reconnect_check_ms = now_ms;
        }
        if resp.method == EngineMethod::SubscribeAllTrades {
            self.last_trades_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
        }
        if resp.method == EngineMethod::UnsubscribeAllTrades {
            self.close_trades_unsubscribe_wait_if_matches(resp.request_uid);
        }
    }

    pub(crate) fn apply_engine_response_meta_bookkeeping(&mut self, meta: EngineResponseMeta) {
        if meta.method == EngineMethod::SubscribeOrderBook {
            let is_reconnect_batch =
                self.pending_orderbook_resubscribe_uid == Some(meta.request_uid);
            if meta.success && (self.subscribed_book_server_token == 0 || is_reconnect_batch) {
                self.subscribed_book_server_token = self.server_token;
            }
            self.close_orderbook_subscribe_wait_if_matches(meta.request_uid);
            if is_reconnect_batch {
                self.pending_orderbook_resubscribe_uid = None;
            }
        }

        if meta.method == EngineMethod::SubscribeAllTrades && meta.success {
            let now_ms = self.now_ms();
            self.last_trades_reconnect_check_ms = now_ms;
        }
        if meta.method == EngineMethod::SubscribeAllTrades {
            self.last_trades_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
        }
        if meta.method == EngineMethod::UnsubscribeAllTrades {
            self.close_trades_unsubscribe_wait_if_matches(meta.request_uid);
        }
    }

    pub(crate) fn process_api_bookkeeping_light(&mut self, payload: &[u8]) {
        let Some(meta) = Self::engine_response_meta_from_payload(payload) else {
            return;
        };
        if meta.method == EngineMethod::GetMarketsIndexes {
            if let Some(resp) = parse_engine_response(payload) {
                self.apply_engine_response_client_bookkeeping(&resp);
            }
        } else {
            self.apply_engine_response_meta_bookkeeping(meta);
        }
    }

    pub(crate) fn dispatch_api_pending_inline(
        api_pending: &ApiPending,
        cmd: u8,
        payload: &[u8],
    ) -> bool {
        if cmd != Command::API.to_byte() {
            return false;
        }
        let Some(uid) = Self::engine_response_request_uid_from_payload(payload) else {
            return false;
        };
        if !api_pending.contains(uid) {
            return false;
        }
        let Some(resp) = parse_engine_response(payload) else {
            return false;
        };
        api_pending.dispatch(resp).is_none()
    }

    pub(crate) fn dispatch_candles_chunk_inline(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        cmd: u8,
        payload: &[u8],
        now_ms: i64,
    ) -> bool {
        if cmd != Command::API.to_byte() {
            return false;
        }
        if Self::engine_response_method_from_payload(payload)
            != Some(EngineMethod::RequestCandlesData)
        {
            return false;
        }
        let Some(uid) = Self::engine_response_request_uid_from_payload(payload) else {
            return false;
        };
        if !pending_candles.contains_key(&uid) {
            return false;
        }
        let Some(resp) = parse_engine_response(payload) else {
            return false;
        };
        Self::handle_candles_chunk_in_map(pending_candles, &resp, now_ms)
    }

    pub(crate) fn client_new_data_decoded(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        sink: &mut DispatchSink<'_>,
    ) {
        if cmd == Command::API.to_byte() {
            match self.process_api_command_decoded(
                payload,
                api_pending_consumed_by_reader,
                candles_chunk_consumed_by_reader,
                sink,
            ) {
                Ok(()) => {
                    return;
                }
                Err(payload) => {
                    sink.deliver_owned(Command::from_byte(cmd), payload);
                    return;
                }
            }
        }

        sink.deliver_owned(Command::from_byte(cmd), payload);
    }

    pub(crate) fn process_api_command_decoded(
        &mut self,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        sink: &mut DispatchSink<'_>,
    ) -> Result<(), Vec<u8>> {
        // Engine API responses: попытаться доставить в pending registry / chunked
        // candles aggregator / internal recovery flags. Если UID не зарегистрирован —
        // пробрасываем как обычный data callback.
        if candles_chunk_consumed_by_reader {
            return Ok(());
        }
        if let Some(resp) = parse_engine_response(&payload) {
            // 1. Chunked candles (RequestCandlesData) — aggregator поддерживает
            // несколько response пакетов с одинаковым UID. До завершения сборки
            // не дропаем slot.
            let now_ms = self.now_ms();
            if resp.method == EngineMethod::RequestCandlesData
                && Self::handle_candles_chunk_in_map(&mut self.pending_candles, &resp, now_ms)
            {
                // Чанк потреблён aggregator'ом. Передаём в on_data только
                // если потребитель НЕ использует async API (тогда тут merged
                // ещё не готов — пусть приложение видит сырые chunks).
                // Однако: чтобы не путать — пропускаем on_data callback.
                // Async-потребитель получит результат через Receiver<MergedCandles>.
                return Ok(());
            }
            // Если slot не зарегистрирован — fallback на pending registry /
            // on_data для fire-and-forget API users.

            self.apply_engine_response_client_bookkeeping(&resp);

            // 2. Pending registry (обычный async API).
            let pending_consumed =
                api_pending_consumed_by_reader || self.api_pending.dispatch(resp).is_none();
            if !pending_consumed || sink.is_buffer() {
                // Если response не ждал конкретный receiver — это обычный API event.
                // Если ждал, но мы в Dispatcher mode, всё равно отдаём raw payload
                // dispatcher'у: active state (markets/indexes/tags) должен обновиться
                // независимо от того, ждёт ли user code этот же ответ через Receiver.
                // Callback mode сохраняет семантику: pending response не
                // дублируется в on_data callback.
                sink.deliver_owned(Command::API, payload);
            }
            return Ok(());
        }
        // Не распарсилось — fallback на raw sink.
        Err(payload)
    }

    /// Поглотить candles chunk через pending aggregator. Возвращает `true` если slot
    /// найден и chunk обработан (даже если merged ещё не готов — копить дальше);
    /// `false` если UID не зарегистрирован (потребитель не использует async API).
    ///
    /// Когда aggregator вернул merged — sender'у отправляется готовый `MergedCandles`,
    /// slot удаляется. Если sender уже дропнут (receiver не ждёт) — slot всё равно
    /// удаляется (semantic = "fire-and-forget с финализацией").
    pub(crate) fn handle_candles_chunk_in_map(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        resp: &EngineResponse,
        _now_ms: i64,
    ) -> bool {
        // Проверяем slot отдельным lookup — потом полное удаление через remove() если merged.
        if !resp.success {
            if let Some(partial) = pending_candles.remove(&resp.request_uid) {
                log::warn!(target: "moonproto::client",
                    "candles request uid={} failed code={} msg={}",
                    resp.request_uid, resp.error_code, resp.error_msg);
                drop(partial);
                return true;
            }
            return false;
        }

        let uid = resp.request_uid;
        let chunk_result = {
            let Some(partial) = pending_candles.get_mut(&uid) else {
                return false;
            };
            let chunk_result = partial.aggregator.on_chunk_result(&resp.data);
            if matches!(
                chunk_result,
                CandlesChunkResult::Stored | CandlesChunkResult::Complete(_)
            ) {
                // Delphi updates `Markets.LastChunkTime` for the UI waiting
                // thread, but does not cancel the protocol-side collector on
                // that timeout. Rust keeps the pending slot until explicit
                // complete/error/reset/caller timeout.
            }
            chunk_result
        };
        if let CandlesChunkResult::Complete(zipped_data) = chunk_result {
            let markets = parse_request_candles_data_response(&zipped_data).unwrap_or_else(|| {
                log::warn!(target: "moonproto::client",
                    "candles aggregator merged but strict parse failed for uid={} ({} bytes); trying Delphi partial apply",
                    uid,
                    zipped_data.len()
                );
                parse_request_candles_data_response_partial_like_delphi(&zipped_data)
                    .unwrap_or_default()
            });
            if let Some(partial) = pending_candles.remove(&uid) {
                let _ = partial.sender.send(MergedCandles {
                    uid,
                    zipped_data,
                    markets,
                });
                // Sender дропается → receiver получает Ok(...) / уже получил.
            }
        }
        true
    }

    /// Auto-compress payload если `cmd` ещё не помечен `COMPRESSED_FLAG`, размер > 64 байт
    /// и `mp_compress` дал savings ≥ 5% (`mp_compress` сам возвращает None если меньше).
    /// Соответствует Delphi `TMoonProtoDataToSend.Create` (MoonProtoIntStruct.pas:661-672).
    ///
    /// Аудит #3 (audit_delphi_deviation): возвращает `Cow<'_, [u8]>` вместо `Vec<u8>`.
    /// Раньше делали безусловный `data.to_vec()` даже когда компрессия не применялась —
    /// 1 alloc на каждый отправляемый H/L/Sliced пакет. В Delphi `TMemoryStream` передаётся
    /// по ссылке, ноль копий. Теперь `Cow::Borrowed` когда без сжатия → zero alloc.
    pub(crate) fn maybe_compress<'a>(cmd: u8, data: &'a [u8]) -> (u8, std::borrow::Cow<'a, [u8]>) {
        if cmd & COMPRESSED_FLAG == 0 && data.len() > MIN_SIZE_TO_COMPRESS {
            if let Some(compressed) = compression::mp_compress(data) {
                return (cmd | COMPRESSED_FLAG, std::borrow::Cow::Owned(compressed));
            }
        }
        (cmd, std::borrow::Cow::Borrowed(data))
    }

    pub(crate) fn crypted_wire_cmd(inner_cmd: u8) -> u8 {
        if inner_cmd & COMPRESSED_FLAG != 0 {
            Command::Crypted.to_byte() | COMPRESSED_FLAG
        } else {
            Command::Crypted.to_byte()
        }
    }

    pub(crate) fn send_raw_packet_cmd(&mut self, cmd: u8, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else {
            return;
        };
        // Zero-alloc fast path: reuse self.send_buf + cached MacContext.
        let extra = moonproto_transport::transport_pack_into_with_mac(
            &mut self.send_buf,
            &self.mac_ctx,
            &self.cfg.mac_key,
            cmd,
            self.cfg.client_id,
            payload,
            self.cfg.mask_ver,
        );
        // Извлекаем packet чтобы borrow checker не ругался на двойной &mut self
        // (dispatch_send берёт &mut self, ему не нужен send_buf после copy в socket).
        // Из send_buf берём slice — оно живёт в self, socket.send_to не сохранит ссылку.
        // SAFETY pattern: take/restore чтобы &mut self в dispatch_send не пересекался с
        // &self.send_buf — но проще: pass slice через owned vec swap.
        let packet = std::mem::take(&mut self.send_buf);
        self.dispatch_send(cmd, &packet, extra.as_deref(), addr);
        // Возвращаем буфер обратно (capacity сохранился, content сейчас не нужен).
        self.send_buf = packet;
        self.send_buf.clear();
    }

    pub(crate) fn send_raw_packet(&mut self, cmd: Command, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else {
            return;
        };
        let extra = moonproto_transport::transport_pack_into_with_mac(
            &mut self.send_buf,
            &self.mac_ctx,
            &self.cfg.mac_key,
            cmd.to_byte(),
            self.cfg.client_id,
            payload,
            self.cfg.mask_ver,
        );
        let packet = std::mem::take(&mut self.send_buf);
        self.dispatch_send(cmd.to_byte(), &packet, extra.as_deref(), addr);
        self.send_buf = packet;
        self.send_buf.clear();
    }

    /// Реально отправляет пакет (плюс optional extra-пакет от moonext) с обработкой ошибок.
    /// Закрывает D-06: send errors больше не игнорируются через `.ok()`.
    /// EWOULDBLOCK логируется как warn (нормальная буферизация ядра). Прочие ошибки логируются,
    /// но не меняют reconnect-state: Delphi `DoSendPacket` возвращает false и не ставит
    /// `ForceDisconnect`.
    pub(crate) fn dispatch_send(
        &mut self,
        cmd: u8,
        packet: &[u8],
        extra: Option<&[u8]>,
        addr: SocketAddr,
    ) {
        if self.debug_outgoing_blackhole.load(Ordering::Relaxed) {
            self.err_emu_diagnostics
                .lock()
                .unwrap()
                .record_outgoing(cmd, true);
            if trace_io_enabled() {
                eprintln!(
                    "[mp-io-tx-blackhole] cmd={:?} raw={} packet_len={} extra_len={} addr={}",
                    Command::from_byte(cmd),
                    cmd,
                    packet.len(),
                    extra.map(|p| p.len()).unwrap_or(0),
                    addr
                );
            }
            return;
        }

        if trace_io_enabled() {
            eprintln!(
                "[mp-io-tx-attempt] cmd={:?} raw={} packet_len={} extra_len={} addr={}",
                Command::from_byte(cmd),
                cmd,
                packet.len(),
                extra.map(|p| p.len()).unwrap_or(0),
                addr
            );
        }
        // Сначала выполняем сетевые операции, собирая Result'ы в owned-переменные,
        // потом обрабатываем через self.should_log без conflicting borrow.
        let extra_result = match (extra, self.socket.as_ref()) {
            (Some(extra_pkt), Some(sock)) => Some(sock.send_to(extra_pkt, addr)),
            _ => None,
        };
        let main_result = match self.socket.as_ref() {
            Some(sock) => sock.send_to(packet, addr),
            None => return,
        };

        if let Some(Err(e)) = extra_result {
            if self.should_log("send_extra_err", 1000) {
                warn!("send_to(extra, cmd={cmd}) failed: {e}");
            }
        }
        match main_result {
            Ok(_) => {
                self.err_emu_diagnostics
                    .lock()
                    .unwrap()
                    .record_outgoing(cmd, false);
                let total_sent = self
                    .total_sent
                    .fetch_add(packet.len() as u64, Ordering::Relaxed)
                    + packet.len() as u64;
                self.track_sent(packet.len() as u64, self.now_ms());
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-ok] cmd={:?} raw={} packet_len={} total_sent={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        total_sent
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-wouldblock] cmd={:?} raw={} packet_len={} err={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        e
                    );
                }
                if self.should_log("send_wouldblock", 1000) {
                    warn!("send_to(cmd={cmd}) would block (kernel send buffer full)");
                }
            }
            Err(e) if is_datagram_too_large_error(&e) => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-too-large] cmd={:?} raw={} packet_len={} err={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        e
                    );
                }
                if self.should_log("send_too_large", 1000) {
                    warn!("send_to(cmd={cmd}) packet too large for current path MTU: {e}");
                }
            }
            Err(e) => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-error] cmd={:?} raw={} packet_len={} err={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        e
                    );
                }
                if self.should_log("send_err", 1000) {
                    error!("send_to(cmd={cmd}) failed: {e}");
                }
            }
        }
    }

    /// Matches TMoonProtoClient.Reset (IntStruct.pas:972-1000)
    /// Does NOT reset: server_token, actual_pmtu, send_datagram_num, pending_h,
    /// sending, api_pending, pending_candles, trip_delay_k, can_send_rate.
    pub(crate) fn full_reset(&mut self) {
        self.crypt_msg_counter.store(0, Ordering::Relaxed);
        self.total_sent.store(0, Ordering::Relaxed);
        self.total_recv = 0;
        self.total_recv_shared.store(0, Ordering::Relaxed);
        self.rs = 1.0;
        self.used_sliced_limit = false;
        self.data_read_state.reset();
        self.send_lock.lock().unwrap().reset_tmp_slider();
        self.recvd_slider = Slider::new();
        self.recv_slicer = slicing::SlicingReceiver::new();
        self.last_online = 0;
        self.last_sent_hello = NEVER_SENT_MS;
    }

    pub(crate) fn bind_socket(&mut self, cur_tm: i64) {
        self.force_disconnect = false;
        if self.next_port < 1024 || self.next_port > 65000 {
            self.next_port = 1024;
        }
        // Bind family выбирается по серверному адресу. Если сервер — IPv6 literal `[2001:db8::1]:3000`
        // или DNS name резолвящийся в AAAA — bindаемся `[::]:port`. Иначе IPv4 `0.0.0.0:port`.
        let bind_family = if self.cfg.server_ip.contains(':') {
            "[::]"
        } else {
            "0.0.0.0"
        };
        let mut last_err: Option<std::io::Error> = None;
        for _ in 0..200 {
            let addr = format!("{}:{}", bind_family, self.next_port);
            match UdpSocket::bind(&addr) {
                Ok(sock) => {
                    if let Err(e) = sock.set_read_timeout(Some(Duration::from_secs(1))) {
                        warn!("set_read_timeout failed: {e}");
                    }
                    set_socket_buffers(&sock);
                    debug!("bound UDP socket on {}:{}", bind_family, self.next_port);
                    self.next_port += 1;
                    self.socket = Some(sock);
                    // Сброс кэша адреса сервера — может измениться при reconnect через DNS.
                    self.cached_server_addr = None;
                    self.start_inline_reader_session();
                    self.reset_bind_failure_tracking();
                    return;
                }
                Err(e) => {
                    last_err = Some(e);
                    self.next_port += 1;
                    if self.next_port > 65000 {
                        self.next_port = 1024;
                    }
                }
            }
        }
        // Все 200 попыток bind упали → не можем создать сокет В ЭТОТ ТИК.
        // НЕ ставим need_connect=false (audit_responsibility H3): на mobile при port
        // exhaustion (CGNAT, iOS background, ulimit) Disconnected заставил бы app
        // пересоздавать Client. Delphi (`MoonProtoUDPClient.pas:680+`) ретраит forever —
        // active library тоже должна.
        //
        // Throttled error-лог чтобы не спамить (раз в 5 сек). Следующий тик main loop
        // снова войдёт в bind_socket — обычно через короткое время порты освободятся.
        if self.should_log("bind_socket_exhausted", 5000) {
            if let Some(ref e) = last_err {
                error!(target: "moonproto::client",
                    "UdpSocket::bind failed after 200 attempts on {}:*, last error: {} (will retry on next tick)",
                    bind_family, e);
            } else {
                error!(target: "moonproto::client",
                    "UdpSocket::bind failed after 200 attempts on {}:* (will retry on next tick)",
                    bind_family);
            }
        }

        self.record_bind_failure(cur_tm);

        // auth_status оставляем Base — main loop попробует bind ещё раз через DEFAULT_SLEEP_MS.
        // Если app явно вызвал disconnect() — он сам выставит need_connect=false.
    }

    pub(crate) fn reset_bind_failure_tracking(&mut self) {
        self.bind_failure_streak = 0;
        self.first_bind_failure_ms = NEVER_TIME_MS;
        self.last_bind_failed_event_ms = NEVER_TIME_MS;
    }

    pub(crate) fn record_bind_failure(&mut self, cur_tm: i64) {
        if self.first_bind_failure_ms == NEVER_TIME_MS {
            self.first_bind_failure_ms = cur_tm;
        }
        self.bind_failure_streak = self.bind_failure_streak.saturating_add(1);

        let first_due =
            cur_tm.saturating_sub(self.first_bind_failure_ms) >= BIND_FAILED_FIRST_EVENT_MS;
        let repeat_due = self.last_bind_failed_event_ms == NEVER_TIME_MS
            || cur_tm.saturating_sub(self.last_bind_failed_event_ms) >= BIND_FAILED_REPEAT_EVENT_MS;

        if first_due && repeat_due {
            self.last_bind_failed_event_ms = cur_tm;
            self.fire_lifecycle(LifecycleEvent::BindFailed {
                consecutive_failures: self.bind_failure_streak,
            });
        }
    }
}
