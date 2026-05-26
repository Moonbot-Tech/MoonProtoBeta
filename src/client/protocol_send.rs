use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    pub(crate) fn send_command(&mut self, cmd: Command, payload: &[u8]) {
        Self::send_command_on_client(self.client, cmd, payload);
    }

    pub(crate) fn send_command_raw(&mut self, cmd: u8, payload: &[u8]) {
        Self::send_command_raw_on_client(self.client, cmd, payload);
    }

    pub(crate) fn send_command_on_client(client: &mut Client, cmd: Command, payload: &[u8]) {
        client.send_raw_packet(cmd, payload);
    }

    pub(crate) fn send_command_raw_on_client(client: &mut Client, cmd: u8, payload: &[u8]) {
        client.send_raw_packet_cmd(cmd, payload);
    }

    pub(crate) fn send_hello(&mut self) {
        let payload = handshake::build_hello_packet(
            &self.client.cfg.master_key,
            self.client.cfg.client_id,
            &mut self.client.client_token,
            self.client.app_token,
            delphi_now(),
        );
        self.send_command(Command::Hello, &payload);
    }

    pub(crate) fn build_hello_again_packet(&mut self) -> Vec<u8> {
        self.client.client_token += 1;
        let mut hello = handshake::Hello::new(self.client.client_token, self.client.app_token);
        hello.timestamp = delphi_now();
        hello.peer_mix = crypto::mix_values(&hello.rnd, hello.mix_ts, self.client.server_token);
        let packed = hello.to_bytes_packed();
        let aad = self.client.cfg.client_id.to_le_bytes();
        if let Some(cipher) = self.client.encode_cipher.as_ref() {
            crypto::encrypt_with_cipher(cipher, &packed, &aad)
        } else {
            // Delphi initializes TMoonProtoClient.MPKeys[true/false] with MasterKey.
            // Early HelloAgain packets before WhoAreYou are real packets encrypted
            // with MasterKey, not skipped.
            crypto::encrypt(&self.client.cfg.master_key, &packed, &aad)
        }
    }

    pub(crate) fn send_hello_again(&mut self) {
        let encrypted = self.build_hello_again_packet();
        self.send_command(Command::HelloAgain, &encrypted);
    }

    pub(crate) fn check_hello_send(&mut self, cur_tm: i64) {
        if !self.client.need_connect || self.client.force_disconnect {
            return;
        }
        let interval = self.client.round_trip_delay.max(1000) * 2;
        if (cur_tm - self.client.last_sent_hello).abs() <= interval {
            return;
        }
        if self.client.soft_reconnect && self.client.server_token != 0 {
            self.send_hello_again();
        } else {
            self.client.soft_reconnect = false;
            self.send_hello();
        }
        self.client.last_sent_hello = cur_tm;
        self.client.waiting_hello = true;
        self.client.waiting_hello_start = cur_tm;
    }

    pub(crate) fn check_offline_reconnect(&mut self, cur_tm: i64) {
        let throttle = (self.client.round_trip_delay + 50).clamp(200, 1500);
        let last_online = self.client.last_online;
        let authorized = self.client.authorized;

        let should = self.client.waiting_hello
            || (authorized
                && !self.client.need_connect
                && (cur_tm - last_online).abs() > OFFLINE_BASE_MS + self.client.round_trip_delay);
        if !should {
            return;
        }
        if (cur_tm - self.client.last_sent_hello).abs() <= throttle {
            return;
        }

        self.client.auth_status = AuthStatus::Offline;
        if !self.client.waiting_hello {
            self.client.waiting_hello_start = cur_tm;
        }
        self.client.waiting_hello = true;
        self.send_hello_again();
        self.client.last_sent_hello = cur_tm;
    }

    pub(crate) fn check_reconnect_timeout(&mut self, cur_tm: i64) {
        if self.client.waiting_hello
            && (cur_tm - self.client.waiting_hello_start).abs() > RECONNECT_WAITING_MS
            && (cur_tm - self.client.last_socket_recreate).abs() > RECONNECT_THROTTLE_MS
        {
            self.client.last_socket_recreate = cur_tm;
            self.client.soft_reconnect = true;
            self.client.force_disconnect = true;
            self.client.need_connect = true;
            self.client.waiting_hello = false;
        }
    }

    pub(crate) fn check_dead_zone(&mut self, cur_tm: i64) {
        let authorized = self.client.authorized;
        let last_online = self.client.last_online;
        if !authorized && !self.client.need_connect && (cur_tm - last_online).abs() > DEAD_ZONE_MS {
            self.client.soft_reconnect = false;
            self.client.force_disconnect = true;
            self.client.need_connect = true;
        }
    }

    pub(crate) fn do_force_disconnect(&mut self) {
        if self.client.connected && !self.client.soft_reconnect {
            self.send_command(Command::LogOff, &[]);
        }
        self.client.clear_recv_poller();
        self.client.socket = None;
        if !self.client.soft_reconnect {
            self.client.full_reset();
        }
        self.client.connected = false;
        self.client.authorized = false;
        self.client.force_disconnect = false;
    }

    pub(crate) fn copy_send_ack_and_check_sening_data(&mut self, cur_tm: i64) {
        let mut copy_send_list = std::mem::take(&mut self.client.copy_send_sliced);
        let mut copy_send_list_h = std::mem::take(&mut self.client.copy_send_high);
        let mut copy_send_list_l = std::mem::take(&mut self.client.copy_send_low);
        let mut copy_acks = std::mem::take(&mut self.client.copy_sliced_acks);

        // Delphi `Execute` under `SendLock`:
        // GetCopySendList; GetCopyAcks; FClient.CopyRecvdData.
        self.get_copy_send_lock_snapshot(
            &mut copy_send_list,
            &mut copy_send_list_h,
            &mut copy_send_list_l,
            &mut copy_acks,
        );

        self.check_sening_data(
            &copy_send_list,
            &mut copy_send_list_h,
            &copy_send_list_l,
            &mut copy_acks,
            cur_tm,
        );
        copy_send_list.clear();
        copy_send_list_h.clear();
        copy_send_list_l.clear();
        copy_acks.clear();
        self.client.copy_send_sliced = copy_send_list;
        self.client.copy_send_high = copy_send_list_h;
        self.client.copy_send_low = copy_send_list_l;
        self.client.copy_sliced_acks = copy_acks;
    }

    pub(crate) fn check_sening_data(
        &mut self,
        copy_send_list: &[SendItem],
        copy_send_list_h: &mut [SendItem],
        copy_send_list_l: &[SendItem],
        copy_acks: &mut Vec<SlicedAck>,
        cur_tm: i64,
    ) {
        // Delphi `CheckSeningData`: Sliced CopySendList first, then SlicedACK,
        // then regular H ACK bitmap, High send/retry, first Low flush, Sliced
        // retry, remaining Low flush. Keep this exact protocol order.
        self.apply_sliced_send_u_key_cleanup(copy_send_list);
        for item in copy_send_list {
            self.create_sliced_and_send(item);
        }
        self.apply_copy_acks(copy_acks, cur_tm);
        self.apply_regular_hl_ack();
        self.apply_high_send_u_key_cleanup(copy_send_list_h);
        for item in copy_send_list_h {
            self.send_h_item(item, cur_tm);
        }
        self.retry_pending_h(cur_tm);
        self.send_low_items_around_sliced_retry(copy_send_list_l, cur_tm);
    }

    pub(crate) fn get_copy_send_lock_snapshot(
        &mut self,
        sliced: &mut Vec<SendItem>,
        h_items: &mut Vec<SendItem>,
        l_items: &mut Vec<SendItem>,
        acks: &mut Vec<SlicedAck>,
    ) {
        let tmp_slider = self
            .client
            .send_lock
            .lock()
            .unwrap()
            .take_send_snapshot(sliced, h_items, l_items, acks);
        if let Some(tmp_slider) = tmp_slider {
            self.client.recvd_slider = tmp_slider;
        }
    }

    #[cfg(test)]
    pub(crate) fn get_copy_acks(&mut self) -> Vec<SlicedAck> {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        let mut acks = Vec::new();
        self.get_copy_send_lock_snapshot(&mut sliced, &mut high, &mut low, &mut acks);
        acks
    }

    #[cfg(test)]
    pub(crate) fn copy_recvd_data(&mut self) {
        if let Some(tmp_slider) = self.client.send_lock.lock().unwrap().copy_tmp_slider() {
            self.client.recvd_slider = tmp_slider;
        }
    }

    pub(crate) fn apply_sliced_send_u_key_cleanup(&mut self, sliced: &[SendItem]) {
        // Delphi `CheckSeningData` keeps the cleanup scopes separate:
        // CopySendList (Sliced) calls `DeleteSendingByKey` before
        // `CreateSlicedObject`. Delphi removes only the first matching entry.
        for item in sliced {
            if !item.u_key.is_none() {
                if let Some(pos) = self
                    .client
                    .sending
                    .iter()
                    .position(|s| s.u_key == item.u_key)
                {
                    self.client.sending.remove(pos);
                }
            }
        }
    }

    pub(crate) fn apply_copy_acks(&mut self, copy_acks: &mut Vec<SlicedAck>, cur_tm: i64) {
        for ack in copy_acks.drain(..) {
            self.client.apply_sliced_ack(ack, cur_tm);
        }
    }

    pub(crate) fn apply_regular_hl_ack(&mut self) {
        let recvd_slider = {
            if !self.client.recvd_slider.has_new_data {
                return;
            }
            self.client.recvd_slider.has_new_data = false;
            self.client.recvd_slider.clone()
        };

        let limit = (recvd_slider.r_count.max(0) as u64) * 64;
        self.client.pending_h.retain(|d| {
            if d.msg_num < recvd_slider.start_num {
                return true;
            }
            let offset = d.msg_num - recvd_slider.start_num;
            if offset >= limit {
                return true;
            }
            let word_idx = (offset >> 6) as usize;
            let bit_idx = offset & 63;
            (recvd_slider.bit_field[word_idx] >> bit_idx) & 1 == 0
        });
    }

    pub(crate) fn apply_high_send_u_key_cleanup(&mut self, h_items: &[SendItem]) {
        // Delphi calls `DeletePendingByKey` for copied High items after
        // `ApplyACK` and `ApplyRegularHLAck`, immediately before sending High.
        // It removes only the first matching PendingH entry.
        for item in h_items {
            if !item.u_key.is_none() {
                if let Some(pos) = self
                    .client
                    .pending_h
                    .iter()
                    .position(|p| p.u_key == item.u_key)
                {
                    self.client.pending_h.remove(pos);
                }
            }
        }
    }

    pub(crate) fn create_sliced_and_send(&mut self, item: &SendItem) {
        let header_size = 15u16;
        let slice_hdr_size = 4u16;

        // TMoonProtoDataToSend.Create compresses before CreateSlicedObject sees
        // the stream. Therefore size/empty checks below use the effective
        // compressed payload, not the original item data.
        let (send_cmd, send_data) = Client::maybe_compress(item.cmd, &item.data);

        // MaxSlicedDataSize check (matches IntStruct.pas:1071-1079)
        let pmtu_for_check_i32 =
            self.client.actual_pmtu as i32 - header_size as i32 - slice_hdr_size as i32;
        if pmtu_for_check_i32 <= 0 {
            return;
        }
        let pmtu_for_check = pmtu_for_check_i32 as usize;
        let max_sliced_data_size = pmtu_for_check * 256 - 12 - 1; // 12=CryptoHeader, 1=cmd byte
        if send_data.len() >= max_sliced_data_size {
            return; // too large, drop (Delphi logs + exits)
        }
        if send_data.is_empty() {
            return; // empty data (Delphi logs + exits before Crypt)
        }

        // Crypt if needed
        let (wire_cmd, wire_data, msg_num) = if item.encrypted {
            let msg_num = if item.msg_num != 0 {
                item.msg_num // retry — reuse existing MsgNum
            } else {
                self.client
                    .crypt_msg_counter
                    .fetch_add(1, Ordering::Relaxed)
                    + 1
            };

            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = send_cmd; // inner cmd (may have COMPRESSED_FLAG)
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };

            let mut plaintext = Vec::with_capacity(12 + send_data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(send_data.as_ref());

            // B-V2-03: используем кэшированный cipher из Client.
            let Some(cipher) = self.client.encode_cipher.as_ref() else {
                error!(target: "moonproto::crypto", "encrypt H-prio called before handshake — packet dropped");
                return;
            };
            let encrypted_data = crypto::encrypt_with_cipher(cipher, &plaintext, &[]);
            // Delphi: NewCmd := MPC_Crypted; if IsCompressed(d.Fcmd) then NewCmd := SetCompressed(NewCmd)
            let wire_cmd = Client::crypted_wire_cmd(send_cmd);
            (wire_cmd, encrypted_data, msg_num)
        } else {
            (send_cmd, send_data.into_owned(), 0u64)
        };

        // CreateSlicedObject
        let pmtu = (self.client.actual_pmtu - header_size - slice_hdr_size) as usize;
        let total_size = wire_data.len() + 1; // +1 cmd byte in block 0
        let n_blocks = total_size.div_ceil(pmtu).max(1);
        let max_block_num = (n_blocks - 1) as u8;
        let datagram_num = self.client.send_datagram_num;
        self.client.send_datagram_num = self.client.send_datagram_num.wrapping_add(1);

        if trace_io_enabled() {
            let api = if item.cmd == Command::API.to_byte() && item.data.len() >= 12 {
                let uid = u64::from_le_bytes(item.data[3..11].try_into().unwrap());
                let method = item.data[11];
                format!(" api_uid={uid} api_method={method}")
            } else {
                String::new()
            };
            eprintln!(
                "[mp-sliced-queue] d={} inner_cmd={:?} raw={} encrypted={} payload_len={} blocks={} max_retries={}{}",
                datagram_num,
                Command::from_byte(item.cmd),
                item.cmd,
                item.encrypted,
                item.data.len(),
                n_blocks,
                item.max_retries,
                api
            );
        }

        let mut data_pos = 0;
        let mut sent_slices = Vec::with_capacity(n_blocks);
        for block_num in 0..n_blocks {
            let mut slice = Vec::with_capacity(4 + pmtu);
            slicing::SliceHeader {
                datagram_num,
                block_num: block_num as u8,
                max_block_num,
            }
            .write_to(&mut slice);

            if block_num == 0 {
                slice.push(wire_cmd);
                let write_size = (pmtu - 1).min(wire_data.len() - data_pos);
                slice.extend_from_slice(&wire_data[data_pos..data_pos + write_size]);
                data_pos += write_size;
            } else {
                let write_size = pmtu.min(wire_data.len() - data_pos);
                slice.extend_from_slice(&wire_data[data_pos..data_pos + write_size]);
                data_pos += write_size;
            }

            sent_slices.push(slice);
        }

        // Store in Sending list with priority insert (matches IntStruct.pas:1112-1116)
        let new_sliced = SentSliced {
            datagram_num,
            // Delphi `TMoonProtoSlice.Create` and `TMoonProtoSlicedData.Create`
            // initialise LastChecked to 0. `CreateSlicedObject` only enqueues the
            // slices; actual sends happen below in `retry_sliced` / CheckSeningData
            // under ClientLimit.
            piece_last_checked: vec![0; n_blocks],
            slices: sent_slices,
            ack_flags: [0u8; 32],
            blocks_count: n_blocks,
            sent_count: 0,
            last_checked: 0,
            retry_count: 0,
            last_retry_inc: 0,
            max_retry_count: item.max_retries,
            u_key: item.u_key,
        };
        // Priority: fewer blocks → earlier in queue (smaller datagrams retry first)
        let insert_pos = self
            .client
            .sending
            .iter()
            .position(|s| s.blocks_count > n_blocks)
            .unwrap_or(self.client.sending.len());
        self.client.sending.insert(insert_pos, new_sliced);
        self.client.last_checked_slices = 0;

        // NB: Sliced retry уже работает через self.sending + retry_sliced (per-piece LastChecked,
        // ClientLimit, FRetryCount → MaxRetryCount). Не добавляем в pending_h — это двойной retry.
        // (Delphi: PendingH используется только для H-priority команд через DoSendMPData, не для Sliced.)
        let _ = msg_num;
    }

    pub(crate) fn send_h_item(&mut self, item: &mut SendItem, cur_tm: i64) {
        // Auto-compression (matches Delphi TMoonProtoDataToSend.Create pas:661-672).
        // Сжимает payload > 64 байт если результат < 95% оригинала. Inner cmd получает
        // COMPRESSED_FLAG (0x80). Закрывает DEVIATION #11.
        let (eff_cmd, eff_data) = Client::maybe_compress(item.cmd, &item.data);

        if item.encrypted {
            let msg_num = if item.msg_num != 0 {
                item.msg_num
            } else {
                self.client
                    .crypt_msg_counter
                    .fetch_add(1, Ordering::Relaxed)
                    + 1
            };

            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = eff_cmd;
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };

            let mut plaintext = Vec::with_capacity(12 + eff_data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(&eff_data);

            // B-V2-03: кэшированный cipher.
            let Some(cipher) = self.client.encode_cipher.as_ref() else {
                error!(target: "moonproto::crypto", "encrypt batch called before handshake — packet dropped");
                return;
            };
            let encrypted = crypto::encrypt_with_cipher(cipher, &plaintext, &[]);

            // Delphi `Client.Crypt`: outer MPC_Crypted carries COMPRESSED_FLAG too
            // when the encrypted inner command is compressed.
            let wire_cmd = Client::crypted_wire_cmd(eff_cmd);

            self.do_send_mp_data_wire(wire_cmd, &encrypted);

            // Add to PendingH for retry (first send only)
            if item.retry_left > 0 && item.msg_num == 0 {
                let mut pending_item = item.clone();
                pending_item.msg_num = msg_num;
                pending_item.last_sent_at = cur_tm;
                // Сохраняем СЖАТЫЕ данные + cmd с COMPRESSED_FLAG — при retry encrypt
                // снова обернёт их (compression deterministic, можно было бы не хранить —
                // но проще не пересжимать).
                pending_item.cmd = eff_cmd;
                // pending_item.data — Vec<u8>, нужно owned. Если eff_data Borrowed —
                // alloc здесь (необходимый — pending_h хранит копию между retry).
                pending_item.data = eff_data.into_owned();
                // Delphi `PendingH` не имеет capacity-cap: H-команды живут до ACK
                // или исчерпания `RetryLeft`. Старые trading-команды не дропаются
                // искусственно при большом burst'е.
                self.client.pending_h.push(pending_item);
            }
        } else {
            self.do_send_mp_data_wire(eff_cmd, &eff_data);
        }
        item.last_sent_at = cur_tm;
    }

    pub(crate) fn retry_pending_h(&mut self, cur_tm: i64) {
        // Delphi: Max(200, Min(500, round(Client.RoundTripDelay * 1.1 + 10)))
        let path_delay =
            ((self.client.round_trip_delay as f64 * 1.1 + 10.0).round() as i64).clamp(200, 500);
        let mut to_drop = Vec::new();
        let mut to_resend = Vec::new();

        for (idx, item) in self.client.pending_h.iter_mut().enumerate() {
            if (item.last_sent_at - cur_tm).abs() > path_delay {
                item.last_sent_at = cur_tm;
                // 1+2. Сначала клонируем с ТЕКУЩИМ retry_left и кладём на resend.
                //      WantACK будет вычислен в send_h_item как `retry_left > 0` — на последнем
                //      retry (когда retry_left=1 ДО decrement) WantACK=true → сервер ACK'нет.
                to_resend.push(item.clone());
                // 3. Decrement.
                item.retry_left -= 1;
                // 4. Drop если исчерпался.
                if item.retry_left <= 0 {
                    to_drop.push(idx);
                }
            }
        }

        // Remove exhausted (reverse order to preserve indices)
        for idx in to_drop.into_iter().rev() {
            self.client.pending_h.remove(idx);
        }

        // Resend via direct MPC_Crypted (NOT through Sliced — matches Delphi DoSendMPData)
        for mut item in to_resend {
            self.send_h_item(&mut item, cur_tm);
        }
    }

    pub(crate) fn retry_sliced(&mut self, cur_tm: i64) {
        let client = &mut self.client;
        if client.sending.is_empty() {
            return;
        }

        // Outer gate: only check if enough time passed (matches Common.pas:970).
        if (cur_tm - client.last_checked_slices).abs() <= client.round_trip_delay {
            return;
        }

        // TripDelayK adaptation every 2s (matches :975-979). Delphi does this
        // before PathDelay is computed, so the same tick uses the new K.
        if (cur_tm - client.last_set_trip_k).abs() > 2000 {
            client.last_set_trip_k = cur_tm;
            if client.avg_dup_count > 5.0 {
                client.trip_delay_k = (client.trip_delay_k + 0.05).min(1.25);
            }
            if client.avg_dup_count == 0.0 {
                client.trip_delay_k = (client.trip_delay_k - 0.01).max(1.05);
            }
        }

        let path_delay =
            (client.round_trip_delay as f64 * client.trip_delay_k + 10.0).round() as i64;
        let cycle_time_ms = 5.0f64.max(client.actual_sleep_time).min(15.0);
        // B-19: * 0.001 вместо / 1000.0 (FDIV → FMUL on hot retry path).
        // Delphi uses `round(Client.CanSendRate * CycleTimeMS / 1000.0)`,
        // so keep rounding instead of truncating on the hot retry boundary.
        let client_limit = (client.can_send_rate as f64 * cycle_time_ms * 0.001).round() as usize;
        let mut bytes_sent_at_once: usize = 0;
        client.last_checked_slices = cur_tm;

        // Аудит #2 (audit_delphi_deviation): индексы вместо clone. Раньше каждый
        // ретранслируемый блок копировался в `to_send: Vec<Vec<u8>>` — 200 alloc/sec
        // при congestion (10 active Sliced × 20 blocks × 2 retries/sec × ~500б).
        // Теперь храним `(sending_idx, block_num)` (16 байт), отправляем по ссылке.
        // Соответствует Delphi `SendCommand(Client, MPC_Sliced, Piece.data)` где Piece.data —
        // `TMemoryStream` по ссылке (ноль копий).
        let mut to_send_indices: Vec<(usize, usize)> = Vec::new();
        let mut to_remove = Vec::new();

        for (idx, sliced) in client.sending.iter_mut().enumerate() {
            if (cur_tm - sliced.last_checked).abs() <= path_delay {
                continue;
            }

            let prev_last_checked = sliced.last_checked;
            let mut sent_on_path_delay = false;
            sliced.last_checked = cur_tm;

            for (block_num, slice_data) in sliced.slices.iter().enumerate() {
                if sliced.is_block_acked(block_num) {
                    continue;
                } // ACK'd

                // Per-piece check (matches :989)
                if sliced.piece_last_checked[block_num] != prev_last_checked {
                    continue;
                }
                if (cur_tm - sliced.piece_last_checked[block_num]).abs() <= path_delay {
                    continue;
                }
                if bytes_sent_at_once >= client_limit {
                    break;
                }

                if trace_io_enabled() {
                    eprintln!(
                        "[mp-sliced-tx] d={} block={}/{} retry_count={} sent_count={} bytes_this_tick={} client_limit={}",
                        sliced.datagram_num,
                        block_num,
                        sliced.blocks_count.saturating_sub(1),
                        sliced.retry_count,
                        sliced.sent_count,
                        bytes_sent_at_once,
                        client_limit
                    );
                }
                if sliced.piece_last_checked[block_num] > 0 {
                    sent_on_path_delay = true;
                }
                to_send_indices.push((idx, block_num));
                sliced.piece_last_checked[block_num] = cur_tm;
                sliced.sent_count += 1;
                bytes_sent_at_once += slice_data.len();
            }

            // Sliced.LastChecked = Min(remaining Piece.LastChecked) (matches :996
            // after Delphi `ApplyACK` removed ACKed pieces from the list).
            sliced.refresh_last_checked_from_unacked(cur_tm);

            // Conditional increment (matches :998-999)
            if prev_last_checked != sliced.last_checked
                && sent_on_path_delay
                && (sliced.last_retry_inc - cur_tm).abs() > path_delay
            {
                sliced.retry_count += 1;
                sliced.last_retry_inc = cur_tm;
            }
            client.last_checked_slices = client.last_checked_slices.min(sliced.last_checked);

            if sliced.retry_count > sliced.max_retry_count {
                to_remove.push(idx);
            }
        }

        // UsedSlicedLimit flag (matches :1009-1011)
        let used_limit_threshold = (client_limit as f64 * 0.8).round() as usize;
        if bytes_sent_at_once >= used_limit_threshold {
            client.used_sliced_limit = true;
        }

        // Аудит #2: отправляем по индексу из self.sending — никаких clone.
        // ВАЖНО: send_raw_packet берёт `&[u8]`, поэтому borrow на self.sending живёт только
        // на время одного send. send_raw_packet требует `&mut self` (внутри пишет в
        // bps/total_sent/socket), а sending borrow read-only — нужен split. Делаем мини-
        // dance: snapshot нужного slice во временный буфер (1 alloc per packet вместо 1
        // alloc на каждый element в общем Vec<Vec<u8>>). Чуть лучше но не zero-alloc.
        // **TODO** для следующей версии: разнести send_raw_packet чтобы slice мог быть
        // передан без holding &mut self на сокет.
        let mut tmp_slice: Vec<u8> = Vec::new();
        for (idx, block_num) in to_send_indices {
            tmp_slice.clear();
            tmp_slice.extend_from_slice(&client.sending[idx].slices[block_num]);
            Self::send_command_on_client(client, Command::Sliced, &tmp_slice);
        }

        for idx in to_remove.into_iter().rev() {
            client.sending.remove(idx);
        }
    }

    pub(crate) fn batch_send_direct(&mut self, item: &SendItem) {
        // Auto-compression (DEVIATION #11 — закрыто).
        let (eff_cmd, eff_data) = Client::maybe_compress(item.cmd, &item.data);

        // Encrypt if needed
        // Аудит #3: wire_data становится Cow — для unencrypted path сохраняем borrowed
        // (zero alloc); для encrypted — Owned (encrypt всегда возвращает Vec).
        let (wire_cmd, wire_data): (u8, std::borrow::Cow<'_, [u8]>) = if item.encrypted {
            let msg_num = self
                .client
                .crypt_msg_counter
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = eff_cmd;
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };
            let mut plaintext = Vec::with_capacity(12 + eff_data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(&eff_data);
            // B-V2-03: кэшированный cipher.
            let cipher = match self.client.encode_cipher.as_ref() {
                Some(c) => c,
                None => {
                    error!(target: "moonproto::crypto", "encrypt batch_direct called before handshake — packet dropped");
                    return;
                }
            };
            let encrypted = crypto::encrypt_with_cipher(cipher, &plaintext, &[]);
            (
                Client::crypted_wire_cmd(eff_cmd),
                std::borrow::Cow::Owned(encrypted),
            )
        } else {
            (eff_cmd, eff_data)
        };

        self.do_send_mp_data_wire(wire_cmd, &wire_data);
    }

    pub(crate) fn send_low_items_around_sliced_retry(&mut self, l_items: &[SendItem], cur_tm: i64) {
        // Delphi CheckSeningData has two Low phases:
        // 1. before Sliced retry: send only CopySendListL[0] with NeedFlush=true
        //    (or just flush accumulated H batch when there is no Low item);
        // 2. after Sliced retry: send the remaining Low items and flush.
        if let Some(first) = l_items.first() {
            self.batch_send_direct(first);
        }
        self.flush_send_batch();

        self.retry_sliced(cur_tm);

        for item in l_items.iter().skip(1) {
            self.batch_send_direct(item);
        }
        self.flush_send_batch();
    }

    pub(crate) fn flush_send_batch(&mut self) {
        if self.client.tmp_send_count == 0 {
            return;
        }

        if self.client.tmp_send_count > 1 {
            // Send as MPC_Grouped
            let mut payload = std::mem::take(&mut self.client.tmp_send_buf);
            self.send_command(Command::Grouped, &payload);
            payload.clear();
            self.client.tmp_send_buf = payload;
        } else {
            // Single item: формат tmp_send_buf = [cmd(1) | sz(2 LE) | data(sz)].
            // Wire-format MPC_Grouped header не нужен → отправляем как обычный пакет.
            let mut buf = std::mem::take(&mut self.client.tmp_send_buf);
            if buf.len() >= 3 {
                let cmd = buf[0];
                // sz прочитан только для slicing data (после 3 байт group-header'а).
                let data = &buf[3..];
                self.send_command_raw(cmd, data);
            }
            buf.clear();
            self.client.tmp_send_buf = buf;
        }
        self.client.tmp_send_count = 0;
        self.client.tmp_send_size = 0;
    }

    pub(crate) fn push_tmp_send_item(
        &mut self,
        wire_cmd: u8,
        wire_data: &[u8],
        accounted_size: usize,
    ) {
        self.client.tmp_send_buf.push(wire_cmd);
        let sz = wire_data.len() as u16;
        self.client
            .tmp_send_buf
            .extend_from_slice(&sz.to_le_bytes());
        self.client.tmp_send_buf.extend_from_slice(wire_data);
        self.client.tmp_send_count += 1;
        self.client.tmp_send_size += accounted_size;
    }

    pub(crate) fn do_send_mp_data_wire(&mut self, wire_cmd: u8, wire_data: &[u8]) {
        // Delphi DoSendMPData uses `sz = d.ms.Size + GetHeaderSize + 3`.
        // The counter intentionally over-accounts the transport header for every
        // buffered item, and its overflow branch may send the current item
        // directly while keeping the previous buffer for a later NeedFlush.
        let accounted_size = wire_data.len() + 15 + 3;
        if self.client.tmp_send_size + accounted_size > self.client.actual_pmtu as usize {
            if self.client.tmp_send_size > accounted_size {
                self.flush_send_batch();
                self.push_tmp_send_item(wire_cmd, wire_data, accounted_size);
            } else {
                self.send_command_raw(wire_cmd, wire_data);
            }
        } else {
            self.push_tmp_send_item(wire_cmd, wire_data, accounted_size);
        }
    }
}
