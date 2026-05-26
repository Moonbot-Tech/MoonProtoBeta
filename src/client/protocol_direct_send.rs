use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
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
