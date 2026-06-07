use super::protocol_core::ProtocolCore;
use super::*;

fn pending_h_path_delay(round_trip_delay: i64) -> i64 {
    // Delphi: Max(200, Min(500, round(Client.RoundTripDelay * 1.1 + 10)))
    ((round_trip_delay as f64 * 1.1 + 10.0).round() as i64).clamp(200, 500)
}

#[inline]
fn retry_due(last_sent_at: i64, cur_tm: i64, path_delay: i64) -> bool {
    (last_sent_at - cur_tm).abs() > path_delay
}

impl ProtocolCore<'_> {
    pub(crate) fn apply_regular_hl_ack(&mut self) {
        let client = &mut *self.client;
        let recvd_slider = &mut client.recv.recvd_slider;
        if !recvd_slider.has_new_data {
            return;
        }
        recvd_slider.has_new_data = false;
        client
            .pending_h
            .retain(|d| !recvd_slider.ack_confirms_msg(d.msg_num));
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
        // Compresses payload > 64 bytes if the result is < 95% of the original. The
        // inner cmd gets the COMPRESSED_FLAG (0x80).
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
                // Store the COMPRESSED data + cmd with COMPRESSED_FLAG — on retry,
                // encrypt wraps them again (compression is deterministic, so we could
                // avoid storing them — but it is simpler not to recompress).
                pending_item.cmd = eff_cmd;
                // pending_item.data is Vec<u8>, must be owned. If eff_data is Borrowed,
                // alloc here (necessary — pending_h keeps a copy between retries).
                pending_item.data = eff_data.into_owned();
                // Delphi `PendingH` has no capacity cap: H commands live until ACK
                // or `RetryLeft` is exhausted. Old trading commands are not dropped
                // artificially during a large burst.
                self.client.pending_h.push(pending_item);
            }
        } else {
            self.do_send_mp_data_wire(eff_cmd, &eff_data);
        }
        item.last_sent_at = cur_tm;
    }

    pub(crate) fn retry_pending_h(&mut self, cur_tm: i64) {
        let path_delay = pending_h_path_delay(self.client.round_trip_delay);
        let mut to_drop = Vec::new();
        let mut to_resend = Vec::new();

        for (idx, item) in self.client.pending_h.iter_mut().enumerate() {
            if retry_due(item.last_sent_at, cur_tm, path_delay) {
                item.last_sent_at = cur_tm;
                // 1+2. First clone with the CURRENT retry_left and queue for resend.
                //      WantACK is computed in send_h_item as `retry_left > 0` — on the last
                //      retry (when retry_left=1 BEFORE decrement) WantACK=true → server ACKs.
                to_resend.push(item.clone());
                // 3. Decrement.
                item.retry_left -= 1;
                // 4. Drop if exhausted.
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
        // Auto-compression for large direct-send payloads.
        let (eff_cmd, eff_data) = Client::maybe_compress(item.cmd, &item.data);

        // Encrypt if needed
        // Unencrypted payloads stay borrowed; encrypted payloads own the AES-GCM
        // output buffer. This keeps the direct-send public path zero-alloc.
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
            // Single item: tmp_send_buf format = [cmd(1) | sz(2 LE) | data(sz)].
            // The MPC_Grouped wire-format header is not needed → send as a plain packet.
            let mut buf = std::mem::take(&mut self.client.tmp_send_buf);
            if buf.len() >= 3 {
                let cmd = buf[0];
                // sz is read only for slicing data (after the 3-byte group header).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_h_path_delay_matches_delphi_bounds() {
        assert_eq!(pending_h_path_delay(0), 200);
        assert_eq!(pending_h_path_delay(200), 230);
        assert_eq!(pending_h_path_delay(10_000), 500);
    }

    #[test]
    fn retry_due_uses_strict_delphi_abs_threshold() {
        assert!(!retry_due(1000, 1200, 200));
        assert!(retry_due(1000, 1201, 200));
        assert!(!retry_due(1200, 1000, 200));
        assert!(retry_due(1201, 1000, 200));
    }
}
