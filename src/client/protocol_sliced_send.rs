use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
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

    pub(crate) fn create_sliced_and_send(&mut self, item: &SendItem) {
        let header_size = 15u16;
        let slice_hdr_size = 4u16;

        // TMoonProtoDataToSend.Create compresses before CreateSlicedObject sees
        // the stream. Therefore size/empty checks below use the effective
        // compressed payload, not the original item data.
        let (send_cmd, send_data) = Client::maybe_compress(item.cmd, &item.data);

        // MaxSlicedDataSize check (matches MoonProtoIntStruct.pas).
        let pmtu_for_check_i32 =
            self.client.actual_pmtu as i32 - header_size as i32 - slice_hdr_size as i32;
        if pmtu_for_check_i32 <= 0 {
            return;
        }
        let pmtu_for_check = pmtu_for_check_i32 as usize;
        let encrypted_fixed_overhead = crypted::CRYPTO_HEADER_SIZE
            + crypto::IV_SIZE
            + crypto::GCM_TAG_SIZE
            + crypto::MAX_PKCS7_PADDING
            + std::mem::size_of::<u8>(); // inner cmd byte in the first slice
        let max_sliced_data_size = pmtu_for_check * 256 - encrypted_fixed_overhead;
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
                "[mp-sliced-queue] t={} d={} inner_cmd={:?} raw={} encrypted={} payload_len={} payload_hash={:016X} payload_head={} blocks={} max_retries={}{}",
                trace_elapsed_ms(),
                datagram_num,
                Command::from_byte(item.cmd),
                item.cmd,
                item.encrypted,
                item.data.len(),
                fnv1a64(&item.data),
                trace_head(&item.data, 16),
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

        // NB: Sliced retry already runs through self.sending + retry_sliced (per-piece LastChecked,
        // ClientLimit, FRetryCount → MaxRetryCount). Do not add to pending_h — that would be a double retry.
        // (Delphi: PendingH is used only for H-priority commands via DoSendMPData, not for Sliced.)
        let _ = msg_num;
    }

    pub(crate) fn retry_sliced(&mut self, cur_tm: i64) {
        let client = &mut self.client;
        if client.sending.is_empty() {
            return;
        }

        let retry_round_trip_delay = if client.round_trip_delay <= 0 {
            UNKNOWN_RTT_SLICED_FLOOR_MS
        } else {
            client.round_trip_delay
        };

        // Outer gate: only check if enough time passed (matches Common.pas:970).
        // Active-lib startup guard: before the first non-zero Ping RTT arrives,
        // do not let `RoundTripDelay=0` collapse the Sliced retry clock to a
        // 10ms local CPU loop.
        if (cur_tm - client.last_checked_slices).abs() <= retry_round_trip_delay {
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
            (retry_round_trip_delay as f64 * client.trip_delay_k + 10.0).round() as i64;
        let cycle_time_ms = 5.0f64.max(client.actual_sleep_time).min(15.0);
        // Retry pacing runs every send tick. Use `* 0.001` instead of `/ 1000.0`
        // so the hot budget calculation avoids floating-point division while
        // preserving the same rounded byte limit.
        let client_limit = (client.can_send_rate as f64 * cycle_time_ms * 0.001).round() as usize;
        let mut bytes_sent_at_once: usize = 0;
        client.last_checked_slices = cur_tm;

        // Queue indices, not cloned blocks. Retransmits send by reference from
        // the retained Sliced object, matching Delphi's Piece.data stream path
        // and avoiding a Vec allocation/copy per retransmitted block.
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
                        "[mp-sliced-tx] t={} proto_t={} d={} block={}/{} retry_count={} sent_count={} bytes_this_tick={} client_limit={} path_delay={} rtt={} slice_hash={:016X} slice_head={}",
                        trace_elapsed_ms(),
                        cur_tm,
                        sliced.datagram_num,
                        block_num,
                        sliced.blocks_count.saturating_sub(1),
                        sliced.retry_count,
                        sliced.sent_count,
                        bytes_sent_at_once,
                        client_limit,
                        path_delay,
                        client.round_trip_delay,
                        fnv1a64(slice_data),
                        trace_head(slice_data, 16)
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
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-sliced-drop] t={} proto_t={} d={} retry_count={} max_retry_count={} sent_count={} path_delay={} rtt={}",
                        trace_elapsed_ms(),
                        cur_tm,
                        sliced.datagram_num,
                        sliced.retry_count,
                        sliced.max_retry_count,
                        sliced.sent_count,
                        path_delay,
                        client.round_trip_delay
                    );
                }
                to_remove.push(idx);
            }
        }

        // UsedSlicedLimit flag (matches :1009-1011)
        let used_limit_threshold = (client_limit as f64 * 0.8).round() as usize;
        if bytes_sent_at_once >= used_limit_threshold {
            client.used_sliced_limit = true;
        }

        for (idx, block_num) in to_send_indices {
            let Some(addr) = client.server_socket_addr() else {
                continue;
            };
            let extra = {
                let slice = &client.sending[idx].slices[block_num];
                crate::transport::pack_client_packet(
                    &mut client.transport.send_buf,
                    &client.transport.mac_ctx,
                    Command::Sliced.to_byte(),
                    client.cfg.client_id,
                    slice,
                    client.cfg.mask_ver.to_byte(),
                    &mut client.transport.transport_mode_state,
                )
            };
            let packet = std::mem::take(&mut client.transport.send_buf);
            client.dispatch_send(Command::Sliced.to_byte(), &packet, extra.as_deref(), addr);
            client.transport.send_buf = packet;
            client.transport.send_buf.clear();
        }

        for idx in to_remove.into_iter().rev() {
            client.sending.remove(idx);
        }
    }
}
