use super::*;

impl Client {
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
        let extra = crate::transport::transport_pack_into_with_mac(
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
        let extra = crate::transport::transport_pack_into_with_mac(
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
}
