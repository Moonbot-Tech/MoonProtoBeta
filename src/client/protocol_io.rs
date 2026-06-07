use super::*;

impl Client {
    /// Auto-compress payload if `cmd` is not yet marked with `COMPRESSED_FLAG`, size > 64 bytes
    /// and `mp_compress` yielded savings ≥ 5% (`mp_compress` itself returns None otherwise).
    /// Matches Delphi `TMoonProtoDataToSend.Create` (MoonProtoIntStruct.pas:661-672).
    ///
    /// Returns `Cow`: uncompressed packets borrow the caller payload and allocate
    /// nothing; only the compressed path owns a new buffer. Outbound send queues
    /// therefore avoid `to_vec()` when the original bytes can be put on the wire
    /// as-is, matching Delphi's pass-by-reference stream path.
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
        // Zero-alloc fast path: reuse self.transport.send_buf + cached MacContext.
        let extra = crate::transport::pack_client_packet(
            &mut self.transport.send_buf,
            &self.transport.mac_ctx,
            cmd,
            self.cfg.client_id,
            payload,
            self.cfg.transport_mode.to_byte(),
            &mut self.transport.transport_mode_state,
        );
        // Take the packet out so the borrow checker does not complain about a double
        // &mut self (dispatch_send takes &mut self and does not need send_buf after the
        // copy into the socket). We take a slice from send_buf — it lives in self, and
        // socket.send_to does not retain the reference. SAFETY pattern: take/restore so
        // that &mut self in dispatch_send does not overlap with &self.transport.send_buf — but
        // simpler: pass the slice via an owned vec swap.
        let packet = std::mem::take(&mut self.transport.send_buf);
        self.dispatch_send(cmd, &packet, extra.as_deref(), addr);
        // Return the buffer (capacity preserved, content not needed right now).
        self.transport.send_buf = packet;
        self.transport.send_buf.clear();
    }

    pub(crate) fn send_raw_packet(&mut self, cmd: Command, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else {
            return;
        };
        let extra = crate::transport::pack_client_packet(
            &mut self.transport.send_buf,
            &self.transport.mac_ctx,
            cmd.to_byte(),
            self.cfg.client_id,
            payload,
            self.cfg.transport_mode.to_byte(),
            &mut self.transport.transport_mode_state,
        );
        let packet = std::mem::take(&mut self.transport.send_buf);
        self.dispatch_send(cmd.to_byte(), &packet, extra.as_deref(), addr);
        self.transport.send_buf = packet;
        self.transport.send_buf.clear();
    }

    /// Actually sends the packet (plus an optional extra transport packet).
    ///
    /// Send errors are logged instead of being collapsed into `.ok()`. They do
    /// not force reconnect: Delphi `DoSendPacket` returns false and leaves
    /// `ForceDisconnect` unchanged, so Rust keeps the same retry/recovery owner.
    pub(crate) fn dispatch_send(
        &mut self,
        cmd: u8,
        packet: &[u8],
        extra: Option<&[u8]>,
        addr: SocketAddr,
    ) {
        #[cfg(any(test, feature = "diagnostics"))]
        {
            if self
                .metrics
                .debug_outgoing_blackhole
                .load(Ordering::Relaxed)
            {
                self.metrics
                    .err_emu_diagnostics
                    .lock()
                    .record_outgoing(cmd, true);
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-blackhole] t={} cmd={:?} raw={} packet_len={} extra_len={} packet_hash={:016X} packet_head={} addr={}",
                        trace_elapsed_ms(),
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        extra.map(|p| p.len()).unwrap_or(0),
                        fnv1a64(packet),
                        trace_head(packet, 16),
                        addr
                    );
                }
                return;
            }
        }

        if trace_io_enabled() {
            eprintln!(
                "[mp-io-tx-attempt] t={} cmd={:?} raw={} packet_len={} extra_len={} packet_hash={:016X} packet_head={} addr={}",
                trace_elapsed_ms(),
                Command::from_byte(cmd),
                cmd,
                packet.len(),
                extra.map(|p| p.len()).unwrap_or(0),
                fnv1a64(packet),
                trace_head(packet, 16),
                addr
            );
        }
        // First perform the network operations, collecting the Results into owned
        // variables, then process them through self.should_log without a conflicting borrow.
        let extra_result = match (extra, self.transport.socket.as_ref()) {
            (Some(extra_pkt), Some(sock)) => Some(sock.send_to(extra_pkt, addr)),
            _ => None,
        };
        let main_result = match self.transport.socket.as_ref() {
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
                #[cfg(any(test, feature = "diagnostics"))]
                self.metrics
                    .err_emu_diagnostics
                    .lock()
                    .record_outgoing(cmd, false);
                let total_sent = self
                    .metrics
                    .total_sent
                    .fetch_add(packet.len() as u64, Ordering::Relaxed)
                    + packet.len() as u64;
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-ok] t={} cmd={:?} raw={} packet_len={} packet_hash={:016X} total_sent={}",
                        trace_elapsed_ms(),
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        fnv1a64(packet),
                        total_sent
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-wouldblock] t={} cmd={:?} raw={} packet_len={} packet_hash={:016X} err={}",
                        trace_elapsed_ms(),
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        fnv1a64(packet),
                        e
                    );
                }
                if self.should_log("send_wouldblock", 1000) {
                    warn!("send_to(cmd={cmd}) would block (kernel send buffer full)");
                }
            }
            Err(e) if is_datagram_too_large_error(&e) => {
                let cmd_name = Command::from_byte(cmd);
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-too-large] t={} cmd={:?} raw={} packet_len={} packet_hash={:016X} err={}",
                        trace_elapsed_ms(),
                        cmd_name,
                        cmd,
                        packet.len(),
                        fnv1a64(packet),
                        e
                    );
                }
                if is_pmtu_probe_ack_command(cmd) {
                    if self.should_log("send_too_large_pmtu_probe", 10_000) {
                        debug!(
                            "PMTU probe ack {:?} len={} exceeded current path MTU; expected negative probe feedback: {e}",
                            cmd_name,
                            packet.len()
                        );
                    }
                } else if self.should_log("send_too_large", 1000) {
                    warn!(
                        "send_to(cmd={:?}/raw={cmd}, len={}) packet too large for current path MTU: {e}",
                        cmd_name,
                        packet.len()
                    );
                }
            }
            Err(e) => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-error] t={} cmd={:?} raw={} packet_len={} packet_hash={:016X} err={}",
                        trace_elapsed_ms(),
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        fnv1a64(packet),
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
