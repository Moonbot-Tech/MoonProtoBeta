use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
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
}
