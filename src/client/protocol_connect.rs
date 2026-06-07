use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    fn new_handshake_rnd(&mut self) {
        self.client.handshake_rnd = rand::random::<[u8; 16]>();
        self.client.handshake_peer_mix = 0;
    }

    fn next_primary_hello_state(&self) -> HelloWaitState {
        if self.client.next_primary_hello_new_session
            || self.client.lifecycle.was_ever_connected
            || self.client.server_token != 0
        {
            HelloWaitState::PrimaryHelloNewSession
        } else {
            HelloWaitState::PrimaryHelloCold
        }
    }

    fn begin_primary_hello(&mut self, cur_tm: i64) {
        let state = self.next_primary_hello_state();
        self.client.soft_reconnect = false;
        self.client.full_reset();
        self.client.clear_outbound_session_data();
        self.client.server_token = 0;
        self.client.peer_app_token = 0;
        self.client.authorized = false;
        self.new_handshake_rnd();
        self.client.start_hello_wait(state, cur_tm);
        self.send_hello();
        self.client.next_primary_hello_new_session = false;
    }

    fn send_rebind_hello_again(&mut self, cur_tm: i64, new_window: bool) {
        if new_window {
            self.new_handshake_rnd();
            self.client
                .start_hello_wait(HelloWaitState::RebindHelloAgain, cur_tm);
        } else {
            self.client
                .set_hello_wait_state(HelloWaitState::RebindHelloAgain);
        }
        self.send_hello_again();
    }

    pub(crate) fn send_hello(&mut self) {
        self.client.client_token = self.client.client_token.wrapping_add(1);
        let mut hello = handshake::Hello::new(self.client.client_token, self.client.app_token);
        hello.rnd = self.client.handshake_rnd;
        hello.timestamp = delphi_now();
        hello.server_token = 0;
        hello.peer_mix = 0;
        let packed = hello.to_bytes_packed();
        let aad = handshake::handshake_aad(self.client.cfg.client_id, Command::Hello.to_byte());
        let payload = crypto::encrypt(&self.client.cfg.master_key, &packed, &aad);
        self.send_command(Command::Hello, &payload);
    }

    pub(crate) fn build_hello_again_packet(&mut self) -> Option<Vec<u8>> {
        if self.client.server_token == 0 {
            return None;
        }
        self.client.client_token = self.client.client_token.wrapping_add(1);
        let mut hello = handshake::Hello::new(self.client.client_token, self.client.app_token);
        hello.rnd = self.client.handshake_rnd;
        hello.timestamp = delphi_now();
        hello.server_token = self.client.server_token;
        hello.peer_mix = crypto::calculate_hello_again_peer_mix(
            &hello.rnd,
            hello.mix_ts,
            self.client.server_token,
            &self.client.session_rnd,
        );
        let packed = hello.to_bytes_packed();
        let aad =
            handshake::handshake_aad(self.client.cfg.client_id, Command::HelloAgain.to_byte());
        Some(crypto::encrypt(&self.client.cfg.master_key, &packed, &aad))
    }

    pub(crate) fn send_hello_again(&mut self) {
        let Some(encrypted) = self.build_hello_again_packet() else {
            return;
        };
        self.send_command(Command::HelloAgain, &encrypted);
    }

    pub(crate) fn build_imfriend_packet(&mut self) -> Option<Vec<u8>> {
        if self.client.server_token == 0 {
            return None;
        }
        self.client.client_token = self.client.client_token.wrapping_add(1);
        let mut hello = handshake::Hello::new(self.client.client_token, self.client.app_token);
        hello.rnd = self.client.handshake_rnd;
        hello.server_token = self.client.server_token;
        hello.peer_mix = self.client.handshake_peer_mix;
        hello.timestamp = delphi_now();
        let packed = hello.to_bytes_packed();
        let aad = handshake::handshake_aad(self.client.cfg.client_id, Command::ImFriend.to_byte());
        let cipher = self.client.encode_cipher.as_ref()?;
        Some(crypto::encrypt_with_cipher(cipher, &packed, &aad))
    }

    pub(crate) fn send_imfriend(&mut self) {
        let Some(encrypted) = self.build_imfriend_packet() else {
            return;
        };
        self.send_command(Command::ImFriend, &encrypted);
    }

    pub(crate) fn check_hello_send(&mut self, cur_tm: i64) {
        if !self.client.need_connect || self.client.force_disconnect {
            return;
        }
        let interval = self.client.round_trip_delay.max(1000) * 2;
        if (cur_tm - self.client.last_sent_hello).abs() <= interval {
            return;
        }

        match self.client.hello_wait_state {
            HelloWaitState::Idle => {
                if self.client.soft_reconnect && self.client.server_token != 0 {
                    self.send_rebind_hello_again(cur_tm, true);
                } else {
                    self.begin_primary_hello(cur_tm);
                }
            }
            HelloWaitState::PrimaryHelloCold | HelloWaitState::PrimaryHelloNewSession => {
                self.send_hello();
            }
            HelloWaitState::PrimaryImFriendSent => {
                self.send_imfriend();
            }
            HelloWaitState::RebindHelloAgain => {
                self.send_hello_again();
            }
        }
        self.client.last_sent_hello = cur_tm;
    }

    pub(crate) fn check_offline_reconnect(&mut self, cur_tm: i64) {
        let throttle = (self.client.round_trip_delay + 50).clamp(200, 1500);
        let last_online = self.client.last_online;
        let authorized = self.client.authorized;
        let waiting_rebind = self.client.hello_wait_state.allows_hello_again_retry();
        let stale_authorized = authorized
            && !self.client.need_connect
            && (cur_tm - last_online).abs() > OFFLINE_BASE_MS + self.client.round_trip_delay;

        if !waiting_rebind && !stale_authorized {
            return;
        }
        if (cur_tm - self.client.last_sent_hello).abs() <= throttle {
            return;
        }
        if self.client.server_token == 0 {
            self.client.auth_status = AuthStatus::Connected;
            self.client.authorized = false;
            self.client.need_connect = true;
            self.client.soft_reconnect = false;
            self.client.last_sent_hello = NEVER_SENT_MS;
            self.client.mark_next_primary_hello_new_session();
            return;
        }

        self.client.auth_status = AuthStatus::Offline;
        self.send_rebind_hello_again(cur_tm, !waiting_rebind);
        self.client.last_sent_hello = cur_tm;
    }

    pub(crate) fn check_reconnect_timeout(&mut self, cur_tm: i64) {
        if self.client.hello_wait_state.is_waiting()
            && (cur_tm - self.client.waiting_hello_start).abs() > RECONNECT_WAITING_MS
            && (cur_tm - self.client.last_socket_recreate).abs() > RECONNECT_THROTTLE_MS
        {
            let timed_out_state = self.client.hello_wait_state;
            self.client.last_socket_recreate = cur_tm;
            self.client.soft_reconnect =
                timed_out_state.allows_hello_again_retry() && self.client.server_token != 0;
            self.client.force_disconnect = true;
            self.client.need_connect = true;
            if !self.client.soft_reconnect
                && !matches!(timed_out_state, HelloWaitState::PrimaryHelloCold)
            {
                self.client.next_primary_hello_new_session = true;
            }
            self.client.clear_hello_wait_state();
            self.client.waiting_hello_start = 0;
            if !self.client.soft_reconnect {
                self.client.last_sent_hello = NEVER_SENT_MS;
            }
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
        if self.client.connected
            && !self.client.soft_reconnect
            && self.client.authorized
            && self.client.server_token != 0
        {
            self.send_session_close(self.client.now_ms());
        }
        self.client.clear_recv_poller();
        self.client.transport.socket = None;
        if !self.client.soft_reconnect {
            self.client.full_reset();
            self.client.clear_outbound_session_data();
        }
        self.client.connected = false;
        self.client.authorized = false;
        self.client.force_disconnect = false;
        self.client.clear_hello_wait_state();
    }

    pub(crate) fn send_session_close(&mut self, cur_tm: i64) {
        let mut item = SendItem {
            data: Vec::new(),
            cmd: Command::SessionClose.to_byte(),
            encrypted: true,
            priority: SendPriority::High,
            retry_left: initial_retry_left(true, 1),
            max_retries: 1,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };
        self.send_h_item(&mut item, cur_tm);
    }
}
