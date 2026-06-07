use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    pub(crate) fn apply_reader_sliced_stats(&mut self, stats: ReaderSlicedStats) {
        let dup_pct = stats.dup_count as f64 / stats.blocks_count.max(1) as f64 * 100.0;
        if self.client.avg_dup_count == 0.0 {
            self.client.avg_dup_count = dup_pct;
        } else {
            self.client.avg_dup_count = (self.client.avg_dup_count * 9.0 + dup_pct) * 0.1;
        }
    }

    pub(crate) fn apply_wrong_hello(&mut self) {
        self.client.auth_status = AuthStatus::Connected;
    }

    pub(crate) fn apply_want_new_hello(&mut self) {
        self.client.full_reset();
        self.client.last_sent_hello = NEVER_SENT_MS;
        self.client.auth_status = AuthStatus::Connected;
        self.client.authorized = false;
        self.client.need_connect = true;
        self.client.soft_reconnect = false;
        self.client.mark_next_primary_hello_new_session();
    }

    pub(crate) fn apply_need_hello_again(&mut self, timestamp_ms: i64) {
        if (timestamp_ms - self.client.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS {
            self.client.last_need_hello_again = timestamp_ms;
            self.client.need_connect = true;
            self.client.soft_reconnect = true;
            if self.client.hello_wait_state.allows_hello_again_retry() {
                self.client.last_sent_hello = NEVER_SENT_MS;
                return;
            }
            self.client.clear_hello_wait_state();
            self.client.waiting_hello_start = 0;
            self.client.last_sent_hello = NEVER_SENT_MS;
        }
    }

    pub(crate) fn apply_hello_and_build_imfriend(&mut self, hello: handshake::Hello) -> Vec<u8> {
        self.client.accepted_server_mix_ts(hello.mix_ts);
        self.client.server_token = hello.server_token;
        let prev_app_token = self.client.peer_app_token;
        self.client.peer_app_token = hello.app_token;
        if prev_app_token != 0 && prev_app_token != hello.app_token {
            self.client.reconnect.indexes_fetch_in_flight = false;
            self.client.reconnect.tracked_indexes_peer_app_token = 0;
            self.client.fire_lifecycle(LifecycleEvent::ServerRestart);
        }

        self.client.session_rnd = hello.rnd;
        let (encode_key, decode_key) = crypto::generate_session_sub_keys(
            &self.client.cfg.master_key,
            self.client.cfg.client_id,
            self.client.server_token,
            &self.client.session_rnd,
        );
        self.client.encode_key = encode_key;
        self.client.decode_key = decode_key;
        let encode_cipher = crate::crypto::cipher_from_key(&self.client.encode_key);
        self.client.encode_cipher = Some(encode_cipher.clone());
        self.client
            .recv
            .data_read_state
            .set_decode_cipher(crate::crypto::cipher_from_key(&self.client.decode_key));
        self.client.refresh_ack_session32();

        self.client.handshake_peer_mix = hello.peer_mix;
        self.build_imfriend_packet().unwrap_or_default()
    }

    pub(crate) fn apply_fine_auth_done(&mut self) {
        let restore_after_reconnect =
            self.client.subscriptions.domain_ready && self.client.lifecycle.was_ever_connected;
        self.client.need_connect = false;
        self.client.auth_status = AuthStatus::AuthDone;
        self.client.authorized = true;
        self.client.clear_hello_wait_state();
        self.client.handshake_peer_mix = 0;
        if restore_after_reconnect {
            self.client.restore_domain_after_reconnect();
        }
    }
}
