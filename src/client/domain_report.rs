use super::*;

pub(crate) const REPORT_RESPONSE_TIMEOUT_MS: i64 = 30_000;

impl Client {
    pub(crate) fn request_report_schema_at(&self, now_ms: i64) -> bool {
        let uid = Self::next_report_sync_ticket().request_uid;
        let payload = crate::commands::report::build_schema_request(uid);
        let sent = self.send_trade(payload);
        if sent {
            self.reconnect
                .last_report_schema_request_ms
                .store(now_ms, Ordering::Relaxed);
        }
        sent
    }

    pub(crate) fn send_report_sync_at(
        &self,
        ticket: crate::state::ReportSyncTicket,
        request: crate::state::ReportSyncRequest,
        now_ms: i64,
    ) -> bool {
        if !request.is_valid() {
            return false;
        }
        let depth_days = if request.from_rec_id > 0 {
            0
        } else {
            request.history_depth.to_wire()
        };
        let payload = crate::commands::report::build_sync_request(
            ticket.request_uid,
            request.from_rec_id,
            depth_days,
        );
        let sent = self.send_trade(payload);
        if sent {
            self.reconnect
                .last_report_sync_request_ms
                .store(now_ms, Ordering::Relaxed);
            self.reconnect
                .pending_report_sync_uid
                .store(ticket.request_uid, Ordering::Relaxed);
            self.reconnect
                .pending_report_server_token
                .store(self.server_token, Ordering::Relaxed);
        }
        sent
    }

    pub(crate) fn record_report_sync_progress(&self, request_uid: u64, now_ms: i64) {
        if self
            .reconnect
            .pending_report_sync_uid
            .load(Ordering::Relaxed)
            == request_uid
        {
            self.reconnect
                .last_report_sync_request_ms
                .store(now_ms, Ordering::Relaxed);
        }
    }

    pub(crate) fn complete_report_sync(&self, request_uid: u64, server_token: u64) {
        if self
            .reconnect
            .pending_report_sync_uid
            .load(Ordering::Relaxed)
            != request_uid
            || self.server_token == 0
            || self.server_token != server_token
            || self
                .reconnect
                .pending_report_server_token
                .load(Ordering::Relaxed)
                != server_token
        {
            return;
        }
        self.reconnect
            .pending_report_sync_uid
            .store(0, Ordering::Relaxed);
        self.reconnect
            .subscribed_report_server_token
            .store(server_token, Ordering::Relaxed);
    }

    pub(crate) fn report_sync_intent(&self) -> Option<crate::state::ReportSyncRequest> {
        self.subscriptions.subscription_registry.lock().report_sync
    }

    pub(crate) fn set_report_sync_intent(&self, request: crate::state::ReportSyncRequest) {
        self.subscriptions.subscription_registry.lock().report_sync = Some(request);
    }

    pub(crate) fn next_report_sync_ticket() -> crate::state::ReportSyncTicket {
        loop {
            let request_uid = rand::random();
            if request_uid != 0 {
                return crate::state::ReportSyncTicket { request_uid };
            }
        }
    }
}
