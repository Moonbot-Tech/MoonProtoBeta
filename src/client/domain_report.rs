use super::*;

pub(crate) const REPORT_RESPONSE_TIMEOUT_MS: i64 = 15_000;

impl Client {
    pub(crate) fn request_report_schema_at(&self, now_ms: i64) -> bool {
        let uid = Self::next_report_request_uid();
        let payload = crate::commands::report::build_schema_request(uid);
        let sent = self.send_trade(payload);
        if sent {
            self.reconnect
                .last_report_schema_request_ms
                .store(now_ms, Ordering::Relaxed);
        }
        sent
    }

    pub(crate) fn record_report_schema_received(&self, server_token: u64) {
        if server_token != 0 && self.server_token == server_token {
            self.reconnect
                .report_schema_server_token
                .store(server_token, Ordering::Relaxed);
        }
    }

    pub(crate) fn report_schema_is_current(&self) -> bool {
        self.server_token != 0
            && self
                .reconnect
                .report_schema_server_token
                .load(Ordering::Relaxed)
                == self.server_token
    }

    pub(crate) fn send_report_sync_at(
        &self,
        request_uid: u64,
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
            request_uid,
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
                .store(request_uid, Ordering::Relaxed);
            self.reconnect
                .pending_report_server_token
                .store(self.server_token, Ordering::Relaxed);
        }
        sent
    }

    pub(crate) fn record_report_page_received(&self, request_uid: u64, server_token: u64) {
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
            .report_page_waiting_apply_uid
            .store(request_uid, Ordering::Relaxed);
        self.reconnect
            .subscribed_report_server_token
            .store(server_token, Ordering::Relaxed);
    }

    pub(crate) fn finish_report_page_apply(
        &self,
        request_uid: u64,
        durable_request: Option<crate::state::ReportSyncRequest>,
    ) -> bool {
        if self
            .reconnect
            .report_page_waiting_apply_uid
            .compare_exchange(request_uid, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        if let Some(request) = durable_request {
            self.set_report_sync_intent(request);
        }
        true
    }

    pub(crate) fn report_page_is_waiting_apply(&self, request_uid: u64) -> bool {
        self.reconnect
            .report_page_waiting_apply_uid
            .load(Ordering::Relaxed)
            == request_uid
    }

    pub(crate) fn report_sync_intent(&self) -> Option<crate::state::ReportSyncRequest> {
        self.subscriptions.subscription_registry.lock().report_sync
    }

    pub(crate) fn set_report_sync_intent(&self, request: crate::state::ReportSyncRequest) {
        self.subscriptions.subscription_registry.lock().report_sync = Some(request);
    }

    pub(crate) fn report_open_rows_intent(&self) -> Arc<[i64]> {
        Arc::clone(
            &self
                .subscriptions
                .subscription_registry
                .lock()
                .report_open_rows,
        )
    }

    pub(crate) fn set_report_open_rows_intent(&self, rec_ids: Arc<[i64]>) {
        self.subscriptions
            .subscription_registry
            .lock()
            .report_open_rows = rec_ids;
        self.reconnect
            .subscribed_report_check_server_token
            .store(0, Ordering::Relaxed);
    }

    pub(crate) fn send_report_open_rows_check_at(&self, rec_ids: &[i64], now_ms: i64) -> bool {
        if rec_ids.is_empty() {
            return false;
        }
        let payload = crate::commands::report::build_check_rows_request(
            Self::next_report_request_uid(),
            rec_ids,
        );
        let sent = self.send_trade(payload);
        if sent {
            self.reconnect
                .last_report_check_request_ms
                .store(now_ms, Ordering::Relaxed);
            self.reconnect
                .pending_report_check_server_token
                .store(self.server_token, Ordering::Relaxed);
        }
        sent
    }

    pub(crate) fn complete_report_open_rows_check(&self, server_token: u64) {
        if server_token == 0 || self.server_token != server_token {
            return;
        }
        self.reconnect
            .subscribed_report_check_server_token
            .store(server_token, Ordering::Relaxed);
    }

    pub(crate) fn next_report_sync_ticket() -> crate::state::ReportSyncTicket {
        crate::state::ReportSyncTicket {
            sync_id: Self::next_report_request_uid(),
        }
    }

    pub(crate) fn next_report_request_uid() -> u64 {
        loop {
            let uid = rand::random();
            if uid != 0 {
                return uid;
            }
        }
    }
}
