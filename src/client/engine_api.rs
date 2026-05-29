use super::*;

impl Client {
    /// Convenience: send an Engine API request (MPS_Sliced, encrypted, MaxRetries=6).
    /// Matches Delphi: `TEngineRequest` has explicit `MoonCmdPriority(MPS_Sliced)`,
    /// and `TCommandRegistry.InitRegistry` gives Sliced commands `MaxRetries=6`.
    #[doc(hidden)]
    pub(crate) fn send_api_request(&self, request_payload: &[u8]) {
        self.send_api_request_at(request_payload, self.now_ms());
    }

    fn mark_engine_request_queued_at(&self, request_payload: &[u8], now_ms: i64) {
        match engine_request_method(request_payload) {
            Some(EngineMethod::SubscribeAllTrades) => {
                self.last_trades_subscribe_request_ms
                    .store(now_ms, Ordering::Relaxed);
            }
            Some(EngineMethod::SubscribeOrderBook) => {
                self.last_orderbook_subscribe_request_ms
                    .store(now_ms, Ordering::Relaxed);
                self.last_orderbook_subscribe_request_uid.store(
                    engine_request_uid(request_payload).unwrap_or(NO_PENDING_ENGINE_REQUEST_UID),
                    Ordering::Relaxed,
                );
            }
            _ => {}
        }
    }

    pub(crate) fn send_api_request_at(&self, request_payload: &[u8], now_ms: i64) {
        self.mark_engine_request_queued_at(request_payload, now_ms);
        self.send_cmd(
            request_payload.to_vec(),
            Command::API,
            SendPriority::Sliced,
            true, // Engine API is always encrypted
            6,    // TEngineRequest effective MaxRetries for MPS_Sliced
        );
    }

    /// Send an Engine API request and register it in `api_pending`.
    ///
    /// The UID is read from the payload at offset `3..11` in the
    /// `TBaseCommand` header. Direct `rx.recv_timeout(...)` is only correct
    /// when another thread is already running the client loop.
    ///
    /// One-shot request helpers remove the pending slot when the caller's
    /// timeout expires. Raw receiver users should keep pumping the client until
    /// the response arrives.
    ///
    /// Before `domain_ready`, only the mandatory Init Engine API requests are
    /// queued. Other raw Engine API requests are rejected before `api_pending`
    /// registration; because this method is non-fallible, it returns a closed
    /// receiver in that case.
    #[doc(hidden)]
    pub(crate) fn send_api_request_async(
        &self,
        request_payload: &[u8],
    ) -> mpsc::Receiver<EngineResponse> {
        // Keep malformed raw requests as errors, not process panics: older code
        // sliced `request_payload[3..11]` directly.
        let Some(uid) = engine_request_uid(request_payload) else {
            log::warn!(target: "moonproto::client",
                "send_api_request_async: malformed Engine API request ({} bytes) — not queued",
                request_payload.len());
            let (_tx, rx) = mpsc::channel();
            return rx;
        };
        if !self.domain_ready
            && !outgoing_allowed_before_domain_ready(Command::API.to_byte(), request_payload)
        {
            log::warn!(target: "moonproto::client",
                "send_api_request_async: domain gate is closed before InitDone — Engine API request uid={} method={:?} not queued",
                uid,
                engine_request_method(request_payload).unwrap_or(EngineMethod::None));
            let (_tx, rx) = mpsc::channel();
            return rx;
        }
        let rx = self.api_pending.register(uid);
        self.send_api_request(request_payload);
        rx
    }

    /// Hidden diagnostic one-shot counterpart to [`Self::send_api_request_async`].
    ///
    /// It registers the pending UID, sends the request, manually pumps the
    /// low-level dispatcher path, and removes the pending slot if the caller's
    /// timeout expires. Regular applications use `MoonClient` non-blocking
    /// intents/events/snapshots instead.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn request_engine_response_for_init(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        request_payload: &[u8],
        timeout: Duration,
    ) -> Result<EngineResponse, mpsc::RecvTimeoutError> {
        let uid = engine_request_uid(request_payload);
        let rx = self.send_api_request_async(request_payload);
        match self.wait_for_receiver_in_owned_runtime(dispatcher, &rx, timeout) {
            Ok(resp) => Ok(resp),
            Err(err) => {
                if let Some(uid) = uid {
                    self.api_pending.remove(uid);
                }
                Err(err)
            }
        }
    }

    pub(crate) fn api_request_candles_data_async_registered(
        &mut self,
    ) -> (u64, mpsc::Receiver<MergedCandles>) {
        let raw = crate::commands::engine_request::request_candles_data();
        // UID comes from the BaseCommand header at offset 3..11, same as
        // `send_api_request_async`.
        let uid = raw
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let (tx, rx) = mpsc::channel();
        let partial = PartialCandles {
            aggregator: CandlesAggregator::new(),
            sender: tx,
        };
        // Replacing an existing slot is allowed: the old sender is dropped and
        // its receiver observes Disconnected, which is the correct double-call
        // behavior for this diagnostic helper.
        self.pending_candles.insert(uid, partial);
        self.send_api_request(&raw);
        (uid, rx)
    }

    /// Request the full chunked candles stream and wait for the merged result
    /// while the client loop keeps running.
    ///
    /// This is the one-shot counterpart to
    /// [`Self::api_request_candles_data_async`]. It registers the chunked
    /// aggregator, sends `emk_RequestCandlesData`, pumps the low-level active
    /// worker in short ticks, and removes the pending
    /// candles slot if the caller's timeout expires before the final chunk.
    #[cfg(test)]
    pub(crate) fn request_candles_data_for_test(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<MergedCandles, mpsc::RecvTimeoutError> {
        let (uid, rx) = self.api_request_candles_data_async_registered();
        match self.wait_for_receiver_in_owned_runtime(dispatcher, &rx, timeout) {
            Ok(merged) => {
                if dispatcher.apply_candles_snapshot(&merged.markets).is_some() {
                    if let Some(rx) = dispatcher.market_history_barrier_async() {
                        let _ = rx.recv();
                    }
                }
                Ok(merged)
            }
            Err(err) => {
                self.pending_candles.remove(&uid);
                Err(err)
            }
        }
    }
}
