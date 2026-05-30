use super::*;

impl Client {
    pub(crate) fn engine_response_request_uid_from_payload(payload: &[u8]) -> Option<u64> {
        // Engine response payload includes 11-byte TBaseCommand header, then
        // RequestUID. This is enough to cheaply check ApiPending without
        // inflating a full response in the receive phase.
        let uid = payload.get(11..19)?;
        Some(u64::from_le_bytes(uid.try_into().unwrap()))
    }

    pub(crate) fn engine_response_meta_from_payload(payload: &[u8]) -> Option<EngineResponseMeta> {
        if payload.len() < 11 {
            return None;
        }
        let mut pos = 11usize;
        let request_uid = u64::from_le_bytes(payload.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let method = EngineMethod::from_byte(*payload.get(pos)?);
        pos += 1;
        let success = *payload.get(pos)? != 0;
        pos += 1;
        // ErrorCode.
        payload.get(pos..pos + 4)?;
        pos += 4;
        // ErrorMsg string, length-prefixed UTF-8. Skip only; no allocation.
        let len = u16::from_le_bytes(payload.get(pos..pos + 2)?.try_into().ok()?) as usize;
        pos += 2;
        payload.get(pos..pos + len)?;
        Some(EngineResponseMeta {
            request_uid,
            method,
            success,
        })
    }

    pub(crate) fn engine_response_method_from_payload(payload: &[u8]) -> Option<EngineMethod> {
        payload.get(19).copied().map(EngineMethod::from_byte)
    }

    pub(crate) fn apply_engine_response_client_bookkeeping(&mut self, resp: &EngineResponse) {
        // Active library: auto-clear indexes_fetch_in_flight on a
        // GetMarketsIndexes response (any one — even unsuccessful, so it never
        // hangs forever).
        if resp.method == EngineMethod::GetMarketsIndexes {
            self.reconnect.indexes_fetch_in_flight = false;
            let indexes_payload_ok = resp.success
                && crate::commands::market::parse_markets_indexes_response(&resp.data).is_some();
            if indexes_payload_ok {
                // Remember that indexes have been received for the current PeerAppToken.
                self.reconnect.tracked_indexes_peer_app_token = self.peer_app_token;
                if self.reconnect.update_markets_after_indexes {
                    self.reconnect.update_markets_after_indexes = false;
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                if self.reconnect.restore_orderbooks_after_indexes {
                    self.reconnect.restore_orderbooks_after_indexes = false;
                    self.restore_orderbook_subscriptions_from_registry();
                }
            }
        }

        // Delphi `DoSubscribeOrderBooks`: only a successful response confirms
        // the current `ServerToken`. For the reconnect batch this is a full
        // `BookSubbed` replay; an ordinary point subscription may set the token
        // only from the initial state, like Delphi `FSubscribedBookServerToken = 0`.
        if resp.method == EngineMethod::SubscribeOrderBook {
            let is_reconnect_batch =
                self.reconnect.pending_orderbook_resubscribe_uid == Some(resp.request_uid);
            if resp.success
                && (self.reconnect.subscribed_book_server_token == 0 || is_reconnect_batch)
            {
                self.reconnect.subscribed_book_server_token = self.server_token;
            }
            self.close_orderbook_subscribe_wait_if_matches(resp.request_uid);
            if is_reconnect_batch {
                self.reconnect.pending_orderbook_resubscribe_uid = None;
            }
        }

        // Delphi `TMoonProtoEngine.SubscribeAllTrades`: successful
        // `emk_SubscribeAllTrades` refreshes `LastReconnectCheck`.
        // Until the first TradesStream packet updates `FTradesServerToken`,
        // this 5s gate prevents immediate unsubscribe/resubscribe churn.
        if resp.method == EngineMethod::SubscribeAllTrades && resp.success {
            let now_ms = self.now_ms();
            self.reconnect.last_trades_reconnect_check_ms = now_ms;
        }
        if resp.method == EngineMethod::SubscribeAllTrades {
            self.reconnect
                .last_trades_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
        }
        if resp.method == EngineMethod::UnsubscribeAllTrades {
            self.close_trades_unsubscribe_wait_if_matches(resp.request_uid);
        }
    }

    pub(crate) fn dispatch_api_pending_inline(
        api_pending: &ApiPending,
        cmd: u8,
        payload: &[u8],
    ) -> bool {
        if cmd != Command::API.to_byte() {
            return false;
        }
        let Some(uid) = Self::engine_response_request_uid_from_payload(payload) else {
            return false;
        };
        api_pending.dispatch_registered_with(uid, || parse_engine_response(payload))
    }

    pub(crate) fn dispatch_candles_chunk_inline(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        cmd: u8,
        payload: &[u8],
        now_ms: i64,
    ) -> bool {
        if cmd != Command::API.to_byte() {
            return false;
        }
        if Self::engine_response_method_from_payload(payload)
            != Some(EngineMethod::RequestCandlesData)
        {
            return false;
        }
        let Some(uid) = Self::engine_response_request_uid_from_payload(payload) else {
            return false;
        };
        if !pending_candles.contains_key(&uid) {
            return false;
        }
        let Some(resp) = parse_engine_response(payload) else {
            return false;
        };
        Self::handle_candles_chunk_in_map(pending_candles, &resp, now_ms)
    }

    pub(crate) fn client_new_data_decoded(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        sink: &mut DispatchSink<'_>,
    ) {
        if cmd == Command::API.to_byte() {
            match self.process_api_command_decoded(
                payload,
                api_pending_consumed_by_reader,
                candles_chunk_consumed_by_reader,
                sink,
            ) {
                Ok(()) => {
                    return;
                }
                Err(payload) => {
                    sink.deliver_owned(Command::from_byte(cmd), payload);
                    return;
                }
            }
        }

        sink.deliver_owned(Command::from_byte(cmd), payload);
    }

    pub(crate) fn process_api_command_decoded(
        &mut self,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        sink: &mut DispatchSink<'_>,
    ) -> Result<(), Vec<u8>> {
        // Engine API responses: try to deliver to the pending registry / chunked
        // candles aggregator / internal recovery flags. If the UID is not
        // registered, pass it through as an ordinary data callback.
        if candles_chunk_consumed_by_reader {
            return Ok(());
        }
        if let Some(meta) = Self::engine_response_meta_from_payload(&payload) {
            // 1. Chunked candles (RequestCandlesData) — the aggregator supports
            // multiple response packets with the same UID. Do not drop the slot
            // until assembly is complete.
            let now_ms = self.now_ms();
            if meta.method == EngineMethod::RequestCandlesData {
                if let Some(resp) = parse_engine_response(&payload) {
                    if Self::handle_candles_chunk_in_map(
                        &mut self.pending_api.pending_candles,
                        &resp,
                        now_ms,
                    ) {
                        // The chunk was consumed by the aggregator. Forward to
                        // on_data only if the consumer does NOT use the async API
                        // (in that case the merged result is not ready yet — let
                        // the application see raw chunks). However: to avoid
                        // confusion we skip the on_data callback. An async consumer
                        // gets the result via Receiver<MergedCandles>.
                        return Ok(());
                    }
                }
            }
            // If the slot is not registered — fall back to the pending registry /
            // on_data for fire-and-forget API users.

            let pending_side_effect_owner = api_pending_consumed_by_reader
                && sink.is_buffer()
                && method_applies_after_pending(meta.method);
            if pending_side_effect_owner {
                // Delphi `ProcessApiCommand` only stores the response into
                // `PendingRequests`; `TMoonProtoEngine.GetMarketsList` /
                // `UpdateMarketsList` applies heavy market state after
                // `SendAndWait` returns. Keep Rust's protocol dispatch path
                // equally thin: the runtime/init owner applies these payloads
                // from the pending receiver instead of doing a second parse here.
                return Ok(());
            }

            let Some(resp) = parse_engine_response(&payload) else {
                return Err(payload);
            };

            self.apply_engine_response_client_bookkeeping(&resp);

            // 2. Pending registry (ordinary async API).
            let pending_consumed = api_pending_consumed_by_reader
                || self.pending_api.api_pending.dispatch(resp).is_none();
            if !pending_consumed || sink.is_buffer() {
                // If no specific receiver was waiting for the response — it is an
                // ordinary API event. If one was waiting but we are in Dispatcher
                // mode, we still hand the raw payload to the dispatcher: active
                // state (markets/indexes/tags) must update regardless of whether
                // user code is also waiting for this response via a Receiver.
                // Callback mode preserves the semantics: a pending response is not
                // duplicated into the on_data callback.
                sink.deliver_owned(Command::API, payload);
            }
            return Ok(());
        }
        // Failed to parse — fall back to the raw sink.
        Err(payload)
    }

    /// Absorb a candles chunk through the pending aggregator. Returns `true` if the
    /// slot was found and the chunk was processed (even if merged is not ready yet —
    /// keep accumulating); `false` if the UID is not registered (the consumer does
    /// not use the async API).
    ///
    /// When the aggregator returns merged, the completed `MergedCandles` is sent to
    /// the sender and the slot is removed. If the sender has already been dropped
    /// (no receiver waiting), the slot is removed anyway (semantics =
    /// "fire-and-forget with finalization").
    pub(crate) fn handle_candles_chunk_in_map(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        resp: &EngineResponse,
        _now_ms: i64,
    ) -> bool {
        // Check the slot with a separate lookup — then full removal via remove() if merged.
        if !resp.success {
            if let Some(partial) = pending_candles.remove(&resp.request_uid) {
                log::warn!(target: "moonproto::client",
                    "candles request uid={} failed code={} msg={}",
                    resp.request_uid, resp.error_code, resp.error_msg);
                drop(partial);
                return true;
            }
            return false;
        }

        let uid = resp.request_uid;
        let chunk_result = {
            let Some(partial) = pending_candles.get_mut(&uid) else {
                return false;
            };
            let chunk_result = partial.aggregator.on_chunk_result(&resp.data);
            if matches!(
                chunk_result,
                CandlesChunkResult::Stored | CandlesChunkResult::Complete(_)
            ) {
                // Delphi updates `Markets.LastChunkTime` for the UI waiting
                // thread, but does not cancel the protocol-side collector on
                // that timeout. Rust keeps the pending slot until explicit
                // complete/error/reset/caller timeout.
            }
            chunk_result
        };
        if let CandlesChunkResult::Complete(zipped_data) = chunk_result {
            if let Some(partial) = pending_candles.remove(&uid) {
                Self::spawn_candles_parse_result(uid, zipped_data, partial.sender);
            }
        }
        true
    }

    fn spawn_candles_parse_result(
        uid: u64,
        zipped_data: Vec<u8>,
        sender: mpsc::Sender<MergedCandles>,
    ) {
        // Full candles parse is deliberately outside the protocol reader. The
        // reader has already ACKed all slices and only has to hand off the
        // completed Delphi StoreCandlesToZip stream; zlib parse/apply can take
        // milliseconds on a large market set and belongs to background work.
        let spawn = thread::Builder::new()
            .name("moonproto-candles-parse".to_string())
            .spawn(move || {
                let markets =
                    parse_request_candles_data_response(&zipped_data).unwrap_or_else(|| {
                        log::warn!(target: "moonproto::client",
                            "candles aggregator merged but strict parse failed for uid={} ({} bytes); trying Delphi partial apply",
                            uid,
                            zipped_data.len()
                        );
                        parse_request_candles_data_response_partial(&zipped_data)
                            .unwrap_or_default()
                    });
                let _ = sender.send(MergedCandles {
                    uid,
                    markets,
                });
            });
        if let Err(err) = spawn {
            log::warn!(target: "moonproto::client",
                "failed to spawn candles parse worker for uid={uid}: {err}");
        }
    }
}

#[inline]
fn method_applies_after_pending(method: EngineMethod) -> bool {
    matches!(
        method,
        EngineMethod::GetMarketsList | EngineMethod::UpdateMarketsList
    )
}
