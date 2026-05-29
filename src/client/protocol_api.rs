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
        // Active library: auto-clear indexes_fetch_in_flight на ответе
        // GetMarketsIndexes (любой — даже неуспешный, чтобы не зависнуть навсегда).
        if resp.method == EngineMethod::GetMarketsIndexes {
            self.indexes_fetch_in_flight = false;
            let indexes_payload_ok = resp.success
                && crate::commands::market::parse_markets_indexes_response(&resp.data).is_some();
            if indexes_payload_ok {
                // Запоминаем что для текущего PeerAppToken индексы получены.
                self.tracked_indexes_peer_app_token = self.peer_app_token;
                if self.update_markets_after_indexes {
                    self.update_markets_after_indexes = false;
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                if self.restore_orderbooks_after_indexes {
                    self.restore_orderbooks_after_indexes = false;
                    self.restore_orderbook_subscriptions_from_registry();
                }
            }
        }

        // Delphi `DoSubscribeOrderBooks`: только успешный ответ подтверждает
        // текущий `ServerToken`. Для reconnect batch это полный `BookSubbed`
        // replay; обычная точечная подписка может выставить token только в
        // initial state, как Delphi `FSubscribedBookServerToken = 0`.
        if resp.method == EngineMethod::SubscribeOrderBook {
            let is_reconnect_batch =
                self.pending_orderbook_resubscribe_uid == Some(resp.request_uid);
            if resp.success && (self.subscribed_book_server_token == 0 || is_reconnect_batch) {
                self.subscribed_book_server_token = self.server_token;
            }
            self.close_orderbook_subscribe_wait_if_matches(resp.request_uid);
            if is_reconnect_batch {
                self.pending_orderbook_resubscribe_uid = None;
            }
        }

        // Delphi `TMoonProtoEngine.SubscribeAllTrades`: successful
        // `emk_SubscribeAllTrades` refreshes `LastReconnectCheck`.
        // Until the first TradesStream packet updates `FTradesServerToken`,
        // this 5s gate prevents immediate unsubscribe/resubscribe churn.
        if resp.method == EngineMethod::SubscribeAllTrades && resp.success {
            let now_ms = self.now_ms();
            self.last_trades_reconnect_check_ms = now_ms;
        }
        if resp.method == EngineMethod::SubscribeAllTrades {
            self.last_trades_subscribe_request_ms
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
        // Engine API responses: попытаться доставить в pending registry / chunked
        // candles aggregator / internal recovery flags. Если UID не зарегистрирован —
        // пробрасываем как обычный data callback.
        if candles_chunk_consumed_by_reader {
            return Ok(());
        }
        if let Some(meta) = Self::engine_response_meta_from_payload(&payload) {
            // 1. Chunked candles (RequestCandlesData) — aggregator поддерживает
            // несколько response пакетов с одинаковым UID. До завершения сборки
            // не дропаем slot.
            let now_ms = self.now_ms();
            if meta.method == EngineMethod::RequestCandlesData {
                if let Some(resp) = parse_engine_response(&payload) {
                    if Self::handle_candles_chunk_in_map(&mut self.pending_candles, &resp, now_ms) {
                        // Чанк потреблён aggregator'ом. Передаём в on_data только
                        // если потребитель НЕ использует async API (тогда тут merged
                        // ещё не готов — пусть приложение видит сырые chunks).
                        // Однако: чтобы не путать — пропускаем on_data callback.
                        // Async-потребитель получит результат через Receiver<MergedCandles>.
                        return Ok(());
                    }
                }
            }
            // Если slot не зарегистрирован — fallback на pending registry /
            // on_data для fire-and-forget API users.

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

            // 2. Pending registry (обычный async API).
            let pending_consumed =
                api_pending_consumed_by_reader || self.api_pending.dispatch(resp).is_none();
            if !pending_consumed || sink.is_buffer() {
                // Если response не ждал конкретный receiver — это обычный API event.
                // Если ждал, но мы в Dispatcher mode, всё равно отдаём raw payload
                // dispatcher'у: active state (markets/indexes/tags) должен обновиться
                // независимо от того, ждёт ли user code этот же ответ через Receiver.
                // Callback mode сохраняет семантику: pending response не
                // дублируется в on_data callback.
                sink.deliver_owned(Command::API, payload);
            }
            return Ok(());
        }
        // Не распарсилось — fallback на raw sink.
        Err(payload)
    }

    /// Поглотить candles chunk через pending aggregator. Возвращает `true` если slot
    /// найден и chunk обработан (даже если merged ещё не готов — копить дальше);
    /// `false` если UID не зарегистрирован (потребитель не использует async API).
    ///
    /// Когда aggregator вернул merged — sender'у отправляется готовый `MergedCandles`,
    /// slot удаляется. Если sender уже дропнут (receiver не ждёт) — slot всё равно
    /// удаляется (semantic = "fire-and-forget с финализацией").
    pub(crate) fn handle_candles_chunk_in_map(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        resp: &EngineResponse,
        _now_ms: i64,
    ) -> bool {
        // Проверяем slot отдельным lookup — потом полное удаление через remove() если merged.
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
                        parse_request_candles_data_response_partial_like_delphi(&zipped_data)
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
