use super::*;

impl Client {
    /// Convenience: send an Engine API request (MPS_Sliced, encrypted, MaxRetries=6).
    /// Matches Delphi: `TEngineRequest` has explicit `MoonCmdPriority(MPS_Sliced)`,
    /// and `TCommandRegistry.InitRegistry` gives Sliced commands `MaxRetries=6`.
    pub fn send_api_request(&self, request_payload: &[u8]) {
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
    /// `TBaseCommand` header. In single-threaded consumer code, prefer
    /// [`Self::request_engine_response`] or wait for the returned receiver
    /// through [`Self::run_until_response`] so the UDP loop keeps running.
    /// Direct `rx.recv_timeout(...)` is only correct when another thread is
    /// already running the client loop.
    ///
    /// One-shot request helpers remove the pending slot when the caller's
    /// timeout expires. Raw receiver users should keep pumping the client until
    /// the response arrives or use [`Self::request_engine_response`] when they
    /// need timeout-owned cleanup.
    ///
    /// Before `domain_ready`, only the mandatory Init Engine API requests are
    /// queued. Other raw Engine API requests are rejected before `api_pending`
    /// registration; because this method is non-fallible, it returns a closed
    /// receiver in that case.
    pub fn send_api_request_async(&self, request_payload: &[u8]) -> mpsc::Receiver<EngineResponse> {
        // D-V2-01 fix: безопасный slice-доступ к uid. Старая версия `request_payload[3..11]`
        // паниковала при len<11 — public API не должен валить процесс из-за плохого input'а.
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

    /// Send one Engine API request and wait for the matching `EngineResponse`
    /// while the client loop keeps running.
    ///
    /// This is the one-shot counterpart to [`Self::send_api_request_async`].
    /// It is the preferred single-threaded API when the caller wants a direct
    /// request/response operation: it registers the pending UID, sends the
    /// request, pumps the low-level active worker in short ticks, and removes
    /// the pending slot if the caller's timeout expires.
    pub fn request_engine_response(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        request_payload: &[u8],
        timeout: Duration,
    ) -> Result<EngineResponse, mpsc::RecvTimeoutError> {
        let uid = engine_request_uid(request_payload);
        let rx = self.send_api_request_async(request_payload);
        match self.run_until_response(dispatcher, &rx, timeout) {
            Ok(resp) => Ok(resp),
            Err(err) => {
                if let Some(uid) = uid {
                    self.api_pending.remove(uid);
                }
                Err(err)
            }
        }
    }

    fn request_engine_parsed<T>(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        request_payload: &[u8],
        timeout: Duration,
        parse: impl FnOnce(&[u8]) -> Option<T>,
    ) -> Result<T, EngineRequestError> {
        let resp = self
            .request_engine_response(dispatcher, request_payload, timeout)
            .map_err(EngineRequestError::from)?;

        if !resp.success {
            return Err(EngineRequestError::Server {
                method: resp.method,
                code: resp.error_code,
                message: resp.error_msg,
            });
        }

        let method = resp.method;
        let len = resp.data.len();
        parse(&resp.data).ok_or(EngineRequestError::MalformedPayload { method, len })
    }

    /// Run `emk_BaseCheck`, store the returned server identity in
    /// [`Self::server_info`], and return it.
    pub fn request_base_check(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<ServerInfo, EngineRequestError> {
        let resp = self
            .request_engine_response(
                dispatcher,
                &crate::commands::engine_request::base_check(),
                timeout,
            )
            .map_err(EngineRequestError::from)?;

        if !resp.success {
            return Err(EngineRequestError::Server {
                method: resp.method,
                code: resp.error_code,
                message: resp.error_msg,
            });
        }

        let info = parse_base_check_response(&resp.data);
        self.set_server_info(info.clone());
        Ok(info)
    }

    /// Run `emk_AuthCheck` and parse the account metadata payload.
    pub fn request_auth_check(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<AuthCheckResponse, EngineRequestError> {
        let auth = self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::auth_check(),
            timeout,
            parse_auth_check_response,
        )?;
        self.set_auth_info(auth.clone());
        Ok(auth)
    }

    /// Run `emk_GetBalance` and parse the returned quantity.
    pub fn request_balance(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        currency: &str,
        timeout: Duration,
    ) -> Result<f64, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::get_balance(currency),
            timeout,
            parse_get_balance_response,
        )
    }

    /// Run `emk_QueryHedgeMode` and parse the returned hedge-mode flag.
    pub fn request_hedge_mode(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<bool, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::query_hedge_mode(),
            timeout,
            parse_query_hedge_mode_response,
        )
    }

    /// Run `emk_CheckAPIExpirationTime` and parse the returned API-key expiration time.
    pub fn request_api_expiration_time(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<ApiExpirationTime, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::check_api_expiration_time(),
            timeout,
            parse_api_expiration_time_response,
        )
    }

    /// Run `emk_UpdateTransferAssets` and parse the transferable asset rows.
    ///
    /// `kind` is the server's exchange-wallet kind ordinal. The response rows
    /// contain the asset symbol, transferable amount, and total amount reported
    /// by the server.
    pub fn request_transfer_assets(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        kind: u8,
        timeout: Duration,
    ) -> Result<Vec<TransferAsset>, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::update_transfer_assets(kind),
            timeout,
            parse_update_transfer_assets_response,
        )
    }

    /// Run `emk_GetCoinCardCandles` and parse the returned historical candles.
    pub fn request_coin_card_candles(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        market: &str,
        ticks: crate::commands::candles::DeepHistoryKind,
        timeout: Duration,
    ) -> Result<Vec<DeepPrice>, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::candles::get_coin_card_candles(market, ticks),
            timeout,
            parse_coin_card_candles_response,
        )
    }

    // ====================================================================
    //  High-level Engine API wrappers (convenience over send_api_request_async)
    // ====================================================================

    /// `emk_BaseCheck` — initial probe (call before AuthCheck during handshake).
    pub fn api_base_check(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::base_check())
    }

    /// `emk_AuthCheck` — verify credentials and get account info.
    pub fn api_auth_check(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::auth_check())
    }

    /// `emk_GetMarketsList` — full markets list snapshot.
    pub fn api_get_markets_list(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_list())
    }

    /// `emk_GetMarketsIndexes` — market names в порядке mIndex.
    pub fn api_get_markets_indexes(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_indexes())
    }

    /// `emk_UpdateMarketsList` — обновление цен по mIndex.
    pub fn api_update_markets_list(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::update_markets_list())
    }

    /// `emk_GetBalance` для одной валюты.
    pub fn api_get_balance(&self, currency: &str) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_balance(currency))
    }

    /// `emk_GetMarketsBalanceFull` — trigger server-side full balance refresh.
    ///
    /// The current Delphi server does not serialize a balance snapshot in this
    /// response yet, so a successful response normally has empty `data`.
    pub fn api_get_markets_balance_full(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_balance_full())
    }

    /// `emk_GetOrder` by order UID.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method`.
    pub fn api_get_order(&self, order_uid: u64) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_order(order_uid))
    }

    /// `emk_GetOpenOrders`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method`.
    pub fn api_get_open_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_open_orders())
    }

    /// `emk_GetActiveOrders`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method`.
    pub fn api_get_active_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_active_orders())
    }

    /// `emk_CancelAllOrders`.
    pub fn api_cancel_all_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::cancel_all_orders())
    }

    /// `emk_SetLeverage(market, new_leverage)`.
    pub fn api_set_leverage(&self, market: &str, new_lev: i32) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_leverage(
            market, new_lev,
        ))
    }

    /// `emk_SetHedgeMode(enabled)`.
    pub fn api_set_hedge_mode(&self, hedge_mode: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_hedge_mode(hedge_mode))
    }

    /// `emk_QueryHedgeMode`.
    pub fn api_query_hedge_mode(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::query_hedge_mode())
    }

    /// `emk_CheckAPIExpirationTime`.
    pub fn api_check_expiration_time(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::check_api_expiration_time())
    }

    /// `emk_CheckBinanceTags` — теги монет.
    pub fn api_check_binance_tags(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::check_binance_tags())
    }

    /// `emk_SubscribeAllTrades`.
    pub fn api_subscribe_all_trades(&self, want_mm_orders: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::subscribe_all_trades(
            want_mm_orders,
        ))
    }

    /// `emk_UnsubscribeAllTrades`.
    pub fn api_unsubscribe_all_trades(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::unsubscribe_all_trades())
    }

    /// `emk_SubscribeOrderBook` — `markets` empty = подписка на все.
    ///
    /// **Low-level вариант** (не обновляет subscription registry, не resolve'ит market_name).
    /// Для нормальной работы используй [`Client::subscribe_orderbook`].
    pub fn api_subscribe_order_book(&self, markets: &[&str]) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::subscribe_order_book(
            markets,
        ))
    }

    /// `emk_UnsubscribeOrderBook` — `markets` empty = отписка от всех.
    ///
    /// **Low-level вариант** (не обновляет registry). См. [`Client::unsubscribe_orderbook`].
    pub fn api_unsubscribe_order_book(&self, markets: &[&str]) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::unsubscribe_order_book(
            markets,
        ))
    }

    /// `emk_RequestOrderBookFull(market_idx, book_kind)` — запрос полного snapshot.
    pub fn api_request_order_book_full(
        &self,
        market_idx: u16,
        book_kind: u8,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::request_order_book_full(
            market_idx, book_kind,
        ))
    }

    /// `emk_ReloadOrderBook`.
    pub fn api_reload_order_book(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::reload_order_book())
    }

    /// `emk_ChangePositionType(market, type, new_market)`.
    pub fn api_change_position_type(
        &self,
        market: &str,
        pos_type: u8,
        new_market: bool,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::change_position_type(
            market, pos_type, new_market,
        ))
    }

    /// `emk_ConvertDustBNB`.
    pub fn api_convert_dust_bnb(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::convert_dust_bnb())
    }

    /// `emk_ConfirmRiskLimit(market)`.
    pub fn api_confirm_risk_limit(&self, market: &str) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::confirm_risk_limit(market))
    }

    /// `emk_SetMAMode(enabled)`.
    pub fn api_set_ma_mode(&self, ma_mode: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_ma_mode(ma_mode))
    }

    /// `emk_DoTransferAsset(asset, q, from, to)`.
    pub fn api_do_transfer_asset(
        &self,
        asset: &str,
        qty: f64,
        from: u8,
        to: u8,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::do_transfer_asset(
            asset, qty, from, to,
        ))
    }

    /// `emk_UpdateTransferAssets(kind)`.
    pub fn api_update_transfer_assets(&self, kind: u8) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::update_transfer_assets(
            kind,
        ))
    }

    /// `emk_TradesResend(packet_nums)` — multi-batch (auto-split по 200).
    /// Возвращает массив receivers (по одному на batch).
    pub fn api_trades_resend_batches(
        &self,
        packet_nums: &[u16],
    ) -> Vec<mpsc::Receiver<EngineResponse>> {
        crate::commands::engine_request::trades_resend_batches(packet_nums)
            .iter()
            .map(|raw| self.send_api_request_async(raw))
            .collect()
    }

    /// `emk_GetCoinCardCandles(market, ticks)` — запрос свечей для CoinCard (не chunked).
    /// Response — `count:i32 + N × TDeepPrice(28 bytes)`. Парсить через
    /// `commands::candles::parse_coin_card_candles_response(&resp.data)`.
    pub fn api_get_coin_card_candles(
        &self,
        market: &str,
        ticks: crate::commands::candles::DeepHistoryKind,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::candles::get_coin_card_candles(
            market, ticks,
        ))
    }

    /// `emk_RequestCandlesData` — низкоуровневый fire-and-forget. Сервер пришлёт
    /// несколько chunked `EngineResponse`-пакетов с одинаковым `request_uid`.
    /// **Для нормальной работы используй [`Client::api_request_candles_data_async`]**
    /// — он автоматически агрегирует chunks через [`CandlesAggregator`] и возвращает
    /// `Receiver<MergedCandles>` для blocking-ожидания финального результата.
    pub fn api_request_candles_data(&self) {
        self.send_api_request(&crate::commands::engine_request::request_candles_data());
    }

    pub(crate) fn api_request_candles_data_async_registered(
        &mut self,
    ) -> (u64, mpsc::Receiver<MergedCandles>) {
        let raw = crate::commands::engine_request::request_candles_data();
        // UID извлекается из BaseCommand header offset 3..11 (тот же что в send_api_request_async).
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
        // Замещение существующего slot'а допустимо — старый sender дропнется, его
        // receiver получит Err(Disconnected) (что корректно при двойном вызове).
        self.pending_candles.insert(uid, partial);
        self.send_api_request(&raw);
        (uid, rx)
    }

    /// **Async-вариант `emk_RequestCandlesData`** — отправляет запрос и регистрирует
    /// chunked aggregator. Возвращает `Receiver<MergedCandles>` — потребитель ждёт
    /// его пока main loop продолжает крутиться и получает уже собранный zlib stream
    /// от Delphi `TMarkets.StoreCandlesToZip` плюс parsed market entries.
    ///
    /// Сервер шлёт несколько `EngineResponse` пакетов с одинаковым `request_uid`,
    /// каждый — chunk `ChunkIndex:u16 + ChunkTotal:u16 + payload`. Liба сама агрегирует
    /// через `CandlesAggregator`, парсит через `parse_request_candles_data_response`,
    /// уведомляет sender → потребитель получает `MergedCandles`.
    ///
    /// Pending slot lives until complete/error, session reset, another request
    /// with the same UID replaces it, or a one-shot caller timeout removes it.
    /// Delphi likewise does not cancel `CandlesRequestUID` when the UI wait
    /// loop stops after `Markets.LastChunkTime` timeout.
    pub fn api_request_candles_data_async(&mut self) -> mpsc::Receiver<MergedCandles> {
        self.api_request_candles_data_async_registered().1
    }

    /// Request the full chunked candles stream and wait for the merged result
    /// while the client loop keeps running.
    ///
    /// This is the one-shot counterpart to
    /// [`Self::api_request_candles_data_async`]. It registers the chunked
    /// aggregator, sends `emk_RequestCandlesData`, pumps the low-level active
    /// worker in short ticks, and removes the pending
    /// candles slot if the caller's timeout expires before the final chunk.
    pub fn request_candles_data(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<MergedCandles, mpsc::RecvTimeoutError> {
        let (uid, rx) = self.api_request_candles_data_async_registered();
        match self.run_until_response(dispatcher, &rx, timeout) {
            Ok(merged) => {
                dispatcher.apply_candles_snapshot(&merged.markets);
                Ok(merged)
            }
            Err(err) => {
                self.pending_candles.remove(&uid);
                Err(err)
            }
        }
    }
}
