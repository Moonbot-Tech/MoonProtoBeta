use super::*;

impl Client {
    // ====================================================================
    //  High-level Trade wrappers (convenience over commands::trade::build_*)
    //  Все шлются как Command::Order (28), Priority=High, encrypted, MaxRetries=3.
    //  Кроме DoClose/DoLimitClose/DoSplit/DoSellOrder/DoMarketSplit — MaxRetries=1.
    // ====================================================================

    /// Send `TNewOrderCommand` (CmdId=3) to open a new order.
    pub fn new_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) {
        let raw = crate::commands::trade::build_new_order(
            ctx, market, is_short, price, strat_id, order_size,
        );
        self.send_trade(raw, 3);
    }

    /// Delphi local replace request + `TOrderReplaceCommand` (CmdId=6,
    /// `UK_OrderMove`) with a new price.
    ///
    /// Requires the local `Orders` read model. The wrapper derives market route
    /// and order type from the local order and repeats the Delphi
    /// `ReplaceSentTime = 0` gate.
    pub fn replace_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, order_type, price)) =
            orders.send_replace_if_requested(uid, new_price, self.now_ms())
        else {
            return false;
        };
        let raw = crate::commands::trade::build_order_replace(ctx, &market, order_type, price);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Replace an order already tracked by `EventDispatcher::orders()`.
    pub fn replace_tracked_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        self.replace_order(orders, uid, new_price)
    }

    /// Send low-level `TAllStatusesReq` (CmdId=9).
    ///
    /// Regular applications should prefer [`Self::request_order_snapshot`].
    pub fn request_all_statuses(&self, uid: u64) {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw, 3);
    }

    /// Request the current order snapshot and wait until it is applied to
    /// `EventDispatcher::orders()`.
    ///
    /// This is the high-level consumer helper for `TAllStatusesReq`. It hides the
    /// protocol UID, pumps the UDP loop while waiting, and also waits for the
    /// active dispatcher to finish Delphi `CleanupMissingWorkers` follow-up
    /// requests for orders absent from the snapshot.
    pub fn request_order_snapshot(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<Vec<crate::state::Order>, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let previous_snapshot_flag = dispatcher.orders().current_snapshot_flag();
        let start = Instant::now();
        self.request_all_statuses(rand::random());

        loop {
            let snapshot_seen =
                dispatcher.orders().current_snapshot_flag() != previous_snapshot_flag;
            if snapshot_seen && dispatcher.orders().missing_after_snapshot().is_empty() {
                return Ok(dispatcher.orders().iter().cloned().collect());
            }

            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let tick = remaining.min(TICK);
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
        }
    }

    /// Delphi local cancel request + `TOrderCancelCommand` (CmdId=10,
    /// `UK_OrderMove`) for one order.
    ///
    /// Requires the local `Orders` read model. The wrapper derives current
    /// status from the local order and clears the local request after queueing.
    pub fn cancel_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(request) = orders.send_cancel_if_requested(uid, self.now_ms()) else {
            return false;
        };
        self.send_order_cancel_request(request);
        true
    }

    /// Cancel an order already tracked by `EventDispatcher::orders()`.
    pub fn cancel_tracked_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        self.cancel_order(orders, uid)
    }

    /// Send `TJoinOrdersCommand` (CmdId=11) to join open orders.
    pub fn join_orders(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_join_orders(ctx, market, is_short);
        self.send_trade(raw, 3);
    }

    /// Send `TSplitOrderCommand` (CmdId=12) to split an order into parts.
    pub fn split_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        let raw = crate::commands::trade::build_split_order(
            ctx,
            market,
            split_parts,
            split_small,
            split_small_sell,
        );
        self.send_trade(raw, 3);
    }

    /// Split an order already tracked by `EventDispatcher::orders()`.
    pub fn split_tracked_order(
        &self,
        order: &crate::state::Order,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        self.split_order(
            order.trade_ctx(),
            &order.market_name,
            split_parts,
            split_small,
            split_small_sell,
        );
    }

    /// `TMoveAllSellsCommand` (CmdId=13), gated like Delphi active-client UI.
    ///
    /// The move mode, price, zone and side live in [`crate::commands::trade::MoveAllSellsParams`]
    /// to keep the public API resistant to swapped positional arguments.
    pub fn move_all_sells(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::MoveAllSellsParams,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_sells_candidate(market, params) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_sells(ctx, market, params);
        self.send_trade(raw, 3);
        true
    }

    /// `TDoClosePositionCommand` (CmdId=14, MaxRetries=1).
    pub fn do_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        market_sell: bool,
    ) {
        let raw = crate::commands::trade::build_do_close_position(ctx, market, market_sell);
        self.send_trade(raw, 1);
    }

    /// `TDoLimitClosePositionCommand` (CmdId=15, MaxRetries=1).
    pub fn do_limit_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_limit_close_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TDoSplitPositionCommand` (CmdId=16, MaxRetries=1).
    pub fn do_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TDoSellOrderCommand` (CmdId=17, MaxRetries=1).
    pub fn do_sell_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        price: f64,
        size: f64,
    ) {
        let raw = crate::commands::trade::build_do_sell_order(ctx, market, price, size);
        self.send_trade(raw, 1);
    }

    /// `TOrderStatusRequest` (CmdId=18) — запросить статус конкретного ордера.
    pub fn request_order_status(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_order_status_request(ctx, market);
        self.send_trade(raw, 3);
    }

    /// Request a fresh status for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn request_tracked_order_status(&self, order: &crate::state::Order) {
        self.request_order_status(order.trade_ctx(), &order.market_name);
    }

    /// Delphi `SendStopsIfChanged` + `TOrderStopsUpdate` (CmdId=20,
    /// UK_OrderMove).
    ///
    /// Requires the local `Orders` read model: if the UID is unknown or the
    /// stop record did not change, Delphi would not put a packet on the wire.
    pub fn update_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, status, stops)) = orders.send_stops_if_changed(uid, stops) else {
            return false;
        };
        let raw = crate::commands::trade::build_order_stops_update(ctx, &market, 0, status, &stops);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        self.update_order_stops(orders, uid, stops)
    }

    /// Delphi `TOrdersWorkers.TurnPanicSell`: set panic sell for every local
    /// active sell order in `market_name`.
    pub fn turn_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        market_name: &str,
        turn_on: bool,
    ) -> usize {
        if !self.domain_ready_for_typed_send() {
            return 0;
        }
        let requests = orders.turn_panic_sell_by_market(market_name, turn_on);
        let queued = requests.len();
        for request in requests {
            self.send_panic_sell_request(request);
        }
        queued
    }

    /// Delphi `TOrdersWorkers.SwitchPanicSellByMarket` button semantics.
    pub fn switch_panic_sell_by_market(
        &self,
        orders: &mut crate::state::Orders,
        market_name: &str,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let (panic_sell_on, requests) = orders.switch_panic_sell_by_market(market_name, turn_on);
        for request in requests {
            self.send_panic_sell_request(request);
        }
        panic_sell_on
    }

    /// Delphi per-worker panic-sell flag + `TTurnPanicSellCommand` (CmdId=21,
    /// UK_OrderMove).
    pub fn turn_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(request) = orders.send_panic_sell_if_changed(uid, turn_on) else {
            return false;
        };
        self.send_panic_sell_request(request);
        true
    }

    /// Toggle panic sell for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn turn_tracked_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        self.turn_order_panic_sell(orders, uid, turn_on)
    }

    /// Apply Delphi `SetImmuneClicks` locally and send `TSetImmuneCommand`
    /// (CmdId=22, `UK_ImmuneClicks`) for found active orders.
    ///
    /// The dedup UID is `sum(items[].uid)`, matching Delphi
    /// `TSetImmuneCommand.SetUKey`.
    pub fn set_immune(
        &self,
        orders: &mut crate::state::Orders,
        items: &[crate::commands::trade::ImmuneItem],
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let applied = orders.set_immune_clicks(items);
        if applied.is_empty() {
            return false;
        }
        let raw = crate::commands::trade::build_set_immune(rand::random(), &applied);
        let items_uid_sum: u64 = applied
            .iter()
            .fold(0u64, |acc, it| acc.wrapping_add(it.uid));
        self.send_trade_keyed(raw, 3, UniqueKey::immune_clicks(items_uid_sum));
        true
    }

    /// `TMoveAllBuysCommand` (CmdId=27), gated like Delphi active-client UI.
    pub fn move_all_buys(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        cmd_type: crate::commands::trade::MoveAllBuysCmdType,
        move_kind: crate::commands::trade::ReplaceMultiKind,
        price: f64,
        side: crate::commands::trade::FixedPosition,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_buys_candidate(market, cmd_type, move_kind, side) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_buys(
            ctx, market, cmd_type, move_kind, price, side,
        );
        self.send_trade(raw, 3);
        true
    }

    /// Delphi `SendVStopIfChanged` + `TVStopUpdate` (CmdId=29, `UK_OrderMove`).
    ///
    /// Requires the local `Orders` read model: the wrapper derives the current
    /// worker status, mutates local VStop state, and queues nothing if the value
    /// did not change.
    pub fn update_vstop(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, params)) =
            orders.send_vstop_if_changed(uid, vstop_on, vstop_fixed, vstop_level, vstop_vol)
        else {
            return false;
        };
        let raw = crate::commands::trade::build_vstop_update(ctx, &market, 0, params);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_vstop(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> bool {
        self.update_vstop(orders, uid, vstop_on, vstop_fixed, vstop_level, vstop_vol)
    }

    /// `TDoMarketSplitPositionCommand` (CmdId=30, MaxRetries=1).
    pub fn do_market_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_market_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TPenaltyCommand` (CmdId=23) to mark a market as under strategy
    /// penalty/cooldown.
    ///
    /// Manual and alert strategies are intentionally not blocked by this server
    /// flag; it affects automatic strategy checks.
    pub fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw, 3);
    }

    // ====================================================================
    //  High-level UI wrappers (Command::UI, encrypted=true)
    //  Покрывают MClient.SendUICmd(T*Command.Create(...)) семантику Delphi.
    //  UID авто-генерируется через rand::random() — потребитель не передаёт.
    //  Priority/MaxRetries/UKey — из атрибутов соответствующих Delphi-классов.
    //  Аудит docs_api B-01: было 14 build_* функций без Client-обёрток.
    // ====================================================================

    /// Send `TClientSettingsCommand` (UI CmdId=1, Sliced,
    /// `UK_BaseUISettings`).
    ///
    /// This sends a full client-settings snapshot and replaces any older
    /// pending settings packet with the same UKey slot.
    pub fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let mut wire_settings = settings.clone();
        wire_settings.uid = rand::random();
        let raw = crate::commands::ui::build_client_settings(&wire_settings);
        self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::base_ui_settings_slot(),
        );
    }

    /// Send `TSettingsRequest` (UI CmdId=2, High) to request current settings.
    pub fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Request the current UI settings snapshot and wait for the next
    /// `TClientSettingsCommand` while pumping the UDP loop.
    ///
    /// This is the UI-channel counterpart to [`Self::run_until_response`] for
    /// Engine API calls. `TSettingsRequest` does not carry a request/response
    /// UID pair on the wire: Delphi answers by sending a fresh
    /// `TClientSettingsCommand`. The helper therefore waits until
    /// `EventDispatcher` observes the next applied settings snapshot; the
    /// snapshot UID is not required to change because the server may resend the
    /// current settings object unchanged. The low-level Delphi command is
    /// fire-and-forget, so this helper reissues `TSettingsRequest` every few
    /// seconds while waiting.
    pub fn request_client_settings(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<crate::commands::ui::ClientSettingsCommand, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let first_new_event = dispatcher.queued_event_count();
        let start = Instant::now();
        let mut next_request_at = start + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
        self.ui_settings_request();

        loop {
            if queued_client_settings_updated_since(dispatcher, first_new_event) {
                if let Some(settings) = dispatcher.settings().client_settings.as_ref() {
                    return Ok(settings.clone());
                }
            }

            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let now = Instant::now();
            if now >= next_request_at {
                self.ui_settings_request();
                next_request_at = now + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
            }

            let tick = remaining.min(TICK);
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
        }
    }

    /// Send `TStratStartStopCommand` (UI CmdId=3, High) to start or stop all
    /// strategies.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommandV2` (UI CmdId=4, High) with an explicit
    /// checked delta.
    ///
    /// Regular active-library callers should prefer
    /// `EventDispatcher::ui_strat_start_stop_v2`, which builds the delta from
    /// owned strategy state like Delphi `TStratStartStopCommandV2.Create`.
    pub fn ui_strat_start_stop_v2(
        &self,
        is_start: bool,
        items: &[crate::commands::strat::StratCheckedItem],
    ) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TMMOrdersSubscribeCommand` (UI CmdId=5, High,
    /// `UK_TurnMMDetection`) to set the market-maker orders subscription flag.
    pub fn ui_mm_subscribe(&self, subscribe: bool) {
        self.sender().ui_mm_subscribe(subscribe);
    }

    /// `TUpdateVersionCommand` (UI CmdId=6, High) — request a MoonBot version update.
    ///
    /// Delphi uses this from the update UI:
    /// - release button sends `VersionName=""`, `IsRelease=true`;
    /// - beta/test install command sends the requested version name and release flag.
    ///
    /// The server handles the command and broadcasts the same UI command back to
    /// clients. Delphi clients then run their local updater in
    /// `HandleRemoteUpdateCommand`; this Rust wrapper only sends the protocol
    /// command and marks Delphi `ServerUpdateSent` so the next init uses the
    /// update-aware BaseCheck retry path.
    pub fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        if self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3) {
            self.mark_server_update_sent();
        }
    }

    /// Send `TEmuTradesCommand` (UI CmdId=7, Sliced) with emulated trades for a
    /// test market.
    pub fn ui_emu_trades(
        &self,
        m_index: u16,
        base_time: f64,
        points: &[crate::commands::ui::EmuTradePoint],
    ) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TLevManageCommand` (UI CmdId=9, Sliced,
    /// `UK_LevManageSettings`) with leverage-management settings.
    pub fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::lev_manage_settings_slot(),
        );
    }

    /// Send `TTriggerManageCommand` (UI CmdId=10, Sliced) for batch trigger
    /// management over all markets or selected market/key pairs.
    pub fn ui_trigger_manage(&self, action: u8, all_markets: bool, markets: &[u16], keys: &[u16]) {
        let raw = crate::commands::ui::build_trigger_manage(
            rand::random(),
            action,
            all_markets,
            markets,
            keys,
        );
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TResetProfitCommand` (UI CmdId=11, High) to reset profit counters.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TArbActivateNotify` (UI CmdId=12, High) with an arbitration-valid
    /// timestamp.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TSwitchDexCommand` (UI CmdId=13, High, `UK_DexSwitch`).
    ///
    /// The DEX name is truncated to the Delphi 15-byte short-string payload.
    pub fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        if self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::dex_switch_for(uid),
        ) {
            self.mark_server_update_sent();
        }
    }

    /// Send `TSwitchSpotCommand` (UI CmdId=14, High, `UK_SpotSwitch`) to select
    /// the spot mode.
    pub fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        if self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::spot_switch_for(uid),
        ) {
            self.mark_server_update_sent();
        }
    }

    // ====================================================================
    //  High-level Strat wrappers (Command::Strat, encrypted=true)
    //  Покрывают MClient.SendStratCmd(T*Command.Create(...)) семантику Delphi.
    //  Аудит docs_api B-02: было 5 build_* функций без Client-обёрток.
    // ====================================================================

    /// Send `TStratSchemaRequest` (Strat CmdId=7, High).
    ///
    /// Agreed active-library behavior: one-time Init requests the live Delphi
    /// strategy schema from the server and stores the decoded result in
    /// `EventDispatcher::strats().strategy_schema()`. Public callers normally
    /// read that state instead of sending this manually.
    pub fn strat_schema_request(&self) {
        let raw = crate::commands::strat::build_schema_request(rand::random());
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    fn send_strat_snapshot_command(&self, raw: Vec<u8>) {
        self.send_domain_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::strat_snapshot(),
        );
    }

    /// `TStratSnapshot` (Strat CmdId=2, Sliced, UK_StratSnapshot) from an already
    /// serialized `TStrategySerializer` payload.
    ///
    /// `data` is only the `TStratSnapshot.Data` blob. The method adds the required
    /// Delphi fields: `ServerEpoch`, `ClientMaxLastDate`, `Size`, and `Full`.
    /// Use [`Client::strat_send_snapshot_batch`] when the application has decoded
    /// `StrategySnapshot` values rather than a prebuilt serializer payload.
    pub fn strat_send_snapshot_payload(
        &self,
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: &[u8],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot(
            uid,
            server_epoch,
            client_max_last_date,
            full,
            data,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// `TStratSnapshot` (Strat CmdId=2, Sliced, UK_StratSnapshot) from typed
    /// strategy snapshots.
    ///
    /// This is the high-level counterpart to Delphi `CreateFromStrats` /
    /// `CreateFromList`: it serializes the batch, computes `ClientMaxLastDate`,
    /// and sends a valid CmdId=2 packet. `schema` must be the live
    /// `TStratSchema` fetched during Init.
    pub fn strat_send_snapshot_batch(
        &self,
        server_epoch: u64,
        full: bool,
        schema: &crate::commands::strategy_schema::StrategySchema,
        strategies: &[crate::commands::strategy_serializer::StrategySnapshot],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot_from_strategies(
            uid,
            server_epoch,
            full,
            schema,
            strategies,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratDelete` (Strat CmdId=3, High) for one strategy or folder.
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSellPriceUpdate` (Strat CmdId=4, High,
    /// `UK_StratSellPriceUpdate`) for one strategy.
    ///
    /// The UKey includes `strategy_id`, so dedup is per strategy.
    pub fn strat_sell_price_update(&self, strategy_id: u64, sell_price: f64) {
        let raw = crate::commands::strat::build_sell_price_update(
            rand::random(),
            strategy_id,
            sell_price,
        );
        self.send_domain_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::High,
            true,
            3,
            UniqueKey::strat_sell_price_update(strategy_id),
        );
    }

    /// Send `TStratCheckedSync` (Strat CmdId=5, Sliced) with explicit checked
    /// items.
    ///
    /// `is_delta = false` sends a full list; `true` sends a delta.
    /// Regular active-library callers should prefer
    /// `EventDispatcher::send_strategy_checked_delta`, which builds Delphi
    /// `TStrategies.GetCheckedDelta` from owned strategy state.
    pub fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }

    // ====================================================================
    //  High-level Balance wrappers (Command::Balance, encrypted=true)
    //  Покрывают MClient.SendBalanceCmd семантику Delphi.
    //  Аудит docs_api B-03: ранее не было ни build_, ни Client-wrapper'а.
    // ====================================================================

    /// Send `TRequestBalanceRefresh` (Balance CmdId=5, High).
    ///
    /// The server responds by broadcasting a fresh balance snapshot through the
    /// normal balance channel.
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }

    /// Request a fresh full balance snapshot and wait until it is applied to
    /// `EventDispatcher::balances()`.
    ///
    /// `TRequestBalanceRefresh` is not an Engine API request and has no response
    /// UID. Delphi handles it by forcing the next balance worker tick to
    /// broadcast `TBalanceSnapshotFull`. This helper hides that fire-and-forget
    /// shape: it sends the request, keeps the UDP loop running, waits for a new
    /// full balance snapshot epoch, then returns a cloned read model.
    pub fn request_balance_snapshot(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<crate::state::BalancesState, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let previous_epoch = dispatcher.balances().last_epoch;
        let start = Instant::now();
        self.balance_request_refresh();

        loop {
            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let first_new_event = dispatcher.queued_event_count();
            let tick = remaining.min(TICK);
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
            if dispatcher.queued_events()[first_new_event..]
                .iter()
                .any(|event| {
                    matches!(
                        event,
                        crate::events::Event::Balance(
                            crate::state::BalanceEvent::SnapshotApplied { epoch, .. }
                        ) if *epoch != previous_epoch
                    )
                })
            {
                return Ok(dispatcher.balances().clone());
            }
        }
    }
}
