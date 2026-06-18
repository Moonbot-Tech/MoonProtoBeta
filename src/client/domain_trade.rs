use super::*;

impl Client {
    // ====================================================================
    //  High-level Trade wrappers (convenience over commands::trade::build_*).
    //  Send priority, encryption, retry count, and UKey come from
    //  commands::registry descriptors matching Delphi command attributes.
    // ====================================================================

    /// Send `TNewOrderCommand` (CmdId=3) to open a new order.
    #[doc(hidden)]
    pub(crate) fn new_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) -> bool {
        let raw = crate::commands::trade::build_new_order(
            ctx, market, is_short, price, strat_id, order_size,
        );
        self.send_trade(raw)
    }

    /// Delphi local replace request + `TOrderReplaceCommand` (CmdId=6,
    /// `UK_OrderMove`) with a new price.
    ///
    /// Requires the local `Orders` read model. The wrapper derives market route
    /// and order type from the local order and repeats the Delphi
    /// `ReplaceSentTime = 0` gate.
    #[doc(hidden)]
    pub(crate) fn replace_order(
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
        self.send_trade_keyed(raw, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Replace an order already tracked by `EventDispatcher::orders()`.
    #[doc(hidden)]
    pub(crate) fn replace_tracked_order(
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
    #[doc(hidden)]
    pub(crate) fn request_all_statuses(&self, uid: u64) -> bool {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw)
    }

    /// Delphi local cancel request + `TOrderCancelCommand` (CmdId=10,
    /// `UK_OrderMove`) for one order.
    ///
    /// Requires the local `Orders` read model. The wrapper derives current
    /// status from the local order and clears the local request after queueing.
    #[doc(hidden)]
    pub(crate) fn cancel_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
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
    #[doc(hidden)]
    pub(crate) fn cancel_tracked_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        self.cancel_order(orders, uid)
    }

    /// Send `TJoinOrdersCommand` (CmdId=11) to join open orders.
    #[doc(hidden)]
    pub(crate) fn join_orders(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_join_orders(ctx, market, is_short);
        self.send_trade(raw)
    }

    /// Send `TSplitOrderCommand` (CmdId=12) to split an order into parts.
    #[doc(hidden)]
    pub(crate) fn split_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_split_order(
            ctx,
            market,
            split_parts,
            split_small,
            split_small_sell,
        );
        self.send_trade(raw)
    }

    /// `TMoveAllSellsCommand` (CmdId=13), gated like Delphi active-client UI.
    ///
    /// The move mode, price/zone and side live in
    /// [`crate::commands::trade::MoveAllSellsParams`], normally built through
    /// named constructors so application code does not assemble packet modes by
    /// hand.
    #[doc(hidden)]
    pub(crate) fn move_all_sells(
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
        self.send_trade(raw);
        true
    }

    /// `TDoClosePositionCommand` (CmdId=14, MaxRetries=1).
    #[doc(hidden)]
    pub(crate) fn do_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        market_sell: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_do_close_position(ctx, market, market_sell);
        self.send_trade(raw)
    }

    /// `TDoLimitClosePositionCommand` (CmdId=15, MaxRetries=1).
    #[doc(hidden)]
    pub(crate) fn do_limit_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_do_limit_close_position(ctx, market, is_short);
        self.send_trade(raw)
    }

    /// `TDoSplitPositionCommand` (CmdId=16, MaxRetries=1).
    #[doc(hidden)]
    pub(crate) fn do_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_do_split_position(ctx, market, is_short);
        self.send_trade(raw)
    }

    /// `TDoSellOrderCommand` (CmdId=17, MaxRetries=1).
    #[doc(hidden)]
    pub(crate) fn do_sell_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        price: f64,
        size: f64,
    ) -> bool {
        let raw = crate::commands::trade::build_do_sell_order(ctx, market, price, size);
        self.send_trade(raw)
    }

    /// `TOrderStatusRequest` (CmdId=18) — request the status of a specific order.
    #[doc(hidden)]
    pub(crate) fn request_order_status(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
    ) -> bool {
        let raw = crate::commands::trade::build_order_status_request(ctx, market);
        self.send_trade(raw)
    }

    /// Request a fresh status for an order already tracked by
    /// `EventDispatcher::orders()`.
    #[doc(hidden)]
    pub(crate) fn request_tracked_order_status(&self, order: &crate::state::Order) -> bool {
        self.request_order_status(order.trade_ctx(), &order.market_name)
    }

    /// Delphi `SendStopsIfChanged` + `TOrderStopsUpdate` (CmdId=20,
    /// UK_OrderMove).
    ///
    /// Requires the local `Orders` read model: if the UID is unknown or the
    /// stop record did not change, Delphi would not put a packet on the wire.
    #[doc(hidden)]
    pub(crate) fn update_order_stops(
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
        self.send_trade_keyed(raw, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    #[doc(hidden)]
    pub(crate) fn update_tracked_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        self.update_order_stops(orders, uid, stops)
    }

    /// Delphi `TOrdersWorkers.SwitchPanicSellByMarket` button semantics.
    #[doc(hidden)]
    pub(crate) fn switch_panic_sell_by_market(
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
    #[doc(hidden)]
    pub(crate) fn turn_order_panic_sell(
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
    #[doc(hidden)]
    pub(crate) fn turn_tracked_order_panic_sell(
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
    #[doc(hidden)]
    pub(crate) fn set_immune(
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
        self.send_trade_keyed(raw, UniqueKey::immune_clicks(items_uid_sum));
        true
    }

    /// `TMoveAllBuysCommand` (CmdId=27), gated like Delphi active-client UI.
    #[doc(hidden)]
    pub(crate) fn move_all_buys(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::MoveAllBuysParams,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_buys_candidate(
            market,
            params.cmd_type,
            params.move_kind,
            params.side,
        ) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_buys(ctx, market, params);
        self.send_trade(raw);
        true
    }

    /// Delphi `SendVStopIfChanged` + `TVStopUpdate` (CmdId=29, `UK_OrderMove`).
    ///
    /// Requires the local `Orders` read model: the wrapper derives the current
    /// worker status, mutates local VStop state, and queues nothing if the value
    /// did not change.
    #[doc(hidden)]
    pub(crate) fn update_vstop(
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
        self.send_trade_keyed(raw, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
    #[doc(hidden)]
    pub(crate) fn update_tracked_order_vstop(
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
    #[doc(hidden)]
    pub(crate) fn do_market_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_do_market_split_position(ctx, market, is_short);
        self.send_trade(raw)
    }

    /// Send `TPenaltyCommand` (CmdId=23) to mark a market as under strategy
    /// penalty/cooldown.
    ///
    /// Manual and alert strategies are intentionally not blocked by this server
    /// flag; it affects automatic strategy checks.
    #[doc(hidden)]
    pub(crate) fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) -> bool {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw)
    }
}
