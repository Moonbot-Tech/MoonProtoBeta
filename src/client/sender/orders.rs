//! `ClientSender` order/trade command helpers.
#![allow(dead_code)]

use super::*;

impl ClientSender {
    fn send_trade(&self, payload: Vec<u8>) -> bool {
        self.send_typed_domain_cmd(payload, Command::Order)
    }

    fn send_trade_keyed(&self, payload: Vec<u8>, u_key: UniqueKey) -> bool {
        self.send_typed_domain_cmd_keyed(payload, Command::Order, u_key)
    }

    pub(super) fn send_order_cancel_request(&self, request: crate::state::orders::OrderCancelSend) {
        match request {
            crate::state::orders::OrderCancelSend::PendingReplaceThenCancel {
                ctx,
                market,
                price,
            } => {
                let replace = crate::commands::trade::build_order_replace(
                    ctx,
                    &market,
                    crate::commands::trade::OrderType::Buy,
                    price,
                );
                self.send_trade_keyed(replace, UniqueKey::order_move(ctx.uid));
                let cancel = crate::commands::trade::build_order_cancel(
                    ctx,
                    &market,
                    0,
                    crate::commands::trade::OrderWorkerStatus::None,
                );
                self.send_trade_keyed(cancel, UniqueKey::order_move(ctx.uid));
            }
            crate::state::orders::OrderCancelSend::Cancel {
                ctx,
                market,
                status,
            } => {
                let raw = crate::commands::trade::build_order_cancel(ctx, &market, 0, status);
                self.send_trade_keyed(raw, UniqueKey::order_move(ctx.uid));
            }
        }
    }

    fn send_panic_sell_request(&self, request: crate::state::orders::PanicSellSend) {
        let raw = crate::commands::trade::build_turn_panic_sell(
            request.ctx,
            &request.market,
            request.turn_on,
        );
        self.send_trade_keyed(raw, UniqueKey::order_move(request.ctx.uid));
    }

    /// Send `TNewOrderCommand` from a thread-safe sender.
    ///
    /// This mirrors [`Client::new_order`]; send metadata is derived from the
    /// command descriptor for `TNewOrderCommand`.
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

    #[inline]
    fn now_ms(&self) -> i64 {
        self.start.elapsed().as_millis() as i64
    }

    /// Apply Delphi replace request locally and send `TOrderReplaceCommand`.
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
    pub(crate) fn replace_tracked_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        self.replace_order(orders, uid, new_price)
    }

    /// Send low-level `TAllStatusesReq`.
    ///
    /// This is fire-and-forget. Use [`Client::request_order_snapshot`] when the
    /// caller owns the `Client` and wants to wait for the applied snapshot.
    #[doc(hidden)]
    pub(crate) fn request_all_statuses(&self, uid: u64) -> bool {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw)
    }

    /// Apply Delphi cancel request locally and send `TOrderCancelCommand`.
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
    pub(crate) fn cancel_tracked_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        self.cancel_order(orders, uid)
    }

    /// Send `TJoinOrdersCommand`.
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

    /// Send `TSplitOrderCommand`.
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

    /// Split an order already tracked by `EventDispatcher::orders()`.
    #[doc(hidden)]
    pub(crate) fn split_tracked_order(
        &self,
        order: &crate::state::Order,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) -> bool {
        self.split_order(
            order.trade_ctx(),
            &order.market_name,
            split_parts,
            split_small,
            split_small_sell,
        )
    }

    /// Send `TMoveAllSellsCommand` if Delphi active-client gate finds a candidate order.
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

    /// Send `TDoClosePositionCommand` (`MaxRetries=1`).
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

    /// Send `TDoLimitClosePositionCommand` (`MaxRetries=1`).
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

    /// Send `TDoSplitPositionCommand` (`MaxRetries=1`).
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

    /// Send `TDoSellOrderCommand` (`MaxRetries=1`).
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

    /// Send `TOrderStatusRequest`.
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

    /// Apply Delphi `SendStopsIfChanged` locally and send `TOrderStopsUpdate`.
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
        self.send_trade_keyed(raw, UniqueKey::stop_move(ctx.uid));
        true
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    pub(crate) fn update_tracked_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        self.update_order_stops(orders, uid, stops)
    }

    /// Delphi `TOrdersWorkers.TurnPanicSell`: set panic sell for every local
    /// active sell order in `market_name`.
    pub(crate) fn turn_panic_sell(
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

    /// Apply Delphi per-worker panic-sell flag and send `TTurnPanicSellCommand`.
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
    pub(crate) fn turn_tracked_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        self.turn_order_panic_sell(orders, uid, turn_on)
    }

    /// Apply Delphi `SetImmuneClicks` locally and send `TSetImmuneCommand`.
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

    /// Send `TMoveAllBuysCommand` if Delphi active-client gate finds a candidate order.
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

    /// Apply Delphi `SendVStopIfChanged` locally and send `TVStopUpdate`.
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
        self.send_trade_keyed(raw, UniqueKey::vstop_move(ctx.uid));
        true
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
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

    /// Send `TDoMarketSplitPositionCommand` (`MaxRetries=1`).
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

    /// Send `TPenaltyCommand`.
    #[doc(hidden)]
    pub(crate) fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) -> bool {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw)
    }
}
