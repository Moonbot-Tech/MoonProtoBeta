use super::*;

impl Client {
    pub(crate) fn new_order(
        &self,
        request_uid: u64,
        market: &str,
        is_short: bool,
        price: f64,
        strategy_id: u64,
        size: f64,
        planned_sell_price: f64,
        use_market_stop: bool,
    ) -> bool {
        let payload = crate::commands::trade::OrderCommandPayload::Start {
            market_name: market.to_owned(),
            is_short,
            use_market_stop,
            strategy_id,
            size,
            price,
            planned_sell_price,
        };
        self.send_order_command_at(request_uid, payload)
    }

    pub(crate) fn replace_order(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        new_price: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(payload) = orders.send_replace_if_requested(uid, new_price, self.now_ms()) else {
            return false;
        };
        self.send_order_command_payload(payload)
    }

    pub(crate) fn replace_tracked_order(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        new_price: f64,
    ) -> bool {
        self.replace_order(orders, uid, new_price)
    }

    pub(crate) fn request_orders_snapshot(&self) -> bool {
        self.request_order_status(0, 0)
    }

    pub(crate) fn cancel_order(&self, orders: &mut crate::state::OrderState, uid: u64) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(payload) = orders.send_cancel_if_requested(uid) else {
            return false;
        };
        self.send_order_command_payload(payload)
    }

    pub(crate) fn cancel_tracked_order(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
    ) -> bool {
        self.cancel_order(orders, uid)
    }

    pub(crate) fn join_orders(&self, request_uid: u64, market: &str, is_short: bool) -> bool {
        self.send_order_command_at(
            request_uid,
            crate::commands::trade::OrderCommandPayload::Join {
                market_name: market.to_owned(),
                is_short,
            },
        )
    }

    pub(crate) fn split_order(
        &self,
        request_uid: u64,
        order_id: u64,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) -> bool {
        self.send_order_command_at(
            request_uid,
            crate::commands::trade::OrderCommandPayload::SplitOrder {
                order_id,
                parts: split_parts,
                split_small,
                split_small_sell,
            },
        )
    }

    pub(crate) fn move_all_sells(
        &self,
        orders: &crate::state::Orders,
        market: &str,
        params: crate::commands::trade::MoveAllSellsParams,
    ) -> bool {
        if !self.domain_ready_for_typed_send()
            || !orders.has_move_all_sells_candidate(market, params)
        {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_sells(market, params);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn move_all_buys(
        &self,
        orders: &crate::state::Orders,
        market: &str,
        params: crate::commands::trade::MoveAllBuysParams,
    ) -> bool {
        if !self.domain_ready_for_typed_send()
            || !orders.has_move_all_buys_candidate(
                market,
                params.cmd_type,
                params.move_kind,
                params.side,
            )
        {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_buys(market, params);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn do_close_position(
        &self,
        request_uid: u64,
        market: &str,
        market_sell: bool,
    ) -> bool {
        let raw = crate::commands::trade::build_do_close_position(request_uid, market, market_sell);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn do_limit_close_position(
        &self,
        request_uid: u64,
        market: &str,
        is_short: bool,
    ) -> bool {
        let raw =
            crate::commands::trade::build_do_limit_close_position(request_uid, market, is_short);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn do_split_position(&self, request_uid: u64, market: &str, is_short: bool) -> bool {
        let raw = crate::commands::trade::build_do_split_position(request_uid, market, is_short);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn do_market_split_position(
        &self,
        request_uid: u64,
        market: &str,
        is_short: bool,
    ) -> bool {
        let raw =
            crate::commands::trade::build_do_market_split_position(request_uid, market, is_short);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn do_sell_order(
        &self,
        request_uid: u64,
        market: &str,
        price: f64,
        size: f64,
    ) -> bool {
        let raw = crate::commands::trade::build_do_sell_order(request_uid, market, price, size);
        self.send_trade_keyed(raw, UniqueKey::none())
    }

    pub(crate) fn panic_sell_all(&self, request_uid: u64) -> bool {
        self.send_order_command_at(
            request_uid,
            crate::commands::trade::OrderCommandPayload::PanicSellAll,
        )
    }

    pub(crate) fn request_order_status(&self, order_id: u64, exact_rev: u64) -> bool {
        let raw = crate::commands::trade::build_order_status_request(order_id, exact_rev);
        self.send_trade(raw)
    }

    pub(crate) fn request_tracked_order_status(&self, order_id: u64) -> bool {
        self.request_order_status(order_id, 0)
    }

    pub(crate) fn update_order_stops(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(payload) = orders.send_stops_if_changed(uid, stops) else {
            return false;
        };
        self.send_order_command_payload(payload)
    }

    pub(crate) fn update_tracked_order_stops(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        self.update_order_stops(orders, uid, stops)
    }

    pub(crate) fn update_vstop(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        enabled: bool,
        fixed: bool,
        level: f64,
        volume: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(payload) = orders.send_vstop_if_changed(uid, enabled, fixed, level, volume) else {
            return false;
        };
        self.send_order_command_payload(payload)
    }

    pub(crate) fn update_tracked_order_vstop(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        enabled: bool,
        fixed: bool,
        level: f64,
        volume: f64,
    ) -> bool {
        self.update_vstop(orders, uid, enabled, fixed, level, volume)
    }

    pub(crate) fn turn_order_panic_sell(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        enabled: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(payload) = orders.send_panic_sell_if_changed(uid, enabled) else {
            return false;
        };
        self.send_order_command_payload(payload)
    }

    pub(crate) fn turn_tracked_order_panic_sell(
        &self,
        orders: &mut crate::state::OrderState,
        uid: u64,
        enabled: bool,
    ) -> bool {
        self.turn_order_panic_sell(orders, uid, enabled)
    }

    pub(crate) fn switch_panic_sell_by_market(
        &self,
        orders: &mut crate::state::OrderState,
        market_name: &str,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let commands = orders.switch_panic_sell_by_market(market_name, turn_on);
        let changed = !commands.is_empty();
        for payload in commands {
            self.send_order_command_payload(payload);
        }
        changed
    }

    pub(crate) fn set_immune(
        &self,
        orders: &mut crate::state::OrderState,
        items: &[crate::commands::trade::ImmuneItem],
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let commands = orders.set_immune_clicks(items);
        let sent = !commands.is_empty();
        for payload in commands {
            self.send_order_command_payload(payload);
        }
        sent
    }

    pub(crate) fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) -> bool {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw)
    }
}
