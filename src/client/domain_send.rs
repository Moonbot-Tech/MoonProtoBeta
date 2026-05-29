use super::*;

impl Client {
    pub(super) fn send_domain_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) -> bool {
        if !self.domain_ready_for_typed_send()
            && !outgoing_allowed_before_domain_ready(cmd.to_byte(), &data)
        {
            return false;
        }
        self.send_cmd(data, cmd, priority, encrypted, max_retries);
        true
    }

    pub(super) fn send_domain_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> bool {
        if !self.domain_ready_for_typed_send()
            && !outgoing_allowed_before_domain_ready(cmd.to_byte(), &data)
        {
            return false;
        }
        self.send_cmd_keyed(data, cmd, priority, encrypted, max_retries, u_key);
        true
    }

    pub(super) fn send_trade(&self, payload: Vec<u8>, max_retries: i32) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
        );
        true
    }

    /// `send_trade` with a UniqueKey — for commands carrying the `[MoonCmdUnique(UK_*)]` attribute.
    /// Older pending commands with the same UKey are removed from `self.sending`/`self.pending_h`
    /// (matches Delphi SendCmdInt:780-785 + CheckSendingData).
    pub(super) fn send_trade_keyed(
        &self,
        payload: Vec<u8>,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd_keyed(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
            u_key,
        );
        true
    }

    pub(crate) fn send_order_cancel_request(&self, request: crate::state::orders::OrderCancelSend) {
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
                self.send_trade_keyed(replace, 3, UniqueKey::order_move(ctx.uid));
                let cancel = crate::commands::trade::build_order_cancel(
                    ctx,
                    &market,
                    0,
                    crate::commands::trade::OrderWorkerStatus::None,
                );
                self.send_trade_keyed(cancel, 3, UniqueKey::order_move(ctx.uid));
            }
            crate::state::orders::OrderCancelSend::Cancel {
                ctx,
                market,
                status,
            } => {
                let raw = crate::commands::trade::build_order_cancel(ctx, &market, 0, status);
                self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
            }
        }
    }

    pub(super) fn send_panic_sell_request(&self, request: crate::state::orders::PanicSellSend) {
        let raw = crate::commands::trade::build_turn_panic_sell(
            request.ctx,
            &request.market,
            request.turn_on,
        );
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(request.ctx.uid));
    }
}
