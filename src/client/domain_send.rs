use super::*;

impl Client {
    pub(super) fn send_typed_domain_cmd(&self, data: Vec<u8>, cmd: Command) -> bool {
        self.send_typed_domain_cmd_int(data, cmd, None)
    }

    pub(super) fn send_typed_domain_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        u_key: UniqueKey,
    ) -> bool {
        self.send_typed_domain_cmd_int(data, cmd, Some(u_key))
    }

    fn send_typed_domain_cmd_int(
        &self,
        data: Vec<u8>,
        cmd: Command,
        explicit_u_key: Option<UniqueKey>,
    ) -> bool {
        if !self.domain_ready_for_typed_send()
            && !outgoing_allowed_before_domain_ready(cmd.to_byte(), &data)
        {
            return false;
        }
        let Some(meta) = typed_send_metadata(cmd, &data, explicit_u_key) else {
            log::error!(target: "moonproto::client",
                "send_typed_domain_cmd: no descriptor/UKey for cmd={:?} payload_cmd_id={:?}",
                cmd,
                data.first().copied());
            return false;
        };
        self.send_cmd_keyed(
            data,
            cmd,
            meta.priority,
            meta.encrypted,
            meta.max_retries,
            meta.u_key,
        );
        true
    }

    pub(super) fn send_trade(&self, payload: Vec<u8>) -> bool {
        self.send_typed_domain_cmd(payload, Command::Order)
    }

    /// `send_trade` with a UniqueKey — for commands carrying the `[MoonCmdUnique(UK_*)]` attribute.
    /// Older pending commands with the same UKey are removed from `self.sending`/`self.pending_h`
    /// (matches Delphi SendCmdInt:780-785 + CheckSendingData).
    pub(super) fn send_trade_keyed(&self, payload: Vec<u8>, u_key: UniqueKey) -> bool {
        self.send_typed_domain_cmd_keyed(payload, Command::Order, u_key)
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

    pub(super) fn send_panic_sell_request(&self, request: crate::state::orders::PanicSellSend) {
        let raw = crate::commands::trade::build_turn_panic_sell(
            request.ctx,
            &request.market,
            request.turn_on,
        );
        self.send_trade_keyed(raw, UniqueKey::order_move(request.ctx.uid));
    }
}
