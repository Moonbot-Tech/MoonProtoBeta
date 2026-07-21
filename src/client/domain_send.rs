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

    pub(super) fn send_trade_keyed(&self, payload: Vec<u8>, u_key: UniqueKey) -> bool {
        self.send_typed_domain_cmd_keyed(payload, Command::Order, u_key)
    }

    pub(super) fn send_order_command_at(
        &self,
        envelope_uid: u64,
        payload: crate::commands::trade::OrderCommandPayload,
    ) -> bool {
        let u_key = order_command_u_key(&payload);
        let raw = crate::commands::trade::build_order_command(envelope_uid, payload);
        self.send_trade_keyed(raw, u_key)
    }

    pub(super) fn send_order_command_payload(
        &self,
        payload: crate::commands::trade::OrderCommandPayload,
    ) -> bool {
        let envelope_uid = if payload.group().is_some() || payload.is_move_all() {
            crate::commands::trade::next_order_action_id()
        } else {
            random_nonzero_u64()
        };
        self.send_order_command_at(envelope_uid, payload)
    }
}

pub(super) fn order_command_u_key(
    payload: &crate::commands::trade::OrderCommandPayload,
) -> UniqueKey {
    match (payload.group(), payload.order_id()) {
        (Some(group), Some(order_id)) => UniqueKey::order_command(group.unique_kind(), order_id),
        _ => UniqueKey::none(),
    }
}

fn random_nonzero_u64() -> u64 {
    loop {
        let value = rand::random();
        if value != 0 {
            return value;
        }
    }
}
