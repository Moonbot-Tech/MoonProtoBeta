//! `ClientSender` strategy command helpers.

use super::*;

impl ClientSender {
    /// Send `TStratSchemaRequest`.
    #[doc(hidden)]
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

    /// Send `TStratSnapshot` from an already serialized strategy payload.
    #[doc(hidden)]
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

    /// Send `TStratSnapshot` from decoded strategy snapshots.
    ///
    /// `schema` must be the live `TStratSchema` fetched during Init; typed
    /// strategy serialization uses it for Delphi field order, PropMask
    /// visibility, TypeID checks, and defaults.
    #[doc(hidden)]
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

    /// Send `TStratDelete` for one strategy or folder.
    #[doc(hidden)]
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSellPriceUpdate` for one strategy.
    #[doc(hidden)]
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

    /// Send `TStratCheckedSync` with explicit items.
    ///
    /// Regular active-library callers should prefer
    /// `EventDispatcher::send_strategy_checked_delta`, which builds
    /// `TStrategies.GetCheckedDelta` from owned strategy state.
    #[doc(hidden)]
    pub fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }
}
