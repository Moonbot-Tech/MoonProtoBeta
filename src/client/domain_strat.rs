use super::*;

impl Client {
    // ====================================================================
    //  High-level Strat wrappers (Command::Strat, encrypted=true)
    //  Cover the Delphi MClient.SendStratCmd(T*Command.Create(...)) semantics.
    // ====================================================================

    /// Send `TStratSchemaRequest` (Strat CmdId=7, High).
    ///
    /// Agreed active-library behavior: one-time Init requests the live Delphi
    /// strategy schema from the server and stores the decoded result in
    /// `EventDispatcher::strats().strategy_schema()`. Public callers normally
    /// read that state instead of sending this manually.
    #[doc(hidden)]
    pub(crate) fn strat_schema_request(&self) {
        let raw = crate::commands::strat::build_schema_request(rand::random());
        self.send_typed_domain_cmd(raw, Command::Strat);
    }

    fn send_strat_snapshot_command(&self, raw: Vec<u8>) {
        self.send_typed_domain_cmd(raw, Command::Strat);
    }

    /// `TStratSnapshot` (Strat CmdId=2, Sliced, UK_StratSnapshot) from an already
    /// serialized `TStrategySerializer` payload.
    ///
    /// `data` is only the `TStratSnapshot.Data` blob. The method adds the required
    /// Delphi fields: `ServerEpoch`, `ClientMaxLastDate`, `Size`, and `Full`.
    /// Regular applications use
    /// `MoonClient::strategies().sync_local_strategies(...)`; the active runtime
    /// owns the decoded list and reuses its cached serializer payload.
    #[doc(hidden)]
    pub(crate) fn strat_send_snapshot_payload(
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

    /// Send `TStratDelete` (Strat CmdId=3, High) for one strategy or folder.
    #[doc(hidden)]
    pub(crate) fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_typed_domain_cmd(raw, Command::Strat);
    }

    /// Send `TStratSellPriceUpdate` (Strat CmdId=4, High,
    /// `UK_StratSellPriceUpdate`) for one strategy.
    ///
    /// The UKey includes `strategy_id`, so dedup is per strategy.
    #[doc(hidden)]
    pub(crate) fn strat_sell_price_update(&self, strategy_id: u64, sell_price: f64) {
        let raw = crate::commands::strat::build_sell_price_update(
            rand::random(),
            strategy_id,
            sell_price,
        );
        self.send_typed_domain_cmd_keyed(
            raw,
            Command::Strat,
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
    #[doc(hidden)]
    pub(crate) fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_typed_domain_cmd(raw, Command::Strat);
    }
}
