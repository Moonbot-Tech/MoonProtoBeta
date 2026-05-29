use super::*;

impl Client {
    // ====================================================================
    //  High-level Balance wrappers (Command::Balance, encrypted=true)
    //  Cover the Delphi MClient.SendBalanceCmd semantics.
    //  Audit docs_api B-03: previously there was neither a build_ nor a Client wrapper.
    // ====================================================================

    /// Send `TRequestBalanceRefresh` (Balance CmdId=5, High).
    #[doc(hidden)]
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }
}
