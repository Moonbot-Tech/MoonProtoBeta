use super::*;

impl Client {
    // ====================================================================
    //  High-level Balance wrappers (Command::Balance, encrypted=true)
    //  Cover the Delphi MClient.SendBalanceCmd semantics.
    // ====================================================================

    /// Send `TRequestBalanceRefresh` (Balance CmdId=5, High).
    #[doc(hidden)]
    pub(crate) fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }
}
