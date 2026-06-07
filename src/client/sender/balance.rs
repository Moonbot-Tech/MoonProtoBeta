//! `ClientSender` balance command helpers.
#![allow(dead_code)]

use super::*;

impl ClientSender {
    /// Send `TRequestBalanceRefresh`.
    #[doc(hidden)]
    pub(crate) fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }
}
