use super::*;

impl Client {
    // ====================================================================
    //  High-level Balance wrappers (Command::Balance, encrypted=true)
    //  Покрывают MClient.SendBalanceCmd семантику Delphi.
    //  Аудит docs_api B-03: ранее не было ни build_, ни Client-wrapper'а.
    // ====================================================================

    /// Send `TRequestBalanceRefresh` (Balance CmdId=5, High).
    ///
    /// The server responds by broadcasting a fresh balance snapshot through the
    /// normal balance channel.
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }

    /// Request a fresh full balance snapshot and wait until it is applied to
    /// `EventDispatcher::balances()`.
    ///
    /// `TRequestBalanceRefresh` is not an Engine API request and has no response
    /// UID. Delphi handles it by forcing the next balance worker tick to
    /// broadcast `TBalanceSnapshotFull`. This helper hides that fire-and-forget
    /// shape: it sends the request, keeps the UDP loop running, waits for a new
    /// full balance snapshot epoch, then returns a cloned read model.
    pub fn request_balance_snapshot(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<crate::state::BalancesState, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let previous_epoch = dispatcher.balances().last_epoch;
        let start = Instant::now();
        self.balance_request_refresh();

        loop {
            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let first_new_event = dispatcher.queued_event_count();
            let tick = remaining.min(TICK);
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
            if dispatcher.queued_events()[first_new_event..]
                .iter()
                .any(|event| {
                    matches!(
                        event,
                        crate::events::Event::Balance(
                            crate::state::BalanceEvent::SnapshotApplied { epoch, .. }
                        ) if *epoch != previous_epoch
                    )
                })
            {
                return Ok(dispatcher.balances().clone());
            }
        }
    }
}
