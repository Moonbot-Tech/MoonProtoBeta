//! Local strategy list API and `TStratSnapshotRequest` reply provider.

use super::{EventDispatcher, StrategySnapshotReply};
use crate::commands::strategy_serializer::StrategySnapshot;

impl EventDispatcher {
    /// Set Delphi `cfg.ServerStratEpoch` analogue for local strategy snapshots.
    ///
    /// Use this when loading persisted local strategy state before init. The
    /// value is written into `TStratSnapshot.ServerEpoch` when the dispatcher
    /// answers a server `TStratSnapshotRequest`.
    pub(crate) fn set_local_strategy_epoch(&mut self, epoch: u64) {
        self.local_strategy_epoch = epoch;
    }

    #[cfg(test)]
    pub(crate) fn local_strategy_epoch(&self) -> u64 {
        self.local_strategy_epoch
    }

    /// Delphi local strategy edit: `Inc(cfg.ServerStratEpoch)`.
    pub(crate) fn mark_local_strategies_changed(&mut self) -> u64 {
        self.local_strategy_epoch = self.local_strategy_epoch.saturating_add(1);
        self.local_strategy_epoch
    }

    /// Set the library-owned strategy list before init.
    ///
    /// This is the normal active-library path. The dispatcher stores the full
    /// decoded snapshots, feeds the post-init strategy snapshot, answers server
    /// `TStratSnapshotRequest` automatically, and keeps the list current when
    /// server strategy snapshots/deltas arrive.
    pub(crate) fn set_local_strategies(&mut self, strategies: &[StrategySnapshot]) {
        self.strats.replace_with_snapshots(strategies);
    }

    /// Change one local strategy checked flag like Delphi `TStrategy.Checked`.
    ///
    /// This does not mark the change acknowledged. The delta stays pending
    /// until a matching `TStratCheckedEcho` or `TStratCheckedSync` arrives from
    /// the server.
    pub(crate) fn set_strategy_checked(&mut self, strategy_id: u64, checked: bool) -> bool {
        self.strats.set_checked(strategy_id, checked)
    }

    /// Read one full decoded strategy snapshot from the active-library state.
    #[cfg(test)]
    pub(crate) fn strategy_snapshot(&self, strategy_id: u64) -> Option<&StrategySnapshot> {
        self.strats.snapshot(strategy_id)
    }

    /// Clone the current strategy snapshot list in Delphi list order.
    #[cfg(test)]
    pub(crate) fn strategy_snapshot_vec(&self) -> Vec<StrategySnapshot> {
        self.strats.snapshot_vec()
    }

    /// Send `TStratCheckedSync.Create(true)` if Delphi checked delta is non-empty.
    ///
    /// Returns the number of delta items queued. The local `PrevChecked` is not
    /// advanced here; Delphi advances it only after server echo/sync.
    pub(crate) fn send_strategy_checked_delta(&self, client: &crate::client::Client) -> usize {
        let items = self.strats.checked_delta();
        if items.is_empty() {
            return 0;
        }
        client.strat_checked_sync(&items, true);
        items.len()
    }

    #[doc(hidden)]
    /// Send Delphi `TStratStartStopCommandV2.Create(is_start)`.
    ///
    /// The command is always queued after the client's Init gate is open, even
    /// when the checked delta is empty, because the same packet also carries the
    /// start/stop action.
    pub(crate) fn ui_strat_start_stop_v2(
        &self,
        client: &crate::client::Client,
        is_start: bool,
    ) -> usize {
        let items = self.strats.checked_delta();
        client.ui_strat_start_stop_v2(is_start, &items);
        items.len()
    }

    /// Register an override provider for fresh strategy snapshots.
    ///
    /// The provider is called with the UID of the incoming
    /// `TStratSnapshotRequest`. The reply itself is sent with a new command UID,
    /// as Delphi creates a fresh `TStratSnapshot` command object for the answer.
    ///
    /// Normal callers should prefer [`Self::set_local_strategies`]. If no
    /// provider is registered, or the provider returns `None`, the dispatcher
    /// sends the current library-owned strategy list. The server request itself
    /// is internal runtime control, not a user-facing strategy event.
    #[cfg(test)]
    pub(crate) fn set_strategy_snapshot_provider<F>(&mut self, provider: F)
    where
        F: FnMut(u64) -> Option<StrategySnapshotReply> + Send + 'static,
    {
        self.strategy_snapshot_provider = Some(Box::new(provider));
    }

    pub(super) fn strategy_snapshot_reply(
        &mut self,
        request_uid: u64,
    ) -> Option<StrategySnapshotReply> {
        self.strategy_snapshot_provider
            .as_mut()
            .and_then(|provider| provider(request_uid))
            .or_else(|| self.local_strategy_snapshot_reply())
    }

    pub(crate) fn pending_or_local_strategy_snapshot_reply(
        &mut self,
    ) -> Option<StrategySnapshotReply> {
        let Some(uid) = self.pending_strategy_snapshot_request_uid.take() else {
            return self.local_strategy_snapshot_reply();
        };
        match self.strategy_snapshot_reply(uid) {
            Some(reply) => Some(reply),
            None => {
                self.pending_strategy_snapshot_request_uid = Some(uid);
                None
            }
        }
    }

    pub(crate) fn local_strategy_snapshot_reply(&mut self) -> Option<StrategySnapshotReply> {
        let cache = self.strats.snapshot_payload_cache()?;
        Some(StrategySnapshotReply::from_payload(
            self.local_strategy_epoch,
            cache.client_max_last_date,
            true,
            cache.data.clone(),
        ))
    }
}
