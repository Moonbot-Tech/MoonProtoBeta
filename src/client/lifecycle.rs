//! Lifecycle callbacks and Delphi `ServerUpdateSent` marker.

use super::*;

/// Runtime/lifecycle state carved out of [`super::Client`].
///
/// Groups the lifecycle callback plumbing (`lifecycle_cb`/`lifecycle_app_tx`),
/// the first-Connected marker (`was_ever_connected`), and the two shutdown/queue
/// flags shared with the runtime thread and `ClientSender` (`app_queue_alive`,
/// `runtime_shutdown`). Field names, types, and meaning are unchanged from when
/// they lived directly on `Client`.
pub(crate) struct ClientLifecycle {
    /// Mirrors the app-queue-alive flag handed to every `ClientSender`. Cleared
    /// on `Drop` so senders stop enqueueing once the owning client is gone.
    pub(crate) app_queue_alive: Arc<AtomicBool>,
    /// Set by the active runtime owner; polled by the protocol loop to break out.
    pub(crate) runtime_shutdown: Arc<AtomicBool>,
    /// Lifecycle callback — queued on channel status change (Connecting ->
    /// Connected{fresh} -> Reconnecting/Disconnected). Set via
    /// `client.on_lifecycle(cb)`. Optional.
    pub(crate) lifecycle_cb: Option<LifecycleFn>,
    /// Shared lifecycle event sender used when the callback is driven from a
    /// dedicated queue thread instead of inline.
    pub(crate) lifecycle_app_tx: Arc<Mutex<Option<mpsc::Sender<LifecycleEvent>>>>,
    /// Whether a successful Connected ever happened (`Fine` received >=1 time).
    /// Used in `LifecycleEvent::Connected { fresh }` — `fresh = !was_ever_connected`
    /// on the FIRST Connected; for all later ones `fresh = false`.
    pub(crate) was_ever_connected: bool,
}

impl ClientLifecycle {
    pub(crate) fn new(app_queue_alive: Arc<AtomicBool>, runtime_shutdown: Arc<AtomicBool>) -> Self {
        Self {
            app_queue_alive,
            runtime_shutdown,
            lifecycle_cb: None,
            lifecycle_app_tx: Arc::new(Mutex::new(None)),
            was_ever_connected: false,
        }
    }
}

impl Client {
    /// Install the lifecycle callback.
    ///
    /// During `run*` calls the callback is queued outside the protocol writer
    /// tick, matching Delphi `TThread.Queue` for status notifications.
    pub fn on_lifecycle(&mut self, cb: LifecycleFn) {
        self.lifecycle.lifecycle_cb = Some(cb);
    }

    pub(super) fn set_lifecycle_event_sender(&self, tx: Option<mpsc::Sender<LifecycleEvent>>) {
        *self.lifecycle.lifecycle_app_tx.lock().unwrap() = tx;
    }

    #[cfg(test)]
    pub(super) fn lifecycle_event_sender_installed(&self) -> bool {
        self.lifecycle.lifecycle_app_tx.lock().unwrap().is_some()
    }

    /// Mark Delphi `ServerUpdateSent`.
    ///
    /// UI wrappers that can trigger a server-side restart/routing update
    /// (`ui_update_version`, `ui_switch_dex`, `ui_switch_spot`) call this
    /// automatically. Use it only when sending the same raw UI commands through
    /// lower-level APIs: the next Init pass consumes the flag and runs
    /// the Delphi BaseCheck retry path.
    pub fn mark_server_update_sent(&self) {
        self.refresh_clocks
            .server_update_sent
            .store(true, Ordering::Relaxed);
    }

    /// Whether a Delphi-style server update marker is pending.
    pub fn server_update_sent(&self) -> bool {
        self.refresh_clocks
            .server_update_sent
            .load(Ordering::Relaxed)
    }

    pub(super) fn take_server_update_sent(&self) -> bool {
        self.refresh_clocks
            .server_update_sent
            .swap(false, Ordering::Relaxed)
    }

    /// Internal hook: invokes the callback on a state transition.
    /// Must be called wherever `self.auth_status` or `self.need_connect` changes.
    pub(super) fn fire_lifecycle(&mut self, ev: LifecycleEvent) {
        let tx = self.lifecycle.lifecycle_app_tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(ev);
            return;
        }
        if let Some(ref mut cb) = self.lifecycle.lifecycle_cb {
            cb(ev);
        }
    }

    /// Check for an auth_status change and emit the matching lifecycle event.
    /// Called from the main loop after each packet.
    pub(super) fn check_lifecycle_transition(&mut self) {
        if self.auth_status == self.prev_auth_status {
            return;
        }
        let new_ev = match (self.prev_auth_status, self.auth_status) {
            // Initial connection (cold start or after Disconnect)
            (AuthStatus::Base, AuthStatus::Connected) => Some(LifecycleEvent::Connecting),
            // Re-handshake after connection loss (soft reconnect) — Offline → Connected
            (AuthStatus::Offline, AuthStatus::Connected) => Some(LifecycleEvent::Connecting),
            // Successful authorization (Fine received) — `fresh = true` only for the first
            // Connected of the session. Afterwards was_ever_connected becomes true and all
            // subsequent re-handshakes send `fresh = false`.
            (_, AuthStatus::AuthDone) if self.prev_auth_status != AuthStatus::AuthDone => {
                let fresh = !self.lifecycle.was_ever_connected;
                self.lifecycle.was_ever_connected = true;
                Some(LifecycleEvent::Connected { fresh })
            }
            // Connection loss
            (AuthStatus::AuthDone, AuthStatus::Offline) => Some(LifecycleEvent::Reconnecting),
            // Disconnect requested by the consumer (explicit)
            (_, AuthStatus::Base)
                if self.prev_auth_status != AuthStatus::Base && !self.need_connect =>
            {
                Some(LifecycleEvent::Disconnected)
            }
            _ => None,
        };
        self.prev_auth_status = self.auth_status;
        if let Some(ev) = new_ev {
            self.fire_lifecycle(ev);
        }
    }
}
