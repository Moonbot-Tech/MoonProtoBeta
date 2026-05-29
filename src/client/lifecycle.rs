//! Lifecycle callbacks and Delphi `ServerUpdateSent` marker.

use super::*;

impl Client {
    /// Install the lifecycle callback.
    ///
    /// During `run*` calls the callback is queued outside the protocol writer
    /// tick, matching Delphi `TThread.Queue` for status notifications.
    pub fn on_lifecycle(&mut self, cb: LifecycleFn) {
        self.lifecycle_cb = Some(cb);
    }

    pub(super) fn set_lifecycle_event_sender(&self, tx: Option<mpsc::Sender<LifecycleEvent>>) {
        *self.lifecycle_app_tx.lock().unwrap() = tx;
    }

    #[cfg(test)]
    pub(super) fn lifecycle_event_sender_installed(&self) -> bool {
        self.lifecycle_app_tx.lock().unwrap().is_some()
    }

    /// Mark Delphi `ServerUpdateSent`.
    ///
    /// UI wrappers that can trigger a server-side restart/routing update
    /// (`ui_update_version`, `ui_switch_dex`, `ui_switch_spot`) call this
    /// automatically. Use it only when sending the same raw UI commands through
    /// lower-level APIs: the next Init pass consumes the flag and runs
    /// the Delphi BaseCheck retry path.
    pub fn mark_server_update_sent(&self) {
        self.server_update_sent.store(true, Ordering::Relaxed);
    }

    /// Whether a Delphi-style server update marker is pending.
    pub fn server_update_sent(&self) -> bool {
        self.server_update_sent.load(Ordering::Relaxed)
    }

    pub(super) fn take_server_update_sent(&self) -> bool {
        self.server_update_sent.swap(false, Ordering::Relaxed)
    }

    /// Internal hook: invokes the callback on a state transition.
    /// Must be called wherever `self.auth_status` or `self.need_connect` changes.
    pub(super) fn fire_lifecycle(&mut self, ev: LifecycleEvent) {
        let tx = self.lifecycle_app_tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(ev);
            return;
        }
        if let Some(ref mut cb) = self.lifecycle_cb {
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
                let fresh = !self.was_ever_connected;
                self.was_ever_connected = true;
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
