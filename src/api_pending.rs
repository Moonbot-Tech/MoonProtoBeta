//! Pending Engine API response registry.
//!
//! Клиент отправляет `TEngineRequest` с уникальным UID; сервер отвечает
//! `TEngineResponse` с тем же UID. `ApiPending` хранит маппинг
//! `uid → mpsc::Sender<EngineResponse>`.
//!
//! Обычные приложения используют `MoonClient` intents/events/snapshots и не
//! трогают этот registry напрямую. Внутренние runtime/test helpers могут
//! зарегистрировать internal receiver, но ответы доставляются только пока живой runtime
//! иначе ответ физически не будет decoded.
//!
//! Прямой `rx.recv_timeout(...)` подходит только когда другой thread уже крутит
//! main loop клиента. Как только `ProtocolCore` receive phase декодировал
//! зарегистрированный `TEngineResponse`, он доставляет его в `ApiPending` сразу.
//! Тяжёлые Delphi-style callers вроде `GetMarketsList` / `UpdateMarketsList`
//! применяют active state из pending receiver'а после `SendAndWait`; unmatched /
//! fire-and-forget responses продолжают идти через active-dispatch.
//!
//! Pending slot lifetime follows Delphi `TMoonProtoEngine.SendAndWait`: normal
//! one-shot callers remove their slot on timeout. Raw async users still get a
//! defensive fixed deadline so a long-running runtime cannot accumulate stale
//! `uid -> Sender` entries forever after dropped receivers or lost responses.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::commands::engine_api::EngineResponse;

/// Default request/response timeout — 12 секунд. Совпадает с Delphi
/// `TMoonProtoEngine.FTimeout = 12000` (MoonProtoEngine.pas) для `SendAndWait`.
pub const DEFAULT_PENDING_TIMEOUT_MS: i64 = 12_000;
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

struct PendingEntry {
    tx: mpsc::Sender<EngineResponse>,
    deadline: Instant,
}

struct PendingState {
    map: HashMap<u64, PendingEntry>,
    last_sweep: Instant,
}

/// Реестр pending Engine API запросов.
///
/// Thread-safe (внутри `Arc<Mutex>`). Можно клонировать `Arc<ApiPending>` и передавать в любые потоки.
///
pub struct ApiPending {
    state: Mutex<PendingState>,
}

impl ApiPending {
    /// Convenience: построить уже обёрнутый `Arc<ApiPending>`. Большинство callers
    /// хотят shared доступ (Client держит, receive phase получает clone'd Arc).
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// D-V2-02 fix: graceful recovery после Mutex poisoning. На long-running клиенте
    /// невозможно гарантировать что какой-то поток не запаникует под локом — в этом
    /// случае Rust помечает Mutex как poisoned и обычный `.lock().unwrap()` тоже
    /// паникнул бы каскадом. Восстанавливаем guard из PoisonError — пусть API
    /// pending registry продолжит работать (потеря части in-flight ответов терпима,
    /// падение всего клиента — нет).
    #[inline]
    fn lock_state(&self) -> std::sync::MutexGuard<'_, PendingState> {
        match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                log::warn!(target: "moonproto::api_pending",
                    "ApiPending mutex poisoned — recovering, in-flight requests may be lost");
                poisoned.into_inner()
            }
        }
    }

    #[inline]
    fn default_timeout() -> Duration {
        Duration::from_millis(DEFAULT_PENDING_TIMEOUT_MS as u64)
    }

    #[inline]
    fn deadline_from(now: Instant, timeout: Duration) -> Instant {
        now.checked_add(timeout)
            .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60))
    }

    fn sweep_expired_locked(state: &mut PendingState, now: Instant, force: bool) -> usize {
        if !force && now.duration_since(state.last_sweep) < SWEEP_INTERVAL {
            return 0;
        }
        state.last_sweep = now;
        let before = state.map.len();
        state.map.retain(|_, entry| entry.deadline > now);
        before - state.map.len()
    }

    /// Зарегистрировать ожидание ответа по `uid`.
    ///
    /// Прямой `rx.recv_timeout(...)` подходит только когда другой thread уже
    /// крутит main loop клиента; receive phase доставит зарегистрированный
    /// response сразу после decode, но writer/send progress всё ещё должен
    /// где-то выполняться.
    ///
    /// Если на тот же `uid` уже была регистрация — старый sender дропается (старый
    /// receiver получит "channel closed").
    pub fn register(&self, uid: u64) -> mpsc::Receiver<EngineResponse> {
        self.register_with_timeout(uid, Self::default_timeout())
    }

    /// Зарегистрировать ожидание с явным deadline. Внутренний runtime использует
    /// это для non-blocking запросов, чтобы потерянный response не оставлял
    /// sender в registry навсегда.
    pub(crate) fn register_with_timeout(
        &self,
        uid: u64,
        timeout: Duration,
    ) -> mpsc::Receiver<EngineResponse> {
        let (tx, rx) = mpsc::channel();
        let now = Instant::now();
        let deadline = Self::deadline_from(now, timeout);
        let mut state = self.lock_state();
        Self::sweep_expired_locked(&mut state, now, false);
        state.map.insert(uid, PendingEntry { tx, deadline });
        rx
    }

    /// Доставить response в ожидающего receiver'а.
    ///
    /// Возвращает `Some(resp)` если UID **не зарегистрирован** (response пришёл "сам",
    /// без активного waitера — потребитель может обработать его через `on_data`).
    /// Возвращает `None` если UID найден и response отправлен в receiver.
    pub fn dispatch(&self, resp: EngineResponse) -> Option<EngineResponse> {
        let mut state = self.lock_state();
        let now = Instant::now();
        let Some(entry) = state.map.remove(&resp.request_uid) else {
            Self::sweep_expired_locked(&mut state, now, false);
            return Some(resp);
        };
        if entry.deadline <= now {
            return Some(resp);
        }
        {
            // Если receiver был дропнут — отправка fails, response теряется.
            // Это нормально: waiter уже не ждёт.
            let _ = entry.tx.send(resp);
            None
        }
    }

    /// Remove the live waiter for `uid`, build the response outside the mutex,
    /// then deliver it. If parsing fails, the waiter is reinserted until its
    /// original deadline, matching the old `contains(uid); parse; dispatch`
    /// behavior where malformed payloads did not drop the pending slot.
    pub(crate) fn dispatch_registered_with<F>(&self, uid: u64, build: F) -> bool
    where
        F: FnOnce() -> Option<EngineResponse>,
    {
        let now = Instant::now();
        let entry = {
            let mut state = self.lock_state();
            let Some(entry) = state.map.remove(&uid) else {
                Self::sweep_expired_locked(&mut state, now, false);
                return false;
            };
            if entry.deadline <= now {
                return false;
            }
            entry
        };

        let Some(resp) = build().filter(|resp| resp.request_uid == uid) else {
            let now = Instant::now();
            if entry.deadline > now {
                let mut state = self.lock_state();
                state.map.entry(uid).or_insert(entry);
            }
            return false;
        };

        let _ = entry.tx.send(resp);
        true
    }

    /// Удалить ожидание (например при timeout) чтобы освободить sender и не накапливать map.
    pub fn remove(&self, uid: u64) {
        self.lock_state().map.remove(&uid);
    }

    /// Test helper: check whether a live waiter exists for `uid`.
    #[cfg(test)]
    pub(crate) fn contains(&self, uid: u64) -> bool {
        let mut state = self.lock_state();
        let now = Instant::now();
        match state.map.get(&uid) {
            Some(entry) if entry.deadline > now => true,
            Some(_) => {
                state.map.remove(&uid);
                false
            }
            None => {
                Self::sweep_expired_locked(&mut state, now, false);
                false
            }
        }
    }

    /// Remove expired pending slots. Throttled unless `force` is set.
    #[cfg(test)]
    fn cleanup_expired(&self, force: bool) -> usize {
        let now = Instant::now();
        let mut state = self.lock_state();
        Self::sweep_expired_locked(&mut state, now, force)
    }

    /// Количество активных ожиданий.
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        let _ = self.cleanup_expired(true);
        self.lock_state().map.len()
    }

    /// Очистить все ожидания (например при reconnect).
    #[cfg(test)]
    pub fn clear(&self) {
        self.lock_state().map.clear();
    }
}

impl Default for ApiPending {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            state: Mutex::new(PendingState {
                map: HashMap::new(),
                last_sweep: now,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use std::time::Duration;

    fn mk_resp(uid: u64) -> EngineResponse {
        EngineResponse {
            ver: 3,
            request_uid: uid,
            method: EngineMethod::BaseCheck,
            success: true,
            error_code: 0,
            error_msg: String::new(),
            data: Vec::new(),
        }
    }

    #[test]
    fn register_dispatch_receives() {
        let p = ApiPending::default();
        let rx = p.register(42);
        let consumed = p.dispatch(mk_resp(42));
        assert!(consumed.is_none(), "should be consumed");
        let resp = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(resp.request_uid, 42);
    }

    #[test]
    fn dispatch_no_waiter_returns_resp() {
        let p = ApiPending::default();
        let resp = p.dispatch(mk_resp(99));
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().request_uid, 99);
    }

    #[test]
    fn remove_drops_sender() {
        let p = ApiPending::default();
        let rx = p.register(10);
        assert_eq!(p.pending_count(), 1);
        p.remove(10);
        assert_eq!(p.pending_count(), 0);
        // recv должен вернуть error (Sender дропнут)
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn re_register_drops_old_sender() {
        let p = ApiPending::default();
        let rx_old = p.register(7);
        let rx_new = p.register(7);
        // Старый sender дропнут — recv должен вернуть error.
        assert!(rx_old.recv_timeout(Duration::from_millis(50)).is_err());
        // Новый sender активен.
        p.dispatch(mk_resp(7));
        let r = rx_new.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(r.request_uid, 7);
    }

    #[test]
    fn clear_removes_all() {
        let p = ApiPending::default();
        let _ = p.register(1);
        let _ = p.register(2);
        let _ = p.register(3);
        assert_eq!(p.pending_count(), 3);
        p.clear();
        assert_eq!(p.pending_count(), 0);
    }

    #[test]
    fn arc_shareable_across_threads() {
        let p = ApiPending::new_arc();
        let rx = p.register(5);
        let p_clone = p.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            p_clone.dispatch(mk_resp(5));
        });
        let resp = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        assert_eq!(resp.request_uid, 5);
        handle.join().unwrap();
    }

    #[test]
    fn clear_disconnects_registered_receivers() {
        let p = ApiPending::default();
        let rx = p.register(1);
        p.clear();
        assert_eq!(p.pending_count(), 0);
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn expired_slot_is_not_consumed() {
        let p = ApiPending::default();
        let rx = p.register_with_timeout(42, Duration::ZERO);
        let returned = p.dispatch(mk_resp(42));
        assert!(returned.is_some(), "expired response should fall through");
        assert_eq!(p.pending_count(), 0);
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn next_register_sweeps_expired_slots() {
        let p = ApiPending::default();
        let rx_old = p.register_with_timeout(1, Duration::ZERO);
        assert!(!p.contains(1));
        let _rx_new = p.register(2);
        assert_eq!(p.pending_count(), 1);
        assert!(rx_old.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn dispatch_registered_with_delivers_parsed_response() {
        let p = ApiPending::default();
        let rx = p.register(42);

        assert!(p.dispatch_registered_with(42, || Some(mk_resp(42))));

        let resp = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(resp.request_uid, 42);
        assert_eq!(p.pending_count(), 0);
    }

    #[test]
    fn dispatch_registered_with_parse_failure_keeps_waiter() {
        let p = ApiPending::default();
        let rx = p.register(42);

        assert!(!p.dispatch_registered_with(42, || None));
        assert_eq!(p.pending_count(), 1);
        assert!(p.dispatch_registered_with(42, || Some(mk_resp(42))));

        let resp = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(resp.request_uid, 42);
        assert_eq!(p.pending_count(), 0);
    }
}
