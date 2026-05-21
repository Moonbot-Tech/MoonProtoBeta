//! Pending Engine API response registry.
//!
//! Клиент отправляет `TEngineRequest` с уникальным UID; сервер отвечает
//! `TEngineResponse` с тем же UID. `ApiPending` хранит маппинг
//! `uid → mpsc::Sender<EngineResponse>`.
//!
//! Обычным приложениям лучше использовать one-shot helpers вроде
//! [`crate::client::Client::request_balance`] или
//! [`crate::client::Client::request_engine_response`]. Если нужен raw async
//! receiver, используй `Client::api_*` wrappers совместно с
//! [`crate::client::Client::run_until_response`] — тогда тот же thread продолжает
//! прокачивать UDP main loop пока ждёт response:
//! ```ignore
//! let rx = client.api_get_markets_list();
//! let response = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
//! ```
//!
//! Прямой `rx.recv_timeout(...)` подходит только когда другой thread уже крутит
//! main loop клиента.
//!
//! Pending slot lifetime follows Delphi `TMoonProtoEngine.SendAndWait`: the
//! caller that waits owns the timeout and removes the slot on timeout. There is
//! no independent fixed-age cleanup in the main loop; hard reconnect/full reset
//! still clears all stale slots because their UIDs belong to the previous
//! session.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::commands::engine_api::EngineResponse;

/// Default request/response timeout — 12 секунд. Совпадает с Delphi
/// `TMoonProtoEngine.FTimeout = 12000` (MoonProtoEngine.pas) для `SendAndWait`.
pub const DEFAULT_PENDING_TIMEOUT_MS: i64 = 12_000;

/// Реестр pending Engine API запросов.
///
/// Thread-safe (внутри `Arc<Mutex>`). Можно клонировать `Arc<ApiPending>` и передавать в любые потоки.
///
pub struct ApiPending {
    map: Mutex<HashMap<u64, mpsc::Sender<EngineResponse>>>,
}

impl ApiPending {
    /// Convenience: построить уже обёрнутый `Arc<ApiPending>`. Большинство callers
    /// хотят shared доступ (Client держит, reader thread получает clone'd Arc).
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
    fn lock_map(&self) -> std::sync::MutexGuard<'_, HashMap<u64, mpsc::Sender<EngineResponse>>> {
        match self.map.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                log::warn!(target: "moonproto::api_pending",
                    "ApiPending mutex poisoned — recovering, in-flight requests may be lost");
                poisoned.into_inner()
            }
        }
    }

    /// Зарегистрировать ожидание ответа по `uid`.
    ///
    /// Для обычного однопоточного клиента передай возвращённый receiver в
    /// [`crate::client::Client::run_until_response`]. Прямой `rx.recv_timeout(...)`
    /// подходит только когда другой thread уже крутит main loop клиента.
    ///
    /// Если на тот же `uid` уже была регистрация — старый sender дропается (старый
    /// receiver получит "channel closed").
    pub fn register(&self, uid: u64) -> mpsc::Receiver<EngineResponse> {
        let (tx, rx) = mpsc::channel();
        self.lock_map().insert(uid, tx);
        rx
    }

    /// Доставить response в ожидающего receiver'а.
    ///
    /// Возвращает `Some(resp)` если UID **не зарегистрирован** (response пришёл "сам",
    /// без активного waitера — потребитель может обработать его через `on_data`).
    /// Возвращает `None` если UID найден и response отправлен в receiver.
    pub fn dispatch(&self, resp: EngineResponse) -> Option<EngineResponse> {
        let mut map = self.lock_map();
        if let Some(tx) = map.remove(&resp.request_uid) {
            // Если receiver был дропнут — отправка fails, response теряется.
            // Это нормально: waiter уже не ждёт.
            let _ = tx.send(resp);
            None
        } else {
            Some(resp)
        }
    }

    /// Удалить ожидание (например при timeout) чтобы освободить sender и не накапливать map.
    pub fn remove(&self, uid: u64) {
        self.lock_map().remove(&uid);
    }

    /// Количество активных ожиданий.
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.lock_map().len()
    }

    /// Очистить все ожидания (например при reconnect).
    pub fn clear(&self) {
        self.lock_map().clear();
    }
}

impl Default for ApiPending {
    fn default() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
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
}
