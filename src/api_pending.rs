//! Pending Engine API responses registry.
//!
//! Клиент отправляет `TEngineRequest` с уникальным UID; сервер отвечает `TEngineResponse`
//! с тем же UID. `ApiPending` хранит маппинг `uid → mpsc::Sender<EngineResponse>` чтобы
//! приложение могло **дождаться** ответа через blocking `recv` или `recv_timeout`.
//!
//! Использование (sync):
//! ```ignore
//! let raw = build_get_markets_list();
//! let uid = u64::from_le_bytes(raw[3..11].try_into().unwrap());
//! let rx = client.api_pending.register(uid);
//! client.send_api_request(&raw);
//! match rx.recv_timeout(Duration::from_secs(10)) {
//!     Ok(resp) => process(resp),
//!     Err(_) => { client.api_pending.remove(uid); /* timeout */ }
//! }
//! ```
//!
//! Для async (tokio) — потребитель оборачивает `recv` в `spawn_blocking`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;

use crate::commands::engine_api::EngineResponse;

/// Реестр pending Engine API запросов.
///
/// Thread-safe (внутри `Arc<Mutex>`). Можно клонировать `Arc<ApiPending>` и передавать в любые потоки.
pub struct ApiPending {
    map: Mutex<HashMap<u64, mpsc::Sender<EngineResponse>>>,
}

impl ApiPending {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { map: Mutex::new(HashMap::new()) })
    }

    /// Зарегистрировать ожидание ответа по `uid`. Возвращает receiver — потребитель
    /// делает `rx.recv_timeout(...)` для ожидания.
    ///
    /// Если на тот же `uid` уже была регистрация — старый sender дропается (старый
    /// receiver получит "channel closed" при попытке recv).
    pub fn register(&self, uid: u64) -> mpsc::Receiver<EngineResponse> {
        let (tx, rx) = mpsc::channel();
        self.map.lock().unwrap().insert(uid, tx);
        rx
    }

    /// Доставить response в ожидающего receiver'а.
    ///
    /// Возвращает `Some(resp)` если UID **не зарегистрирован** (response пришёл "сам",
    /// без активного waitера — потребитель может обработать его через `on_data`).
    /// Возвращает `None` если UID найден и response отправлен в receiver.
    pub fn dispatch(&self, resp: EngineResponse) -> Option<EngineResponse> {
        let mut map = self.map.lock().unwrap();
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
        self.map.lock().unwrap().remove(&uid);
    }

    /// Количество активных ожиданий.
    pub fn pending_count(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Очистить все ожидания (например при reconnect).
    pub fn clear(&self) {
        self.map.lock().unwrap().clear();
    }
}

impl Default for ApiPending {
    fn default() -> Self {
        Self { map: Mutex::new(HashMap::new()) }
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
        let p = ApiPending::new();
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
}
