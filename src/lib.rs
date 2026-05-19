//! # moonproto
//!
//! Rust клиент MoonProto — UDP-протокол связи с торговым ботом MoonBot
//! (Delphi-сервер на VPS). Криптография AES-128-GCM, аутентифицированный
//! HMAC-CRC32C MAC, replay protection через sliding bitmap window,
//! reliable delivery поверх UDP через Sliced+ACK, PMTU discovery.
//!
//! ## Quick start
//!
//! ```ignore
//! use std::time::Duration;
//! use moonproto::client::{Client, ClientConfig, LifecycleEvent};
//! use moonproto::events::EventDispatcher;
//! use moonproto::key_import;
//! use moonproto::ntp;
//!
//! // 1. Импорт ключа из base64-экспорта MoonBot (Settings → Export Key).
//! let keys = key_import::import_key(KEY_B64).expect("invalid key");
//!
//! // 2. NTP sync — рекомендуется для корректных timestamp'ов в ордерах.
//! let ntp_result = ntp::get_best_ntp("pool.ntp.org", 4);
//! if ntp_result.synced {
//!     moonproto::client::set_ntp_offset(ntp_result.time_offset);
//! }
//!
//! // 3. Конфиг клиента.
//! let cfg = ClientConfig {
//!     server_ip:   "127.0.0.1".to_string(),
//!     server_port: 3000,
//!     master_key:  keys.master_key,
//!     mac_key:     keys.mac_key,
//!     mask_ver:    0,                 // 0 = base transport, 1/2 требует moonext
//!     client_id:   rand::random(),
//! };
//! let mut client = Client::new(cfg);
//!
//! // 4. Lifecycle callback (опционально) — Connecting / Connected{fresh} / Reconnecting / etc.
//! client.on_lifecycle(Box::new(|ev: LifecycleEvent| {
//!     println!("[lifecycle] {:?}", ev);
//! }));
//!
//! // 5. EventDispatcher — авто-парсит входящие команды в типизированные события
//! //    (OrderEvent / TradesEvent / BalanceEvent / etc.) и обновляет sync-state.
//! let mut dispatcher = EventDispatcher::new();
//!
//! // 6. Запуск main loop — блокирующий вызов; возвращается через `duration` или
//! //    при `client.disconnect()`. Для async — оберни в `std::thread::spawn`.
//! client.run(Duration::from_secs(60), Box::new(move |cmd, payload| {
//!     let now_ms = std::time::SystemTime::now()
//!         .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
//!     for event in dispatcher.dispatch(cmd, payload, now_ms) {
//!         // обработка типизированных событий — см. `moonproto::events::Event`
//!         let _ = event;
//!     }
//! }));
//! ```
//!
//! Полный рабочий пример — `examples/client_test.rs`.
//!
//! ## Архитектура
//!
//! - [`crypto`] — AES-128-GCM с PKCS7 padding, SHAKE-128 key derivation
//! - [`protocol`] — Slider (replay protection), SlicingReceiver (re-assembly), CryptedHeader
//! - [`client`] — Client struct, lifecycle, handshake, retry, NTP, lifecycle events
//! - [`commands`] — wire-format builders/parsers для 11 каналов команд
//! - [`state`] — sync-state модели: Orders, OrderBooks, Trades, Balances, Strats, ...
//! - [`events`] — EventDispatcher — авто-роутинг входящих команд в типизированные события
//! - [`key_import`] — парсинг base64-экспорта ключей MoonBot
//! - [`ntp`] — SNTP клиент с 4-запросной reliability (TryCount=4)
//! - [`api_pending`] — registry для async-ответов на Engine API запросы
//! - [`compression`] — SynLZ decompress (byte-exact с mORMot)
//!
//! Зависит от [`moonproto_transport`] — низкоуровневый wire-layer (MAC, обфускация,
//! headers, опциональная загрузка `moonext` для extended transport mode 1/2).
//!
//! ## Gotchas
//!
//! - `client.run(duration, on_data)` **блокирует** вызывающий поток — для async
//!   запускай через `std::thread::spawn`.
//! - NTP-sync рекомендован до старта (без него timestamps в ордерах будут с
//!   uncorrected системным временем).
//! - PMTU стартует с 508 байт и растёт по probe — первые Sliced-сообщения
//!   фрагментируются мелко, нормально.
//! - UKey-dedup: команды с тем же UniqueKey замещают предыдущие в очереди
//!   отправки. Если шлёшь `replace_order` 5 раз подряд — сервер увидит только
//!   последний (полезное свойство, но знай об этом).
//! - На `LifecycleEvent::ServerRestart` нужно сбросить кэшированные market
//!   indexes и заново подписаться на order books — индексы рынков на сервере
//!   могли измениться при перезапуске.
//! - Compression auto-applied на payload > 64 байт (MIN_SIZE_TO_COMPRESS) —
//!   потребитель не управляет этим вручную.
//! - `LifecycleEvent::Reconnecting` — клиент сам пытается soft-reconnect, никакого
//!   действия не требуется. `Disconnected` — финальное состояние, нужен новый Client.
//!
//! ## Wire-format reference
//!
//! Подробная wire-документация per-команда: `moonproto/docs/commands/`.
//! Концептуальная архитектура задач протокола: внутренний `ARCHITECTURE.md`
//! проекта (поставляется в отдельной публикации для архитектора порта).

pub mod crypto;
pub mod protocol;
pub mod client;
pub mod compression;
pub mod commands;
pub mod state;
pub mod key_import;
pub mod ntp;
pub mod api_pending;
pub mod events;

pub use moonproto_transport::{MoonKey, ServerMsgHeader};
