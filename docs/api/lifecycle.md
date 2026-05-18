# Lifecycle callbacks

Типизированные события состояния канала. Уведомляют потребителя о фазах подключения: handshake, ready, reconnect, server restart, disconnect.

## LifecycleEvent

```rust
pub enum LifecycleEvent {
    /// Handshake начат (Hello отправлен), но Fine ещё не получен.
    /// Эмитится при первом подключении (cold start) И при soft reconnect (после Offline).
    Connecting,
    
    /// Fine получен — канал авторизован и готов к работе.
    Authenticated,
    
    /// Потеря связи (>= 7000ms без активности), ждём соединения.
    /// Эмитится при переходе AuthDone → Offline.
    Reconnecting,
    
    /// Сервер перезапустился (PeerAppToken изменился между сессиями).
    /// Прикладной слой должен сбросить кэши и сделать re-init (markets, balances, ...).
    /// Эмитится в `handle_handshake` при детекции изменения `peer_app_token`.
    ServerRestart,
    
    /// Канал закрыт. Возможные причины:
    /// - Явный `client.disconnect()` от потребителя.
    /// - `bind_socket` failure (200 неудачных попыток подряд).
    Disconnected,
}
```

## Использование

```rust
use moonproto::client::{Client, LifecycleEvent};

let mut client = Client::new(cfg);
client.on_lifecycle(Box::new(|ev| {
    match ev {
        LifecycleEvent::Connecting => {
            ui.show_status("Подключение...");
        }
        LifecycleEvent::Authenticated => {
            ui.show_status("Подключено");
            // Запросить начальный snapshot
        }
        LifecycleEvent::Reconnecting => {
            ui.show_status("Переподключение...");
        }
        LifecycleEvent::ServerRestart => {
            // Сервер рестарт — сбросить кэши, перезапросить markets/balance
            cache.clear();
            client.api_get_markets_list();  // и т.д.
        }
        LifecycleEvent::Disconnected => {
            ui.show_status("Отключено");
        }
    }
}));

client.run(/* ... */);
```

## State machine

```
       ┌──────────────────────────────────────────────────┐
       │                                                  │
       ▼                                                  │
   ┌─────┐  Connecting   ┌───────────┐  Authenticated ┌──────────┐
   │Base │──────────────▶│ Connected │──────────────▶│ AuthDone │
   └─────┘               └───────────┘                └─────┬────┘
       ▲                                                    │
       │                                                    │ Reconnecting
       │ Disconnected                                       ▼
       │                                              ┌──────────┐
       │                  Connecting                  │ Offline  │
       │       ┌──────────────────────────────────────┴──────────┘
       │       │
       │       └──▶ (back to Connected → AuthDone)
       │
       └─────── on `client.disconnect()` или bind failure
```

`ServerRestart` — горизонтальное событие, эмитится во время `handshake` при переходе `Connecting → Authenticated` если детектирована смена `peer_app_token` (не меняет state-machine выше).

## Семантика переходов

| From → To | Event |
|---|---|
| `Base → Connected` | `Connecting` (cold start) |
| `Offline → Connected` | `Connecting` (soft reconnect) |
| `* → AuthDone` (если был не AuthDone) | `Authenticated` |
| `AuthDone → Offline` | `Reconnecting` |
| `* → Base` (явный disconnect или bind fail) | `Disconnected` |
| при handshake | `ServerRestart` (если `peer_app_token` изменился) |

## ServerRestart detection — детали

В Delphi (`MoonProtoEngine.pas:694-696`):
```pascal
If (MClient.Client.PeerAppToken <> FLastServerAppToken)
   and (FLastServerAppToken <> 0)
   and not FirstCreateMarkets then
    FServerWasRestarted := true;
```

В Rust порте (`handle_handshake` при `Command::WhoAreYou`):
```rust
let prev_app_token = self.peer_app_token;
self.peer_app_token = hello.app_token;
if prev_app_token != 0 && prev_app_token != hello.app_token {
    self.fire_lifecycle(LifecycleEvent::ServerRestart);
}
```

- `prev_app_token != 0` — исключает cold start (первое подключение тоже меняет 0 → hello.app_token, но это не "restart").
- `prev != new` — реальная смена → сервер перезапустился между нашими сессиями.

## Disconnect от bind_socket failure

Если `bind_socket()` не смог открыть сокет 200 попыток подряд (все порты заняты или нет прав), эмитится `Disconnected` с переходом в `Base`:

```rust
// bind_socket внутри:
if self.auth_status != AuthStatus::Base {
    self.auth_status = AuthStatus::Base;
    self.need_connect = false;
    self.fire_lifecycle(LifecycleEvent::Disconnected);
}
```

Это уведомление потребителю что **транспорт сдохол** — раньше клиент молча висел.

## Тяжёлые операции в callback'е

Lifecycle callback вызывается из **main thread** Client'а (тот же поток что обрабатывает приём UDP и retry/heartbeat). Тяжёлые операции в нём блокируют send loop. **Правило**: складывать события в очередь и обрабатывать в отдельном потоке:

```rust
let (tx, rx) = std::sync::mpsc::channel::<LifecycleEvent>();
client.on_lifecycle(Box::new(move |ev| { let _ = tx.send(ev); }));

std::thread::spawn(move || {
    while let Ok(ev) = rx.recv() {
        // тяжёлая обработка
    }
});
```

## См. также

- [client.md](client.md) — `Client::on_lifecycle`, AuthStatus, soft/hard reconnect.
- [events.md](events.md) — `EventDispatcher` для других каналов.
- DEVIATION #18 — Lifecycle abstraction (Rust extension, нет в Delphi).
