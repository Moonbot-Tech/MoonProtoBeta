# Lifecycle callbacks

Типизированные события состояния канала связи с сервером. Подключай callback
через [`Client::on_lifecycle`](client.md). Callback выполняется в **main thread**
(тот же что обрабатывает приём UDP, retry, heartbeat).

## LifecycleEvent

```rust
pub enum LifecycleEvent {
    /// Handshake начат (Hello отправлен), Fine ещё не получен. Никаких действий
    /// от app не нужно — клиент сам пробует, retry'ит, переключает порты.
    Connecting,

    /// Fine получен — канал авторизован и готов.
    ///
    /// `fresh: true`  — это **первый** Connected с момента `Client::new`.
    ///                  App может одноразово показать "Welcome" / выполнить init.
    /// `fresh: false` — это re-connect после потери связи / server restart /
    ///                  port rotation. Registry уже выполнил re-subscribe — app
    ///                  ничего делать не должен.
    Connected { fresh: bool },

    /// Канал закрыт явным `client.disconnect()`. Финальное состояние — для
    /// возобновления связи нужен новый `Client::new`.
    Disconnected,

    /// Потеря связи > порога (RECONNECT_WAITING_MS = 7с). Клиент **сам** пытается
    /// soft-reconnect (HelloAgain без полного handshake). Если HelloAgain не
    /// проходит (сервер не помнит этого клиента) — следующий цикл начнётся с
    /// нового Hello → новый Connecting. **Никаких actionable действий от app**,
    /// только UI индикатор "переподключаемся".
    Reconnecting,

    /// ⚠ КРИТИЧЕСКОЕ: переполнен буфер pending H-priority команд (MAX_PENDING_H = 256).
    /// При долгой server silence без ACK retry-копии накапливаются, либа **молча
    /// выбрасывает** самые старые. Среди старых могут быть cancel_order /
    /// replace_order — потеря таких команд = торговый риск.
    ///
    /// App **обязан** реагировать: показать критический индикатор пользователю,
    /// возможно retry команды через свой механизм (если знает что недоотменено).
    SendBacklogCritical {
        cmd:       u8,    // TMoonProtoCommand этой команды (обычно Command::Order as u8)
        u_key_uid: u64,   // UKey.uid потерянного pending (для cancel/replace = Order.uid)
    },

    /// ⚠ КРИТИЧЕСКОЕ: невозможно открыть локальный UDP-сокет. 200 попыток `bind`
    /// упали несколько раз подряд. Типичные причины:
    /// - iOS/Android background restrictions (приложение в фоне);
    /// - CGNAT / ulimit (исчерпаны эфемерные порты);
    /// - EPERM / SELinux (нет прав на bind);
    /// - VPN config conflict (порт занят VPN-tunnel'ом).
    ///
    /// Либа сама retry'ит forever. App должен показать
    /// "Cannot bind UDP socket — check OS network permissions" вместо обычного
    /// "Connecting..." — иначе пользователь будет ждать молча.
    BindFailed {
        consecutive_failures: u32,   // сколько раз подряд весь 200-port retry упал
    },

    /// Детектирован перезапуск сервера: `PeerAppToken` изменился между сессиями.
    /// Liба сама:
    /// - отметила MarketsState.indexes_synchronized = false;
    /// - отправила api_get_markets_indexes();
    /// - блокирует обработку TradesStream/OrderBook пакетов до синхронизации;
    /// - после ServerToken change auto-replays subscriptions из registry.
    ///
    /// **Никаких actionable действий от app** — только UI tooltip "сервер был перезапущен".
    ServerRestart,
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
        LifecycleEvent::Connected { fresh: true } => {
            ui.show_status("Подключено");
            // Welcome-баннер / one-time init.
        }
        LifecycleEvent::Connected { fresh: false } => {
            ui.show_status("Подключено");
            // Никаких re-subscribe — либа сама сделала.
        }
        LifecycleEvent::Reconnecting => {
            ui.show_status("Переподключение...");
        }
        LifecycleEvent::ServerRestart => {
            ui.tooltip("Сервер был перезапущен. Данные обновляются.");
            // НЕ нужно делать api_get_markets_list() — либа сама.
        }
        LifecycleEvent::SendBacklogCritical { cmd, u_key_uid } => {
            ui.show_critical(format!(
                "Команда {cmd}/{u_key_uid} потерялась. Проверьте состояние ордера."
            ));
        }
        LifecycleEvent::BindFailed { consecutive_failures } => {
            ui.show_critical(format!(
                "Не могу открыть UDP-сокет ({} попыток). Проверьте сетевые разрешения.",
                consecutive_failures
            ));
        }
        LifecycleEvent::Disconnected => {
            ui.show_status("Отключено");
        }
    }
}));

client.run_with_dispatcher(/* ... */);
```

## State machine

```
       ┌─────────────────────────────────────────────────────┐
       │                                                     │
       ▼                                                     │
   ┌─────┐   Connecting    ┌───────────┐   Connected{fresh}  │
   │Base │────────────────▶│ Connected │──────────────────▶ ┌──────────┐
   └─────┘                 └───────────┘                    │ AuthDone │
       ▲                                                    └─────┬────┘
       │                                                          │
       │ Disconnected                                              │ Reconnecting
       │                                                           ▼
       │                  Connecting                         ┌──────────┐
       │       ┌────────────────────────────────────────────┤ Offline  │
       │       │                                              └──────────┘
       │       └──▶ (back to Connected → AuthDone, fresh:false)
       │
       └─────── on `client.disconnect()` или forever-retry эскалация
```

`ServerRestart` — горизонтальное событие, эмитится во время handshake при
смене `peer_app_token` (не меняет state-machine).

`SendBacklogCritical` и `BindFailed` — горизонтальные алерты, эмитятся в любом
состоянии (`SendBacklogCritical` обычно в AuthDone, `BindFailed` обычно в Base).

## Семантика переходов

| From → To | Event |
|---|---|
| `Base → Connected` | `Connecting` (cold start) |
| `Offline → Connected` | `Connecting` (soft reconnect) |
| `* → AuthDone` (первый раз за life Client) | `Connected { fresh: true }` |
| `Offline → AuthDone` (re-handshake) | `Connected { fresh: false }` |
| `AuthDone → Offline` | `Reconnecting` |
| `* → Disconnected` | `Disconnected` (от `disconnect()`) |
| на handshake (PeerAppToken changed) | `ServerRestart` |
| при переполнении pending_h | `SendBacklogCritical { cmd, u_key_uid }` |
| при многократных bind failures | `BindFailed { consecutive_failures }` |

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

- `prev_app_token != 0` — исключает cold start (первое подключение тоже меняет
  0 → app_token, но это не "restart").
- `prev != new` — реальная смена → сервер перезапустился между нашими сессиями.

После эмиссии события либа автоматически:
1. Помечает `MarketsState.indexes_synchronized = false`.
2. Отправляет `api_get_markets_indexes()`.
3. Блокирует обработку TradesStream/OrderBook пакетов до синхронизации.
4. После ServerToken change → `replay_subscriptions()` шлёт все subscriptions заново.

App только красит UI индикатор.

## `Connected { fresh: bool }` — точная семантика

`fresh = true` только при **первом** Connected за всю жизнь `Client` (флаг
`was_ever_connected` в Client). Для всех последующих re-handshake'ев — `false`.

Удобно для UI: показать "Welcome" / запустить init **один раз**, не дублировать
на каждом soft-reconnect.

В Delphi-боте этой различкой нет — там was-ever-connected отслеживался в
application layer. В Rust перенесено в либу (active library principle —
useful info для UI всегда).

## SendBacklogCritical — что делать

`pending_h` (pending H-priority команд с retry) имеет лимит `MAX_PENDING_H = 256`.
При server silence без ACK команды накапливаются. На превышении лимита — drop
oldest (FIFO) + emit event с информацией о dropped command.

**Среди dropped могут быть critical trade commands** (`replace_order`,
`cancel_order` с UK_OrderMove). Если такая команда не дошла — ордер не отменился /
не перенесён. Это торговый риск.

App pattern:
```rust
LifecycleEvent::SendBacklogCritical { cmd, u_key_uid } => {
    // 1. Показать алерт пользователю.
    ui.alert(format!("Команда cmd={cmd} uid={u_key_uid} потерялась"));

    // 2. Если знаем что это была отмена/переустановка ордера — попробовать
    //    через api_get_order(u_key_uid) узнать текущее состояние на сервере.
    let rx = client.api_get_order(u_key_uid);
    if let Ok(resp) = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(5)) {
        // Решить retry или ничего.
    }
}
```

См. `audit_robustness` C3 (HANDOFF reference).

## BindFailed — что делать

При невозможности `bind` UDP socket'а 200 попыток подряд — серия упала. На
каждой серии (cycle main loop) эмитится событие с `consecutive_failures` (1, 2, ...).

Типовое значение `consecutive_failures = 1` — первая серия. `≥ 3` = systemic
проблема (15+ секунд retry'ев впустую).

App pattern:
```rust
LifecycleEvent::BindFailed { consecutive_failures: n } => {
    if n >= 3 {
        ui.show_blocking_error(
            "Невозможно подключиться: операционная система блокирует UDP socket.\n\
             Проверьте network permissions / firewall / VPN-конфигурацию."
        );
    }
}
```

См. `audit_robustness` H9.

## Тяжёлые операции в callback'е

Lifecycle callback вызывается из main thread. Тяжёлые операции в нём блокируют
send loop. **Правило**: складывать события в очередь и обрабатывать в отдельном
потоке:

```rust
use std::sync::mpsc;
use std::thread;

let (tx, rx) = mpsc::channel::<LifecycleEvent>();
client.on_lifecycle(Box::new(move |ev| { let _ = tx.send(ev); }));

thread::spawn(move || {
    while let Ok(ev) = rx.recv() {
        // тяжёлая обработка — log в DB, push notification, etc.
    }
});
```

## См. также

- [client.md](client.md) — `Client::on_lifecycle`, AuthStatus, soft/hard reconnect.
- [events.md](events.md) — `EventDispatcher` для data событий (Order/Trades/...).
- [multi_server.md](multi_server.md) — independent lifecycle на Client'ы.
