# UI channel (MPC_UI)

Канал UI настроек и управляющих команд между терминалом и сервером.

## Что это

UI канал отвечает за все настройки бота (xSell, стоп-лоссы, чёрный список монет,
авто-старт, hot-keys, ArbConfig), переключение биржи/спота, управление
маркет-мейкер ордерами, версионирование и старт/стоп стратегий.

В либе:
1. **Wire-парсеры и билдеры** в `commands::ui`.
2. **Sync state** в `state::SettingsState` — snapshot последних настроек.
3. **High-level Client wrappers** — `client.ui_*` методы.

## Подкоманды

| CmdId | Команда | Направление | Priority | Что |
|---|---|---|---|---|
| 1 | `ClientSettings` | both | Sliced | Полный snapshot UI настроек (~100-1000 байт) |
| 2 | `SettingsRequest` | C→S | High | Запросить у сервера актуальные настройки |
| 3 | `StratStartStop` | C→S | High | Старт/стоп всех активных стратегий (v1) |
| 4 | `StratStartStopV2` | C→S | High | То же + дельта `checked` стратегий |
| 5 | `MMOrdersSubscribe` | C→S | High | Включить/выключить детектирование MM ордеров |
| 6 | `UpdateVersion` | both | High | Уведомление о доступной новой версии |
| 7 | `EmuTrades` | S→C | Sliced | Серия эмулированных тиков для одного маркета |
| 8 | `NewMarketNotify` | S→C | High | Появился новый маркет на бирже |
| 9 | `LevManage` | C→S | Sliced | Автоматическое управление плечом |
| 10 | `TriggerManage` | C→S | Sliced | Управление hotkey-триггерами по маркетам |
| 11 | `ResetProfit` | C→S | High | Сбросить накопленный профит |
| 12 | `ArbActivateNotify` | S→C | High | Arb-лицензия активирована до момента `arb_valid` |
| 13 | `SwitchDex` | C→S | High | Переключить активный DEX |
| 14 | `SwitchSpot` | C→S | High | Переключить spot/predict рынок |

## Получение событий (через EventDispatcher)

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::SettingsEvent;

let mut dispatcher = EventDispatcher::new();
client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Settings(SettingsEvent::ClientSettingsUpdated) => {
        let s = dispatcher.settings().client_settings.as_ref().unwrap();
        // применить s.x_sell, s.fixed_sell_price, etc. в UI
    }
    Event::Settings(SettingsEvent::DexSwitched(d)) => {
        // юзер на сервере переключил DEX
    }
    Event::Settings(SettingsEvent::ArbActivated(_)) => {
        // Arb лицензия продлена до dispatcher.settings().arb_valid_until
    }
    _ => {}
}));
```

## Низкоуровневый парсер

```rust
use moonproto::commands::ui::UICommand;

if let Some(cmd) = UICommand::parse(&payload) {
    match cmd {
        UICommand::ClientSettings(s) => {
            println!("xSell = {}, xSellScalp = {}", s.x_sell, s.x_sell_scalp);
        }
        UICommand::SwitchDex(d) => {
            println!("Active DEX: {}", d.dex_name);
        }
        UICommand::Unknown { cmd_id, .. } => {
            eprintln!("Unknown UI sub-command: {}", cmd_id);
        }
        _ => {}
    }
}
```

Forward-compatibility: если `ver` в заголовке больше текущего (3), возвращается
`UICommand::Unknown` — клиент не падает на будущих версиях формата.

## Sync state — SettingsState

`SettingsState` хранит последний snapshot настроек и пропускает action-команды
через события:

| Поле | Тип | Что |
|---|---|---|
| `client_settings` | `Option<ClientSettingsCommand>` | Последний полный snapshot UI настроек |
| `lev_manage` | `Option<LevManage>` | Последний snapshot leverage management |
| `mm_orders_subscribed` | `bool` | Включено ли детектирование MM ордеров |
| `current_dex` | `Option<String>` | Текущий выбранный DEX |
| `current_spot` | `Option<u8>` | 0=Crypto, 1=Predict |
| `arb_valid_until` | `Option<f64>` | TDateTime до момента активной Arb лицензии |

Доступ через `dispatcher.settings()`.

## Client wrappers — отправка команд

```rust
// Полный snapshot настроек (Sliced + UK_BaseUISettings)
let cmd = moonproto::commands::ui::ClientSettingsCommand {
    uid: 1,
    x_sell: 50,
    /* ... все поля ... */
};
client.ui_send_settings(&cmd);

// Запросить настройки с сервера
client.ui_settings_request();

// Старт/стоп всех стратегий
client.ui_strat_start_stop(true);

// V2 со списком конкретных стратегий
let items = vec![
    moonproto::commands::strat::StratCheckedItem { strategy_id: 100, checked: true },
];
client.ui_strat_start_stop_v2(true, &items);

// Включить/выключить детектирование MM ордеров (UK_TurnMMDetection)
client.ui_mm_subscribe(true);

// Уведомить о версии клиента
client.ui_update_version("MoonKernel v1.0", /* is_release = */ true);

// Эмуляция трейдов для тестового рынка
let points = vec![ /* ... EmuTradePoint */ ];
client.ui_emu_trades(market_idx, base_time, &points);

// Уведомить о новом маркете
client.ui_new_market_notify();

// Leverage management (UK_LevManageSettings)
let lev = moonproto::commands::ui::LevManage { /* ... */ };
client.ui_lev_manage(&lev);

// Trigger management (batch hotkey'ев)
client.ui_trigger_manage(action, all_markets, &markets, &keys);

// Сброс profit-счётчиков
client.ui_reset_profit(kind);

// Уведомление об активации Arb
client.ui_arb_activate_notify(arb_valid_datetime);

// Переключить DEX (UK_DexSwitch, ShortString[15])
client.ui_switch_dex("Uni");

// Переключить spot режим (UK_SpotSwitch)
client.ui_switch_spot(spot_index);
```

## ClientSettings — полный snapshot

Основная и самая большая команда. Содержит:

- **Базовые настройки продажи**: `x_sell`, `x_sell_scalp`, `x_tmode`,
  `fixed_sell_mode` + `fixed_sell_price`.
- **Стоп-лосс**: `price_drop_level`, `trailing_drop`, `g_take_profit` +
  `use_g_take_profit`, `panic_if_price_drop`.
- **Iceberg**: `buy_iceberg`, `sell_iceberg`.
- **Подпись ордеров**: `sign_orders`.
- **Чёрный список монет**: `coins_black_list_text`, `use_coins_black_list`,
  `temp_bl_symbols/times`.
- **Ручная стратегия**: `use_manual_strategy`, `manual_strategy_id`.
- **Stop market**: `free_position_check`, `vol_drop_level`, `use_stop_market`.
- **AutoStart**: `as_cfg` (104 байта) + `as_cfg2` (168 байт) — opaque binary blobs.
  Размеры доступны как `commands::ui::AS_CFG_SIZE` / `AS_CFG2_SIZE`.
- **Hot-keys**: `s_price[6]`, `sb_num`.
- **MultiOrders**: `join_sell_kind` (0=None, 1=FixPrice, 2=FixProfit).
- **ArbConfig**: `arb_config: ArbConfigCompact` — `wanted[256]` маска платформ + флаги.

### Дефолты ArbConfig

Если поле отсутствует в payload (старый формат):
```rust
ArbConfigCompact {
    wanted: [false; 256],
    show_lines: true,
    show_percent: true,
    show_absolute: false,
    show_numbers: false,
    show_right: false,
}
```

## UniqueKeys (UK)

| Команда | UKey |
|---|---|
| `ClientSettings` | `UK_BaseUISettings` (UID = 1) |
| `MMOrdersSubscribe` | `UK_TurnMMDetection` |
| `LevManage` | `UK_LevManageSettings` (UID = 1) |
| `SwitchDex` | `UK_DexSwitch` |
| `SwitchSpot` | `UK_SpotSwitch` |

Для команд с UK последняя отправка побеждает.

## Wire format reference

Все команды начинаются с базового заголовка `TBaseUICommand`:
```
cmd_id:u8 + ver:u16 LE + UID:u64 LE  (11 bytes)
```

Затем идёт class-specific payload. Целые/плавающие — LE. Строки — UTF-8 с
u16 LE префиксом. Boolean — 1 байт.

### ShortString[15] (SwitchDex)
16 байт: `len:u8 (0..15) + bytes(15)`. Незаполненный хвост — нулями.

### ArbConfig compact
```
arb_ver:u8 = 1
wanted:bytes(32)       // 256-bit маска
flags:u8               // bit0=Absolute, bit1=Numbers, bit2=Lines, bit3=Percent, bit4=Right
color_count:u8 + bytes(color_count * 5)   // legacy, skip
```

### AutoStartConfig / AutoStartConfig2
```
size:u16 LE            // SizeOf(TAutoStartConfig) или SizeOf(TAutoStartConfig2)
bytes(size)            // raw packed record
```

Soft-size: если `size` больше известного — лишние байты skip; если меньше —
частичное чтение.

## См. также

- [strats.md](strats.md) — связанный канал стратегий (UK_StratSnapshot).
- [client.md](client.md) — Client::ui_* wrappers + lifecycle.
- [events.md](events.md) — EventDispatcher + Event::Settings.
- [arb.md](arb.md) — `TArbPricesCommand` (поток Arb цен после ArbActivateNotify).
