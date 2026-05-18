# UI channel (MPC_UI)

Канал UI настроек и управляющих команд между терминалом и сервером.

## Что это

UI канал отвечает за все настройки бота (xSell, стоп-лоссы, чёрный список монет, авто-старт, hot-keys, ArbConfig), переключение биржи/спота, управление маркет-мейкер ордерами, версионирование и старт/стоп стратегий.

В либе реализовано два уровня:
1. **Wire-парсеры и билдеры** в `commands::ui` — байтовый протокол.
2. **Sync state** в `state::SettingsState` — snapshot последних настроек.

---

## Подкоманды

| CmdId | Команда | Направление | Priority | Что |
|---|---|---|---|---|
| 1 | `ClientSettings` | both | Sliced | Полный snapshot UI настроек (большой, ~100-1000 байт) |
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

---

## Парсинг входящих

Канал MPC_UI приходит в общем потоке `MoonProtoCommand`. После dispatch'а по `MPC_UI` payload передаётся в `UICommand::parse()`:

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

Forward-compatibility: если `ver` в заголовке больше текущего (3), возвращается `UICommand::Unknown` — клиент не падает на будущих версиях формата.

---

## Sync state

`SettingsState` хранит последний snapshot настроек и пропускает action-команды через события:

```rust
use moonproto::state::SettingsState;

let mut settings = SettingsState::new();

let event = settings.apply(cmd);

match event {
    SettingsEvent::ClientSettingsUpdated => {
        let s = settings.client_settings.as_ref().unwrap();
        // применить s.x_sell, s.fixed_sell_price, etc. в UI
    }
    SettingsEvent::DexSwitched(s) => {
        // юзер на сервере переключил DEX
    }
    SettingsEvent::ArbActivated(_) => {
        // Arb лицензия продлена до settings.arb_valid_until
    }
    _ => {}
}
```

Хранимые поля:

| Поле | Тип | Что |
|---|---|---|
| `client_settings` | `Option<ClientSettingsCommand>` | Последний полный snapshot UI настроек |
| `lev_manage` | `Option<LevManage>` | Последний snapshot leverage management |
| `mm_orders_subscribed` | `bool` | Включено ли детектирование MM ордеров |
| `current_dex` | `Option<String>` | Текущий выбранный DEX |
| `current_spot` | `Option<u8>` | 0=Crypto, 1=Predict |
| `arb_valid_until` | `Option<f64>` | TDateTime до какого момента активна Arb лицензия |

---

## ClientSettings — полный snapshot

Основная и самая большая команда. Содержит:

- **Базовые настройки продажи**: `x_sell`, `x_sell_scalp`, `x_tmode` (множитель x10), `fixed_sell_mode` + `fixed_sell_price`.
- **Стоп-лосс**: `price_drop_level`, `trailing_drop`, `g_take_profit` + `use_g_take_profit`, `panic_if_price_drop`.
- **Iceberg**: `buy_iceberg`, `sell_iceberg`.
- **Подпись ордеров**: `sign_orders` (требовать подпись на placement ордеров).
- **Чёрный список монет**: `coins_black_list_text` (CSV), `use_coins_black_list`, `temp_bl_symbols/times` (временные).
- **Ручная стратегия**: `use_manual_strategy`, `manual_strategy_id`.
- **Stop market**: `free_position_check`, `vol_drop_level`, `use_stop_market`.
- **AutoStart**: `as_cfg` (104 байта) + `as_cfg2` (168 байт) — opaque binary blobs из Delphi `TAutoStartConfig/TAutoStartConfig2`. Размеры доступны как `commands::ui::AS_CFG_SIZE` / `AS_CFG2_SIZE`. На проводе передаются с soft-size префиксом (старые версии могут быть короче).
- **Hot-keys**: `s_price[6]` (цены sell для кнопок 1..6), `sb_num` (выбранная кнопка).
- **MultiOrders**: `join_sell_kind` (0=None, 1=FixPrice, 2=FixProfit).
- **ArbConfig**: `arb_config: ArbConfigCompact` — `wanted[256]` маска платформ + флаги отображения.

### Дефолты ArbConfig

Если поле отсутствует в payload (старый формат), используются defaults:
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

---

## Построение исходящих

Для каждой подкоманды есть `build_*` функция. UID для уникальных команд (с UKey) — обычно `1` (overlap последних), для остальных — `rand::random::<u64>()`.

```rust
use moonproto::commands::ui::*;

// Запросить настройки у сервера
let req = build_settings_request(rand::random::<u64>());
client.send(MPC_UI, &req).await?;

// Сменить DEX
let raw = build_switch_dex(1, "Uni");  // UID=1 (UK_DexSwitch)
client.send(MPC_UI, &raw).await?;

// Старт всех стратегий
let raw = build_strat_start_stop(rand::random::<u64>(), true);
client.send(MPC_UI, &raw).await?;

// MM подписка
let raw = build_mm_orders_subscribe(1, true);  // UID=1 (UK_TurnMMDetection)
client.send(MPC_UI, &raw).await?;
```

### Отправить полный snapshot настроек

```rust
let cmd = ClientSettingsCommand {
    uid: 1,  // UK_BaseUISettings → overlap
    x_sell: 50,
    x_sell_scalp: 10,
    // ... все поля ...
    as_cfg:  vec![0; AS_CFG_SIZE],
    as_cfg2: vec![0; AS_CFG2_SIZE],
    s_price: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    sb_num: 0,
    join_sell_kind: 0,
    arb_config: ArbConfigCompact::default(),
};

let raw = build_client_settings(&cmd);
client.send(MPC_UI, &raw).await?;
```

---

## UniqueKeys (UK)

Несколько команд имеют unique-key — серверный sliding window заменяет старые повторы новыми. Это значит при многократной отправке последняя побеждает.

| Команда | UKey |
|---|---|
| `ClientSettings` | `UK_BaseUISettings` (UID = 1) |
| `MMOrdersSubscribe` | `UK_TurnMMDetection` |
| `LevManage` | `UK_LevManageSettings` (UID = 1) |
| `SwitchDex` | `UK_DexSwitch` |
| `SwitchSpot` | `UK_SpotSwitch` |

Для этих команд используйте фиксированный `UID = 1` чтобы предыдущие отправки автоматически заменялись.

---

## Wire format reference

Все команды начинаются с базового заголовка `TBaseUICommand`:
```
cmd_id:u8 + ver:u16 LE + UID:u64 LE  (11 bytes)
```
Затем идёт class-specific payload. Все целые и плавающие — little-endian. Строки — UTF-8 с u16 LE префиксом длины. Boolean — 1 байт (`0`=false, иначе true).

### ShortString[15] (SwitchDex)
16 байт на проводе: `len:u8 (0..15) + bytes(15)`. Незаполненный хвост — нулями.

### ArbConfig compact
```
arb_ver:u8 = 1  
wanted:bytes(32)        // 256-bit маска: bit i байта i/8 = wanted[i]
flags:u8                // bit0=Absolute, bit1=Numbers, bit2=Lines, bit3=Percent, bit4=Right
color_count:u8 + bytes(color_count * 5)   // legacy, skip
```

### AutoStartConfig / AutoStartConfig2
```
size:u16 LE              // SizeOf(TAutoStartConfig) или SizeOf(TAutoStartConfig2)
bytes(size)              // raw packed record
```
Soft-size: если `size` больше известного размера — лишние байты skip'аются; если меньше — частичное чтение.

---

## См. также

- `commands::strat` — связанный канал стратегий (UK_StratSnapshot).
- `commands::engine_request` — `emk_GetUISettings` запрос UI настроек через Engine API.
- `commands::balance::TArbPricesCommand` — поток Arb цен (после `ArbActivateNotify`).
