# Arb channel (MPC_Balance CmdId=6)

Arbitrage prices stream — поток цен на ту же монету с разных платформ для арбитража.

## Что это

`TArbPricesCommand` — подкоманда канала `MPC_Balance` (CmdId=6). Сервер шлёт raw payload с компактной таблицей цен `{market_index: price}` для каждой подписанной арбитражной платформы (Forex, UpBit, OKX, BinAlpha, HL deployers, etc.).

Активируется сервером после `TArbActivateNotify` (UI канал, CmdId=12), которое содержит `arb_valid` — TDateTime до какого момента активна Arb лицензия.

## Использование

```rust
use moonproto::commands::arb::parse_arb_prices;

if let Some(arb) = parse_arb_prices(&payload) {
    // arb.payload — raw bytes от kernel'а в компактном формате.
    // Структурный декодер ParseArbPayloadCompact — TODO (Stage 3+).
    // Сейчас потребитель должен сам распарсить если нужно.
    println!("Arb update UID={}, {} bytes", arb.uid, arb.payload.len());
}
```

## ParseArbPayloadCompact (TODO)

Полный декодер компактного формата (Delphi `ParseArbPayloadCompact` в ArbU.pas) не портирован. В текущей версии `payload: Vec<u8>` пробрасывается потребителю как-есть.

Когда понадобится — см. оригинал в `X:\proj-X\MoonBot\src\Arb\ArbU.pas`.

## Структура

```rust
pub struct ArbPricesCommand {
    pub uid: u64,
    pub payload: Vec<u8>,
}
```

## Wire format

```
TBaseBalanceCommand header: CmdId=6 + ver:u16 + UID:u64 = 11 bytes
len: i32 LE
payload: bytes(len) — компактная таблица цен
```

## См. также

- [ui.md](ui.md) — `TArbActivateNotify` активирует Arb (UI канал CmdId=12)
- [balances.md](balances.md) — Arb идёт в том же канале MPC_Balance
