# MoonProto Commands Reference

Wire format documentation for all protocol commands.
Used by terminal UI developers to understand what data arrives from the server.

## Command Channels

| Channel | Command byte | Description | Sub-commands |
|---------|-------------|-------------|--------------|
| Order | 28 (MPC_Order) | Trading operations | 30 sub-types (new order, status, replace, cancel, etc.) |
| Balance | 32 (MPC_Balance) | Account balances | 6 sub-types (snapshot, incremental, refresh) |
| Strategy | 30 (MPC_Strat) | Strategy management | 6 sub-types (snapshot, delete, checked sync) |
| UI | 29 (MPC_UI) | Settings and notifications | 15 sub-types |
| Engine API | 31 (MPC_API) | RPC calls | 31 methods |
| TradesStream | 33 (MPC_TradesStream) | Real-time trade feed | Single format, sections per market |
| OrderBook | 36 (MPC_OrderBook) | Order book updates | Full snapshot or diff |

The public `Command` type is a raw one-byte Delphi `TMoonProtoCommand` ordinal
wrapper. Known channels are constants (`Command::Order`, `Command::API`, ...);
unknown channel bytes are preserved after stripping the compressed flag, matching
Delphi `GetRealCommand`.

## Common Wire Format

Every command starts with:
```
CmdId    (1 byte)  — sub-command identifier within channel
ver      (2 bytes) — protocol version (currently 3), LE
UID      (8 bytes) — unique command ID, LE
[payload]          — command-specific data
```

Version gate: if received `ver > 3`, command is skipped (forward compatibility).

## String Encoding

All strings in the protocol use UTF-8 with a 2-byte LE length prefix:
```
Length (2 bytes, u16 LE) + UTF-8 bytes (no null terminator)
```

Delphi stores the length in a `Word` and then writes exactly that declared
number of bytes. Overlong strings therefore wrap to the low 16-bit length and
only those leading bytes are present in the packet body.

## Files

- [trades_stream.md](trades_stream.md) — Real-time trade feed format
- [order_book.md](order_book.md) — Order book snapshot/diff format
- [balance.md](balance.md) — Balance updates with bitmask optimization
- [engine_api.md](engine_api.md) — RPC request/response format
- [trade_commands.md](trade_commands.md) — Trading command sub-types
