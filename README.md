# moonproto

Rust implementation of the MoonProto binary UDP protocol for connecting to MoonBot trading servers.

## What it does

- Connects to a MoonBot server via encrypted UDP
- Handles the full connection lifecycle: handshake, keepalive, reconnect
- Receives market data: trades stream, order book, balances
- Sends trading commands: new orders, cancel, modify
- Supports large message fragmentation (slicing) and delivery acknowledgment

## Quick start

Add to your `Cargo.toml`:
```toml
[dependencies]
moonproto = { path = "../moonproto" }
```

### Connect to a server

```rust
use moonproto::MoonKey;
use moonproto::crypto;
use moonproto::protocol::{Command, handshake};
use moonproto_transport;

// Your keys (from MoonBot settings export)
let master_key: MoonKey = [/* 16 bytes */];
let mac_key: MoonKey = [/* 16 bytes */];

// 1. Bind UDP socket (port rotation for NAT compatibility)
let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

// 2. Build and send Hello
let hello = handshake::build_hello_packet(&master_key, client_id, &mut token, app_token);
let (packet, _) = moonproto_transport::transport_pack(
    &mac_key, Command::Hello as u8, client_id, &hello, 0,
);
socket.send_to(&packet, "server_ip:port").unwrap();

// 3. Receive WhoAreYou, decrypt, generate session keys
// 4. Send ImFriend (twice, 32ms apart)
// 5. Receive Fine → connected!
```

See `examples/connect.rs` for a complete working example.

### Run the example

```bash
cargo run --example connect
```

Connects to the configured server, performs handshake, exchanges pings for 30 seconds, receives and decrypts commands, responds to PMTU probes.

## Architecture

```
moonproto (this crate)
├── crypto/         — AES-128-GCM encryption, SHAKE-128 key derivation
├── protocol/       — Handshake, ping, slicing, replay protection, command dispatch
└── depends on: moonproto-transport (packet framing, MAC, obfuscation)
```

## Transport modes

| Mode | Description | Requires |
|------|-------------|----------|
| 0 | Base transport | Nothing — works without any additional dependencies |
| 1 | Extended transport mode 1 | `moonext` library (pre-built binary) |
| 2 | Extended transport mode 2 | `moonext` library (pre-built binary) |

**The transport mode is determined by the server configuration** (set in MoonBot settings). The client must use the same mode as the server.

- **Mode 0**: Works out of the box. No additional files needed. Fully open source.
- **Modes 1/2**: Place the `moonext` library next to your executable. Download pre-built binaries for your platform from Releases. If `moonext` is not present and the server requires mode 1/2, connection will fail.

## Protocol overview

### Connection flow
```
Client                          Server
  |--- Hello (encrypted) -------->|
  |<-- WhoAreYou (encrypted) -----|
  |--- ImFriend (session key) --->|
  |<-- Fine ----------------------|
  |                                |
  |<-- Ping (every ~1s) ----------|
  |--- Ping response ------------>|
  |<-- Data (Sliced/Crypted) -----|
  |--- SlicedACK ---------------->|
```

### Key types
- **MasterKey** (16 bytes): Pre-shared key for initial handshake encryption
- **MacKey** (16 bytes): Packet integrity (HMAC-CRC32C) and obfuscation seed
- **Session keys** (derived): AES-128-GCM keys for post-handshake communication

### Packet structure

Every UDP packet:
1. Client→Server header (15 bytes) or Server→Client header (7 bytes)
2. Payload (command-specific)

Packets are: MAC'd → obfuscated → optionally wrapped (mode 1/2) → sent via UDP.

### Commands

| Command | Direction | Description |
|---------|-----------|-------------|
| Hello/WhoAreYou/ImFriend/Fine | Handshake | Connection establishment |
| Ping | Both | Keepalive + channel quality metrics |
| Sliced/SlicedACK | Both | Large message fragmentation |
| Crypted | Both | Encrypted command envelope |
| Order | Both | Trade commands (28 sub-types) |
| Balance | S→C | Account balance updates |
| TradesStream | S→C | Real-time trade feed |
| OrderBook | S→C | Order book snapshots/diffs |
| API | Both | RPC calls (27 methods) |

## API reference

### `moonproto::crypto`

```rust
/// AES-128-GCM encrypt. Returns IV(12) + Tag(16) + Ciphertext.
pub fn encrypt(key: &MoonKey, plaintext: &[u8], aad: &[u8]) -> Vec<u8>;

/// AES-128-GCM decrypt. Verifies tag, strips PKCS7 padding.
pub fn decrypt(key: &MoonKey, data: &[u8], aad: &[u8]) -> Option<Vec<u8>>;

/// Derive session key pair from master key + server token.
/// Returns (encode_key, decode_key) for client side.
pub fn generate_sub_keys(master_key: &MoonKey, server_token: u64) -> (MoonKey, MoonKey);
```

### `moonproto::protocol::slider`

```rust
/// 4096-bit sliding window for replay protection.
pub struct Slider { ... }

impl Slider {
    pub fn new() -> Self;
    /// Returns true if message is NEW (not replay).
    pub fn check_revd(&mut self, num: u64) -> bool;
    /// Build ACK bitmap for piggybacking on pings.
    pub fn build_ack_half(&self) -> (u64, Vec<u64>);
}
```

### `moonproto::protocol::slicing`

```rust
/// Receives fragmented packets and reassembles them.
pub struct SlicingReceiver { ... }

impl SlicingReceiver {
    pub fn new() -> Self;
    /// Process incoming slice. Returns assembled message + ACK to send back.
    pub fn on_new_sliced(&mut self, payload: &[u8]) -> (Option<(u8, Vec<u8>)>, [u8; 34]);
}
```

### `moonproto::protocol::crypted`

```rust
/// Decrypt an MPC_Crypted envelope. Returns (cmd, payload, want_ack).
pub fn decrypt_command(
    decode_key: &MoonKey,
    encrypted_data: &[u8],
    slider: &mut Slider,
) -> Option<(u8, Vec<u8>, bool)>;
```

### `moonproto_transport`

```rust
/// Pack a command into a wire-ready UDP packet.
pub fn transport_pack(
    mac_key: &MoonKey, cmd: u8, client_id: u64,
    payload: &[u8], mask_ver: u8,
) -> (Vec<u8>, Option<Vec<u8>>);

/// Unpack a received UDP packet. Verifies MAC.
pub fn transport_unpack(
    mac_key: &MoonKey, raw: &[u8], mask_ver: u8,
) -> Option<(ServerMsgHeader, Vec<u8>)>;
```

## Building

Requires Rust 1.75+ (stable). No system dependencies.

```bash
cargo build --release
cargo test
cargo bench  # performance benchmarks
```

## Performance

Measured on x86_64, release build:

| Component | Throughput |
|-----------|-----------|
| Packet obfuscation | ~920 MB/s |
| Packet MAC (HMAC-CRC32C) | ~5-7 GB/s |
| AES-128-GCM | Hardware-accelerated (AES-NI) |

## License

Open source. See LICENSE file.
