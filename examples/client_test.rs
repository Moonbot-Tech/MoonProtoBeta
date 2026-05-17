use std::time::Duration;
use moonproto::MoonKey;
use moonproto::client::{Client, ClientConfig};
use moonproto::protocol::Command;
use moonproto::commands;

const MASTER_KEY: MoonKey = [
    0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5,
    0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25, 0xcb, 0xd6,
];
const MAC_KEY: MoonKey = [
    0x29, 0x05, 0xa9, 0xc4, 0x13, 0x10, 0xe4, 0x3f,
    0x07, 0x04, 0x93, 0x63, 0x40, 0xfa, 0x45, 0xa5,
];

fn main() {
    println!("=== MoonProto Client (library API) ===");

    let cfg = ClientConfig {
        server_ip: "207.148.91.186".to_string(),
        server_port: 3000,
        master_key: MASTER_KEY,
        mac_key: MAC_KEY,
        mask_ver: 0,
        client_id: rand::random(),
    };

    let mut client = Client::new(cfg);

    // Phase 1: connect and auth (5 seconds)
    println!("[run] Phase 1: connecting...");
    client.run(Duration::from_secs(5), Box::new(|_cmd, _data| {}));

    if !client.is_authorized() {
        println!("[FAIL] Not authorized after 5s");
        return;
    }
    println!("[auth] Connected! Subscribing to trades...");

    // Subscribe to trades
    let req = commands::engine_request::subscribe_all_trades();
    client.send_api_request(&req);

    // Phase 2: receive data (15 seconds)
    println!("[run] Phase 2: receiving data...");
    let mut trades_count = 0u32;
    let mut book_count = 0u32;

    client.run(Duration::from_secs(15), Box::new(move |cmd, data| {
        // LOG EVERYTHING
        println!("[{}] {:?} {} bytes", trades_count, cmd, data.len());
        match cmd {
            Command::Ping => {}
            Command::TradesStream => {
                if let Some(pkt) = commands::trades_stream::parse_trades_packet(data) {
                    let mut total_trades = 0;
                    for sec in &pkt.sections {
                        if let commands::TradeSection::Trades(t) = sec {
                            total_trades += t.len();
                        }
                    }
                    trades_count += 1;
                    if trades_count <= 3 || trades_count % 10 == 0 {
                        println!("[trades] pkt#{} sections={} trades={}",
                                 pkt.packet_num, pkt.sections.len(), total_trades);
                    }
                }
            }
            Command::OrderBook => {
                if let Some(book) = commands::order_book::parse_order_book_packet(data) {
                    book_count += 1;
                    if book_count <= 3 || book_count % 10 == 0 {
                        println!("[book] mkt={} seq={} full={} buys={} sells={}",
                                 book.market_index, book.seq, book.is_full,
                                 book.buys.len(), book.sells.len());
                    }
                }
            }
            Command::Balance => {
                // Parse balance — CmdId is first byte of data
                if data.len() > 11 {
                    let cmd_id = data[0];
                    if let Some(bal) = commands::balance::parse_balance(cmd_id, &data[11..]) {
                        println!("[balance] cmd={} epoch={} items={} btc={:.4}",
                                 cmd_id, bal.epoch, bal.items.len(), bal.btc_balance_total);
                    }
                }
            }
            Command::Order | Command::Strat | Command::UI => {
                println!("[data] {:?} ({} bytes)", cmd, data.len());
            }
            Command::LogMsg => {
                let msg = String::from_utf8_lossy(data);
                if msg.contains("Check") || msg.len() < 50 {
                    println!("[log] {}", msg.trim());
                }
            }
            Command::API => {
                if data.len() > 11 {
                    println!("[api] Response ({} bytes) first_bytes={:02x?}", data.len(), &data[..data.len().min(20)]);
                }
            }
            other => {
                println!("[other] {:?} ({} bytes)", other, data.len());
            }
        }
    }));

    println!();
    println!("[done] Auth: {:?}", client.auth_status());
    println!("[done] Pings: {}", client.ping_count());
    println!("[done] Sent: {} bytes, Recv: {} bytes", client.total_sent(), client.total_recv());
}
