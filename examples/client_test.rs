use std::time::Duration;
use std::env;
use std::fs;
use moonproto::client::{Client, ClientConfig};
use moonproto::protocol::Command;
use moonproto::commands;
use moonproto::key_import;

/// Прочитать значение `key` из простого `key = value` конфиг файла.
/// Возвращает `None` если файла нет, ключа нет, или файл не читается.
/// Формат: одна пара на строку, `#` начинает комментарий, пустые строки игнорируются.
fn read_config_value(path: &str, key: &str) -> Option<String> {
    let content = fs::read_to_string(path).ok()?;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            if k.trim() == key {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: client_test <key_base64> [ip:port]");
        eprintln!("  Default: 127.0.0.1:3000");
        std::process::exit(1);
    }

    let key_b64 = &args[1];
    let (ip, port) = if args.len() >= 3 {
        let parts: Vec<&str> = args[2].splitn(2, ':').collect();
        (parts[0].to_string(), parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(3000u16))
    } else {
        ("127.0.0.1".to_string(), 3000u16)
    };

    println!("=== MoonProto Client Test ===");

    // NTP host из конфига (если есть) либо default pool.ntp.org.
    // Скопировать `moonproto.conf.example` → `moonproto.conf` и отредактировать
    // чтобы изменить.
    let ntp_host = read_config_value("moonproto.conf", "ntp_host")
        .unwrap_or_else(|| "pool.ntp.org".to_string());
    println!("[ntp] host = {}", ntp_host);

    // NTP sync (matches TMoonProtoTymeSyncer startup)
    let ntp_result = moonproto::ntp::get_best_ntp(&ntp_host, 4);
    if ntp_result.synced {
        moonproto::client::set_ntp_offset(ntp_result.time_offset);
        println!("[ntp] offset={:.1}ms rtt={}ms", ntp_result.time_offset * 1000.0, ntp_result.round_trip_ms);
    } else {
        println!("[ntp] sync failed, using system clock");
    }

    let keys = key_import::import_key(key_b64).expect("Failed to import key");
    println!("[key] OK, connecting to {}:{}", ip, port);

    let cfg = ClientConfig::new(ip, port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);

    // Phase 1: connect and auth (10 seconds)
    println!("[run] Phase 1: connecting...");
    client.run(Duration::from_secs(10), Box::new(|cmd, _data| {
        if cmd != Command::Ping { println!("[p1] {:?}", cmd); }
    }));

    if !client.is_authorized() {
        println!("[FAIL] Not authorized after 5s");
        return;
    }
    println!("[auth] Connected! Sending BaseCheck first...");

    // Send BaseCheck first (simplest API call — server must respond)
    let req = commands::engine_request::base_check();
    client.send_api_request(&req);

    // Then subscribe
    let req = commands::engine_request::subscribe_all_trades(false);
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

    client.disconnect();
    println!();
    println!("[done] Auth: {:?}", client.auth_status());
    println!("[done] Pings: {}", client.ping_count());
    println!("[done] Sent: {} bytes, Recv: {} bytes", client.total_sent(), client.total_recv());
}
