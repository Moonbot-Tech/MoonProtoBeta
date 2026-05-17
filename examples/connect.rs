use std::net::UdpSocket;
use std::time::{Duration, Instant};
use moonproto::MoonKey;
use moonproto::crypto;
use moonproto::protocol::{Command, handshake, slider::Slider, slicing, crypted};
use moonproto_transport;

const SERVER_IP: &str = "207.148.91.186";
const SERVER_PORT: u16 = 3000;

const MASTER_KEY: MoonKey = [
    0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5,
    0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25, 0xcb, 0xd6,
];
const MAC_KEY: MoonKey = [
    0x29, 0x05, 0xa9, 0xc4, 0x13, 0x10, 0xe4, 0x3f,
    0x07, 0x04, 0x93, 0x63, 0x40, 0xfa, 0x45, 0xa5,
];

const MASK_VER: u8 = 0; // 0=base, 1=ext mode 1, 2=ext mode 2. Must match server config.
const PING_SIZE: usize = 50;

#[derive(Debug, Clone)]
struct Ping {
    time: f64,
    initial_time: f64,
    trip_delay: i32,
    pmtu: u16,
    global_timing_orders: u16,
    overheat: u8,
    total_sent_bytes: u64,
    total_recv_bytes: u64,
    rsq: u8,
    ack_start: u64,
}

impl Ping {
    fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < PING_SIZE { return None; }
        Some(Self {
            time:                 f64::from_le_bytes(data[0..8].try_into().unwrap()),
            initial_time:         f64::from_le_bytes(data[8..16].try_into().unwrap()),
            trip_delay:           i32::from_le_bytes(data[16..20].try_into().unwrap()),
            pmtu:                 u16::from_le_bytes(data[20..22].try_into().unwrap()),
            global_timing_orders: u16::from_le_bytes(data[22..24].try_into().unwrap()),
            overheat:             data[24],
            total_sent_bytes:     u64::from_le_bytes(data[25..33].try_into().unwrap()),
            total_recv_bytes:     u64::from_le_bytes(data[33..41].try_into().unwrap()),
            rsq:                  data[41],
            ack_start:            u64::from_le_bytes(data[42..50].try_into().unwrap()),
        })
    }

    fn to_bytes(&self) -> [u8; PING_SIZE] {
        let mut buf = [0u8; PING_SIZE];
        buf[0..8].copy_from_slice(&self.time.to_le_bytes());
        buf[8..16].copy_from_slice(&self.initial_time.to_le_bytes());
        buf[16..20].copy_from_slice(&self.trip_delay.to_le_bytes());
        buf[20..22].copy_from_slice(&self.pmtu.to_le_bytes());
        buf[22..24].copy_from_slice(&self.global_timing_orders.to_le_bytes());
        buf[24] = self.overheat;
        buf[25..33].copy_from_slice(&self.total_sent_bytes.to_le_bytes());
        buf[33..41].copy_from_slice(&self.total_recv_bytes.to_le_bytes());
        buf[41] = self.rsq;
        buf[42..50].copy_from_slice(&self.ack_start.to_le_bytes());
        buf
    }
}

struct Session {
    socket: UdpSocket,
    server_addr: String,
    client_id: u64,
    total_sent: u64,
    total_recv: u64,
    ping_count: u32,
    slider: Slider,
    slicer: slicing::SlicingReceiver,
    decode_key: MoonKey,
    sliced_assembled: u32,
    crypted_decoded: u32,
}

impl Session {
    fn send_packet(&mut self, cmd: Command, payload: &[u8]) {
        let (packet, extra) = moonproto_transport::transport_pack(
            &MAC_KEY, cmd as u8, self.client_id, payload, MASK_VER,
        );
        if let Some(extra_pkt) = extra {
            self.socket.send_to(&extra_pkt, &self.server_addr).ok();
        }
        self.socket.send_to(&packet, &self.server_addr).ok();
        self.total_sent += packet.len() as u64;
    }

    fn send_ping_response(&mut self, mut ping: Ping) {
        ping.time = delphi_now();
        ping.total_sent_bytes = self.total_sent;
        ping.total_recv_bytes = self.total_recv;

        // Build ping payload with ACK slider data (matches Delphi SendPing exactly)
        let (ack_start, ack_words) = self.slider.build_ack_half();
        ping.ack_start = ack_start;

        let mut payload = Vec::with_capacity(PING_SIZE + ack_words.len() * 8);
        payload.extend_from_slice(&ping.to_bytes());
        for w in &ack_words {
            payload.extend_from_slice(&w.to_le_bytes());
        }

        self.send_packet(Command::Ping, &payload);
    }

    fn send_sliced_ack(&mut self, ack_bytes: &[u8; slicing::ACK256_WIRE_SIZE]) {
        self.send_packet(Command::SlicedACK, ack_bytes);
    }

    /// Send SizeAck — matches Delphi SendSizeAck exactly.
    /// Wire: TMoonSizeTestData (6 bytes: Size:u16, PacketNum:u16, SeriesNum:u16) + padding to FSize
    fn send_size_ack(&mut self, size: u16, series_num: u16) {
        let mut payload = vec![0u8; size as usize];
        // TMoonSizeTestData at start
        payload[0..2].copy_from_slice(&size.to_le_bytes());
        payload[2..4].copy_from_slice(&0u16.to_le_bytes()); // PacketNum (unused in ack)
        payload[4..6].copy_from_slice(&series_num.to_le_bytes());
        self.send_packet(Command::SizeAck, &payload);
    }

    fn recv_packet(&mut self) -> Option<(Command, Vec<u8>)> {
        let mut buf = [0u8; 65535];
        let (n, _) = self.socket.recv_from(&mut buf).ok()?;
        self.total_recv += n as u64;
        let (hdr, payload) = moonproto_transport::transport_unpack(&MAC_KEY, &buf[..n], MASK_VER)?;
        Some((Command::from_byte(hdr.cmd), payload))
    }

    fn handle_sliced(&mut self, payload: &[u8]) {
        let now_ms = Instant::now().elapsed().as_millis() as i64; // approximation
        self.slicer.set_last_online(now_ms + 10000); // offset to avoid 0-check issues

        let (assembled, ack) = self.slicer.on_new_sliced(payload);
        self.send_sliced_ack(&ack);

        if let Some((cmd, data)) = assembled {
            self.sliced_assembled += 1;
            let real_cmd = Command::from_byte(cmd);
            // If the assembled command is MPC_Crypted, decrypt it
            if real_cmd == Command::Crypted || (cmd & 0x80 != 0 && Command::from_byte(cmd & 0x7F) == Command::Crypted) {
                self.handle_crypted(&data);
            } else {
                println!("[sliced] Assembled cmd={:?} ({} bytes)", real_cmd, data.len());
            }
        }
    }

    fn handle_crypted(&mut self, data: &[u8]) {
        if let Some((cmd, payload, _want_ack)) = crypted::decrypt_command(&self.decode_key, data, &mut self.slider) {
            self.crypted_decoded += 1;
            let real_cmd = Command::from_byte(cmd);
            println!("[crypted] Decrypted cmd={:?} ({} bytes)", real_cmd, payload.len());
        }
    }

    fn handle_size_test(&mut self, payload: &[u8]) {
        // TMoonSizeTestData: Size(2) + PacketNum(2) + SeriesNum(2) = first 6 bytes
        if payload.len() < 6 {
            return;
        }
        let size = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let series_num = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        self.send_size_ack(size, series_num);
    }
}

fn main() {
    println!("=== MoonProto Full Client (Stage 1) ===");
    println!("Server: {}:{}", SERVER_IP, SERVER_PORT);
    println!();

    let client_id: u64 = rand::random();
    let app_token: u64 = rand::random();
    let mut client_token: u64 = rand::random::<u64>() & 0x0000_FFFF_FFFF_FFFF;

    // Port rotation: Delphi-exact (1024→65000, 200 attempts)
    let mut next_port: u16 = 1024 + (rand::random::<u16>() % (65000 - 1024));
    let socket = bind_with_rotation(&mut next_port);
    socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    println!("[net] Bound to {:?}", socket.local_addr().unwrap());

    let mut sess = Session {
        server_addr: format!("{}:{}", SERVER_IP, SERVER_PORT),
        socket,
        client_id,
        total_sent: 0,
        total_recv: 0,
        ping_count: 0,
        slider: Slider::new(),
        slicer: slicing::SlicingReceiver::new(),
        decode_key: [0u8; 16], // will be set after handshake
        sliced_assembled: 0,
        crypted_decoded: 0,
    };

    // === HANDSHAKE ===
    let server_token = match do_handshake(&mut sess, &mut client_token, app_token) {
        Some(t) => t,
        None => { println!("[FAIL] Handshake failed"); return; }
    };

    // Set session decode key (MPKeys[true] for client)
    let (_encode_key, decode_key) = crypto::generate_sub_keys(&MASTER_KEY, server_token);
    sess.decode_key = decode_key;

    println!("[auth] Handshake OK! ServerToken={:016X}", server_token);
    println!();

    // === MAIN LOOP (30 seconds) ===
    println!("--- Running full client for 30s ---");
    sess.socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(30) {
        let Some((cmd, payload)) = sess.recv_packet() else { continue };

        match cmd {
            Command::Ping => {
                if let Some(ping) = Ping::from_bytes(&payload) {
                    sess.ping_count += 1;
                    if sess.ping_count <= 3 || sess.ping_count % 10 == 0 {
                        println!("[ping] #{} RTT={}ms PMTU={} RS={:.0}%",
                                 sess.ping_count, ping.trip_delay, ping.pmtu,
                                 ping.rsq as f64 / 255.0 * 100.0);
                    }
                    sess.send_ping_response(ping);
                }
            }
            Command::Sliced => {
                sess.handle_sliced(&payload);
            }
            Command::Crypted => {
                sess.handle_crypted(&payload);
            }
            Command::SizeTest => {
                sess.handle_size_test(&payload);
            }
            Command::LogMsg => {
                let msg = String::from_utf8_lossy(&payload);
                println!("[log] {}", msg.trim());
            }
            Command::WantNewHello | Command::WrongHello | Command::NeedHelloAgain => {
                println!("[warn] Server wants reconnect: {:?}", cmd);
            }
            _ => {}
        }
    }

    // === Summary ===
    println!();
    println!("--- Session Summary ---");
    println!("Pings: {}", sess.ping_count);
    println!("Sliced assembled: {}", sess.sliced_assembled);
    println!("Crypted decoded: {}", sess.crypted_decoded);
    println!("Total sent: {} bytes", sess.total_sent);
    println!("Total recv: {} bytes", sess.total_recv);
    println!("Duration: {:.1}s", start.elapsed().as_secs_f64());

    sess.send_packet(Command::LogOff, &[]);
    println!("[done] LogOff sent.");
}

fn do_handshake(sess: &mut Session, client_token: &mut u64, app_token: u64) -> Option<u64> {
    let hello_payload = handshake::build_hello_packet(
        &MASTER_KEY, sess.client_id, client_token, app_token,
    );
    sess.send_packet(Command::Hello, &hello_payload);
    println!("[send] Hello");

    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            return None;
        }

        let Some((cmd, payload)) = sess.recv_packet() else {
            let hello_payload = handshake::build_hello_packet(
                &MASTER_KEY, sess.client_id, client_token, app_token,
            );
            sess.send_packet(Command::Hello, &hello_payload);
            continue;
        };

        match cmd {
            Command::WhoAreYou => {
                let decrypted = crypto::decrypt(&MASTER_KEY, &payload, &[])?;
                let hello = handshake::Hello::from_bytes(&decrypted)?;
                let server_token = hello.server_token;
                let (encode_key, _) = crypto::generate_sub_keys(&MASTER_KEY, server_token);

                *client_token += 1;
                let mut im_hello = hello;
                im_hello.mix_ts = *client_token;
                im_hello.app_token = app_token;
                im_hello.timestamp = delphi_now();
                let packed = im_hello.to_bytes_packed();
                let encrypted = crypto::encrypt(&encode_key, &packed, &[]);

                sess.send_packet(Command::ImFriend, &encrypted);
                std::thread::sleep(Duration::from_millis(32));
                sess.send_packet(Command::ImFriend, &encrypted);
                println!("[send] ImFriend (x2)");

                // Wait for Fine
                for _ in 0..20 {
                    let Some((cmd2, _)) = sess.recv_packet() else { continue };
                    if cmd2 == Command::Fine { return Some(server_token); }
                    if cmd2 == Command::WrongHello { return None; }
                }
                return None;
            }
            Command::WrongHello => return None,
            _ => continue,
        }
    }
}

fn delphi_now() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25569.0 + secs / 86400.0
}

fn bind_with_rotation(next_port: &mut u16) -> UdpSocket {
    if *next_port < 1024 || *next_port > 65000 {
        *next_port = 1024;
    }
    for _ in 0..200 {
        let addr = format!("0.0.0.0:{}", *next_port);
        match UdpSocket::bind(&addr) {
            Ok(sock) => {
                *next_port += 1;
                return sock;
            }
            Err(_) => {
                *next_port += 1;
                if *next_port > 65000 {
                    *next_port = 1024;
                }
            }
        }
    }
    panic!("Failed to bind UDP socket after 200 attempts");
}
