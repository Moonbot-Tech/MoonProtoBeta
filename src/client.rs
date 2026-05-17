/// MoonProto UDP Client — full state machine with reconnect.
/// Byte-exact port of TMoonProtoUDPClient from MoonProtoUDPClient.pas + MoonProtoCommon.pas.
/// See MAPPING.md for line-by-line correspondence.

use std::net::UdpSocket;
use std::time::{Duration, Instant};
use crate::MoonKey;
use crate::crypto;
use crate::compression;
use crate::protocol::{Command, handshake, slider::Slider, slicing, crypted};
use crate::commands;

// === Constants matching Delphi exactly ===
const DEFAULT_SLEEP_MS: u64 = 5;           // MoonProtoFunc.pas:19
const RECONNECT_WAITING_MS: i64 = 7000;    // MoonProtoUDPClient.pas:88
const RECONNECT_THROTTLE_MS: i64 = 15000;  // MoonProtoUDPClient.pas:89
const OFFLINE_BASE_MS: i64 = 2300;         // MoonProtoUDPClient.pas:772
const DEAD_ZONE_MS: i64 = 5000;            // MoonProtoUDPClient.pas:799
const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
const CLEANUP_INTERVAL_MS: i64 = 5000;     // MoonProtoIntStruct.pas:828 (DoCleanUp threshold)

// Compression flag
const COMPRESSED_FLAG: u8 = 0x80;          // MoonProtoDataStruct.pas:27

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthStatus {
    Base,
    Connected,
    AuthDone,
    Offline,
}

pub type OnDataFn = Box<dyn FnMut(Command, &[u8]) + Send>;

pub struct ClientConfig {
    pub server_ip: String,
    pub server_port: u16,
    pub master_key: MoonKey,
    pub mac_key: MoonKey,
    pub mask_ver: u8,
    pub client_id: u64,
}

pub struct Client {
    cfg: ClientConfig,
    socket: Option<UdpSocket>,
    auth_status: AuthStatus,
    authorized: bool,
    need_connect: bool,
    force_disconnect: bool,
    soft_reconnect: bool,
    waiting_hello: bool,

    // Session
    client_token: u64,
    server_token: u64,
    app_token: u64,
    encode_key: MoonKey,
    decode_key: MoonKey,

    // Timing
    start: Instant,
    last_online: i64,
    last_sent_hello: i64,
    waiting_hello_start: i64,
    last_socket_recreate: i64,
    last_need_hello_again: i64, // MAPPING #13: 700ms throttle
    last_cleanup: i64,          // MAPPING #9: DoCleanUp

    // Channel metrics (MAPPING #19: from Ping)
    round_trip_delay: i64,
    actual_pmtu: u16,           // MAPPING L1
    rs: f64,                    // MAPPING L2: channel quality 0..1
    overheat: u8,

    // Protocol state
    slider: Slider,
    recv_slider: Slider,        // MAPPING #26: server's ACK slider from Ping
    tmp_slider_data: Option<(u64, Vec<u64>)>, // TmpSlider data from Ping
    slicer: slicing::SlicingReceiver,
    total_sent: u64,
    total_recv: u64,
    next_port: u16,
    ping_count: u32,
}

impl Client {
    pub fn new(cfg: ClientConfig) -> Self {
        let app_token: u64 = rand::random();
        let client_token: u64 = rand::random::<u64>() & 0x0000_FFFF_FFFF_FFFF;
        let next_port = 1024 + (rand::random::<u16>() % (65000 - 1024));

        Self {
            cfg,
            socket: None,
            auth_status: AuthStatus::Base,
            authorized: false,
            need_connect: true,
            force_disconnect: false,
            soft_reconnect: false,
            waiting_hello: false,
            client_token,
            server_token: 0,
            app_token,
            encode_key: [0; 16],
            decode_key: [0; 16],
            start: Instant::now(),
            last_online: 0,
            last_sent_hello: 0,
            waiting_hello_start: 0,
            last_socket_recreate: 0,
            last_need_hello_again: 0,
            last_cleanup: 0,
            round_trip_delay: 0,
            actual_pmtu: 508, // MinSafeDatagramSize
            rs: 1.0,
            overheat: 0,
            slider: Slider::new(),
            recv_slider: Slider::new(),
            tmp_slider_data: None,
            slicer: slicing::SlicingReceiver::new(),
            total_sent: 0,
            total_recv: 0,
            next_port,
            ping_count: 0,
        }
    }

    fn now_ms(&self) -> i64 {
        self.start.elapsed().as_millis() as i64
    }

    fn server_addr(&self) -> String {
        format!("{}:{}", self.cfg.server_ip, self.cfg.server_port)
    }

    pub fn run(&mut self, duration: Duration, mut on_data: OnDataFn) {
        let run_start = Instant::now();

        loop {
            if run_start.elapsed() >= duration {
                break;
            }

            let cur_tm = self.now_ms();

            if self.socket.is_none() && self.need_connect {
                self.bind_socket();
            }

            if self.socket.is_some() {
                self.poll_recv(&mut on_data);

                // MAPPING #9: DoCleanUp (clear old Receiving every 5s)
                if (cur_tm - self.last_cleanup).abs() > CLEANUP_INTERVAL_MS {
                    self.slicer.clear_old();
                    self.last_cleanup = cur_tm;
                }

                // MAPPING #55: Apply server's ACK slider to PendingH
                // (currently client doesn't send H-commands, but structure is ready)

                self.check_hello_send(cur_tm);
                self.check_offline_reconnect(cur_tm);
                self.check_reconnect_timeout(cur_tm);
                self.check_dead_zone(cur_tm);

                if self.force_disconnect {
                    self.do_force_disconnect();
                }
            }

            std::thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
        }

        if self.authorized {
            self.send_packet(Command::LogOff, &[]);
        }
    }

    fn bind_socket(&mut self) {
        self.force_disconnect = false;
        if self.next_port < 1024 || self.next_port > 65000 {
            self.next_port = 1024;
        }
        for _ in 0..200 {
            let addr = format!("0.0.0.0:{}", self.next_port);
            match UdpSocket::bind(&addr) {
                Ok(sock) => {
                    sock.set_read_timeout(Some(Duration::from_millis(1))).ok();
                    sock.set_nonblocking(false).ok();
                    // MAPPING #29: 8MB socket buffers
                    set_socket_buffers(&sock);
                    self.next_port += 1;
                    self.socket = Some(sock);
                    self.auth_status = AuthStatus::Connected;
                    return;
                }
                Err(_) => {
                    self.next_port += 1;
                    if self.next_port > 65000 { self.next_port = 1024; }
                }
            }
        }
    }

    // MAPPING #30: Hello send with interval = Max(1000, RTT) * 2
    fn check_hello_send(&mut self, cur_tm: i64) {
        if !self.need_connect || self.force_disconnect { return; }

        let hello_interval = self.round_trip_delay.max(1000) * 2;
        if (cur_tm - self.last_sent_hello).abs() <= hello_interval { return; }

        if self.soft_reconnect && self.server_token != 0 {
            self.send_hello_again();
        } else {
            self.soft_reconnect = false;
            self.send_hello();
        }
        self.last_sent_hello = cur_tm;
        self.waiting_hello = true;
        self.waiting_hello_start = cur_tm;
    }

    // MAPPING #31-32: Offline detection with HelloAgain throttle
    fn check_offline_reconnect(&mut self, cur_tm: i64) {
        // HelloAgainThrottle = Min(1500, Max(200, RoundTripDelay + 50))
        let throttle = (self.round_trip_delay + 50).max(200).min(1500);

        let should_reconnect = self.waiting_hello
            || (self.authorized && !self.need_connect
                && (cur_tm - self.last_online).abs() > OFFLINE_BASE_MS + self.round_trip_delay);

        if !should_reconnect { return; }
        if (cur_tm - self.last_sent_hello).abs() <= throttle { return; }

        self.auth_status = AuthStatus::Offline;
        if !self.waiting_hello {
            self.waiting_hello_start = cur_tm;
        }
        self.waiting_hello = true;
        self.send_hello_again();
        self.last_sent_hello = cur_tm;
    }

    // MAPPING #33: HelloAgain timeout 7s → socket recreate
    fn check_reconnect_timeout(&mut self, cur_tm: i64) {
        if self.waiting_hello
            && (cur_tm - self.waiting_hello_start).abs() > RECONNECT_WAITING_MS
            && (cur_tm - self.last_socket_recreate).abs() > RECONNECT_THROTTLE_MS
        {
            self.last_socket_recreate = cur_tm;
            self.soft_reconnect = true;
            self.force_disconnect = true;
            self.need_connect = true;
            self.waiting_hello = false;
        }
    }

    // MAPPING #34: Dead zone detection
    fn check_dead_zone(&mut self, cur_tm: i64) {
        if !self.authorized && !self.need_connect
            && (cur_tm - self.last_online).abs() > DEAD_ZONE_MS
        {
            self.soft_reconnect = false;
            self.force_disconnect = true;
            self.need_connect = true;
        }
    }

    // MAPPING #35: ForceDisconnect with full Reset
    fn do_force_disconnect(&mut self) {
        if self.authorized && !self.soft_reconnect {
            self.send_packet(Command::LogOff, &[]);
        }
        self.socket = None;
        if !self.soft_reconnect {
            self.full_reset();
        }
        self.authorized = false;
        self.force_disconnect = false;
    }

    // MAPPING #12, #35: Full client reset matching TMoonProtoClient.Reset
    fn full_reset(&mut self) {
        self.server_token = 0;
        self.slider = Slider::new();
        self.recv_slider = Slider::new();
        self.tmp_slider_data = None;
        self.slicer = slicing::SlicingReceiver::new();
        self.total_sent = 0;
        self.total_recv = 0;
        self.last_online = 0;
        self.last_sent_hello = 0;
        self.rs = 1.0;
        self.actual_pmtu = 508;
    }

    fn poll_recv(&mut self, on_data: &mut OnDataFn) {
        let mut buf = [0u8; 65535];

        for _ in 0..50 {
            let n = {
                let sock = self.socket.as_ref().unwrap();
                match sock.recv_from(&mut buf) {
                    Ok((n, _)) => n,
                    Err(_) => break,
                }
            };
            self.total_recv += n as u64;
            self.last_online = self.now_ms();

            let raw = &buf[..n];
            let Some((hdr, payload)) = moonproto_transport::transport_unpack(
                &self.cfg.mac_key, raw, self.cfg.mask_ver,
            ) else { continue };

            let cmd = Command::from_byte(hdr.cmd);
            self.handle_udp_command(cmd, hdr.cmd, &payload, on_data);
        }
    }

    /// UDPRead command dispatch — matches MoonProtoUDPClient.pas:545-663
    fn handle_udp_command(&mut self, cmd: Command, raw_cmd: u8, payload: &[u8], on_data: &mut OnDataFn) {
        // MAPPING #10: Handshake commands → clear waiting_hello
        if matches!(cmd, Command::WantNewHello | Command::WrongHello | Command::WhoAreYou | Command::Fine) {
            self.waiting_hello = false;
        }

        match cmd {
            // MAPPING #11
            Command::WrongHello => {
                self.auth_status = AuthStatus::Connected;
            }
            // MAPPING #12: full Reset
            Command::WantNewHello => {
                self.full_reset();
                self.last_sent_hello = 0;
                self.auth_status = AuthStatus::Connected;
                self.authorized = false;
                self.need_connect = true;
                self.soft_reconnect = false;
            }
            // MAPPING #13: 700ms throttle
            Command::NeedHelloAgain => {
                let now = self.now_ms();
                if (now - self.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS {
                    self.last_need_hello_again = now;
                    if !self.waiting_hello { self.waiting_hello_start = now; }
                    self.waiting_hello = true;
                    self.last_sent_hello = 0;
                }
            }
            // MAPPING #14
            Command::WhoAreYou | Command::Fine => {
                self.handle_handshake(cmd, payload);
            }
            // MAPPING #15
            Command::SizeTest => {
                self.handle_size_test(payload);
            }
            // MAPPING #16: ProbeMTU → ProbeMTUAck (with DontFragment)
            Command::ProbeMTU => {
                self.handle_probe_mtu(payload);
            }
            // MAPPING #17
            Command::Sliced => {
                self.slicer.set_last_online(self.last_online);
                let (assembled, ack) = self.slicer.on_new_sliced(payload);
                self.send_packet(Command::SlicedACK, &ack);
                if let Some((inner_cmd, data)) = assembled {
                    self.data_read_int(inner_cmd, &data, on_data);
                }
            }
            // MAPPING #18: SlicedACK (for when client sends Sliced)
            Command::SlicedACK => {
                // Client-side: apply ACK to our Sending list (not yet implemented — client doesn't send large data yet)
            }
            // MAPPING #19: Ping → update metrics + rate control
            Command::Ping => {
                self.handle_ping(payload, on_data);
            }
            // MAPPING #20: all other → DataRead
            _ => {
                self.data_read(raw_cmd, payload, on_data);
            }
        }
    }

    /// DataRead — matches MoonProtoCommon.pas:541-577
    /// MAPPING #21-23: Grouped unpacking + single dispatch
    fn data_read(&mut self, raw_cmd: u8, payload: &[u8], on_data: &mut OnDataFn) {
        let cmd = Command::from_byte(raw_cmd);

        if cmd == Command::Grouped {
            // MAPPING #21-22: Unpack sub-commands
            let mut pos = 0;
            while pos + 3 <= payload.len() {
                let sub_cmd = payload[pos];
                pos += 1;
                let sz = u16::from_le_bytes([payload[pos], payload[pos+1]]) as usize;
                pos += 2;
                if pos + sz > payload.len() { break; }
                let sub_data = &payload[pos..pos+sz];
                pos += sz;
                self.data_read_int(sub_cmd, sub_data, on_data);
            }
        } else {
            // MAPPING #23: single command
            self.data_read_int(raw_cmd, payload, on_data);
        }
    }

    /// DataReadInt — matches MoonProtoCommon.pas:488-538
    /// MAPPING #24-27: Crypted → Decompress → Ping slider → callback
    fn data_read_int(&mut self, raw_cmd: u8, data: &[u8], on_data: &mut OnDataFn) {
        let mut cmd = raw_cmd;
        let mut payload = data.to_vec();

        // MAPPING #24: MPC_Crypted → decrypt
        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            if let Some((inner_cmd, inner_data, _want_ack)) = crypted::decrypt_command(&self.decode_key, &payload, &mut self.slider) {
                cmd = inner_cmd;
                payload = inner_data;
            } else {
                return;
            }
        }

        // MAPPING #25: IsCompressed → decompress
        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F; // strip flag
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = decompressed;
            } else {
                return; // decompression failed
            }
        }

        // MAPPING #26: Ping → read server's ACK slider (TmpSlider)
        let real_cmd = Command::from_byte(cmd);
        if real_cmd == Command::Ping && payload.len() > 50 {
            // Read ACK bitmap piggybacked on Ping
            let ack_start = u64::from_le_bytes(payload[42..50].try_into().unwrap());
            let ack_data_len = payload.len() - 50;
            let max_words = (ack_data_len / 8).min(64); // MPSliderLen = 64
            if max_words > 0 {
                let mut words = vec![0u64; max_words];
                for i in 0..max_words {
                    let off = 50 + i * 8;
                    words[i] = u64::from_le_bytes(payload[off..off+8].try_into().unwrap());
                }
                self.tmp_slider_data = Some((ack_start, words));
            }
        }

        // MAPPING #27: OnNewData callback
        on_data(real_cmd, &payload);
    }

    // MAPPING #19: Ping handler — update RTT, PMTU, OverHeat, RS
    fn handle_ping(&mut self, payload: &[u8], on_data: &mut OnDataFn) {
        if payload.len() < 50 { return; }
        self.ping_count += 1;

        // Read metrics from server's Ping (MoonProtoUDPClient.pas:633-639)
        self.round_trip_delay = i32::from_le_bytes(payload[16..20].try_into().unwrap()) as i64;
        self.actual_pmtu = u16::from_le_bytes(payload[20..22].try_into().unwrap());
        self.overheat = payload[24];
        self.rs = payload[41] as f64 / 255.0;
        self.need_connect = false;

        // Send Ping response (MAPPING #37-40)
        let mut response = payload[..50].to_vec();
        response[0..8].copy_from_slice(&delphi_now().to_le_bytes());
        response[25..33].copy_from_slice(&self.total_sent.to_le_bytes());
        response[33..41].copy_from_slice(&self.total_recv.to_le_bytes());

        let (ack_start, ack_words) = self.slider.build_ack_half();
        response[42..50].copy_from_slice(&ack_start.to_le_bytes());
        for w in &ack_words {
            response.extend_from_slice(&w.to_le_bytes());
        }

        self.send_packet(Command::Ping, &response);

        // Pass Ping through DataRead (Delphi does: DataRead(MPC_Ping, AData, FClient) at line 663)
        self.data_read(Command::Ping as u8, payload, on_data);
    }

    fn handle_handshake(&mut self, cmd: Command, payload: &[u8]) {
        if cmd == Command::WhoAreYou {
            let Some(decrypted) = crypto::decrypt(&self.cfg.master_key, payload, &[]) else { return };
            let Some(hello) = handshake::Hello::from_bytes(&decrypted) else { return };

            self.server_token = hello.server_token;
            let (enc, dec) = crypto::generate_sub_keys(&self.cfg.master_key, self.server_token);
            self.encode_key = enc;
            self.decode_key = dec;

            self.client_token += 1;
            let mut im = hello;
            im.mix_ts = self.client_token;
            im.app_token = self.app_token;
            im.timestamp = delphi_now();
            let packed = im.to_bytes_packed();
            let encrypted = crypto::encrypt(&self.encode_key, &packed, &[]);

            self.send_packet(Command::ImFriend, &encrypted);
            std::thread::sleep(Duration::from_millis(32));
            self.send_packet(Command::ImFriend, &encrypted);
        }

        if cmd == Command::Fine {
            self.need_connect = false;
            self.authorized = true;
            self.auth_status = AuthStatus::AuthDone;
            self.waiting_hello = false;
        }
    }

    // MAPPING #15: SizeAck
    fn handle_size_test(&mut self, payload: &[u8]) {
        if payload.len() < 6 { return; }
        let size = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let series = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        // Delphi: response padded to `size` bytes, with TMoonSizeTestData at start
        let mut ack = vec![0u8; size as usize];
        ack[0..2].copy_from_slice(&size.to_le_bytes());
        // PacketNum at [2..4] = 0
        ack[4..6].copy_from_slice(&series.to_le_bytes());
        // TODO MAPPING #M5: DontFragment flag (platform-specific, not yet implemented)
        self.send_packet(Command::SizeAck, &ack);
    }

    // MAPPING #16: ProbeMTU → ProbeMTUAck
    fn handle_probe_mtu(&mut self, payload: &[u8]) {
        if payload.len() < 5 { return; }
        // TMoonProtoProbeMTU: ProbeID(2) + ProbeIndex(1) + TestSize(2) = 5 bytes
        let probe_id = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let probe_index = payload[2];
        let test_size = u16::from_le_bytes(payload[3..5].try_into().unwrap());

        // TMoonProtoProbeMTUAck: ProbeID(2) + ProbeIndex(1) + ReceivedSize(2) = 5 bytes
        let mut ack = vec![0u8; test_size as usize];
        ack[0..2].copy_from_slice(&probe_id.to_le_bytes());
        ack[2] = probe_index;
        ack[3..5].copy_from_slice(&test_size.to_le_bytes());
        // TODO MAPPING #M5: DontFragment flag
        self.send_packet(Command::ProbeMTUAck, &ack);
    }

    fn send_hello(&mut self) {
        let payload = handshake::build_hello_packet(
            &self.cfg.master_key, self.cfg.client_id, &mut self.client_token, self.app_token,
        );
        self.send_packet(Command::Hello, &payload);
    }

    fn send_hello_again(&mut self) {
        self.client_token += 1;
        let mut hello = handshake::Hello::new(self.client_token, self.app_token);
        hello.timestamp = delphi_now();
        hello.peer_mix = crypto::mix_values(&hello.rnd, hello.mix_ts, self.server_token);
        let packed = hello.to_bytes_packed();
        let encrypted = crypto::encrypt(&self.encode_key, &packed, &[]);
        self.send_packet(Command::HelloAgain, &encrypted);
    }

    fn send_packet(&mut self, cmd: Command, payload: &[u8]) {
        let Some(sock) = &self.socket else { return };
        let (packet, extra) = moonproto_transport::transport_pack(
            &self.cfg.mac_key, cmd as u8, self.cfg.client_id, payload, self.cfg.mask_ver,
        );
        let addr = self.server_addr();
        if let Some(extra_pkt) = extra {
            sock.send_to(&extra_pkt, &addr).ok();
        }
        sock.send_to(&packet, &addr).ok();
        self.total_sent += packet.len() as u64;
    }

    /// Send an Engine API request via MPC_Crypted envelope.
    /// Matches Delphi: SendCrypted(MPC_API, ms, MPS_High)
    /// The request is wrapped in TMoonProtoCryptoHeader + AES-GCM encrypted.
    pub fn send_api_request(&mut self, request_payload: &[u8]) {
        if !self.authorized { return; }

        // Build CryptoHeader (12 bytes): Rnd(2) + MsgNum(8) + cmd(1) + WantACK(1)
        let mut crypto_hdr = [0u8; 12];
        let rnd: u16 = rand::random();
        crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
        // MsgNum = 0 for now (no retry tracking on client side yet)
        crypto_hdr[10] = Command::API as u8; // inner command
        crypto_hdr[11] = 0; // WantACK = false (no retry)

        // Plaintext = CryptoHeader + request_payload
        let mut plaintext = Vec::with_capacity(12 + request_payload.len());
        plaintext.extend_from_slice(&crypto_hdr);
        plaintext.extend_from_slice(request_payload);

        // Encrypt with session encode key
        let encrypted = crypto::encrypt(&self.encode_key, &plaintext, &[]);

        // Send as MPC_Crypted
        self.send_packet(Command::Crypted, &encrypted);
    }

    pub fn auth_status(&self) -> AuthStatus { self.auth_status }
    pub fn is_authorized(&self) -> bool { self.authorized }
    pub fn ping_count(&self) -> u32 { self.ping_count }
    pub fn total_sent(&self) -> u64 { self.total_sent }
    pub fn total_recv(&self) -> u64 { self.total_recv }
    pub fn pmtu(&self) -> u16 { self.actual_pmtu }
    pub fn rs(&self) -> f64 { self.rs }
}

fn delphi_now() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25569.0 + secs / 86400.0
}

/// Set 8MB socket buffers (MAPPING #29)
fn set_socket_buffers(_sock: &UdpSocket) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::io::AsRawSocket;
        let raw = _sock.as_raw_socket();
        let buf_size: i32 = 8 * 1024 * 1024;
        unsafe {
            // SO_RCVBUF = 0x1002, SO_SNDBUF = 0x1001, SOL_SOCKET = 0xFFFF
            extern "system" {
                fn setsockopt(s: usize, level: i32, optname: i32, optval: *const i8, optlen: i32) -> i32;
            }
            setsockopt(raw as usize, 0xFFFF, 0x1002, &buf_size as *const i32 as *const i8, 4);
            setsockopt(raw as usize, 0xFFFF, 0x1001, &buf_size as *const i32 as *const i8, 4);
        }
    }
}
