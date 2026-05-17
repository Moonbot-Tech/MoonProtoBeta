/// MoonProto UDP Client — two-thread architecture matching Delphi exactly.
///
/// Architecture (matches TMoonProtoUDPClient):
/// - Thread 1 (Main/Send): Execute loop — send queues, retry, reconnect, sleep(5ms)
/// - Thread 2 (Reader): UDPRead — blocking recv, process packets, dispatch
/// - Communication: shared state protected by Mutex (≡ Delphi FastLock, benchmarked: same perf)
///
/// See MAPPING.md for line-by-line correspondence.

use std::net::UdpSocket;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use crate::MoonKey;
use crate::crypto;
use crate::compression;
use crate::protocol::{Command, handshake, slider::Slider, slicing, crypted};

// === Constants matching Delphi exactly ===
const DEFAULT_SLEEP_MS: u64 = 5;           // MoonProtoFunc.pas:19
const RECONNECT_WAITING_MS: i64 = 7000;    // MoonProtoUDPClient.pas:88
const RECONNECT_THROTTLE_MS: i64 = 15000;  // MoonProtoUDPClient.pas:89
const OFFLINE_BASE_MS: i64 = 2300;         // MoonProtoUDPClient.pas:772
const DEAD_ZONE_MS: i64 = 5000;            // MoonProtoUDPClient.pas:799
const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
const CLEANUP_INTERVAL_MS: i64 = 5000;     // MoonProtoIntStruct.pas:828
const COMPRESSED_FLAG: u8 = 0x80;          // MoonProtoDataStruct.pas:27
const MIN_SIZE_TO_COMPRESS: usize = 64;    // MoonProtoDataStruct.pas:31

// Send priority (matches TMoonProtoSendPriority)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendPriority {
    Sliced, // MPS_Sliced: large, through slicing engine
    High,   // MPS_High: small, direct send, retry with ACK
    Low,    // MPS_Low: best effort, one per cycle
}

/// Item in the send queue (matches TMoonProtoDataToSend subset)
#[derive(Clone)]
pub struct SendItem {
    pub data: Vec<u8>,         // serialized command stream
    pub cmd: u8,               // TMoonProtoCommand ordinal
    pub encrypted: bool,       // FCrypted
    pub priority: SendPriority,
    pub retry_left: i32,       // RetryLeft
    pub max_retries: i32,      // MaxRetryCount
    pub msg_num: u64,          // for ACK tracking (assigned in Crypt)
    pub last_sent_at: i64,     // ms timestamp of last send
}

/// Message from reader thread to main loop
struct RecvMsg {
    cmd: u8,
    payload: Vec<u8>,
    recv_bytes: u64,
    timestamp_ms: i64,
}

/// Message from app to main loop (send command request)
/// Matches Delphi: SendCmd → DataToSend queue
#[derive(Clone)]
pub struct SendMsg {
    pub item: SendItem,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthStatus {
    Base,
    Connected,
    AuthDone,
    Offline,
}

pub struct ClientConfig {
    pub server_ip: String,
    pub server_port: u16,
    pub master_key: MoonKey,
    pub mac_key: MoonKey,
    pub mask_ver: u8,
    pub client_id: u64,
}

pub type OnDataFn = Box<dyn FnMut(Command, &[u8]) + Send>;

/// Public handle to the client. Allows sending commands from any thread.
pub struct Client {
    cfg: ClientConfig,

    // Channels
    recv_rx: Option<mpsc::Receiver<RecvMsg>>,     // reader → main
    send_tx: mpsc::Sender<SendMsg>,               // app → main (public clone for send_cmd)
    send_rx: mpsc::Receiver<SendMsg>,             // main reads from here

    // Pending H-commands (main thread only, no sharing)
    pending_h: Vec<SendItem>,

    // Main thread state
    socket: Option<UdpSocket>,
    authorized: bool,
    last_online: i64,
    total_recv: u64,
    auth_status: AuthStatus,
    need_connect: bool,
    force_disconnect: bool,
    soft_reconnect: bool,
    waiting_hello: bool,

    client_token: u64,
    server_token: u64,
    app_token: u64,
    encode_key: MoonKey,
    decode_key: MoonKey,

    start: Instant,
    last_sent_hello: i64,
    waiting_hello_start: i64,
    last_socket_recreate: i64,
    last_need_hello_again: i64,
    last_cleanup: i64,

    crypt_msg_counter: u64,
    send_datagram_num: u16,

    round_trip_delay: i64,
    actual_pmtu: u16,
    rs: f64,
    overheat: u8,

    slider: Slider,
    recv_slider: Slider,
    slicer: slicing::SlicingReceiver,
    total_sent: u64,
    next_port: u16,
    ping_count: u32,
}

impl Client {
    pub fn new(cfg: ClientConfig) -> Self {
        let (send_tx, send_rx) = mpsc::channel();

        Self {
            cfg,
            recv_rx: None,
            send_tx,
            send_rx,
            pending_h: Vec::new(),
            socket: None,
            authorized: false,
            last_online: 0,
            total_recv: 0,
            auth_status: AuthStatus::Base,
            need_connect: true,
            force_disconnect: false,
            soft_reconnect: false,
            waiting_hello: false,
            client_token: rand::random::<u64>() & 0x0000_FFFF_FFFF_FFFF,
            server_token: 0,
            app_token: rand::random(),
            encode_key: [0; 16],
            decode_key: [0; 16],
            start: Instant::now(),
            last_sent_hello: 0, // Delphi: 0 initially. now_ms() is huge (system time) → diff > interval → Hello sends immediately
            waiting_hello_start: 0,
            last_socket_recreate: 0,
            last_need_hello_again: 0,
            last_cleanup: 0,
            crypt_msg_counter: 0,
            send_datagram_num: 0,
            round_trip_delay: 0,
            actual_pmtu: 508,
            rs: 1.0,
            overheat: 0,
            slider: Slider::new(),
            recv_slider: Slider::new(),
            slicer: slicing::SlicingReceiver::new(),
            total_sent: 0,
            next_port: 1024 + (rand::random::<u16>() % (65000 - 1024)),
            ping_count: 0,
        }
    }

    /// Public API: queue a command for sending (thread-safe, via channel).
    /// Matches Delphi: SendCmd → SendCmdInt → DataToSend/H/L.
    /// Can be called from any thread (send_tx is cloneable).
    pub fn send_cmd(&self, data: Vec<u8>, cmd: Command, priority: SendPriority, encrypted: bool, max_retries: i32) {
        let item = SendItem {
            data,
            cmd: cmd as u8,
            encrypted,
            priority,
            retry_left: if encrypted { max_retries - 1 } else { 0 },
            max_retries,
            msg_num: 0,
            last_sent_at: 0,
        };
        self.send_tx.send(SendMsg { item }).ok();
    }

    /// Get a clone of send_tx for use from other threads (e.g. terminal UI).
    pub fn sender(&self) -> mpsc::Sender<SendMsg> {
        self.send_tx.clone()
    }

    /// Convenience: send an Engine API request (MPS_Sliced, encrypted, 6 retries).
    /// Matches: SendAPICmd → SendCmd → DataToSend(MPS_Sliced, FCrypted=true, MaxRetries=6)
    pub fn send_api_request(&self, request_payload: &[u8]) {
        eprintln!("[DBG send_api] payload hex ({} bytes): {:02x?}", request_payload.len(), &request_payload[..request_payload.len().min(40)]);
        self.send_cmd(
            request_payload.to_vec(),
            Command::API,
            SendPriority::Sliced,
            true,    // Engine API is always encrypted
            6,       // MPS_Sliced default MaxRetryCount
        );
    }

    /// GetTimeMS equivalent — system time in milliseconds (matches Delphi GetTickCount64).
    /// MUST use same time base everywhere (reader thread, main thread, slicing).
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn server_addr(&self) -> String {
        format!("{}:{}", self.cfg.server_ip, self.cfg.server_port)
    }

    /// Run the client. Spawns reader thread, runs main loop for `duration`.
    /// Matches TMoonProtoUDPClient.Execute.
    pub fn run(&mut self, duration: Duration, mut on_data: OnDataFn) {
        let run_start = Instant::now();

        loop {
            if run_start.elapsed() >= duration { break; }
            let cur_tm = self.now_ms();

            // Bind socket if needed
            if self.socket.is_none() && self.need_connect {
                self.bind_socket();
                self.spawn_reader();
            }

            if self.socket.is_some() {
                // Get received packets from reader thread (≡ Delphi: UDPRead in reader thread)
                self.process_received(&mut on_data);

                // Get send items from app (≡ GetCopySendList: drain channel)
                let mut sliced = Vec::new();
                let mut h_items = Vec::new();
                let mut l_item = None;
                while let Ok(msg) = self.send_rx.try_recv() {
                    match msg.item.priority {
                        SendPriority::Sliced => sliced.push(msg.item),
                        SendPriority::High => h_items.push(msg.item),
                        SendPriority::Low => { if l_item.is_none() { l_item = Some(msg.item); } },
                    }
                }

                // CheckSeningData: process Sliced queue
                for item in &sliced {
                    self.create_sliced_and_send(item);
                }

                // CheckSeningData: process H queue + retry
                for mut item in h_items {
                    self.send_h_item(&mut item, cur_tm);
                }

                // Retry pending H (≡ PendingH retry loop)
                self.retry_pending_h(cur_tm);

                // L queue: one per cycle, flush
                if let Some(item) = l_item {
                    self.send_direct(&item);
                }

                // Cleanup
                if (cur_tm - self.last_cleanup).abs() > CLEANUP_INTERVAL_MS {
                    self.slicer.clear_old();
                    self.last_cleanup = cur_tm;
                }

                // Reconnect logic
                self.check_hello_send(cur_tm);
                self.check_offline_reconnect(cur_tm);
                self.check_reconnect_timeout(cur_tm);
                self.check_dead_zone(cur_tm);

                if self.force_disconnect {
                    self.do_force_disconnect();
                }
            }

            thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
        }

        // Graceful disconnect
        if self.authorized {
            self.send_raw_packet(Command::LogOff, &[]);
        }
    }

    /// Spawn reader thread (≡ Indy TIdUDPListenerThread).
    fn spawn_reader(&mut self) {
        let Some(ref sock) = self.socket else { return; };
        let sock_clone = sock.try_clone().expect("Failed to clone socket");
        let mac_key = self.cfg.mac_key;
        let mask_ver = self.cfg.mask_ver;
        let (tx, rx) = mpsc::channel();
        self.recv_rx = Some(rx);

        thread::spawn(move || {
            let mut buf = [0u8; 65535];
            loop {
                let n = match sock_clone.recv_from(&mut buf) {
                    Ok((n, _)) => n,
                    Err(_) => {
                        // Socket closed (force disconnect) or timeout → exit thread
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                };

                // Transport unpack (OLC + MAC + ver check)
                let Some((hdr, payload)) = moonproto_transport::transport_unpack(
                    &mac_key, &buf[..n], mask_ver,
                ) else { continue; };

                let timestamp_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                // Send to main thread via channel (6.9ns per op — faster than any lock)
                if tx.send(RecvMsg { cmd: hdr.cmd, payload, recv_bytes: n as u64, timestamp_ms }).is_err() {
                    break; // main thread dropped rx → exit
                }
            }
        });
    }

    // ... (remaining methods: process_received, handshake, ping, slicing, reconnect)
    // To be continued — this is the architectural skeleton.
    // Each method will be ported byte-exact from Delphi with self-check.

    fn process_received(&mut self, on_data: &mut OnDataFn) {
        // Drain channel into local vec to avoid borrow conflict
        let mut msgs = Vec::new();
        if let Some(ref rx) = self.recv_rx {
            while let Ok(msg) = rx.try_recv() {
                msgs.push(msg);
            }
        }
        for msg in msgs {
            self.total_recv += msg.recv_bytes;
            self.last_online = msg.timestamp_ms;
            self.handle_udp_command(Command::from_byte(msg.cmd), msg.cmd, &msg.payload, on_data);
        }
    }

    fn handle_udp_command(&mut self, cmd: Command, raw_cmd: u8, payload: &[u8], on_data: &mut OnDataFn) {
        if matches!(cmd, Command::WantNewHello | Command::WrongHello | Command::WhoAreYou | Command::Fine) {
            self.waiting_hello = false;
        }

        match cmd {
            Command::WrongHello => { self.auth_status = AuthStatus::Connected; }
            Command::WantNewHello => {
                self.full_reset();
                self.last_sent_hello = 0;
                self.auth_status = AuthStatus::Connected;
                self.authorized = false;
                self.need_connect = true;
                self.soft_reconnect = false;
            }
            Command::NeedHelloAgain => {
                let now = self.now_ms();
                if (now - self.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS {
                    self.last_need_hello_again = now;
                    if !self.waiting_hello { self.waiting_hello_start = now; }
                    self.waiting_hello = true;
                    self.last_sent_hello = 0;
                }
            }
            Command::WhoAreYou | Command::Fine => { self.handle_handshake(cmd, payload); }
            Command::SizeTest => { self.handle_size_test(payload); }
            Command::ProbeMTU => { self.handle_probe_mtu(payload); }
            Command::Sliced => {
                self.slicer.set_last_online(self.now_ms());
                let (assembled, ack) = self.slicer.on_new_sliced(payload);
                self.send_raw_packet(Command::SlicedACK, &ack);
                if let Some((inner_cmd, data)) = assembled {
                    self.data_read_int(inner_cmd, &data, on_data);
                }
            }
            Command::SlicedACK => { /* TODO: apply to our Sending list when we send Sliced */ }
            Command::Ping => { self.handle_ping(payload, on_data); }
            _ => { self.data_read(raw_cmd, payload, on_data); }
        }
    }

    fn data_read(&mut self, raw_cmd: u8, payload: &[u8], on_data: &mut OnDataFn) {
        let cmd = Command::from_byte(raw_cmd);
        if cmd == Command::Grouped {
            let mut pos = 0;
            while pos + 3 <= payload.len() {
                let sub_cmd = payload[pos]; pos += 1;
                let sz = u16::from_le_bytes([payload[pos], payload[pos+1]]) as usize; pos += 2;
                if pos + sz > payload.len() { break; }
                self.data_read_int(sub_cmd, &payload[pos..pos+sz], on_data);
                pos += sz;
            }
        } else {
            self.data_read_int(raw_cmd, payload, on_data);
        }
    }

    fn data_read_int(&mut self, raw_cmd: u8, data: &[u8], on_data: &mut OnDataFn) {
        let mut cmd = raw_cmd;
        let mut payload = data.to_vec();

        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            if let Some((inner_cmd, inner_data, _)) = crypted::decrypt_command(&self.decode_key, &payload, &mut self.slider) {
                cmd = inner_cmd;
                payload = inner_data;
                eprintln!("[DBG decrypt] inner_cmd={} payload_len={}", cmd, payload.len());
            } else { return; }
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = decompressed;
            } else { return; }
        }

        // NOTE: ApplyRegularHLAck (ACK parsing from Ping) is SERVER-SIDE logic only.
        // Client does NOT parse incoming Ping for ACK bitmap.
        // Server confirms our H-commands via SlicedACK, not via Ping.

        on_data(Command::from_byte(cmd), &payload);
    }

    fn handle_ping(&mut self, payload: &[u8], on_data: &mut OnDataFn) {
        if payload.len() < 50 { return; }
        self.ping_count += 1;
        self.round_trip_delay = i32::from_le_bytes(payload[16..20].try_into().unwrap()) as i64;
        self.actual_pmtu = u16::from_le_bytes(payload[20..22].try_into().unwrap());
        self.overheat = payload[24];
        self.rs = payload[41] as f64 / 255.0;
        self.need_connect = false;

        // Send ping response (matches Delphi SendPing exactly):
        // - Struct written first (AckStart at offset 42 = SERVER's value, untouched)
        // - BuildAckHalf provides AckWords APPENDED after struct
        // - AckStart in struct is NOT overwritten (Delphi writes struct then calls BuildAckHalf)
        let mut response = payload[..50].to_vec();
        response[0..8].copy_from_slice(&delphi_now().to_le_bytes());
        response[25..33].copy_from_slice(&self.total_sent.to_le_bytes());
        response[33..41].copy_from_slice(&self.total_recv.to_le_bytes());
        // response[42..50] = AckStart — DO NOT OVERWRITE (keep server's echo value)
        let (_ack_start, ack_words) = self.slider.build_ack_half();
        for w in &ack_words { response.extend_from_slice(&w.to_le_bytes()); }
        self.send_raw_packet(Command::Ping, &response);

        // Client does NOT do ApplyRegularHLAck from incoming Ping
        // (that's server-side logic only — MoonProtoCommon.pas:513-525)
        on_data(Command::Ping, payload);
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
            self.send_raw_packet(Command::ImFriend, &encrypted);
            thread::sleep(Duration::from_millis(32));
            self.send_raw_packet(Command::ImFriend, &encrypted);
        }
        if cmd == Command::Fine {
            self.need_connect = false;
            self.waiting_hello = false;
            self.auth_status = AuthStatus::AuthDone;
            self.authorized = true;
        }
    }

    fn handle_size_test(&mut self, payload: &[u8]) {
        if payload.len() < 6 { return; }
        let size = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let series = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        let mut ack = vec![0u8; size as usize];
        ack[0..2].copy_from_slice(&size.to_le_bytes());
        ack[4..6].copy_from_slice(&series.to_le_bytes());
        self.send_raw_packet(Command::SizeAck, &ack);
    }

    fn handle_probe_mtu(&mut self, payload: &[u8]) {
        if payload.len() < 5 { return; }
        let probe_id = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let probe_index = payload[2];
        let test_size = u16::from_le_bytes(payload[3..5].try_into().unwrap());
        let mut ack = vec![0u8; test_size as usize];
        ack[0..2].copy_from_slice(&probe_id.to_le_bytes());
        ack[2] = probe_index;
        ack[3..5].copy_from_slice(&test_size.to_le_bytes());
        self.send_raw_packet(Command::ProbeMTUAck, &ack);
    }

    /// Crypt + CreateSlicedObject + send (matches MoonProtoIntStruct.pas:1058-1196)
    fn create_sliced_and_send(&mut self, item: &SendItem) {
        let header_size = 15u16;
        let slice_hdr_size = 4u16;

        // Crypt if needed
        let (wire_cmd, wire_data, msg_num) = if item.encrypted {
            // FixedMsgNum: retry reuses same MsgNum (Delphi MoonProtoIntStruct.pas:1180)
            let msg_num = if item.msg_num != 0 {
                item.msg_num  // retry — reuse existing MsgNum
            } else {
                self.crypt_msg_counter += 1;
                self.crypt_msg_counter
            };

            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = item.cmd;
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };

            let mut plaintext = Vec::with_capacity(12 + item.data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(&item.data);

            let encrypted_data = crypto::encrypt(&self.encode_key, &plaintext, &[]);
            // Delphi: NewCmd := MPC_Crypted; if IsCompressed(d.Fcmd) then NewCmd := SetCompressed(NewCmd)
            let wire_cmd = if item.cmd & 0x80 != 0 {
                Command::Crypted as u8 | 0x80
            } else {
                Command::Crypted as u8
            };
            (wire_cmd, encrypted_data, msg_num)
        } else {
            (item.cmd, item.data.clone(), 0u64)
        };

        // CreateSlicedObject
        let pmtu = (self.actual_pmtu - header_size - slice_hdr_size) as usize;
        let total_size = wire_data.len() + 1; // +1 cmd byte in block 0
        let n_blocks = ((total_size + pmtu - 1) / pmtu).max(1);
        let max_block_num = (n_blocks - 1) as u8;
        let datagram_num = self.send_datagram_num;
        self.send_datagram_num = self.send_datagram_num.wrapping_add(1);

        let mut data_pos = 0;
        for block_num in 0..n_blocks {
            let mut slice = Vec::with_capacity(4 + pmtu);
            slice.extend_from_slice(&datagram_num.to_le_bytes());
            slice.push(block_num as u8);
            slice.push(max_block_num);

            if block_num == 0 {
                slice.push(wire_cmd);
                let write_size = (pmtu - 1).min(wire_data.len() - data_pos);
                slice.extend_from_slice(&wire_data[data_pos..data_pos + write_size]);
                data_pos += write_size;
            } else {
                let write_size = pmtu.min(wire_data.len() - data_pos);
                slice.extend_from_slice(&wire_data[data_pos..data_pos + write_size]);
                data_pos += write_size;
            }

            if block_num == 0 && n_blocks == 1 {
                eprintln!("[DBG slice] dgram={} wire_cmd={} slice_len={} encrypted_len={}",
                         datagram_num, wire_cmd as u8, slice.len(), wire_data.len());
            }
            self.send_raw_packet(Command::Sliced, &slice);
        }

        // Add to PendingH for retry if encrypted + has retries + first send only
        // (retry calls create_sliced_and_send with item.msg_num already set → don't re-add)
        if item.encrypted && item.retry_left > 0 && item.msg_num == 0 {
            let mut pending_item = item.clone();
            pending_item.msg_num = msg_num;
            pending_item.last_sent_at = self.now_ms();
            self.pending_h.push(pending_item);
        }
    }

    /// Send H-priority item directly (DoSendMPData for small packets)
    fn send_h_item(&mut self, item: &mut SendItem, cur_tm: i64) {
        self.create_sliced_and_send(item);
        item.last_sent_at = cur_tm;
    }

    /// Retry pending H-commands (matches CheckSeningData:944-954)
    fn retry_pending_h(&mut self, cur_tm: i64) {
        let path_delay = self.round_trip_delay.max(200).min(500);
        let mut to_drop = Vec::new();
        let mut to_resend = Vec::new();

        for (idx, item) in self.pending_h.iter_mut().enumerate() {
            if (item.last_sent_at - cur_tm).abs() > path_delay {
                item.last_sent_at = cur_tm;
                item.retry_left -= 1;
                to_resend.push(item.clone());
                if item.retry_left <= 0 {
                    to_drop.push(idx);
                }
            }
        }

        // Remove exhausted (reverse order to preserve indices)
        for idx in to_drop.into_iter().rev() {
            self.pending_h.remove(idx);
        }

        // Resend (outside of borrow)
        for item in to_resend {
            self.create_sliced_and_send(&item);
        }
    }

    /// Send a packet directly (low-level, no queue)
    fn send_direct(&mut self, item: &SendItem) {
        self.create_sliced_and_send(item);
    }

    fn send_raw_packet(&mut self, cmd: Command, payload: &[u8]) {
        let Some(sock) = &self.socket else { return };
        let (packet, extra) = moonproto_transport::transport_pack(
            &self.cfg.mac_key, cmd as u8, self.cfg.client_id, payload, self.cfg.mask_ver,
        );
        let addr = self.server_addr();
        if let Some(extra_pkt) = extra { sock.send_to(&extra_pkt, &addr).ok(); }
        sock.send_to(&packet, &addr).ok();
        self.total_sent += packet.len() as u64;
    }

    fn send_hello(&mut self) {
        let payload = handshake::build_hello_packet(
            &self.cfg.master_key, self.cfg.client_id, &mut self.client_token, self.app_token,
        );
        self.send_raw_packet(Command::Hello, &payload);
    }

    fn send_hello_again(&mut self) {
        self.client_token += 1;
        let mut hello = handshake::Hello::new(self.client_token, self.app_token);
        hello.timestamp = delphi_now();
        hello.peer_mix = crypto::mix_values(&hello.rnd, hello.mix_ts, self.server_token);
        let packed = hello.to_bytes_packed();
        let encrypted = crypto::encrypt(&self.encode_key, &packed, &[]);
        self.send_raw_packet(Command::HelloAgain, &encrypted);
    }

    fn check_hello_send(&mut self, cur_tm: i64) {
        if !self.need_connect || self.force_disconnect { return; }
        let interval = self.round_trip_delay.max(1000) * 2;
        if (cur_tm - self.last_sent_hello).abs() <= interval { return; }
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

    fn check_offline_reconnect(&mut self, cur_tm: i64) {
        let throttle = (self.round_trip_delay + 50).max(200).min(1500);
        let last_online = self.last_online;
        let authorized = self.authorized;

        let should = self.waiting_hello
            || (authorized && !self.need_connect && (cur_tm - last_online).abs() > OFFLINE_BASE_MS + self.round_trip_delay);
        if !should { return; }
        if (cur_tm - self.last_sent_hello).abs() <= throttle { return; }

        self.auth_status = AuthStatus::Offline;
        if !self.waiting_hello { self.waiting_hello_start = cur_tm; }
        self.waiting_hello = true;
        self.send_hello_again();
        self.last_sent_hello = cur_tm;
    }

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

    fn check_dead_zone(&mut self, cur_tm: i64) {
        let authorized = self.authorized;
        let last_online = self.last_online;
        if !authorized && !self.need_connect && (cur_tm - last_online).abs() > DEAD_ZONE_MS {
            self.soft_reconnect = false;
            self.force_disconnect = true;
            self.need_connect = true;
        }
    }

    fn do_force_disconnect(&mut self) {
        let authorized = self.authorized;
        if authorized && !self.soft_reconnect {
            self.send_raw_packet(Command::LogOff, &[]);
        }
        self.socket = None; // drops socket, reader thread will error and stop
        if !self.soft_reconnect { self.full_reset(); }
        self.authorized = false;
        self.force_disconnect = false;
    }

    fn full_reset(&mut self) {
        self.server_token = 0;
        self.crypt_msg_counter = 0;
        self.send_datagram_num = 0;
        self.slider = Slider::new();
        self.recv_slider = Slider::new();
        self.slicer = slicing::SlicingReceiver::new();
        self.total_sent = 0;
        self.rs = 1.0;
        self.actual_pmtu = 508;
        self.total_recv = 0;
        self.last_online = 0;
        self.pending_h.clear();
    }

    fn bind_socket(&mut self) {
        self.force_disconnect = false;
        if self.next_port < 1024 || self.next_port > 65000 { self.next_port = 1024; }
        for _ in 0..200 {
            let addr = format!("0.0.0.0:{}", self.next_port);
            match UdpSocket::bind(&addr) {
                Ok(sock) => {
                    sock.set_read_timeout(Some(Duration::from_secs(1))).ok();
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

    pub fn is_authorized(&self) -> bool { self.authorized }
    pub fn auth_status(&self) -> AuthStatus { self.auth_status }
    pub fn ping_count(&self) -> u32 { self.ping_count }
    pub fn total_sent(&self) -> u64 { self.total_sent }
    pub fn total_recv(&self) -> u64 { self.total_recv }
}

fn delphi_now() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25569.0 + secs / 86400.0
}

fn set_socket_buffers(_sock: &UdpSocket) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::io::AsRawSocket;
        let raw = _sock.as_raw_socket();
        let buf_size: i32 = 8 * 1024 * 1024;
        unsafe {
            extern "system" {
                fn setsockopt(s: usize, level: i32, optname: i32, optval: *const i8, optlen: i32) -> i32;
            }
            setsockopt(raw as usize, 0xFFFF, 0x1002, &buf_size as *const i32 as *const i8, 4);
            setsockopt(raw as usize, 0xFFFF, 0x1001, &buf_size as *const i32 as *const i8, 4);
        }
    }
}
