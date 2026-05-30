use super::*;
use crate::transport::{
    outer_light_crypt, ClientMsgHeader, MacContext, ServerMsgHeader, TRANSPORT_VER,
};

static ERR_EMU_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct ErrEmuTestGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl Drop for ErrEmuTestGuard {
    fn drop(&mut self) {
        set_err_emu(0);
    }
}

fn err_emu_test_guard() -> ErrEmuTestGuard {
    let guard = ERR_EMU_TEST_LOCK.lock().unwrap();
    set_err_emu(0);
    ErrEmuTestGuard { _lock: guard }
}

fn dummy_cfg_for_server(server_addr: SocketAddr) -> ClientConfig {
    ClientConfig {
        server_ip: server_addr.ip().to_string(),
        server_port: server_addr.port(),
        master_key: [0; 16],
        mac_key: [0x11; 16],
        mask_ver: TransportMode::V0,
        client_id: 0x1234_5678_9ABC_DEF0,
        ntp_host: None,
        refresh: RefreshConfig::default(),
    }
}

fn pack_server_packet(mac_key: &MoonKey, cmd: Command, payload: &[u8]) -> Vec<u8> {
    let hdr = ServerMsgHeader {
        rnd: 0x5A,
        checksum: 0,
        ver: TRANSPORT_VER,
        cmd: cmd.to_byte(),
    };
    let mut buf = hdr.to_bytes().to_vec();
    buf.extend_from_slice(payload);
    let mac_ctx = MacContext::new(mac_key);
    let mac = mac_ctx.mac(&buf);
    buf[1..5].copy_from_slice(&mac.to_le_bytes());
    outer_light_crypt(&mut buf, mac_key);
    buf
}

fn unpack_client_packet(mac_key: &MoonKey, raw: &[u8]) -> (ClientMsgHeader, Vec<u8>) {
    const CLIENT_HDR_SIZE: usize = 15;
    let mut buf = raw.to_vec();
    outer_light_crypt(&mut buf, mac_key);
    let hdr = ClientMsgHeader::from_bytes(&buf).unwrap();
    let saved = [buf[1], buf[2], buf[3], buf[4]];
    buf[1..5].copy_from_slice(&0u32.to_le_bytes());
    let mac = MacContext::new(mac_key).mac(&buf);
    assert_eq!(mac, hdr.checksum);
    buf[1..5].copy_from_slice(&saved);
    (hdr, buf[CLIENT_HDR_SIZE..].to_vec())
}

fn recv_client_packet(server_sock: &UdpSocket, client: &mut Client) -> (ClientMsgHeader, Vec<u8>) {
    let _events = pump_inline_reader_collect(client);
    let mut ack_buf = [0u8; 2048];
    let (n, _from) = server_sock.recv_from(&mut ack_buf).unwrap();
    unpack_client_packet(&client.cfg.mac_key, &ack_buf[..n])
}

fn recv_client_packet_with_events(
    server_sock: &UdpSocket,
    client: &mut Client,
) -> ((ClientMsgHeader, Vec<u8>), Vec<(Command, Vec<u8>)>) {
    let events = pump_inline_reader_collect(client);
    let mut ack_buf = [0u8; 2048];
    let (n, _from) = server_sock.recv_from(&mut ack_buf).unwrap();
    (
        unpack_client_packet(&client.cfg.mac_key, &ack_buf[..n]),
        events,
    )
}

fn inline_reader_test_client() -> (UdpSocket, SocketAddr, Client) {
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.testing_set_domain_ready(true);
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    (server_sock, client_addr, client)
}

fn pump_inline_reader(client: &mut Client) {
    let _events = pump_inline_reader_collect(client);
}

fn pump_inline_reader_collect(client: &mut Client) -> Vec<(Command, Vec<u8>)> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_cb = Arc::clone(&events);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            events_cb.lock().unwrap().push((cmd, payload.to_vec()));
        }),
    };
    ProtocolCore { client }.recv_drain_phase(
        0,
        Instant::now() + Duration::from_millis(30),
        &mut mode,
    );
    drop(mode);
    Arc::try_unwrap(events).unwrap().into_inner().unwrap()
}

fn assert_no_inline_reader_events(client: &mut Client, why: &str) {
    let deadline = Instant::now() + Duration::from_millis(30);
    while Instant::now() < deadline {
        let events = pump_inline_reader_collect(client);
        assert!(events.is_empty(), "{why}: got {events:?}");
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_reader_total_recv(client: &mut Client, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while client.total_recv() < expected && Instant::now() < deadline {
        pump_inline_reader(client);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(client.total_recv(), expected);
}

fn service_ping_payload(
    trip_delay: i32,
    pmtu: u16,
    global_timing_orders: u16,
    overheat: u8,
    rsq: u8,
) -> Vec<u8> {
    let mut payload = vec![0u8; 50];
    payload[16..20].copy_from_slice(&trip_delay.to_le_bytes());
    payload[20..22].copy_from_slice(&pmtu.to_le_bytes());
    payload[22..24].copy_from_slice(&global_timing_orders.to_le_bytes());
    payload[24] = overheat;
    payload[41] = rsq;
    payload
}

fn encrypted_server_hello(
    master_key: &MoonKey,
    cmd: Command,
    client_id: u64,
    server_token: u64,
    peer_app_token: u64,
) -> Vec<u8> {
    let mut hello = handshake::Hello::new(0x1111, peer_app_token);
    hello.server_token = server_token;
    hello.app_token = peer_app_token;
    hello.timestamp = delphi_now();
    let aad = handshake::handshake_aad(client_id, cmd.to_byte());
    crypto::encrypt(master_key, &hello.to_bytes_packed(), &aad)
}

fn install_active_session(client: &mut Client, server_token: u64) -> (MoonKey, MoonKey) {
    let (encode_key, decode_key) = crypto::generate_sub_keys(&client.cfg.master_key, server_token);
    client.server_token = server_token;
    client.encode_key = encode_key;
    client.decode_key = decode_key;
    client.encode_cipher = Some(crypto::cipher_from_key(&encode_key));
    client
        .data_read_state
        .set_decode_cipher(crypto::cipher_from_key(&decode_key));
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.need_connect = false;
    client.was_ever_connected = true;
    (encode_key, decode_key)
}

#[test]
fn service_cmds_include_handshake_and_keepalive() {
    for cmd in [
        Command::Ping,
        Command::WantNewHello,
        Command::WrongHello,
        Command::WhoAreYou,
        Command::Fine,
        Command::NeedHelloAgain,
        Command::SizeTest,
        Command::ProbeMTU,
        Command::SlicedACK,
    ] {
        assert!(is_service_cmd(cmd.to_byte()), "{cmd:?} must be service");
    }
}

#[test]
fn data_channels_are_not_service_cmds() {
    for cmd in [
        Command::Order,
        Command::UI,
        Command::Strat,
        Command::API,
        Command::Balance,
        Command::TradesStream,
        Command::OrderBook,
    ] {
        assert!(!is_service_cmd(cmd.to_byte()), "{cmd:?} must stay data");
    }
}

#[test]
fn sliced_is_not_err_emu_service() {
    assert!(
        !is_service_cmd(Command::Sliced.to_byte()),
        "ErrEmu must drop MPC_Sliced with the full configured rate like Delphi"
    );
}

#[test]
fn err_emu_halves_service_drop_rate_like_delphi() {
    assert_eq!(
        err_emu_drop_rate_for_cmd(50, Command::Fine.to_byte()),
        25,
        "Delphi MoonProtoErrEmu halves service/handshake commands"
    );
    assert_eq!(
        err_emu_drop_rate_for_cmd(50, Command::NeedHelloAgain.to_byte()),
        25
    );
    assert_eq!(
        err_emu_drop_rate_for_cmd(50, Command::SlicedACK.to_byte()),
        25
    );
    assert_eq!(
        err_emu_drop_rate_for_cmd(50, Command::Sliced.to_byte()),
        50,
        "MPC_Sliced data must keep the full configured drop rate"
    );
    assert_eq!(err_emu_drop_rate_for_cmd(250, Command::API.to_byte()), 100);
}

#[test]
fn run_drains_udp_data_without_wake_fifo() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut client = Client::new(dummy_cfg_for_server(server_sock.local_addr().unwrap()));
    client.testing_set_domain_ready(true);
    client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    client.need_connect = false;
    client.start_inline_reader_session();
    let client_addr = client.socket.as_ref().unwrap().local_addr().unwrap();
    let packet = pack_server_packet(&client.cfg.mac_key, Command::UI, &[0xAA, 0xBB]);
    server_sock.send_to(&packet, client_addr).unwrap();

    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    client.run(
        Duration::from_millis(DEFAULT_SLEEP_MS + 5),
        Box::new(move |cmd, payload| {
            assert_eq!(cmd, Command::UI);
            assert_eq!(payload, &[0xAA, 0xBB]);
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    );

    assert_eq!(
        delivered.load(Ordering::Relaxed),
        1,
        "run loop must drain UDP data directly, without a wake FIFO"
    );
}

#[test]
fn reader_sends_sliced_ack_without_main_loop_tick() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.testing_set_domain_ready(true);
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let slice_payload = vec![
        0x2A,
        0x00, // DatagramNum = 42
        0x00, // BlockNum = 0
        0x00, // MaxBlockNum = 0
        Command::API.to_byte(),
        0xDE,
        0xAD,
    ];
    let packet = pack_server_packet(&client.cfg.mac_key, Command::Sliced, &slice_payload);
    server_sock.send_to(&packet, client_addr).unwrap();

    let ((hdr, ack_payload), events) = recv_client_packet_with_events(&server_sock, &mut client);

    assert_eq!(hdr.cmd, Command::SlicedACK.to_byte());
    assert_eq!(ack_payload.len(), slicing::ACK256_WIRE_SIZE);
    assert_eq!(ack_payload[0] & 0x01, 0x01);
    assert_eq!(&ack_payload[32..34], &42u16.to_le_bytes());
    assert_eq!(events, vec![(Command::API, vec![0xDE, 0xAD])]);
    assert_no_inline_reader_events(
        &mut client,
        "single-owner receive drains decoded payload in the same datagram step",
    );
    assert!(
        client.total_sent() > 0,
        "ProtocolCore receive must send ACK immediately before write tick"
    );
}

#[test]
fn reader_handles_sliced_ack_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.testing_set_domain_ready(true);
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let datagram_num = 0x3344u16;
    let mut ack_payload = vec![0u8; slicing::ACK256_WIRE_SIZE];
    ack_payload[0] = 0b1010_0101;
    ack_payload[32..34].copy_from_slice(&datagram_num.to_le_bytes());
    let packet = pack_server_packet(&client.cfg.mac_key, Command::SlicedACK, &ack_payload);
    server_sock.send_to(&packet, client_addr).unwrap();

    wait_reader_total_recv(&mut client, packet.len() as u64);
    let deadline = Instant::now() + Duration::from_secs(1);
    while client
        .send_lock
        .lock()
        .unwrap()
        .incoming_sliced_acks
        .is_empty()
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    let ack = client
        .send_lock
        .lock()
        .unwrap()
        .incoming_sliced_acks
        .pop()
        .unwrap();
    assert_no_inline_reader_events(
        &mut client,
        "Delphi OnNewSlicedACK only queues ACK; no DataReadInt/no reader event",
    );

    assert_eq!(ack.datagram_num, datagram_num);
    assert_eq!(ack.flags[0], 0b1010_0101);
    assert_eq!(client.total_recv, packet.len() as u64);
}

#[test]
fn reader_handles_partial_sliced_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.testing_set_domain_ready(true);
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let datagram_num = 43u16;
    let slice_payload = vec![
        datagram_num as u8,
        (datagram_num >> 8) as u8,
        0x00, // BlockNum = 0
        0x01, // MaxBlockNum = 1, so this packet is only a partial datagram
        Command::API.to_byte(),
        0xCA,
        0xFE,
    ];
    let packet = pack_server_packet(&client.cfg.mac_key, Command::Sliced, &slice_payload);
    server_sock.send_to(&packet, client_addr).unwrap();

    let ((hdr, ack_payload), first_events) =
        recv_client_packet_with_events(&server_sock, &mut client);
    wait_reader_total_recv(&mut client, packet.len() as u64);
    assert!(first_events.is_empty());
    assert_no_inline_reader_events(
        &mut client,
        "partial Sliced must only ACK and stay in Receiving; no DataReadInt before completion",
    );

    assert_eq!(hdr.cmd, Command::SlicedACK.to_byte());
    assert_eq!(ack_payload.len(), slicing::ACK256_WIRE_SIZE);
    assert_eq!(ack_payload[0] & 0x01, 0x01);
    assert_eq!(&ack_payload[32..34], &datagram_num.to_le_bytes());

    let slice_payload_2 = vec![
        datagram_num as u8,
        (datagram_num >> 8) as u8,
        0x01, // BlockNum = 1
        0x01, // MaxBlockNum = 1
        0xBE,
        0xEF,
    ];
    let packet2 = pack_server_packet(&client.cfg.mac_key, Command::Sliced, &slice_payload_2);
    server_sock.send_to(&packet2, client_addr).unwrap();
    let ((hdr2, ack_payload2), second_events) =
        recv_client_packet_with_events(&server_sock, &mut client);

    assert_eq!(hdr2.cmd, Command::SlicedACK.to_byte());
    assert_eq!(ack_payload2[0] & 0x03, 0x03);
    assert_eq!(&ack_payload2[32..34], &datagram_num.to_le_bytes());
    assert_eq!(
        second_events,
        vec![(Command::API, vec![0xCA, 0xFE, 0xBE, 0xEF])]
    );
    assert_eq!(client.total_recv, (packet.len() + packet2.len()) as u64);
}

#[test]
fn reader_handles_size_test_without_main_loop_tick() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let size = 64u16;
    let packet_num = 9u16;
    let series = 0xBEEFu16;
    let mut size_test = Vec::new();
    size_test.extend_from_slice(&size.to_le_bytes());
    size_test.extend_from_slice(&packet_num.to_le_bytes());
    size_test.extend_from_slice(&series.to_le_bytes());
    let packet = pack_server_packet(&client.cfg.mac_key, Command::SizeTest, &size_test);
    server_sock.send_to(&packet, client_addr).unwrap();

    let (hdr, ack_payload) = recv_client_packet(&server_sock, &mut client);
    wait_reader_total_recv(&mut client, packet.len() as u64);
    assert_no_inline_reader_events(
        &mut client,
        "SizeTest sends SizeAck immediately and does not enqueue DataReadInt",
    );

    assert_eq!(hdr.cmd, Command::SizeAck.to_byte());
    assert_eq!(ack_payload.len(), size as usize);
    assert_eq!(&ack_payload[0..2], &size.to_le_bytes());
    assert_eq!(&ack_payload[4..6], &series.to_le_bytes());
    assert_eq!(client.data_read_state.data_size_ack_series_num, series);
    assert_eq!(client.total_recv, packet.len() as u64);
}

#[test]
fn reader_handles_probe_mtu_without_main_loop_tick() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let probe_id = 0x1234u16;
    let probe_index = 1u8;
    let test_size = 80u16;
    let mut probe = Vec::new();
    probe.extend_from_slice(&probe_id.to_le_bytes());
    probe.push(probe_index);
    probe.extend_from_slice(&test_size.to_le_bytes());
    let packet = pack_server_packet(&client.cfg.mac_key, Command::ProbeMTU, &probe);
    server_sock.send_to(&packet, client_addr).unwrap();

    let (hdr, ack_payload) = recv_client_packet(&server_sock, &mut client);
    wait_reader_total_recv(&mut client, packet.len() as u64);
    assert_no_inline_reader_events(
        &mut client,
        "ProbeMTU sends ProbeMTUAck immediately and does not enqueue DataReadInt",
    );

    assert_eq!(hdr.cmd, Command::ProbeMTUAck.to_byte());
    assert_eq!(ack_payload.len(), test_size as usize);
    assert_eq!(&ack_payload[0..2], &probe_id.to_le_bytes());
    assert_eq!(ack_payload[2], probe_index);
    assert_eq!(&ack_payload[3..5], &test_size.to_le_bytes());
    assert_eq!(client.total_recv, packet.len() as u64);
}

#[test]
fn reader_handles_ping_response_without_main_loop_tick() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.testing_set_domain_ready(true);
    client.total_sent.store(777, Ordering::Relaxed);
    client.auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.need_connect = true;
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let ping = service_ping_payload(123, 8_224, 456, 7, 128);
    let packet = pack_server_packet(&client.cfg.mac_key, Command::Ping, &ping);
    server_sock.send_to(&packet, client_addr).unwrap();

    let ((hdr, response), events) = recv_client_packet_with_events(&server_sock, &mut client);

    assert_eq!(hdr.cmd, Command::Ping.to_byte());
    assert_eq!(response.len(), 50);
    assert_eq!(
        u64::from_le_bytes(response[25..33].try_into().unwrap()),
        777
    );
    assert_eq!(
        u64::from_le_bytes(response[33..41].try_into().unwrap()),
        packet.len() as u64,
        "Delphi SendPing writes TotalRecvBytes after UDPRead counted the current packet"
    );
    assert_eq!(
        u64::from_le_bytes(response[42..50].try_into().unwrap()),
        2048,
        "empty MPSlider BuildAckHalf still writes the tail-half AckStart"
    );
    assert_eq!(events, vec![(Command::Ping, ping.clone())]);
    assert_no_inline_reader_events(
        &mut client,
        "single-owner receive applies Ping update and drains callback in the same datagram step",
    );
    assert_eq!(client.round_trip_delay, 123);
    assert_eq!(client.actual_pmtu, 8_224);
    assert_eq!(client.global_timing_orders, 456);
    assert_eq!(client.ping_count, 1);
    assert_eq!(client.total_recv, packet.len() as u64);
    assert_eq!(client.auth_status, AuthStatus::AuthDone);
    assert!(!client.need_connect);

    assert_eq!(client.round_trip_delay_ms(), 123);
    assert_eq!(client.actual_pmtu(), 8_224);
    assert_eq!(client.global_timing_orders(), 456);
    assert_eq!(client.ping_count(), 1);
    assert_eq!(client.total_recv(), packet.len() as u64);
    assert!(!client.need_connect);
}

#[test]
fn reader_handles_who_are_you_imfriend_without_main_loop_tick() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    let token_before = client.client_token;
    let app_token = client.app_token;
    let server_token = 0x2222_3333_4444_5555;
    let peer_app_token = 0xAAAA_BBBB_CCCC_DDDD;
    client.socket = Some(client_sock);
    client.start_inline_reader_session();
    client.start_hello_wait(HelloWaitState::PrimaryHelloCold, 0);

    let who = encrypted_server_hello(
        &client.cfg.master_key,
        Command::WhoAreYou,
        client.cfg.client_id,
        server_token,
        peer_app_token,
    );
    let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, &who);
    server_sock.send_to(&packet, client_addr).unwrap();

    let started = Instant::now();
    let ((hdr1, imfriend1), events1) = recv_client_packet_with_events(&server_sock, &mut client);
    let elapsed = started.elapsed();
    let (hdr2, imfriend2) = recv_client_packet(&server_sock, &mut client);

    assert_eq!(hdr1.cmd, Command::ImFriend.to_byte());
    assert_eq!(hdr2.cmd, Command::ImFriend.to_byte());
    assert!(events1.is_empty());
    assert_eq!(
        imfriend1, imfriend2,
        "Rust keeps Delphi duplicate ImFriend wire effect"
    );
    assert!(
        elapsed >= Duration::from_millis(25),
        "WhoAreYou handler must preserve Delphi's 32ms ImFriend barrier; elapsed={elapsed:?}"
    );
    let (encode_key, decode_key) = crypto::generate_sub_keys(&client.cfg.master_key, server_token);
    let aad = handshake::handshake_aad(client.cfg.client_id, Command::ImFriend.to_byte());
    let decrypted = crypto::decrypt(&encode_key, &imfriend1, &aad)
        .expect("ImFriend decrypts with client encode key");
    let im = handshake::Hello::from_bytes(&decrypted).expect("valid ImFriend Hello");
    assert_eq!(im.mix_ts, token_before.wrapping_add(1));
    assert_eq!(im.app_token, app_token);

    assert_eq!(client.server_token, server_token);
    assert_eq!(client.peer_app_token, peer_app_token);
    assert_eq!(client.client_token, token_before.wrapping_add(1));
    assert_eq!(client.encode_key, encode_key);
    assert_eq!(client.decode_key, decode_key);
    assert_eq!(client.client_token, token_before.wrapping_add(1));
    assert_eq!(client.encode_key, encode_key);
    assert_eq!(client.decode_key, decode_key);
}

#[test]
fn reader_who_are_you_uses_writer_updated_hello_token() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    let token_before = client.client_token;
    let server_token = 0x2222_3333_4444_5555;
    let peer_app_token = 0xAAAA_BBBB_CCCC_DDDD;
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    ProtocolCore {
        client: &mut client,
    }
    .check_hello_send(100);

    let (hello_hdr, hello_payload) = recv_client_packet(&server_sock, &mut client);
    assert_eq!(hello_hdr.cmd, Command::Hello.to_byte());
    let aad = handshake::handshake_aad(client.cfg.client_id, Command::Hello.to_byte());
    let hello = crypto::decrypt(&client.cfg.master_key, &hello_payload, &aad)
        .and_then(|payload| handshake::Hello::from_bytes(&payload))
        .expect("sent Hello decrypts with master key");
    assert_eq!(hello.mix_ts, token_before.wrapping_add(1));

    let who = encrypted_server_hello(
        &client.cfg.master_key,
        Command::WhoAreYou,
        client.cfg.client_id,
        server_token,
        peer_app_token,
    );
    let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, &who);
    server_sock.send_to(&packet, client_addr).unwrap();

    let ((hdr1, imfriend1), events1) = recv_client_packet_with_events(&server_sock, &mut client);
    let (hdr2, _imfriend2) = recv_client_packet(&server_sock, &mut client);

    assert_eq!(hdr1.cmd, Command::ImFriend.to_byte());
    assert_eq!(hdr2.cmd, Command::ImFriend.to_byte());
    assert!(events1.is_empty());
    let (encode_key, _decode_key) = crypto::generate_sub_keys(&client.cfg.master_key, server_token);
    let imfriend_aad = handshake::handshake_aad(client.cfg.client_id, Command::ImFriend.to_byte());
    let im = crypto::decrypt(&encode_key, &imfriend1, &imfriend_aad)
        .and_then(|payload| handshake::Hello::from_bytes(&payload))
        .expect("ImFriend decrypts with client encode key");
    assert_eq!(
        im.mix_ts,
        token_before.wrapping_add(2),
        "Delphi server requires ImFriend.MixTS > original Hello.MixTS",
    );
    assert_eq!(client.client_token, token_before.wrapping_add(2));
}

#[test]
fn reader_handles_fine_auth_done_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.need_connect = true;
    client.start_hello_wait(HelloWaitState::PrimaryImFriendSent, 0);
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let fine = encrypted_server_hello(
        &client.cfg.master_key,
        Command::Fine,
        client.cfg.client_id,
        0x2222,
        0x3333,
    );
    let packet = pack_server_packet(&client.cfg.mac_key, Command::Fine, &fine);
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert!(client.authorized);
    assert_eq!(client.auth_status, AuthStatus::AuthDone);
    assert!(!client.need_connect);
    assert!(!client.waiting_hello);
}

#[test]
fn reader_keeps_primary_wait_after_invalid_who_are_you() {
    let _err_emu_guard = err_emu_test_guard();
    let (server_sock, client_addr, mut client) = inline_reader_test_client();
    client.start_hello_wait(HelloWaitState::PrimaryHelloCold, 0);
    client.server_token = 0x1234;

    let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, b"bad");
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert!(client.waiting_hello);
    assert_eq!(
        client.server_token, 0x1234,
        "invalid handshake payload must not apply WhoAreYou fields",
    );
}

#[test]
fn reader_keeps_fine_wait_after_invalid_fine() {
    let _err_emu_guard = err_emu_test_guard();
    let (server_sock, client_addr, mut client) = inline_reader_test_client();
    client.start_hello_wait(HelloWaitState::PrimaryImFriendSent, 0);
    client.need_connect = true;

    let packet = pack_server_packet(&client.cfg.mac_key, Command::Fine, b"bad");
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert!(client.waiting_hello);
    assert!(client.need_connect);
    assert!(!client.authorized);
    assert_ne!(client.auth_status, AuthStatus::AuthDone);
}

#[test]
fn reader_handles_wrong_hello_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let (server_sock, client_addr, mut client) = inline_reader_test_client();
    client.auth_status = AuthStatus::Offline;

    let packet = pack_server_packet(&client.cfg.mac_key, Command::WrongHello, &[]);
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert!(!client.waiting_hello);
}

#[test]
fn reader_handles_want_new_hello_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let (server_sock, client_addr, mut client) = inline_reader_test_client();
    client.authorized = true;
    client.auth_status = AuthStatus::Offline;
    client.need_connect = false;
    client.soft_reconnect = true;
    client.set_hello_wait_state(HelloWaitState::RebindHelloAgain);
    client.last_sent_hello = 12345;
    client.crypt_msg_counter.store(77, Ordering::Relaxed);
    client.total_sent.store(123, Ordering::Relaxed);
    client.recvd_slider.has_new_data = true;

    let packet = pack_server_packet(&client.cfg.mac_key, Command::WantNewHello, &[]);
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert!(!client.authorized);
    assert!(client.need_connect);
    assert!(!client.soft_reconnect);
    assert_eq!(client.crypt_msg_counter.load(Ordering::Relaxed), 0);
    assert_eq!(client.total_sent(), 0);
    assert!(!client.recvd_slider.has_new_data);
    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert!(!client.authorized);
    assert!(client.need_connect);
    assert!(!client.soft_reconnect);
}

#[test]
fn reader_handles_need_hello_again_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let (server_sock, client_addr, mut client) = inline_reader_test_client();
    install_active_session(&mut client, 1);
    client.last_sent_hello = 12345;

    let packet = pack_server_packet(&client.cfg.mac_key, Command::NeedHelloAgain, &[]);
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert!(client.waiting_hello);
    assert!(client.waiting_hello_start >= 0);
    assert_eq!(client.last_need_hello_again, client.waiting_hello_start);
    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
}

#[test]
fn nat_binding_change_rebinds_with_hello_again_from_new_socket() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let old_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let old_addr = old_socket.local_addr().unwrap();
    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    let server_token = 0x2222_3333_4444_5555;
    let (encode_key, _decode_key) = install_active_session(&mut client, server_token);
    client.socket = Some(old_socket);
    client.start_inline_reader_session();

    client.clear_recv_poller();
    let new_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let new_addr = new_socket.local_addr().unwrap();
    assert_ne!(
        old_addr, new_addr,
        "test must emulate a changed NAT binding"
    );
    client.socket = Some(new_socket);
    client.start_inline_reader_session();

    let need = pack_server_packet(&client.cfg.mac_key, Command::NeedHelloAgain, &[]);
    server_sock.send_to(&need, new_addr).unwrap();
    pump_inline_reader(&mut client);

    assert!(client.waiting_hello);
    assert!(client.hello_wait_state.allows_hello_again_retry());
    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);

    ProtocolCore {
        client: &mut client,
    }
    .check_offline_reconnect(100);

    let mut raw = [0u8; 2048];
    let (n, from) = server_sock.recv_from(&mut raw).unwrap();
    assert_eq!(from, new_addr);
    let (hdr, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
    assert_eq!(hdr.cmd, Command::HelloAgain.to_byte());
    let aad = handshake::handshake_aad(client.cfg.client_id, Command::HelloAgain.to_byte());
    let hello_again = crypto::decrypt(&encode_key, &payload, &aad)
        .and_then(|bytes| handshake::Hello::from_bytes(&bytes))
        .expect("HelloAgain decrypts with session encode key");
    assert_eq!(
        hello_again.peer_mix,
        crypto::mix_values(&hello_again.rnd, hello_again.mix_ts, server_token),
    );

    let fine = encrypted_server_hello(
        &client.cfg.master_key,
        Command::Fine,
        client.cfg.client_id,
        server_token,
        0xAAAA_BBBB_CCCC_DDDD,
    );
    let fine_packet = pack_server_packet(&client.cfg.mac_key, Command::Fine, &fine);
    server_sock.send_to(&fine_packet, new_addr).unwrap();
    pump_inline_reader(&mut client);

    assert!(client.authorized);
    assert_eq!(client.auth_status, AuthStatus::AuthDone);
    assert!(!client.waiting_hello);
    assert!(!client.need_connect);
}

#[test]
fn reader_need_hello_again_without_session_falls_back_to_hard_hello() {
    let _err_emu_guard = err_emu_test_guard();
    let (server_sock, client_addr, mut client) = inline_reader_test_client();
    client.last_sent_hello = 12345;

    let packet = pack_server_packet(&client.cfg.mac_key, Command::NeedHelloAgain, &[]);
    server_sock.send_to(&packet, client_addr).unwrap();

    pump_inline_reader(&mut client);

    assert!(!client.waiting_hello);
    assert!(client.need_connect);
    assert!(!client.soft_reconnect);
    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
    assert!(client.next_primary_hello_new_session);
}

#[test]
fn data_read_decodes_regular_data_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.testing_set_domain_ready(true);
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let packet = pack_server_packet(&client.cfg.mac_key, Command::UI, &[0xAA, 0xBB]);
    server_sock.send_to(&packet, client_addr).unwrap();

    let events = pump_inline_reader_collect(&mut client);
    assert_eq!(events, vec![(Command::UI, vec![0xAA, 0xBB])]);
    assert_no_inline_reader_events(
        &mut client,
        "regular data must be delivered immediately, not left in decoded queue",
    );
    assert_eq!(client.total_recv, packet.len() as u64);
}

#[test]
fn reader_err_emu_drop_updates_stats_without_recv_event_backlog() {
    let _err_emu_guard = err_emu_test_guard();
    set_err_emu(100);
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr = server_sock.local_addr().unwrap();

    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    client_sock
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let client_addr = client_sock.local_addr().unwrap();

    let mut client = Client::new(dummy_cfg_for_server(server_addr));
    client.socket = Some(client_sock);
    client.start_inline_reader_session();

    let packet = pack_server_packet(&client.cfg.mac_key, Command::UI, &[0xAA, 0xBB]);
    server_sock.send_to(&packet, client_addr).unwrap();

    wait_reader_total_recv(&mut client, packet.len() as u64);
    assert_no_inline_reader_events(
        &mut client,
        "Delphi ErrEmu exits after stats side effects; no protocol/user event",
    );

    assert!(client.connected);
    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert_eq!(client.total_recv, packet.len() as u64);
    assert!(client.last_online >= 0);
}

#[test]
fn datagram_too_large_errors_are_non_fatal_pmtu_feedback() {
    for code in [90, 10040] {
        let err = std::io::Error::from_raw_os_error(code);
        assert!(is_datagram_too_large_error(&err), "os error {code}");
    }
    assert!(is_pmtu_probe_ack_command(Command::SizeAck.to_byte()));
    assert!(is_pmtu_probe_ack_command(Command::ProbeMTUAck.to_byte()));
    assert!(
        !is_pmtu_probe_ack_command(Command::API.to_byte()),
        "ordinary application data must still be sliced before it can exceed PMTU"
    );
    let bsd_emsgsize = std::io::Error::from_raw_os_error(40);
    assert_eq!(
        is_datagram_too_large_error(&bsd_emsgsize),
        cfg!(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
        )),
    );

    let permission = std::io::Error::from_raw_os_error(13);
    assert!(!is_datagram_too_large_error(&permission));
}

#[test]
fn generic_send_error_logs_without_force_disconnect() {
    let mut client = Client::new(ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        mask_ver: TransportMode::V0,
        client_id: 0,
        ntp_host: None,
        refresh: RefreshConfig::default(),
    });
    client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    let incompatible_addr: SocketAddr = "[::1]:9".parse().unwrap();

    client.dispatch_send(Command::Ping.to_byte(), &[0xAA], None, incompatible_addr);

    assert_eq!(client.total_sent(), 0, "IPv4 socket → IPv6 addr must fail");
    assert!(
        !client.force_disconnect,
        "Delphi send error only logs; it must not start reconnect"
    );
}
