use super::*;

fn dummy_cfg() -> ClientConfig {
    ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        mask_ver: TransportMode::V0,
        client_id: 0,
        ntp_host: None,
        refresh: RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        },
        market_history: crate::state::MarketHistorySizing::default(),
    }
}

fn unpack_client_packet(mac_key: &MoonKey, raw: &[u8]) -> (u8, Vec<u8>) {
    const CLIENT_HDR_SIZE: usize = 15;
    let mut buf = raw.to_vec();
    crate::transport::outer_light_crypt(
        &mut buf,
        crate::transport::MacContext::new(mac_key).obf_key(),
    );
    let hdr = crate::transport::ClientMsgHeader::from_bytes(&buf).unwrap();
    let saved = [buf[1], buf[2], buf[3], buf[4]];
    buf[1..5].copy_from_slice(&0u32.to_le_bytes());
    let mac = crate::transport::MacContext::new(mac_key).mac(&buf);
    assert_eq!(mac, hdr.checksum);
    buf[1..5].copy_from_slice(&saved);
    (hdr.cmd, buf[CLIENT_HDR_SIZE..].to_vec())
}

fn ping_payload_with_pmtu(pmtu: u16) -> Vec<u8> {
    let mut payload = vec![0u8; 50];
    payload[20..22].copy_from_slice(&pmtu.to_le_bytes());
    payload[41] = 255; // RSQ
    payload
}

fn ping_payload_with_ack(ack_start: u64, ack_words: &[u64]) -> Vec<u8> {
    let mut payload = ping_payload_with_pmtu(508);
    payload[42..50].copy_from_slice(&ack_start.to_le_bytes());
    for word in ack_words {
        payload.extend_from_slice(&word.to_le_bytes());
    }
    payload
}

fn pending_h_item(msg_num: u64) -> SendItem {
    SendItem {
        data: vec![0x11],
        cmd: Command::UI.to_byte(),
        encrypted: true,
        priority: SendPriority::High,
        retry_left: 1,
        max_retries: 3,
        msg_num,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    }
}

fn sent_sliced_with_lengths(lengths: &[usize], last_checked: i64) -> SentSliced {
    SentSliced {
        datagram_num: 1,
        slices: lengths.iter().map(|len| vec![0xA5; *len]).collect(),
        piece_last_checked: vec![last_checked; lengths.len()],
        ack_flags: [0; 32],
        blocks_count: lengths.len(),
        sent_count: lengths.len(),
        last_checked,
        retry_count: 0,
        last_retry_inc: 0,
        max_retry_count: 6,
        u_key: UniqueKey::none(),
    }
}

fn writer(client: &mut Client) -> ProtocolCore<'_> {
    ProtocolCore { client }
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.Reset
fn full_reset_preserves_sending_and_api_slots() {
    let mut client = Client::new(dummy_cfg());
    client.sending.push(sent_sliced_with_lengths(&[8], 0));
    client.pending_h.push(pending_h_item(42));
    let _rx = client.pending_api.api_pending.register(0x4455);

    client.crypt_msg_counter.store(77, Ordering::Relaxed);
    client.metrics.total_sent.store(1234, Ordering::Relaxed);
    client.metrics.total_recv = 5678;
    client.rs = 0.25;
    client.used_sliced_limit = true;
    client.recv.recvd_slider.has_new_data = true;
    client.last_online = 999;
    client.last_sent_hello = 888;

    client.full_reset();

    assert_eq!(
        client.sending.len(),
        1,
        "Delphi Reset does not clear Sending"
    );
    assert_eq!(
        client.pending_h.len(),
        1,
        "Delphi Reset does not clear pending H commands"
    );
    assert_eq!(
        client.pending_api.api_pending.pending_count(),
        1,
        "API waiters are not part of Delphi TMoonProtoClient.Reset"
    );
    assert_eq!(client.crypt_msg_counter.load(Ordering::Relaxed), 0);
    assert_eq!(client.total_sent(), 0);
    assert_eq!(client.metrics.total_recv, 0);
    assert_eq!(client.rs, 1.0);
    assert!(!client.used_sliced_limit);
    assert!(!client.recv.recvd_slider.has_new_data);
    assert_eq!(client.last_online, 0);
    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
}

fn process_ping_reader_msg(
    client: &mut Client,
    payload: &[u8],
    raw_now_dt: f64,
    corrected_now_dt: f64,
) -> Vec<(Command, Vec<u8>)> {
    let recv_bytes = payload.len() as u64;
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            delivered_for_cb
                .lock()
                .unwrap()
                .push((cmd, payload.to_vec()));
        }),
    };
    let mut writer = writer(client);
    writer.apply_recv_side_effects(recv_bytes, 123);
    let total_sent = writer.client.metrics.total_sent.load(Ordering::Relaxed);
    writer
        .client
        .apply_ping_and_build_response(
            payload,
            raw_now_dt,
            corrected_now_dt,
            total_sent,
            recv_bytes,
        )
        .expect("valid ping payload");
    writer.client_new_data(
        Command::Ping.to_byte(),
        payload.to_vec(),
        false,
        false,
        123,
        &mut mode,
    );
    drop(mode);
    Arc::try_unwrap(delivered).unwrap().into_inner().unwrap()
}

#[test]
fn ping_pmtu_above_8192_is_preserved() {
    let mut client = Client::new(dummy_cfg());

    let delivered = process_ping_reader_msg(&mut client, &ping_payload_with_pmtu(8_224), 0.0, 0.0);

    assert_eq!(client.actual_pmtu(), 8_224);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, Command::Ping);
}

#[test]
fn ping_adaptive_can_send_rate_uses_delphi_used_limit_gate() {
    let mut client = Client::new(dummy_cfg());
    let payload = ping_payload_with_pmtu(508);

    client.can_send_rate = 2 * 1024 * 1024;
    client.used_sliced_limit = false;
    client
        .apply_ping_and_build_response(&payload, 0.0, 0.0, 0, 50)
        .expect("valid ping");
    assert_eq!(
        client.can_send_rate,
        2 * 1024 * 1024,
        "Delphi changes CanSendRate only after UsedSlicedLimit"
    );

    client.can_send_rate = 2 * 1024 * 1024;
    client.used_sliced_limit = true;
    client
        .apply_ping_and_build_response(&payload, 0.0, 0.0, 0, 50)
        .expect("valid ping");
    assert_eq!(
        client.can_send_rate, 2_160_067,
        "Delphi raises healthy channel by max(round(rate*0.03), 32KB/s)"
    );
    assert!(
        !client.used_sliced_limit,
        "Delphi clears UsedSlicedLimit after the adaptive update"
    );

    let mut congested = payload;
    congested[41] = 0;
    client.can_send_rate = 1_000_000;
    client.used_sliced_limit = true;
    client
        .apply_ping_and_build_response(&congested, 0.0, 0.0, 0, 50)
        .expect("valid ping");
    assert_eq!(
        client.can_send_rate, 850_000,
        "Delphi cuts congested channel by round(rate*0.85)"
    );
}

#[test]
fn ping_server_time_delta_uses_raw_now_not_ntp_corrected_now() {
    let mut client = Client::new(dummy_cfg());
    let raw_now: f64 = 45_000.0;
    let corrected_now: f64 = raw_now + 3600.0 / 86400.0;
    let initial_time: f64 = raw_now + 2.0 / 86400.0;
    let server_time: f64 = corrected_now + 3.0 / 86400.0;
    let mut payload = ping_payload_with_pmtu(508);
    payload[0..8].copy_from_slice(&server_time.to_le_bytes());
    payload[8..16].copy_from_slice(&initial_time.to_le_bytes());

    let delivered = process_ping_reader_msg(&mut client, &payload, raw_now, corrected_now);

    assert!(
        ((client.server_time_delta_days() * 86400.0) - 2.0).abs() < 0.001,
        "Delphi ClientNewData uses raw Now for ServerTimeDelta, not NTP-corrected SendPing time"
    );
    assert_eq!(client.net_lag_ping_ms(), 3000);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, Command::Ping);
}

#[test]
fn tiny_ping_pmtu_does_not_underflow_sliced_send() {
    let mut client = Client::new(dummy_cfg());
    process_ping_reader_msg(&mut client, &ping_payload_with_pmtu(18), 0.0, 0.0);
    assert_eq!(client.actual_pmtu(), 18);

    let item = SendItem {
        data: vec![1],
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Sliced,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).create_sliced_and_send(&item);
    assert!(client.sending.is_empty());
}

#[test]
fn ping_ack_does_not_drop_pending_h_until_writer_copy_apply() {
    let mut client = Client::new(dummy_cfg());
    client.pending_h.push(pending_h_item(42));

    // AckStart=40, bit 2 set => MsgNum 42 is ACKed by the server.
    process_ping_reader_msg(&mut client, &ping_payload_with_ack(40, &[1 << 2]), 0.0, 0.0);

    assert_eq!(
        client.pending_h.len(),
        1,
        "Delphi DataReadInt(MPC_Ping) writes TmpSlider only; PendingH is writer work"
    );
    assert!(client.send_lock.lock().unwrap().tmp_slider.has_new_data);
    assert!(!client.recv.recvd_slider.has_new_data);

    ProtocolCore {
        client: &mut client,
    }
    .copy_recvd_data();
    assert!(!client.send_lock.lock().unwrap().tmp_slider.has_new_data);
    assert!(client.recv.recvd_slider.has_new_data);

    ProtocolCore {
        client: &mut client,
    }
    .apply_regular_hl_ack();
    assert!(
        client.pending_h.is_empty(),
        "CheckSeningData/ApplyRegularHLAck must drop ACKed High packet"
    );
}

#[test]
fn ping_ack_reader_core_is_not_reapplied_by_main_ping_branch() {
    let mut client = Client::new(dummy_cfg());
    let payload = ping_payload_with_ack(40, &[1 << 2]);
    client
        .send_lock
        .lock()
        .unwrap()
        .apply_ping_ack_bitmap(&payload);
    ProtocolCore {
        client: &mut client,
    }
    .copy_recvd_data();
    assert!(!client.send_lock.lock().unwrap().tmp_slider.has_new_data);
    assert!(client.recv.recvd_slider.has_new_data);

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            delivered_for_cb
                .lock()
                .unwrap()
                .push((cmd, payload.to_vec()));
        }),
    };
    ProtocolCore {
        client: &mut client,
    }
    .client_new_data(
        Command::Ping.to_byte(),
        payload.clone(),
        false,
        false,
        123,
        &mut mode,
    );
    drop(mode);
    let delivered = Arc::try_unwrap(delivered).unwrap().into_inner().unwrap();

    assert!(
        !client.send_lock.lock().unwrap().tmp_slider.has_new_data,
        "main Ping branch must not write TmpSlider again after reader DataReadInt core"
    );
    assert_eq!(delivered.len(), 1);
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.DeleteSendingByKey
fn sliced_u_key_cleanup_does_not_drop_pending_h() {
    let mut client = Client::new(dummy_cfg());
    let key = UniqueKey::order_move(42);

    let mut old_sliced = sent_sliced_with_lengths(&[8], 0);
    old_sliced.u_key = key;
    client.sending.push(old_sliced);
    let mut second_old_sliced = sent_sliced_with_lengths(&[8], 0);
    second_old_sliced.u_key = key;
    client.sending.push(second_old_sliced);

    let mut pending_h = pending_h_item(10);
    pending_h.u_key = key;
    client.pending_h.push(pending_h);
    let mut second_pending_h = pending_h_item(11);
    second_pending_h.u_key = key;
    client.pending_h.push(second_pending_h);

    let new_sliced = SendItem {
        data: vec![0x22],
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Sliced,
        retry_left: 0,
        max_retries: 6,
        msg_num: 0,
        last_sent_at: 0,
        u_key: key,
    };

    ProtocolCore {
        client: &mut client,
    }
    .apply_sliced_send_u_key_cleanup(&[new_sliced]);

    assert_eq!(
        client.sending.len(),
        1,
        "Delphi DeleteSendingByKey removes only the first matching Sliced entry"
    );
    assert_eq!(
        client.pending_h.len(),
        2,
        "Delphi DeleteSendingByKey must not remove PendingH entries"
    );

    let new_high = SendItem {
        data: vec![0x33],
        cmd: Command::UI.to_byte(),
        encrypted: true,
        priority: SendPriority::High,
        retry_left: 1,
        max_retries: 3,
        msg_num: 0,
        last_sent_at: 0,
        u_key: key,
    };

    ProtocolCore {
        client: &mut client,
    }
    .apply_high_send_u_key_cleanup(&[new_high]);

    assert_eq!(
        client.pending_h.len(),
        1,
        "Delphi DeletePendingByKey removes only the first matching PendingH entry"
    );
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.ApplyRegularHLAck
fn high_u_key_cleanup_runs_after_regular_ack() {
    let mut client = Client::new(dummy_cfg());
    let key = UniqueKey::order_move(42);

    let mut acked_same_key = pending_h_item(42);
    acked_same_key.u_key = key;
    client.pending_h.push(acked_same_key);
    let mut not_acked_same_key = pending_h_item(43);
    not_acked_same_key.u_key = key;
    client.pending_h.push(not_acked_same_key);

    {
        client.recv.recvd_slider.start_num = 40;
        client.recv.recvd_slider.bit_field[0] = 1 << 2;
        client.recv.recvd_slider.has_new_data = true;
        client.recv.recvd_slider.r_count = 1;
    }

    let new_high = SendItem {
        data: vec![0x33],
        cmd: Command::UI.to_byte(),
        encrypted: true,
        priority: SendPriority::High,
        retry_left: 1,
        max_retries: 3,
        msg_num: 0,
        last_sent_at: 0,
        u_key: key,
    };

    ProtocolCore {
        client: &mut client,
    }
    .apply_regular_hl_ack();
    assert_eq!(
        client.pending_h.len(),
        1,
        "Delphi ApplyRegularHLAck runs before CopySendListH DeletePendingByKey"
    );
    ProtocolCore {
        client: &mut client,
    }
    .apply_high_send_u_key_cleanup(&[new_high]);
    assert!(
        client.pending_h.is_empty(),
        "then Delphi DeletePendingByKey removes the first remaining same-key High entry"
    );
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.CreateSlicedObject
fn create_sliced_object_queues_without_immediate_send() {
    let mut client = Client::new(dummy_cfg());
    let item = SendItem {
        data: vec![0x11, 0x22, 0x33],
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Sliced,
        retry_left: 0,
        max_retries: 5,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).create_sliced_and_send(&item);

    assert_eq!(client.sending.len(), 1);
    assert_eq!(client.sending[0].sent_count, 0);
    assert_eq!(client.sending[0].last_checked, 0);
    assert!(client.sending[0]
        .piece_last_checked
        .iter()
        .all(|&last_checked| last_checked == 0));
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.CreateSlicedObject
fn sliced_size_check_uses_compressed_size() {
    let mut client = Client::new(dummy_cfg());
    let item = SendItem {
        data: (0..130_000).map(|i| (i % 4) as u8).collect(),
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Sliced,
        retry_left: 0,
        max_retries: 5,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).create_sliced_and_send(&item);

    assert_eq!(
        client.sending.len(),
        1,
        "Delphi compresses TMoonProtoDataToSend before CreateSlicedObject size check"
    );
    assert_eq!(
        client.sending[0].slices[0][4],
        Command::UI.to_byte() | COMPRESSED_FLAG
    );
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.CreateSlicedObject
fn encrypted_empty_sliced_is_dropped_before_crypt() {
    let mut client = Client::new(dummy_cfg());
    client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));
    let item = SendItem {
        data: Vec::new(),
        cmd: Command::UI.to_byte(),
        encrypted: true,
        priority: SendPriority::Sliced,
        retry_left: 1,
        max_retries: 5,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).create_sliced_and_send(&item);

    assert!(
        client.sending.is_empty(),
        "Delphi CreateSlicedObject drops empty data.ms before Crypt(data)"
    );
}

#[test]
fn encrypted_low_batch_size_uses_wire_size_after_crypt() {
    let mut client = Client::new(dummy_cfg());
    client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));

    let item = SendItem {
        data: vec![0xA5; 10],
        cmd: Command::UI.to_byte(),
        encrypted: true,
        priority: SendPriority::Low,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).batch_send_direct(&item);

    let wire_len = u16::from_le_bytes([client.tmp_send_buf[1], client.tmp_send_buf[2]]) as usize;
    assert_eq!(client.tmp_send_buf[0], Command::Crypted.to_byte());
    assert_eq!(wire_len, 50);
    assert_eq!(client.tmp_send_buf.len(), 3 + wire_len);
    assert_eq!(client.tmp_send_size, 15 + 3 + wire_len);
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:MaxSlicedDataSize
fn encrypted_sliced_max_size_counts_full_gcm_overhead() {
    let mut client = Client::new(dummy_cfg());
    client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));
    client.actual_pmtu = 100;

    let pmtu = usize::from(client.actual_pmtu) - 15 - 4;
    let max_sliced_data_size = pmtu * 256
        - crate::protocol::crypted::CRYPTO_HEADER_SIZE
        - crate::crypto::IV_SIZE
        - crate::crypto::GCM_TAG_SIZE
        - 1;

    let mut accepted = SendItem {
        data: vec![0xA5; max_sliced_data_size - 1],
        cmd: Command::UI.to_byte() | COMPRESSED_FLAG,
        encrypted: true,
        priority: SendPriority::Sliced,
        retry_left: 1,
        max_retries: 5,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).create_sliced_and_send(&accepted);
    assert_eq!(
        client.sending[0].blocks_count, 256,
        "max-1 payload still fits exactly into byte MaxBlockNum range"
    );

    client.sending.clear();
    accepted.data.push(0xA5);
    writer(&mut client).create_sliced_and_send(&accepted);
    assert!(
        client.sending.is_empty(),
        "payload at MaxSlicedDataSize is dropped before encrypted 257th slice"
    );
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.DoSendMPData
fn do_send_mp_data_sends_oversized_item_direct() {
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();
    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();

    let mut cfg = dummy_cfg();
    cfg.server_port = server_addr.port();
    let mut client = Client::new(cfg);
    client.transport.socket = Some(client_sock);
    client.actual_pmtu = 100;

    let small = SendItem {
        data: vec![0x11; 10], // Delphi sz = 10 + header(15) + item hdr(3) = 28
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Low,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };
    let large = SendItem {
        data: vec![0x22; 80], // sz = 98; 28 + 98 > PMTU and 28 > 98 is false
        cmd: Command::API.to_byte(),
        encrypted: false,
        priority: SendPriority::Low,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).batch_send_direct(&small);
    writer(&mut client).batch_send_direct(&large);

    let mut raw = [0u8; 256];
    let (n, _) = server_sock.recv_from(&mut raw).unwrap();
    let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
    assert_eq!(
        cmd,
        Command::API.to_byte(),
        "Delphi DoSendMPData sends the current oversized item directly and keeps the older buffer"
    );
    assert_eq!(payload, large.data);
    assert_eq!(client.tmp_send_count, 1);
    assert_eq!(client.tmp_send_buf[0], Command::UI.to_byte());
    assert_eq!(
        u16::from_le_bytes([client.tmp_send_buf[1], client.tmp_send_buf[2]]) as usize,
        small.data.len()
    );
    assert_eq!(&client.tmp_send_buf[3..], small.data.as_slice());

    writer(&mut client).flush_send_batch();
    let (n, _) = server_sock.recv_from(&mut raw).unwrap();
    let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
    assert_eq!(cmd, Command::UI.to_byte());
    assert_eq!(payload, small.data);
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.CheckSeningData
fn low_priority_items_are_split_around_sliced_retry() {
    let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    server_sock
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let server_addr = server_sock.local_addr().unwrap();
    let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();

    let mut cfg = dummy_cfg();
    cfg.server_port = server_addr.port();
    let mut client = Client::new(cfg);
    client.transport.socket = Some(client_sock);
    client.actual_pmtu = 508;
    client.round_trip_delay = 0;
    client.trip_delay_k = 1.1;
    client.can_send_rate = 1_000_000;
    client.sending.push(sent_sliced_with_lengths(&[8], 0));

    let first_low = SendItem {
        data: vec![0x11],
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Low,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };
    let second_low = SendItem {
        data: vec![0x22],
        cmd: Command::API.to_byte(),
        encrypted: false,
        priority: SendPriority::Low,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };
    let l_items = vec![first_low.clone(), second_low.clone()];

    writer(&mut client).send_low_items_around_sliced_retry(&l_items, 1000);

    let mut raw = [0u8; 256];
    let (n, _) = server_sock.recv_from(&mut raw).unwrap();
    let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
    assert_eq!(cmd, Command::UI.to_byte());
    assert_eq!(payload, first_low.data);

    let (n, _) = server_sock.recv_from(&mut raw).unwrap();
    let (cmd, _payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
    assert_eq!(
        cmd,
        Command::Sliced.to_byte(),
        "Delphi retries Sliced after only the first Low item was flushed"
    );

    let (n, _) = server_sock.recv_from(&mut raw).unwrap();
    let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
    assert_eq!(cmd, Command::API.to_byte());
    assert_eq!(payload, second_low.data);
}

#[test]
fn encrypted_low_batch_preserves_outer_compressed_flag() {
    let mut client = Client::new(dummy_cfg());
    client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));

    let item = SendItem {
        data: vec![0xA5; 10],
        cmd: Command::UI.to_byte() | COMPRESSED_FLAG,
        encrypted: true,
        priority: SendPriority::Low,
        retry_left: 0,
        max_retries: 0,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).batch_send_direct(&item);

    assert_eq!(
        client.tmp_send_buf[0],
        Command::Crypted.to_byte() | COMPRESSED_FLAG
    );
}

#[test]
fn encrypted_high_send_preserves_outer_compressed_flag() {
    let mut client = Client::new(dummy_cfg());
    client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));

    let mut item = SendItem {
        data: vec![0xA5; 10],
        cmd: Command::UI.to_byte() | COMPRESSED_FLAG,
        encrypted: true,
        priority: SendPriority::High,
        retry_left: 1,
        max_retries: 2,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::none(),
    };

    writer(&mut client).send_h_item(&mut item, 123);

    assert_eq!(
        client.tmp_send_buf[0],
        Command::Crypted.to_byte() | COMPRESSED_FLAG
    );
    assert_eq!(client.pending_h.len(), 1);
    assert_eq!(
        client.pending_h[0].cmd,
        Command::UI.to_byte() | COMPRESSED_FLAG
    );
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.CheckSeningData
fn sliced_retry_client_limit_is_rounded() {
    let mut client = Client::new(dummy_cfg());
    client.round_trip_delay = 100;
    client.trip_delay_k = 1.1;
    client.actual_sleep_time = 5.0;
    client.can_send_rate = 262_120; // 262120 * 5ms / 1000 = 1310.6 -> 1311
    client.sending.push(sent_sliced_with_lengths(&[1310, 1], 0));

    writer(&mut client).retry_sliced(1000);

    assert_eq!(client.sending[0].sent_count, 4);
}

#[test]
fn sliced_retry_start_budget_sends_delphi_full_slice_counts() {
    for (cycle_time_ms, expected_sends) in [(5.0, 8), (10.0, 15), (15.0, 22)] {
        let mut client = Client::new(dummy_cfg());
        client.round_trip_delay = 100;
        client.trip_delay_k = 1.1;
        client.actual_sleep_time = cycle_time_ms;
        client.can_send_rate = 2 * 1024 * 1024;
        client
            .sending
            .push(sent_sliced_with_lengths(&vec![1442; 64], 0));

        writer(&mut client).retry_sliced(1000);

        assert_eq!(
            client.sending[0].sent_count,
            64 + expected_sends,
            "Delphi checks BytesSentAtOnce < ClientLimit before sending the next slice"
        );
        assert_eq!(
            client.sending[0].retry_count, 0,
            "primary timestamp groups must not burn Sliced retry budget"
        );
        assert!(
            client.used_sliced_limit,
            "Delphi marks UsedSlicedLimit when the tick reaches 80% of ClientLimit"
        );
    }
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.CheckSeningData
fn sliced_retry_used_limit_threshold_is_rounded() {
    let mut client = Client::new(dummy_cfg());
    client.round_trip_delay = 100;
    client.trip_delay_k = 1.1;
    client.actual_sleep_time = 5.0;
    client.can_send_rate = 262_120; // ClientLimit = 1311, 80% threshold = round(1048.8) = 1049
    client.sending.push(sent_sliced_with_lengths(&[1048], 0));

    writer(&mut client).retry_sliced(1000);

    assert!(!client.used_sliced_limit);
}

#[test]
fn sliced_retry_uses_delphi_last_checked_slices_outer_gate() {
    let mut client = Client::new(dummy_cfg());
    client.round_trip_delay = 100;
    client.trip_delay_k = 1.1; // PathDelay = round(100 * 1.1 + 10) = 120
    client.actual_sleep_time = 5.0;
    client.can_send_rate = 1_000_000;
    client.last_checked_slices = 1000;
    client.sending.push(sent_sliced_with_lengths(&[10], 1000));

    writer(&mut client).retry_sliced(1105);
    assert_eq!(
        client.sending[0].sent_count, 1,
        "Delphi outer gate may run before PathDelay and sends nothing"
    );
    assert_eq!(
        client.last_checked_slices, 1105,
        "Delphi still writes LastCheckedSlices := CurTm on that empty pass"
    );

    writer(&mut client).retry_sliced(1126);
    assert_eq!(
        client.sending[0].sent_count, 1,
        "after the empty pass Delphi waits another RoundTripDelay before retry"
    );

    writer(&mut client).retry_sliced(1206);
    assert_eq!(client.sending[0].sent_count, 2);
}

#[test]
fn sliced_retry_uses_startup_floor_until_ping_reports_rtt() {
    let mut client = Client::new(dummy_cfg());
    client.round_trip_delay = 0;
    client.trip_delay_k = 1.1;
    client.last_set_trip_k = 1000;
    client.actual_sleep_time = 5.0;
    client.can_send_rate = 1_000_000;
    client.sending.push(sent_sliced_with_lengths(&[10], 0));

    writer(&mut client).retry_sliced(1000);
    assert_eq!(client.sending[0].sent_count, 2);

    writer(&mut client).retry_sliced(1010);
    assert_eq!(
        client.sending[0].sent_count, 2,
        "unknown RTT must not burn Sliced retries on a 10ms local tick"
    );

    writer(&mut client).retry_sliced(1231);
    assert_eq!(
        client.sending[0].sent_count, 3,
        "retry uses the startup RTT floor until Ping supplies a real RTT"
    );
    assert_eq!(client.sending[0].retry_count, 1);
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.CheckSeningData
fn sliced_retry_updates_trip_delay_k_before_path_delay() {
    let mut client = Client::new(dummy_cfg());
    client.round_trip_delay = 1000;
    client.trip_delay_k = 1.1;
    client.avg_dup_count = 10.0;
    client.last_set_trip_k = 0;
    client.last_checked_slices = 0;
    client.actual_sleep_time = 5.0;
    client.can_send_rate = 1_000_000;
    client.sending.push(sent_sliced_with_lengths(&[10], 1360));

    writer(&mut client).retry_sliced(2500);

    assert!((client.trip_delay_k - 1.15).abs() < 1e-12);
    assert_eq!(
        client.sending[0].sent_count, 1,
        "Delphi raises TripDelayK before PathDelay; this tick is not due yet with the new K"
    );
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoSlicedData.ApplyACK
fn sliced_retry_clock_ignores_acked_blocks() {
    let mut client = Client::new(dummy_cfg());
    client.round_trip_delay = 100;
    client.trip_delay_k = 1.1;
    client.actual_sleep_time = 5.0;
    client.can_send_rate = 1_000_000;
    let mut sliced = sent_sliced_with_lengths(&[10, 10, 10], 100);
    sliced.retry_count = 5;
    client.sending.push(sliced);

    let mut ack = [0u8; 34];
    ack[0] = 0b0000_0011; // blocks 0 and 1 ACKed; block 2 still pending.
    ack[32..34].copy_from_slice(&1u16.to_le_bytes());
    client.on_new_sliced_ack(&ack);
    {
        let mut writer = ProtocolCore {
            client: &mut client,
        };
        let mut copy_acks = writer.get_copy_acks();
        writer.apply_copy_acks(&mut copy_acks, 300);
    }
    assert_eq!(
        client.sending[0].retry_count, 0,
        "Delphi TMoonProtoSlicedData.ApplyACK resets FRetryCount when ACK adds new bits"
    );
    assert_eq!(
        client.sending[0].last_checked, 100,
        "current Delphi ApplyACK preserves retry clocks of remaining holes"
    );
    assert_eq!(
        client.sending[0].piece_last_checked[2], 100,
        "current Delphi ApplyACK preserves LastChecked for remaining unACKed pieces"
    );

    client.sending[0].retry_count = 4;
    client.on_new_sliced_ack(&ack);
    {
        let mut writer = ProtocolCore {
            client: &mut client,
        };
        let mut copy_acks = writer.get_copy_acks();
        writer.apply_copy_acks(&mut copy_acks, 300);
    }
    assert_eq!(
        client.sending[0].retry_count, 4,
        "duplicate ACK without progress must be a no-op like Delphi ACK.ApplyACK=false"
    );
    assert_eq!(
        client.sending[0].piece_last_checked[2], 100,
        "duplicate ACK must not rebuild or reset the remaining retry group"
    );

    writer(&mut client).retry_sliced(300);
    assert_eq!(client.sending[0].sent_count, 4);
    assert_eq!(client.sending[0].piece_last_checked[2], 300);
    assert_eq!(client.sending[0].last_checked, 300);

    writer(&mut client).retry_sliced(500);
    assert_eq!(
        client.sending[0].sent_count, 5,
        "unACKed block must be retried again; ACKed old blocks must not pin LastChecked"
    );
    assert_eq!(client.sending[0].piece_last_checked[2], 500);
    assert_eq!(client.sending[0].last_checked, 500);
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.CheckSeningData
fn sliced_ack_applies_only_first_matching_datagram() {
    let mut client = Client::new(dummy_cfg());

    let mut first = sent_sliced_with_lengths(&[10], 100);
    first.datagram_num = 7;
    let mut second = sent_sliced_with_lengths(&[10, 10], 100);
    second.datagram_num = 7;
    client.sending.push(first);
    client.sending.push(second);

    let mut ack = [0u8; 34];
    ack[0] = 0b0000_0001; // complete for first datagram, partial for second if wrongly applied.
    ack[32..34].copy_from_slice(&7u16.to_le_bytes());

    client.on_new_sliced_ack(&ack);
    {
        let mut writer = ProtocolCore {
            client: &mut client,
        };
        let mut copy_acks = writer.get_copy_acks();
        writer.apply_copy_acks(&mut copy_acks, 100);
    }

    assert_eq!(client.sending.len(), 1);
    assert_eq!(client.sending[0].blocks_count, 2);
    assert_eq!(
            client.sending[0].ack_flags[0], 0,
            "Delphi breaks after the first matching Sending item; a wrapped DatagramNum ACK must not mutate the next item"
        );
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.OnNewSlicedACK
fn sliced_ack_reader_queues_writer_applies() {
    let mut client = Client::new(dummy_cfg());
    client.sending.push(sent_sliced_with_lengths(&[10], 100));

    let mut ack = [0u8; 34];
    ack[0] = 0b0000_0001;
    ack[32..34].copy_from_slice(&1u16.to_le_bytes());

    client.on_new_sliced_ack(&ack);
    assert_eq!(
        client.sending.len(),
        1,
        "Delphi OnNewSlicedACK only queues ACKs; ApplyACK is writer/CheckSeningData work"
    );

    {
        let mut writer = ProtocolCore {
            client: &mut client,
        };
        let mut copy_acks = writer.get_copy_acks();
        writer.apply_copy_acks(&mut copy_acks, 200);
    }
    assert!(
        client.sending.is_empty(),
        "writer copy/apply phase must remove completed sliced datagram"
    );
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.CheckSeningData
fn writer_tick_copies_ack_queues_then_check_sening_data() {
    let mut client = Client::new(dummy_cfg());
    client.sending.push(sent_sliced_with_lengths(&[10], 100));
    client.pending_h.push(pending_h_item(42));
    client
        .send_lock
        .lock()
        .unwrap()
        .apply_ping_ack_bitmap(&ping_payload_with_ack(40, &[1 << 2]));

    let mut ack = [0u8; 34];
    ack[0] = 0b0000_0001;
    ack[32..34].copy_from_slice(&1u16.to_le_bytes());
    client.on_new_sliced_ack(&ack);

    ProtocolCore {
        client: &mut client,
    }
    .copy_send_ack_and_check_sening_data(200);

    assert!(
        client.sending.is_empty(),
        "writer tick must apply queued SlicedACK inside CheckSeningData"
    );
    assert!(
        client.pending_h.is_empty(),
        "writer tick must CopyRecvdData then ApplyRegularHLAck inside CheckSeningData"
    );
    assert!(
        ProtocolCore {
            client: &mut client,
        }
        .get_copy_acks()
        .is_empty(),
        "GetCopyAcks must clear reader-to-writer ACK queue before CheckSeningData"
    );
    assert!(
        !client.send_lock.lock().unwrap().tmp_slider.has_new_data,
        "CopyRecvdData must clear TmpSlider after snapshot"
    );
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.GetCopyAcks
fn send_lock_snapshot_copies_send_acks_and_tmp_slider_atomically() {
    let mut client = Client::new(dummy_cfg());
    let send_item = SendItem {
        data: vec![0x44],
        cmd: Command::UI.to_byte(),
        encrypted: false,
        priority: SendPriority::Sliced,
        retry_left: 0,
        max_retries: 6,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey::base_ui_settings_slot(),
    };
    let mut ack = [0u8; 34];
    ack[0] = 1;
    ack[32..34].copy_from_slice(&9u16.to_le_bytes());

    {
        let mut send_lock = client.send_lock.lock().unwrap();
        send_lock.push_send_cmd_int(send_item);
        send_lock.push_sliced_ack(Client::parse_sliced_ack_payload(&ack).unwrap());
        send_lock.apply_ping_ack_bitmap(&ping_payload_with_ack(40, &[1 << 2]));
    }

    let mut sliced = Vec::new();
    let mut high = Vec::new();
    let mut low = Vec::new();
    let mut acks = Vec::new();
    ProtocolCore {
        client: &mut client,
    }
    .get_copy_send_lock_snapshot(&mut sliced, &mut high, &mut low, &mut acks);

    assert_eq!(sliced.len(), 1);
    assert!(high.is_empty());
    assert!(low.is_empty());
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].datagram_num, 9);
    assert!(client.recv.recvd_slider.has_new_data);
    let send_lock = client.send_lock.lock().unwrap();
    assert!(send_lock.send_queues.is_empty());
    assert!(send_lock.incoming_sliced_acks.is_empty());
    assert!(
        !send_lock.tmp_slider.has_new_data,
        "Delphi FClient.CopyRecvdData clears TmpSlider in the same SendLock snapshot"
    );
}
