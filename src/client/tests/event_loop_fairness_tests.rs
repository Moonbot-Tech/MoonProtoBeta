use super::*;
use crate::events::EventDispatcher;
use crate::transport::{outer_light_crypt, MacContext, ServerMsgHeader, TRANSPORT_VER};

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

fn send_server_packet_to_client_socket(client: &Client, cmd: Command, payload: &[u8]) {
    let addr = client
        .transport
        .socket
        .as_ref()
        .expect("client socket")
        .local_addr()
        .expect("client socket addr");
    let server = UdpSocket::bind("127.0.0.1:0").expect("test server socket");
    let packet = pack_server_packet(&client.cfg.mac_key, cmd, payload);
    server.send_to(&packet, addr).expect("send test datagram");
}

#[test]
fn send_phase_runs_with_ready_send_queue() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    let mut dispatcher = EventDispatcher::new();

    client.send_cmd(
        vec![1, 2, 3, 4],
        Command::UI,
        SendPriority::Sliced,
        false,
        6,
    );

    let total_sent_before = client.total_sent();
    client.run_dispatcher_steps_for_test(1, &mut dispatcher);

    assert!(
        client.send_lock.lock().unwrap().is_empty(),
        "writer must copy direct Delphi-style send queues without app-event bridge"
    );
    assert!(
        !client.sending.is_empty(),
        "Sliced item with retry budget must remain in Sending until ACK or retry exhaustion"
    );
    assert!(
        client.total_sent() > total_sent_before,
        "writer tick must send from copied queue"
    );
}

#[test]
fn pre_init_raw_client_send_cmd_is_gated_but_init_api_is_allowed() {
    let client = Client::new(dummy_cfg());

    client.send_cmd(vec![1, 2, 3, 4], Command::UI, SendPriority::High, true, 3);
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty());
    assert!(high.is_empty());
    assert!(low.is_empty());

    client.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
        false,
    ));
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty());
    assert!(high.is_empty());
    assert!(low.is_empty());

    let base_check = crate::commands::engine_request::base_check();
    client.send_api_request(&base_check);
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert_eq!(sliced.len(), 1);
    assert_eq!(sliced[0].data, base_check);
    assert_eq!(sliced[0].cmd, Command::API.to_byte());
    assert!(high.is_empty());
    assert!(low.is_empty());
}

#[test]
fn pre_init_async_api_does_not_register_pending_for_gated_methods() {
    let client = Client::new(dummy_cfg());
    let subscribe = crate::commands::engine_request::subscribe_all_trades(false);

    let rx = client.send_api_request_async(&subscribe);

    assert_eq!(client.api_pending.pending_count(), 0);
    assert!(rx.recv_timeout(Duration::from_millis(1)).is_err());
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty());
    assert!(high.is_empty());
    assert!(low.is_empty());
}

#[test]
fn raw_run_delivers_callback_on_app_thread() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.prev_auth_status = AuthStatus::AuthDone;
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    send_server_packet_to_client_socket(&client, Command::OrderBook, &[0xAA]);

    let caller_thread = thread::current().id();
    let (tx, rx) = mpsc::channel();
    client.run(
        Duration::from_millis(5),
        Box::new(move |cmd, payload| {
            assert_eq!(cmd, Command::OrderBook);
            assert_eq!(payload, &[0xAA]);
            tx.send(thread::current().id()).unwrap();
        }),
    );

    let writer_thread = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("writer callback thread id");
    assert_ne!(
        writer_thread, caller_thread,
        "app callback must not run on the caller thread"
    );
}

#[test]
fn raw_run_callback_block_does_not_extend_protocol_writer_tick() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    send_server_packet_to_client_socket(&client, Command::OrderBook, &[0xAA]);

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        client.run(
            Duration::from_millis(20),
            Box::new(move |cmd, payload| {
                assert_eq!(cmd, Command::OrderBook);
                assert_eq!(payload, &[0xAA]);
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        );
        client
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("raw callback started");
    thread::sleep(Duration::from_millis(80));
    release_tx.send(()).unwrap();

    let client = handle.join().expect("client run thread");
    let snapshot = client.protocol_metrics_snapshot();
    assert!(
        snapshot.writer_tick_max_ns < 50_000_000,
        "blocking raw app callback leaked into protocol tick: max={}ns",
        snapshot.writer_tick_max_ns
    );
}

#[test]
fn lifecycle_callback_block_does_not_extend_protocol_writer_tick() {
    let mut client = Client::new(dummy_cfg());
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    client.auth_status = AuthStatus::Connected;
    client.prev_auth_status = AuthStatus::Base;

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    client.on_lifecycle(Box::new(move |event| {
        assert_eq!(event, LifecycleEvent::Connecting);
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    }));

    let handle = thread::spawn(move || {
        let mut dispatcher = EventDispatcher::new();
        client.run_dispatcher_steps_for_test(1, &mut dispatcher);
        client
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("lifecycle callback started");
    thread::sleep(Duration::from_millis(80));
    release_tx.send(()).unwrap();

    let client = handle.join().expect("client run thread");
    let snapshot = client.protocol_metrics_snapshot();
    assert!(
        snapshot.writer_tick_max_ns < 50_000_000,
        "blocking lifecycle app callback leaked into protocol tick: max={}ns",
        snapshot.writer_tick_max_ns
    );
}

#[test]
fn app_send_queue_is_not_blocked_by_data_read_delivery() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    let mut dispatcher = EventDispatcher::new();

    send_server_packet_to_client_socket(&client, Command::OrderBook, &[0xAA]);
    client.send_cmd(
        vec![1, 2, 3, 4],
        Command::UI,
        SendPriority::Sliced,
        false,
        0,
    );

    client.run_dispatcher_steps_for_test(1, &mut dispatcher);

    assert!(
        !client.sending.is_empty(),
        "app/user sends must use the separate outgoing queue, not wait behind pending reader work"
    );
}

#[test]
fn production_protocol_step_does_not_drain_udp_until_empty() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.prev_auth_status = AuthStatus::AuthDone;
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    client.register_recv_poller();

    send_server_packet_to_client_socket(&client, Command::OrderBook, &[0xAA]);
    send_server_packet_to_client_socket(&client, Command::OrderBook, &[0xBB]);

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_cb = Arc::clone(&events);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            events_cb.lock().unwrap().push((cmd, payload.to_vec()));
        }),
    };

    assert!((ProtocolCore {
        client: &mut client
    })
    .run_step(&mut mode));
    drop(mode);

    let events = Arc::try_unwrap(events).unwrap().into_inner().unwrap();
    assert_eq!(
        events.len(),
        1,
        "production run_step must process a bounded receive unit before returning to runtime publish/command work"
    );
    assert_eq!(events[0], (Command::OrderBook, vec![0xAA]));
}

#[test]
fn err_emu_drop_updates_valid_packet_stats_before_protocol_drop() {
    let mut client = Client::new(dummy_cfg());
    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |_, _| {
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    };
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::OrderBook.to_byte(),
        &[],
        1234,
        777,
        true,
        None,
        &mut mode,
    );

    assert!(client.connected);
    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert_eq!(client.metrics.total_recv, 1234);
    assert_eq!(client.last_online, 777);
    assert_eq!(
        delivered.load(Ordering::Relaxed),
        0,
        "ErrEmu drop must happen after Delphi stats side effects but before protocol delivery"
    );
}

#[test]
fn pre_init_domain_pushes_are_dropped_before_callback_delivery() {
    let mut client = Client::new(dummy_cfg());
    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |_, _| {
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    };

    for (idx, cmd) in [
        Command::Order,
        Command::Strat,
        Command::Balance,
        Command::TradesStream,
        Command::TradesResendResponse,
        Command::OrderBook,
        Command::UI,
    ]
    .into_iter()
    .enumerate()
    {
        ProtocolCore {
            client: &mut client,
        }
        .data_read_int_inline(
            cmd.to_byte(),
            &[idx as u8],
            10 + idx as u64,
            100 + idx as i64,
            true,
            None,
            &mut mode,
        );
    }

    assert_eq!(
        delivered.load(Ordering::Relaxed),
        0,
        "Delphi ClientNewData drops domain pushes before InitDone/domain_ready"
    );
    assert_eq!(
        client.metrics.total_recv, 91,
        "transport receive side effects still happen before the domain gate"
    );
}

#[test]
fn post_init_trades_stream_requires_explicit_subscription_intent() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            assert_eq!(cmd, Command::TradesStream);
            assert_eq!(payload, &[0xAA]);
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    };
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::TradesStream.to_byte(),
        &[0xAA],
        1,
        1,
        false,
        None,
        &mut mode,
    );
    assert_eq!(
        delivered.load(Ordering::Relaxed),
        0,
        "optional-trades deviation: no API subscription means incoming trades are dropped"
    );

    client.subscribe_all_trades(false);
    let _ = client.take_send_queues_for_test();
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::TradesStream.to_byte(),
        &[0xAA],
        1,
        2,
        false,
        None,
        &mut mode,
    );

    assert_eq!(delivered.load(Ordering::Relaxed), 1);
}

#[test]
fn data_read_sliced_payload_bypasses_recv_event_backlog() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);

    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            assert_eq!(cmd, Command::OrderBook);
            assert_eq!(payload, &[0xAA, 0xBB]);
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    };
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::OrderBook.to_byte(),
        &[0xAA, 0xBB],
        321,
        123,
        true,
        Some(ReaderSlicedStats {
            datagram_num: 1,
            dup_count: 1,
            blocks_count: 4,
        }),
        &mut mode,
    );

    assert_eq!(delivered.load(Ordering::Relaxed), 1);
    assert_eq!(client.avg_dup_count, 25.0);
    assert_eq!(client.metrics.total_recv, 321);
    assert_eq!(client.last_online, 123);
}

#[test]
fn data_read_grouped_payload_applies_recv_effects_once() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    let mut grouped = Vec::new();
    // Non-sensitive sub-commands: S1 drops plaintext sensitive cmds even inside a
    // group, so the grouped-delivery test uses non-sensitive commands.
    grouped.push(Command::OrderBook.to_byte());
    grouped.extend_from_slice(&1u16.to_le_bytes());
    grouped.push(0xAA);
    grouped.push(Command::Data.to_byte());
    grouped.extend_from_slice(&1u16.to_le_bytes());
    grouped.push(0xBB);

    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            match delivered_cb.load(Ordering::Relaxed) {
                0 => {
                    assert_eq!(cmd, Command::OrderBook);
                    assert_eq!(payload, &[0xAA]);
                }
                1 => {
                    assert_eq!(cmd, Command::Data);
                    assert_eq!(payload, &[0xBB]);
                }
                _ => panic!("unexpected extra grouped payload"),
            }
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    };

    ProtocolCore {
        client: &mut client,
    }
    .data_read_inline(
        Command::Grouped.to_byte(),
        &grouped,
        77,
        456,
        true,
        &mut mode,
    );

    assert_eq!(delivered.load(Ordering::Relaxed), 2);
    assert_eq!(client.metrics.total_recv, 77);
    assert_eq!(client.last_online, 456);
}

#[test]
fn s1_drops_plaintext_sensitive_commands_but_delivers_non_sensitive() {
    // S1 (эталон MoonProtoCommon.pas DataReadInt): a non-crypted command in
    // MoonProtoSensitiveCmds (Order/UI/Strat/Balance — and API on the server)
    // must be dropped before it reaches the application. The transport MAC only
    // proves authenticity against a known PSK; without this gate a peer that
    // never completed the AES-GCM handshake could inject order/strat/balance
    // state in plaintext. Non-sensitive commands pass through, and the same
    // commands arriving Crypted are delivered (handshake/crypted paths cover that).
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);

    let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            // Only the non-sensitive OrderBook fed below may ever reach the sink;
            // every plaintext sensitive command must have been dropped earlier.
            assert_eq!(cmd, Command::OrderBook);
            assert_eq!(payload, &[0xAA]);
            delivered_cb.fetch_add(1, Ordering::Relaxed);
        }),
    };

    for sensitive in [
        Command::Order,
        Command::UI,
        Command::Strat,
        Command::Balance,
    ] {
        ProtocolCore {
            client: &mut client,
        }
        .data_read_int_inline(
            sensitive.to_byte(),
            &[0x01, 0x02],
            0,
            0,
            false,
            None,
            &mut mode,
        );
    }
    assert_eq!(
        delivered.load(Ordering::Relaxed),
        0,
        "plaintext sensitive commands must be dropped by the S1 gate"
    );

    // A non-sensitive command on the same plaintext path is delivered as usual.
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::OrderBook.to_byte(),
        &[0xAA],
        0,
        0,
        false,
        None,
        &mut mode,
    );
    assert_eq!(delivered.load(Ordering::Relaxed), 1);
}
