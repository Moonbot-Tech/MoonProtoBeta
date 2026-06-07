use super::*;
use crate::events::EventDispatcher;
use crate::transport::{outer_light_crypt, MacContext, ServerMsgHeader, TRANSPORT_VER};

fn dummy_cfg() -> ClientConfig {
    ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        transport_mode: TransportMode::V0,
        client_id: 0,
        ntp_host: None,
        refresh: RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        },
        market_history: crate::state::MarketHistorySizing::default(),
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
    outer_light_crypt(&mut buf, MacContext::new(mac_key).obf_key());
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

fn install_send_session(client: &mut Client) {
    client.server_token = 1;
    client.session_rnd = client.handshake_rnd;
    let (encode_key, decode_key) = crate::crypto::generate_session_sub_keys(
        &client.cfg.master_key,
        client.cfg.client_id,
        1,
        &client.session_rnd,
    );
    client.encode_key = encode_key;
    client.decode_key = decode_key;
    client.encode_cipher = Some(crate::crypto::cipher_from_key(&encode_key));
    client.refresh_ack_session32();
}

#[test]
fn send_phase_runs_with_ready_send_queue() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.prev_auth_status = AuthStatus::AuthDone;
    client.need_connect = false;
    install_send_session(&mut client);
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    let mut dispatcher = EventDispatcher::new();

    client.send_cmd(vec![1, 2, 3, 4], Command::UI, SendPriority::Sliced, true, 6);

    client.run_dispatcher_steps_for_test(1, &mut dispatcher);
    assert!(
        client.send_lock.lock().is_empty(),
        "writer must copy direct Delphi-style send queues without app-event bridge"
    );
    assert!(
        !client.sending.is_empty(),
        "Sliced item with retry budget must remain in Sending until ACK or retry exhaustion"
    );
    assert_eq!(
        client.total_sent(),
        0,
        "first tick only creates the Sliced object; actual slice transmit is paced by retry_sliced"
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

    assert_eq!(client.pending_api.api_pending.pending_count(), 0);
    assert!(rx.recv_timeout(Duration::from_millis(1)).is_err());
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty());
    assert!(high.is_empty());
    assert!(low.is_empty());
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
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.prev_auth_status = AuthStatus::AuthDone;
    client.need_connect = false;
    install_send_session(&mut client);
    client.transport.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
    let mut dispatcher = EventDispatcher::new();

    send_server_packet_to_client_socket(&client, Command::OrderBook, &[0xAA]);
    client.send_cmd(vec![1, 2, 3, 4], Command::UI, SendPriority::Sliced, true, 6);

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

    client.server_token = 1;
    client.reconnect.subscribed_book_server_token = 1;
    let mut dispatcher = EventDispatcher::new();
    dispatcher.markets.indexes_synchronized = true;
    let mut mode = RunMode::new(&mut dispatcher);

    assert!((ProtocolCore {
        client: &mut client
    })
    .run_step(&mut mode));
    drop(mode);

    let events = dispatcher.take_queued_events();
    assert_eq!(
        events.len(),
        1,
        "production run_step must process a bounded receive unit before returning to runtime publish/command work"
    );
    assert!(matches!(
        events[0],
        crate::events::Event::ParseFailed {
            cmd: Command::OrderBook,
            ..
        }
    ));
}

#[test]
fn err_emu_drop_updates_valid_packet_stats_before_protocol_drop() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::new(&mut dispatcher);
    ProtocolCore {
        client: &mut client,
    }
    .dispatch_command(
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
        dispatcher.take_queued_events().len(),
        0,
        "ErrEmu drop must happen after Delphi stats side effects but before protocol delivery"
    );
}

#[test]
fn pre_init_domain_pushes_are_dropped_before_callback_delivery() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::new(&mut dispatcher);

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
        .dispatch_command(
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
        dispatcher.take_queued_events().len(),
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
    client.authorized = true;
    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::new(&mut dispatcher);
    ProtocolCore {
        client: &mut client,
    }
    .dispatch_command(
        Command::TradesStream.to_byte(),
        &[0xAA],
        1,
        1,
        false,
        None,
        &mut mode,
    );
    assert_eq!(
        mode.dispatcher.take_queued_events().len(),
        0,
        "optional-trades deviation: no API subscription means incoming trades are dropped"
    );

    client.subscribe_all_trades(false);
    let _ = client.take_send_queues_for_test();
    mode.dispatcher.markets.indexes_synchronized = true;
    ProtocolCore {
        client: &mut client,
    }
    .dispatch_command(
        Command::TradesStream.to_byte(),
        &[0xAA],
        1,
        2,
        false,
        None,
        &mut mode,
    );

    assert!(mode
        .dispatcher
        .take_queued_events()
        .iter()
        .any(|event| matches!(
            event,
            crate::events::Event::ParseFailed {
                cmd: Command::TradesStream,
                ..
            }
        )));
}

#[test]
fn data_read_sliced_payload_bypasses_recv_event_backlog() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.authorized = true;

    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::new(&mut dispatcher);
    ProtocolCore {
        client: &mut client,
    }
    .dispatch_command(
        Command::Data.to_byte(),
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

    drop(mode);
    assert!(dispatcher.take_queued_events().iter().any(|event| matches!(
        event,
        crate::events::Event::Raw {
            cmd: Command::Data,
            payload
        } if payload == &[0xAA, 0xBB]
    )));
    assert_eq!(client.avg_dup_count, 25.0);
    assert_eq!(client.metrics.total_recv, 321);
    assert_eq!(client.last_online, 123);
}

#[test]
fn data_read_grouped_payload_applies_recv_effects_once() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.authorized = true;
    let mut grouped = Vec::new();
    // Non-sensitive sub-commands: S1 drops plaintext sensitive cmds even inside a
    // group, so the grouped-delivery test uses non-sensitive commands.
    grouped.push(Command::Data.to_byte());
    grouped.extend_from_slice(&1u16.to_le_bytes());
    grouped.push(0xAA);
    grouped.push(Command::Ping.to_byte());
    grouped.extend_from_slice(&1u16.to_le_bytes());
    grouped.push(0xBB);

    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::new(&mut dispatcher);

    ProtocolCore {
        client: &mut client,
    }
    .dispatch_packet_commands(
        Command::Grouped.to_byte(),
        &grouped,
        77,
        456,
        true,
        &mut mode,
    );

    drop(mode);
    let events = dispatcher.take_queued_events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0],
        crate::events::Event::Raw {
            cmd: Command::Data,
            payload
        } if payload == &[0xAA]
    ));
    assert!(matches!(
        &events[1],
        crate::events::Event::Raw {
            cmd: Command::Ping,
            payload
        } if payload == &[0xBB]
    ));
    assert_eq!(client.metrics.total_recv, 77);
    assert_eq!(client.last_online, 456);
}

#[test]
fn active_dispatch_panic_drops_payload_without_rebuilding_dispatcher() {
    let mut client = Client::new(dummy_cfg());
    client.testing_set_domain_ready(true);
    client.authorized = true;
    let mut dispatcher = EventDispatcher::new();
    dispatcher.panic_next_active_dispatch_for_test();

    let result = {
        let mut mode = RunMode::new(&mut dispatcher);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ProtocolCore {
                client: &mut client,
            }
            .client_new_data(
                Command::Balance.to_byte(),
                vec![1, 2],
                false,
                false,
                1,
                &mut mode,
            );
        }))
    };

    assert!(
        result.is_ok(),
        "active dispatch panic must be isolated per payload, not unwind into runtime supervisor"
    );
    assert!(
        dispatcher.take_queued_events().is_empty(),
        "the panicking payload is dropped instead of publishing partial events"
    );

    {
        let mut mode = RunMode::new(&mut dispatcher);
        ProtocolCore {
            client: &mut client,
        }
        .client_new_data(
            Command::Balance.to_byte(),
            vec![1, 2],
            false,
            false,
            2,
            &mut mode,
        );
    }

    assert!(
        dispatcher.take_queued_events().iter().any(|event| matches!(
            event,
            crate::events::Event::ParseFailed {
                cmd: Command::Balance,
                ..
            }
        )),
        "the same dispatcher must keep running after one dropped payload"
    );
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
    client.authorized = true;

    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::new(&mut dispatcher);

    for sensitive in [
        Command::Order,
        Command::UI,
        Command::Strat,
        Command::Balance,
    ] {
        ProtocolCore {
            client: &mut client,
        }
        .dispatch_command(
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
        mode.dispatcher.take_queued_events().len(),
        0,
        "plaintext sensitive commands must be dropped by the S1 gate"
    );

    // A non-sensitive command on the same plaintext path is delivered as usual.
    ProtocolCore {
        client: &mut client,
    }
    .dispatch_command(
        Command::Data.to_byte(),
        &[0xAA],
        0,
        0,
        false,
        None,
        &mut mode,
    );
    assert!(mode
        .dispatcher
        .take_queued_events()
        .iter()
        .any(|event| matches!(
            event,
            crate::events::Event::Raw {
                cmd: Command::Data,
                payload
            } if payload == &[0xAA]
        )));
}
