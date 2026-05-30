use super::*;
use crate::commands::engine_api::EngineMethod;
use crate::commands::market::build_markets_indexes_response;
use crate::commands::strategy_serializer::{FieldValue, StrategyFields, StrategySnapshot};
use crate::commands::ui::{build_client_settings, ClientSettingsCommand};
use crate::events::EventDispatcher;
use crate::transport::{outer_light_crypt, MacContext, ServerMsgHeader, TRANSPORT_VER};
use std::net::UdpSocket;

fn write_str8(out: &mut Vec<u8>, value: &str) {
    out.push(value.len() as u8);
    out.extend_from_slice(value.as_bytes());
}

fn deflate_raw(data: &[u8]) -> Vec<u8> {
    use flate2::write::DeflateEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn apply_comment_strategy_schema(dispatcher: &mut EventDispatcher) {
    let mut body = Vec::new();
    body.push(crate::commands::strategy_schema::SCHEMA_FORMAT_VERSION);
    body.push(1); // kind_count
    body.push(1); // kind ordinal
    write_str8(&mut body, "Kind1");
    body.extend_from_slice(&1u16.to_le_bytes()); // field_count
    write_str8(&mut body, "Comment");
    body.push(crate::commands::strategy_serializer::TID_STRING);
    body.push(0);
    body.push(1); // visible for kind 1

    let data = deflate_raw(&body);
    let mut payload = Vec::new();
    payload.push(8); // TStratSchema
    payload.extend_from_slice(&crate::commands::registry::CURRENT_PROTO_CMD_VER.to_le_bytes());
    payload.extend_from_slice(&1u64.to_le_bytes());
    payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
    payload.extend_from_slice(&data);

    let mut out = Vec::new();
    dispatcher.dispatch_into(Command::Strat, &payload, 0, &mut out);
    assert!(out.iter().any(|ev| {
        matches!(
            ev,
            crate::events::Event::Strat(crate::state::StratEvent::SchemaApplied {
                kind_count: 1,
                field_count: 1,
                ..
            })
        )
    }));
}

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

fn subscribe_all_trades_want_mm(payload: &[u8]) -> Option<bool> {
    if engine_request_method(payload)? != EngineMethod::SubscribeAllTrades {
        return None;
    }
    payload.last().map(|v| *v != 0)
}

fn build_engine_response_payload(request_uid: u64, method: EngineMethod, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1u8); // TEngineResponse CmdId
    buf.extend_from_slice(&3u16.to_le_bytes()); // version
    buf.extend_from_slice(&0xAABB_CCDD_u64.to_le_bytes());
    buf.extend_from_slice(&request_uid.to_le_bytes());
    buf.push(method.to_byte());
    buf.push(1u8); // success
    buf.extend_from_slice(&0i32.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // empty error_msg
    buf.push(0u8); // not compressed
    buf.extend_from_slice(&(data.len() as i32).to_le_bytes());
    buf.extend_from_slice(data);
    buf
}

fn install_server_decode_session(client: &mut Client, server_token: u64) -> MoonKey {
    let (encode_key, decode_key) = crypto::generate_sub_keys(&client.cfg.master_key, server_token);
    client.server_token = server_token;
    client.encode_key = encode_key;
    client.decode_key = decode_key;
    client.encode_cipher = Some(crypto::cipher_from_key(&encode_key));
    client
        .recv
        .data_read_state
        .set_decode_cipher(crypto::cipher_from_key(&decode_key));
    decode_key
}

fn build_server_crypted_payload(
    decode_key: &MoonKey,
    msg_num: u64,
    cmd: Command,
    payload: &[u8],
) -> Vec<u8> {
    let mut plaintext =
        Vec::with_capacity(crate::protocol::crypted::CRYPTO_HEADER_SIZE + payload.len());
    plaintext.extend_from_slice(&0xCAFEu16.to_le_bytes());
    plaintext.extend_from_slice(&msg_num.to_le_bytes());
    plaintext.push(cmd.to_byte());
    plaintext.push(0);
    plaintext.extend_from_slice(payload);
    let cipher = crypto::cipher_from_key(decode_key);
    crypto::encrypt_with_cipher(&cipher, &plaintext, &[])
}

fn drain_base_check_sends(client: &mut Client) -> usize {
    let mut count = 0;
    let (sliced, high, low) = client.take_send_queues_for_test();
    for item in sliced.into_iter().chain(high).chain(low) {
        if item.cmd == Command::API.to_byte()
            && item.data.get(11) == Some(&(EngineMethod::BaseCheck.to_byte()))
        {
            assert_eq!(item.priority, SendPriority::Sliced);
            assert!(item.encrypted);
            assert_eq!(item.max_retries, 6);
            count += 1;
        }
    }
    count
}

fn drain_api_methods(client: &Client) -> Vec<u8> {
    let mut out = Vec::new();
    let (sliced, high, low) = client.take_send_queues_for_test();
    for item in sliced.into_iter().chain(high).chain(low) {
        if item.cmd == Command::API.to_byte() && item.data.len() >= 12 {
            out.push(item.data[11]);
        }
    }
    out
}

#[test]
fn server_update_ui_commands_mark_delphi_base_check_flag() {
    let mut client = Client::new(dummy_cfg());
    client.set_domain_ready(true);

    assert!(!client.server_update_sent());
    client.ui_update_version("MoonBot-1", true);
    assert!(client.server_update_sent());
    assert!(client.take_server_update_sent());
    assert!(!client.server_update_sent());

    client.ui_switch_dex("Main");
    assert!(client.server_update_sent());
    assert!(client.take_server_update_sent());

    client.ui_switch_spot(1);
    assert!(client.server_update_sent());
}

#[test]
fn base_check_without_server_update_uses_one_sendandwait_attempt() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();
    let mut result = InitResult::default();

    let status = run_base_check_delphi(
        &mut client,
        &mut dispatcher,
        &mut result,
        Duration::ZERO,
        false,
        Duration::ZERO,
    )
    .expect("zero-timeout BaseCheck should return a status, not disconnect");

    assert!(matches!(status, CriticalInitStatus::TimedOut));
    assert_eq!(drain_base_check_sends(&mut client), 1);
}

#[test]
fn base_check_after_server_update_uses_delphi_retry_count() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();
    let mut result = InitResult::default();

    let status = run_base_check_delphi(
        &mut client,
        &mut dispatcher,
        &mut result,
        Duration::ZERO,
        true,
        Duration::ZERO,
    )
    .expect("zero-timeout BaseCheck should return a status, not disconnect");

    assert!(matches!(status, CriticalInitStatus::TimedOut));
    assert_eq!(
        drain_base_check_sends(&mut client),
        1 + DELPHI_BASE_CHECK_UPDATE_RETRIES
    );
}

#[test]
fn init_base_auth_failure_uses_delphi_retry_branch() {
    let mut client = Client::new(dummy_cfg());
    client.authorized = true;
    client.connected = true;
    client.need_connect = false;
    let mut dispatcher = EventDispatcher::new();

    let err = run_init_sequence(
        &mut client,
        &mut dispatcher,
        InitConfig {
            step_timeout: Some(Duration::ZERO),
            ..Default::default()
        },
    )
    .expect_err("zero-timeout live requests must fail");

    assert!(matches!(err, InitError::CriticalStepTimedOut("AuthCheck")));
    assert_eq!(
        drain_api_methods(&client),
        vec![
            EngineMethod::BaseCheck.to_byte(),
            EngineMethod::BaseCheck.to_byte(),
            EngineMethod::AuthCheck.to_byte(),
        ],
        "Delphi InitInt retry branch is Sleep(200); BaseCheck; AuthCheck"
    );
}

#[test]
fn pending_api_response_still_reaches_dispatcher_state() {
    let mut client = Client::new(dummy_cfg());
    let request_uid = 0x1122_3344_5566_7788;
    let rx = client.pending_api.api_pending.register(request_uid);

    let names = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    let response_data = build_markets_indexes_response(&names);
    let payload =
        build_engine_response_payload(request_uid, EngineMethod::GetMarketsIndexes, &response_data);

    let mut payloads = Vec::new();
    {
        let mut sink = DispatchSink::Buffer(&mut payloads);
        client.client_new_data_decoded(Command::API.to_byte(), payload, false, false, &mut sink);
    }

    let resp = rx.try_recv().expect("pending receiver must get response");
    assert_eq!(resp.request_uid, request_uid);
    assert_eq!(resp.method, EngineMethod::GetMarketsIndexes);

    assert_eq!(
        payloads.len(),
        1,
        "dispatcher buffer must also receive API payload",
    );
    let (cmd, dispatcher_payload) = payloads.pop().unwrap();
    assert_eq!(cmd, Command::API);

    let mut dispatcher = EventDispatcher::new();
    let mut out = Vec::new();
    let ctx = crate::events::ActiveDispatchContext::from_client(&client);
    let mut actions = Vec::new();
    dispatcher.dispatch_into_active_actions(
        cmd,
        &dispatcher_payload,
        client.now_ms(),
        &mut out,
        &ctx,
        &mut actions,
    );
    client.apply_active_actions(actions.drain(..));

    assert!(dispatcher.markets().indexes_synchronized);
    assert_eq!(dispatcher.markets().market_index_names(), names.as_slice());
}

#[test]
fn pending_heavy_markets_response_is_applied_by_pending_owner_not_inline_dispatch() {
    let mut client = Client::new(dummy_cfg());
    let request_uid = 0x1020_3040_5060_7080;
    let rx = client.pending_api.api_pending.register(request_uid);
    let payload = build_engine_response_payload(request_uid, EngineMethod::GetMarketsList, &[]);

    let consumed = Client::dispatch_api_pending_inline(
        client.pending_api.api_pending.as_ref(),
        Command::API.to_byte(),
        &payload,
    );
    assert!(consumed);

    let mut payloads = Vec::new();
    {
        let mut sink = DispatchSink::Buffer(&mut payloads);
        client.client_new_data_decoded(Command::API.to_byte(), payload, true, false, &mut sink);
    }

    let resp = rx.try_recv().expect("pending receiver must own response");
    assert_eq!(resp.request_uid, request_uid);
    assert_eq!(resp.method, EngineMethod::GetMarketsList);
    assert!(
        payloads.is_empty(),
        "Delphi ProcessApiCommand only parks pending GetMarketsList; the SendAndWait owner applies it"
    );
}

#[test]
fn data_read_api_response_reaches_pending_receiver_before_run_loop() {
    let mut client = Client::new(dummy_cfg());
    let decode_key = install_server_decode_session(&mut client, 0x0123_4567_89AB_CDEF);
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    let request_uid = 0x7766_5544_3322_1100;
    let rx = client.pending_api.api_pending.register(request_uid);
    let response_payload = build_engine_response_payload(request_uid, EngineMethod::AuthCheck, &[]);
    // AuthCheck is not an UnencryptedMethod, so the server sends it Crypted; feed
    // the crypted command path (S1 part 2 drops a plaintext AuthCheck as a spoof).
    let encrypted_response =
        build_server_crypted_payload(&decode_key, 1, Command::API, &response_payload);
    let mut mode = RunMode::Callback {
        on_data: Box::new(|_, _| panic!("pending response must not be duplicated")),
    };

    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::Crypted.to_byte(),
        &encrypted_response,
        64,
        123,
        true,
        None,
        &mut mode,
    );
    let resp = rx
        .try_recv()
        .expect("receiver must be signalled by receive-side API dispatch");
    assert_eq!(resp.request_uid, request_uid);
    assert_eq!(resp.method, EngineMethod::AuthCheck);
}

#[test]
fn crypted_app_packets_before_auth_do_not_advance_slider_or_pending_api() {
    let mut client = Client::new(dummy_cfg());
    let decode_key = install_server_decode_session(&mut client, 0x0123_4567_89AB_CDEF);
    client.authorized = false;
    client.auth_status = AuthStatus::Connected;

    let stale_domain = build_server_crypted_payload(&decode_key, 5000, Command::UI, &[0, 0, 0]);
    let request_uid = 0x1111_2222_3333_4444;
    let rx = client.pending_api.api_pending.register(request_uid);
    let response_payload = build_engine_response_payload(request_uid, EngineMethod::BaseCheck, &[]);
    let encrypted_response =
        build_server_crypted_payload(&decode_key, 1, Command::API, &response_payload);
    let mut mode = RunMode::Callback {
        on_data: Box::new(|_, _| panic!("pre-auth encrypted app data must be dropped")),
    };

    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::Crypted.to_byte(),
        &stale_domain,
        128,
        100,
        true,
        None,
        &mut mode,
    );
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::Crypted.to_byte(),
        &encrypted_response,
        128,
        101,
        true,
        None,
        &mut mode,
    );
    assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::Crypted.to_byte(),
        &encrypted_response,
        128,
        102,
        true,
        None,
        &mut mode,
    );

    let resp = rx
        .try_recv()
        .expect("same encrypted response must remain valid after AuthDone");
    assert_eq!(resp.request_uid, request_uid);
    assert_eq!(resp.method, EngineMethod::BaseCheck);
}

#[test]
fn primary_hello_resets_transport_receive_state_before_new_session() {
    let mut client = Client::new(dummy_cfg());
    install_server_decode_session(&mut client, 0x2222_3333_4444_5555);
    client.peer_app_token = 0x7777;
    client.need_connect = true;
    client.last_sent_hello = NEVER_SENT_MS;
    client.recv.data_read_state.slider.check_revd(5000);
    assert!(
        !client.recv.data_read_state.slider.check_revd(1),
        "test setup must prove the old receive slider would reject fresh low msg_num",
    );

    ProtocolCore {
        client: &mut client,
    }
    .check_hello_send(100);

    assert_eq!(client.server_token, 0);
    assert_eq!(client.peer_app_token, 0);
    assert!(!client.authorized);
    assert!(
        client.recv.data_read_state.slider.check_revd(1),
        "primary Hello starts a new transport session and clears the old replay window",
    );
    assert!(matches!(
        client.hello_wait_state,
        HelloWaitState::PrimaryHelloNewSession
    ));
}

#[test]
fn reader_consumed_api_response_is_not_duplicated_to_callback_sink() {
    let mut client = Client::new(dummy_cfg());
    let payload = build_engine_response_payload(0x55, EngineMethod::BaseCheck, &[]);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_for_cb = calls.clone();
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |_cmd, _payload| {
            calls_for_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }),
    };

    ProtocolCore {
        client: &mut client,
    }
    .client_new_data(Command::API.to_byte(), payload, true, false, 123, &mut mode);

    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn reader_consumed_api_response_still_reaches_dispatcher_state() {
    let mut client = Client::new(dummy_cfg());
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    let payload = build_engine_response_payload(0x66, EngineMethod::AuthCheck, &[]);
    let mut dispatcher = EventDispatcher::new();
    let mut mode = RunMode::Dispatcher {
        dispatcher: &mut dispatcher,
        on_event: DispatcherEventFn::Queue,
        event_buf: Vec::new(),
        payload_buf: Vec::new(),
        active_actions_buf: Vec::new(),
    };

    ProtocolCore {
        client: &mut client,
    }
    .client_new_data(Command::API.to_byte(), payload, true, false, 123, &mut mode);

    let queued = dispatcher.take_queued_events();
    assert!(queued.iter().any(|event| matches!(
        event,
        crate::events::Event::EngineResponse(resp)
            if resp.request_uid == 0x66 && resp.method == EngineMethod::AuthCheck
    )));
}

#[test]
fn s1_drops_plaintext_api_response_with_non_unencrypted_method() {
    // S1 part 2 (эталон MoonProtoClient.pas ClientNewData MPC_API guard): a
    // plaintext API response is only legitimate for UnencryptedMethods
    // (GetMarketsList / UpdateMarketsList / RequestCandlesData). The sensitive
    // gate (part 1) intentionally lets MPC_API through, so without this method
    // check a spoofer could inject a forged GetBalance / order-status response
    // over the authenticity-only transport MAC.
    let mut client = Client::new(dummy_cfg());

    // Forged plaintext balance response — must be dropped on both decode paths.
    let forged = build_engine_response_payload(0x77, EngineMethod::GetBalance, &[]);
    assert!(
        Client::decode_data_read_int_payload_shared(
            &mut client.recv.data_read_state,
            Command::API.to_byte(),
            &forged,
        )
        .is_none(),
        "plaintext API response with a non-UnencryptedMethods method must be dropped"
    );
    assert!(
        Client::decode_data_read_int_payload_owned(
            &mut client.recv.data_read_state,
            Command::API.to_byte(),
            forged,
        )
        .is_none(),
        "owned decode path drops the same forged plaintext API response"
    );

    // An unparseable / short plaintext API payload is not a valid engine response.
    // The server never sends such a thing in plaintext, so it is dropped rather
    // than delivered raw — matching the эталон, where a non-response either fails
    // the TEngineResponse gate or no-ops in ProcessApiCommand (no state change).
    let unparseable = vec![0u8; 5];
    assert!(
        Client::decode_data_read_int_payload_shared(
            &mut client.recv.data_read_state,
            Command::API.to_byte(),
            &unparseable,
        )
        .is_none(),
        "unparseable plaintext API must be dropped, not delivered raw"
    );

    // Legitimate plaintext responses (public market data / already-zlib candles)
    // still pass through the gate.
    for method in [
        EngineMethod::GetMarketsList,
        EngineMethod::UpdateMarketsList,
        EngineMethod::RequestCandlesData,
    ] {
        let ok = build_engine_response_payload(0x88, method, &[]);
        let decoded = Client::decode_data_read_int_payload_shared(
            &mut client.recv.data_read_state,
            Command::API.to_byte(),
            &ok,
        );
        assert!(
            decoded.is_some(),
            "plaintext API response with UnencryptedMethods method {method:?} must pass"
        );
        assert_eq!(decoded.unwrap().0, Command::API.to_byte());
    }
}

#[test]
fn decoded_batch_uses_receive_timestamp_for_active_timers() {
    #[derive(Debug, PartialEq)]
    struct Summary {
        apply_events: usize,
        gap_events: usize,
        resend_requests: Vec<Vec<u16>>,
        trades_resend_sends: usize,
        last_packet_num: u16,
        used_buckets: usize,
    }

    fn minimal_trades_payload(packet_num: u16) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&45_000.0f64.to_le_bytes());
        payload.extend_from_slice(&packet_num.to_le_bytes());
        payload.push(0);
        payload
    }

    fn trades_packet(packet_num: u16, timestamp_ms: i64) -> (Vec<u8>, i64) {
        (minimal_trades_payload(packet_num), timestamp_ms)
    }

    fn run_sequence(batch: bool) -> Summary {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;
        client.subscribe_all_trades(false);
        let _ = client.take_send_queues_for_test();

        let mut dispatcher = EventDispatcher::new();
        dispatcher.markets.indexes_synchronized = true;
        let messages = [
            trades_packet(100, 1_000),
            trades_packet(105, 1_010),
            trades_packet(106, 1_500),
        ];

        {
            let mut mode = RunMode::Dispatcher {
                dispatcher: &mut dispatcher,
                on_event: DispatcherEventFn::Queue,
                event_buf: Vec::new(),
                payload_buf: Vec::new(),
                active_actions_buf: Vec::new(),
            };
            if batch {
                for (payload, timestamp_ms) in messages.iter() {
                    ProtocolCore {
                        client: &mut client,
                    }
                    .data_read_int_inline(
                        Command::TradesStream.to_byte(),
                        payload,
                        64,
                        *timestamp_ms,
                        true,
                        None,
                        &mut mode,
                    );
                }
            } else {
                for (payload, timestamp_ms) in messages {
                    ProtocolCore {
                        client: &mut client,
                    }
                    .data_read_int_inline(
                        Command::TradesStream.to_byte(),
                        &payload,
                        64,
                        timestamp_ms,
                        true,
                        None,
                        &mut mode,
                    );
                }
            }
        }

        let queued = dispatcher.take_queued_events();
        let apply_events = queued
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    crate::events::Event::Trade(crate::state::TradesEvent::Applied { .. })
                )
            })
            .count();
        let gap_events = queued
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    crate::events::Event::Trade(crate::state::TradesEvent::GapDetected {
                        start: 101,
                        end: 104
                    })
                )
            })
            .count();
        let resend_requests = queued
            .iter()
            .filter_map(|event| match event {
                crate::events::Event::Trade(crate::state::TradesEvent::ResendRequested {
                    packet_nums,
                }) => Some(packet_nums.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let (sliced, high, low) = client.take_send_queues_for_test();
        let trades_resend_sends = sliced
            .into_iter()
            .chain(high)
            .chain(low)
            .filter(|item| {
                item.cmd == Command::API.to_byte()
                    && item.data.get(11) == Some(&(EngineMethod::TradesResend.to_byte()))
            })
            .count();

        Summary {
            apply_events,
            gap_events,
            resend_requests,
            trades_resend_sends,
            last_packet_num: dispatcher.trades_recovery().last_packet_num(),
            used_buckets: dispatcher.trades_recovery().used_buckets(),
        }
    }

    let inline = run_sequence(false);
    let batch = run_sequence(true);
    assert_eq!(
        inline, batch,
        "batch boundary must not change active machine effect when packet order is preserved"
    );
    assert_eq!(batch.apply_events, 3);
    assert_eq!(batch.gap_events, 1);
    assert_eq!(batch.resend_requests, vec![vec![101, 102, 103, 104]]);
    assert_eq!(
            batch.trades_resend_sends, 1,
            "old Rust-only writer-tick timestamping skipped this resend when several decoded packets drained in one tick"
        );
    assert_eq!(batch.last_packet_num, 106);
    assert_eq!(batch.used_buckets, 1);
}

#[test]
fn data_read_candles_chunks_complete_receiver_from_background_parse_worker() {
    let mut client = Client::new(dummy_cfg());
    let (uid, rx) = client.api_request_candles_data_async_registered();
    let chunk0 = [0u8, 0, 2, 0, 1, 2];
    let chunk1 = [1u8, 0, 2, 0, 3, 4];
    let payload0 = build_engine_response_payload(uid, EngineMethod::RequestCandlesData, &chunk0);
    let payload1 = build_engine_response_payload(uid, EngineMethod::RequestCandlesData, &chunk1);

    let mut mode = RunMode::Callback {
        on_data: Box::new(|_, _| panic!("candles chunks must be consumed")),
    };

    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::API.to_byte(),
        &payload0,
        64,
        10,
        true,
        None,
        &mut mode,
    );
    assert!(
        rx.try_recv().is_err(),
        "first chunk stores progress but does not complete receiver"
    );

    ProtocolCore {
        client: &mut client,
    }
    .data_read_int_inline(
        Command::API.to_byte(),
        &payload1,
        64,
        20,
        true,
        None,
        &mut mode,
    );

    let merged = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second chunk should complete candles receiver after background parse");
    assert_eq!(merged.uid, uid);
    assert!(merged.markets.is_empty());
    assert!(client.pending_api.pending_candles.is_empty());
}

#[test]
fn reader_consumed_candles_chunk_is_not_delivered_to_callback_or_dispatcher() {
    let mut client = Client::new(dummy_cfg());
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    let payload = build_engine_response_payload(
        0x1234,
        EngineMethod::RequestCandlesData,
        &[0u8, 0, 1, 0, 1, 2],
    );
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_for_cb = calls.clone();
    let mut callback_mode = RunMode::Callback {
        on_data: Box::new(move |_cmd, _payload| {
            calls_for_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }),
    };
    ProtocolCore {
        client: &mut client,
    }
    .client_new_data(
        Command::API.to_byte(),
        payload.clone(),
        false,
        true,
        123,
        &mut callback_mode,
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);

    let mut dispatcher = EventDispatcher::new();
    let mut dispatcher_mode = RunMode::Dispatcher {
        dispatcher: &mut dispatcher,
        on_event: DispatcherEventFn::Queue,
        event_buf: Vec::new(),
        payload_buf: Vec::new(),
        active_actions_buf: Vec::new(),
    };
    ProtocolCore {
        client: &mut client,
    }
    .client_new_data(
        Command::API.to_byte(),
        payload,
        false,
        true,
        123,
        &mut dispatcher_mode,
    );
    assert!(dispatcher.take_queued_events().is_empty());
}

#[test]
fn pending_api_response_is_not_duplicated_to_callback_sink() {
    let mut client = Client::new(dummy_cfg());
    let request_uid = 7;
    let rx = client.pending_api.api_pending.register(request_uid);
    let payload = build_engine_response_payload(request_uid, EngineMethod::BaseCheck, &[]);

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_for_cb = calls.clone();
    let mut cb: OnDataFn = Box::new(move |_cmd, _payload| {
        calls_for_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    {
        let mut sink = DispatchSink::Callback(&mut cb);
        client.client_new_data_decoded(Command::API.to_byte(), payload, false, false, &mut sink);
    }

    assert!(rx.try_recv().is_ok(), "pending receiver must get response");
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn failed_compressed_payload_is_delivered_with_real_cmd_like_delphi() {
    let mut client = Client::new(dummy_cfg());
    let compressed_garbage = vec![4, 0, 1, 0, 0, 0, 0x0F, 0];
    let mut payloads = Vec::new();
    // OrderBook stands in for "a non-sensitive data command": S1 drops plaintext
    // sensitive cmds (Order/Strat/UI/Balance) in decode, so this compressed-fail
    // delivery test uses a non-sensitive command.
    let (cmd, payload) = Client::decode_data_read_int_payload_shared(
        &mut client.recv.data_read_state,
        Command::OrderBook.to_byte() | COMPRESSED_FLAG,
        &compressed_garbage,
    )
    .expect("failed compressed payload still has a decoded real command");

    {
        let mut sink = DispatchSink::Buffer(&mut payloads);
        client.client_new_data_decoded(cmd, payload, false, false, &mut sink);
    }

    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].0, Command::OrderBook);
    assert_eq!(payloads[0].1, compressed_garbage);
}

#[test]
fn owned_data_read_keeps_plain_sliced_payload_allocation() {
    let mut client = Client::new(dummy_cfg());
    let payload = vec![1, 2, 3, 4, 5];
    let ptr = payload.as_ptr();

    // Generic non-API command: this test pins buffer ownership of the owned
    // decode path, not API semantics. API now carries the S1 part-2 gate, which
    // would drop this non-response payload.
    let (cmd, decoded) = Client::decode_data_read_int_payload_owned(
        &mut client.recv.data_read_state,
        Command::Data.to_byte(),
        payload,
    )
    .expect("plain owned payload must decode");

    assert_eq!(cmd, Command::Data.to_byte());
    assert_eq!(decoded, vec![1, 2, 3, 4, 5]);
    assert_eq!(
        decoded.as_ptr(),
        ptr,
        "plain Sliced completion already owns the buffer; reader must not clone it"
    );
}

#[test]
fn malformed_api_request_async_returns_closed_receiver_without_pending_slot() {
    let client = Client::new(dummy_cfg());

    let rx = client.send_api_request_async(&[2, 3, 0]);

    assert_eq!(client.pending_api.api_pending.pending_count(), 0);
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
}

#[test]
fn request_candles_data_timeout_removes_pending_slot() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();

    let err = client
        .request_candles_data_for_test(&mut dispatcher, Duration::from_millis(0))
        .expect_err("zero timeout should expire before any chunk arrives");

    assert!(matches!(err, mpsc::RecvTimeoutError::Timeout));
    assert!(client.pending_api.pending_candles.is_empty());
}

#[test]
fn request_client_settings_waits_for_applied_event_not_uid_change() {
    let mut dispatcher = EventDispatcher::new();
    let settings = ClientSettingsCommand {
        uid: 0x7788,
        x_sell: 7,
        ..ClientSettingsCommand::default()
    };
    let payload = build_client_settings(&settings);

    let first_events = dispatcher.dispatch(Command::UI, &payload, 0);
    dispatcher.queue_events(first_events);
    let first_new_event = dispatcher.queued_event_count();

    let repeated_events = dispatcher.dispatch(Command::UI, &payload, 1);
    dispatcher.queue_events(repeated_events);

    assert_eq!(
        dispatcher
            .settings()
            .client_settings
            .as_ref()
            .map(|settings| settings.uid),
        Some(0x7788)
    );
    assert!(
        queued_client_settings_updated_since(&dispatcher, first_new_event),
        "same-UID TClientSettingsCommand is still a fresh applied snapshot"
    );
}

#[test]
fn protocol_metrics_snapshot_reports_public_event_queue_without_control_effects() {
    let client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();
    dispatcher.queue_events(vec![crate::events::Event::Raw {
        cmd: Command::UI,
        payload: vec![1, 2, 3],
    }]);

    let snapshot = client.protocol_metrics_snapshot_with_dispatcher(&dispatcher);
    assert_eq!(snapshot.recv_count, 0);
    assert_eq!(snapshot.public_event_queue_len, 1);
}

#[test]
fn wait_for_receiver_in_owned_runtime_does_not_overflow_huge_timeout_when_ready() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = EventDispatcher::new();
    let (tx, rx) = mpsc::channel();
    tx.send(123u32).unwrap();

    let value = client
        .wait_for_receiver_in_owned_runtime(&mut dispatcher, &rx, Duration::MAX)
        .expect("ready response should be returned without touching timeout arithmetic");

    assert_eq!(value, 123);
}

#[test]
fn wait_for_receiver_in_owned_runtime_queues_events_seen_while_waiting() {
    let mut client = Client::new(dummy_cfg());
    client.transport.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    client.need_connect = false;
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    let decode_key = install_server_decode_session(&mut client, 0x0123_4567_89AB_CDEF);

    let request_uid = 0x55AA;
    let rx = client.pending_api.api_pending.register(request_uid);
    let response_payload = build_engine_response_payload(request_uid, EngineMethod::AuthCheck, &[]);
    // AuthCheck is sent Crypted by the server (not an UnencryptedMethod); feed the
    // crypted command so S1 part 2 does not drop it as a plaintext spoof.
    let encrypted_response =
        build_server_crypted_payload(&decode_key, 1, Command::API, &response_payload);
    send_server_packet_to_client_socket(&client, Command::Crypted, &encrypted_response);

    let mut dispatcher = EventDispatcher::new();
    let resp = client
        .wait_for_receiver_in_owned_runtime(&mut dispatcher, &rx, Duration::from_millis(200))
        .expect("pending response should be delivered while the loop is pumped");

    assert_eq!(resp.request_uid, request_uid);
    assert_eq!(dispatcher.queued_event_count(), 1);

    let queued = dispatcher.take_queued_events();
    assert_eq!(dispatcher.queued_event_count(), 0);
    match &queued[0] {
        crate::events::Event::EngineResponse(event_resp) => {
            assert_eq!(event_resp.request_uid, request_uid);
            assert_eq!(event_resp.method, EngineMethod::AuthCheck);
        }
        other => panic!("expected queued EngineResponse, got {other:?}"),
    }
}

#[test]
fn post_init_resync_enqueues_delphi_commands() {
    let mut client = Client::new(dummy_cfg());
    client.set_domain_ready(true);
    let cfg = InitConfig {
        mm_orders_subscribe: Some(true),
        ..Default::default()
    };
    let mut dispatcher = EventDispatcher::new();
    apply_comment_strategy_schema(&mut dispatcher);
    dispatcher.set_local_strategy_epoch(55);
    let mut fields = StrategyFields::new();
    fields.insert("Comment", FieldValue::String("post-init".to_string()));
    let strategy = StrategySnapshot {
        strategy_id: 0x5157,
        strategy_ver: 3,
        last_date: 1234,
        checked: true,
        kind: 1,
        path: "Init".into(),
        fields,
    };
    dispatcher.set_local_strategies(std::slice::from_ref(&strategy));
    let mut result = InitResult::default();

    send_post_init_resync(&mut client, &mut dispatcher, &cfg, &mut result);

    assert!(result.post_init_resync_sent);

    let mut seen_order_req = false;
    let mut seen_strat_snapshot = false;
    let mut seen_settings_req = false;
    let mut seen_mm_orders_true = false;
    let mut seen_balance_refresh = false;

    let (sliced, high, low) = client.take_send_queues_for_test();
    for item in sliced.into_iter().chain(high).chain(low) {
        let data = item.data;
        match Command::from_byte(item.cmd) {
            Command::Order if data.first().copied() == Some(9) => {
                seen_order_req = true;
            }
            Command::Strat if data.first().copied() == Some(2) => {
                let cmd = crate::commands::strat::StratCommand::parse(&data)
                    .expect("post-init strategy snapshot must parse");
                let crate::commands::strat::StratCommand::Snapshot(snapshot) = cmd else {
                    panic!("expected TStratSnapshot");
                };
                assert_eq!(snapshot.server_epoch, 55);
                assert_eq!(snapshot.client_max_last_date, 1234);
                assert!(snapshot.full);
                let batch =
                    crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
                        .expect("post-init strategy batch must parse");
                assert_eq!(batch.strategies.len(), 1);
                assert_eq!(batch.strategies[0].strategy_id, strategy.strategy_id);
                seen_strat_snapshot = true;
            }
            Command::UI if data.first().copied() == Some(2) => {
                seen_settings_req = true;
            }
            Command::UI if data.first().copied() == Some(5) && data.last().copied() == Some(1) => {
                seen_mm_orders_true = true;
            }
            Command::Balance if data.first().copied() == Some(5) => {
                seen_balance_refresh = true;
            }
            _ => {}
        }
    }

    assert!(seen_order_req, "post-init must request TAllStatuses");
    assert!(
        seen_strat_snapshot,
        "post-init must send TStratSnapshot.CreateFromStrats equivalent"
    );
    assert!(seen_settings_req, "post-init must request settings");
    assert!(
        seen_mm_orders_true,
        "post-init must send TMMOrdersSubscribeCommand"
    );
    assert!(
        seen_balance_refresh,
        "post-init must request balance refresh"
    );
}

#[test]
fn post_init_mm_orders_does_not_fallback_to_subscribe_trades() {
    let mut client = Client::new(dummy_cfg());
    client.set_domain_ready(true);
    let cfg = InitConfig {
        mm_orders_subscribe: None,
        subscribe_trades: Some(crate::client::TradesStreamMode::TradesAndMarketMakers),
        ..Default::default()
    };
    let mut dispatcher = EventDispatcher::new();
    let mut result = InitResult::default();

    send_post_init_resync(&mut client, &mut dispatcher, &cfg, &mut result);

    let mut mm_orders_value = None;
    let (sliced, high, low) = client.take_send_queues_for_test();
    for item in sliced.into_iter().chain(high).chain(low) {
        if let Some(value) = Client::outgoing_mm_orders_subscribe_intent(&item) {
            mm_orders_value = Some(value);
        }
    }

    assert_eq!(
            mm_orders_value,
            Some(false),
            "Delphi post-init TMMOrdersSubscribeCommand uses cfg.ShowHeatMap, not SubscribeAllTrades want_mm"
        );
}

#[test]
fn post_init_mm_orders_does_not_overwrite_prequeued_all_trades_want_mm() {
    let mut client = Client::new(dummy_cfg());
    client.set_domain_ready(true);
    client.with_subscription_registry_mut(|registry| {
        registry.trades_sub = Some(TradesSubscription { want_mm: true });
    });
    let cfg = InitConfig {
        mm_orders_subscribe: None,
        ..Default::default()
    };
    let mut dispatcher = EventDispatcher::new();
    let mut result = InitResult::default();

    send_post_init_resync(&mut client, &mut dispatcher, &cfg, &mut result);
    client.send_registry_subscriptions_after_init();

    let mut post_init_mm_orders = None;
    let mut trades_want_mm = None;
    let (sliced, high, low) = client.take_send_queues_for_test();
    for item in sliced.into_iter().chain(high).chain(low) {
        if let Some(value) = Client::outgoing_mm_orders_subscribe_intent(&item) {
            post_init_mm_orders = Some(value);
        }
        if item.cmd == Command::API.to_byte() {
            trades_want_mm = subscribe_all_trades_want_mm(&item.data);
        }
    }

    assert_eq!(
        post_init_mm_orders,
        Some(false),
        "post-init UI command still mirrors cfg.ShowHeatMap/default false"
    );
    assert_eq!(
        trades_want_mm,
        Some(true),
        "the later registry SubscribeAllTrades flush must keep its own want_mm"
    );
    assert_eq!(
            client.with_subscription_registry(|registry| registry.mm_orders_sub),
            Some(true),
            "after the later SubscribeAllTrades wire command, reconnect intent follows the final server MMOrders flag"
        );
}
