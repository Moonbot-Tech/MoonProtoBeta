use super::*;

#[cfg(test)]
mod bps_tests {
    use super::*;

    #[test]
    fn bps_counter_empty() {
        let c = BpsCounter::new();
        assert_eq!(c.bytes_per_sec(), 0);
    }

    #[test]
    fn bps_counter_within_second_just_accumulates() {
        let mut c = BpsCounter::new();
        c.add(100, 1000);
        c.add(200, 1500);
        // Не прошла секунда → ema_10sec не обновился → bytes_per_sec = 0.
        assert_eq!(c.bytes_per_sec(), 0);
        // Но bucket собрал 300.
        assert_eq!(c.cur_sec_bytes, 300);
    }

    #[test]
    fn bps_counter_steady_state_converges() {
        let mut c = BpsCounter::new();
        // Эмулируем 100 секунд равномерного потока: 1000 байт/сек.
        // Используем шаг 1100мс между бакетами чтобы условие `> 1000` срабатывало надёжно.
        for sec in 1..101i64 {
            let bucket_start = sec * 1100;
            for _ in 0..10 {
                c.add(100, bucket_start);
            }
        }
        // EMA должна сойтись к ~10000 (= 10 × 1000 byte/sec — формула Delphi).
        // bytes_per_sec возвращает ema/10 = ~1000.
        let bps = c.bytes_per_sec();
        assert!(bps > 850 && bps < 1100, "bps={}, expected ~1000", bps);
    }
}

#[cfg(test)]
mod api_pending_dispatch_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use crate::commands::market::build_markets_indexes_response;
    use crate::commands::strategy_serializer::{FieldValue, StrategyFields, StrategySnapshot};
    use crate::commands::ui::{build_client_settings, ClientSettingsCommand};
    use crate::events::EventDispatcher;
    use moonproto_transport::{outer_light_crypt, MacContext, ServerMsgHeader, TRANSPORT_VER};
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
            mask_ver: 0,
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

    fn build_engine_response_payload(
        request_uid: u64,
        method: EngineMethod,
        data: &[u8],
    ) -> Vec<u8> {
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
        let rx = client.api_pending.register(request_uid);

        let names = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let response_data = build_markets_indexes_response(&names);
        let payload = build_engine_response_payload(
            request_uid,
            EngineMethod::GetMarketsIndexes,
            &response_data,
        );

        let mut payloads = Vec::new();
        {
            let mut sink = DispatchSink::Buffer(&mut payloads);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                payload,
                false,
                false,
                &mut sink,
            );
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
        assert_eq!(dispatcher.markets().market_indexes, names);
    }

    #[test]
    fn data_read_api_response_reaches_pending_receiver_before_run_loop() {
        let mut client = Client::new(dummy_cfg());
        let request_uid = 0x7766_5544_3322_1100;
        let rx = client.api_pending.register(request_uid);
        let payload = build_engine_response_payload(request_uid, EngineMethod::AuthCheck, &[]);
        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("pending response must not be duplicated")),
        };

        ProtocolCore {
            client: &mut client,
        }
        .data_read_int_inline(
            Command::API.to_byte(),
            &payload,
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
                last_packet_num: dispatcher.trades().last_packet_num(),
                used_buckets: dispatcher.trades().used_buckets(),
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
    fn data_read_candles_chunks_complete_receiver_before_run_loop() {
        let mut client = Client::new(dummy_cfg());
        let (uid, rx) = client.api_request_candles_data_async_registered();
        let chunk0 = [0u8, 0, 2, 0, 1, 2];
        let chunk1 = [1u8, 0, 2, 0, 3, 4];
        let payload0 =
            build_engine_response_payload(uid, EngineMethod::RequestCandlesData, &chunk0);
        let payload1 =
            build_engine_response_payload(uid, EngineMethod::RequestCandlesData, &chunk1);

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
            .try_recv()
            .expect("second chunk should complete candles receiver in reader");
        assert_eq!(merged.uid, uid);
        assert_eq!(merged.zipped_data, vec![1, 2, 3, 4]);
        assert!(merged.markets.is_empty());
        assert!(client.pending_candles.is_empty());
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
        let rx = client.api_pending.register(request_uid);
        let payload = build_engine_response_payload(request_uid, EngineMethod::BaseCheck, &[]);

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut cb: OnDataFn = Box::new(move |_cmd, _payload| {
            calls_for_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        {
            let mut sink = DispatchSink::Callback(&mut cb);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                payload,
                false,
                false,
                &mut sink,
            );
        }

        assert!(rx.try_recv().is_ok(), "pending receiver must get response");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn failed_compressed_payload_is_delivered_with_real_cmd_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let compressed_garbage = vec![4, 0, 1, 0, 0, 0, 0x0F, 0];
        let mut payloads = Vec::new();
        let (cmd, payload) = Client::decode_data_read_int_payload_shared(
            &mut client.data_read_state,
            Command::UI.to_byte() | COMPRESSED_FLAG,
            &compressed_garbage,
        )
        .expect("failed compressed payload still has a decoded real command");

        {
            let mut sink = DispatchSink::Buffer(&mut payloads);
            client.client_new_data_decoded(cmd, payload, false, false, &mut sink);
        }

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].0, Command::UI);
        assert_eq!(payloads[0].1, compressed_garbage);
    }

    #[test]
    fn owned_data_read_keeps_plain_sliced_payload_allocation() {
        let mut client = Client::new(dummy_cfg());
        let payload = vec![1, 2, 3, 4, 5];
        let ptr = payload.as_ptr();

        let (cmd, decoded) = Client::decode_data_read_int_payload_owned(
            &mut client.data_read_state,
            Command::API.to_byte(),
            payload,
        )
        .expect("plain owned payload must decode");

        assert_eq!(cmd, Command::API.to_byte());
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

        assert_eq!(client.api_pending.pending_count(), 0);
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
            .request_candles_data(&mut dispatcher, Duration::from_millis(0))
            .expect_err("zero timeout should expire before any chunk arrives");

        assert!(matches!(err, mpsc::RecvTimeoutError::Timeout));
        assert!(client.pending_candles.is_empty());
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
    fn run_until_response_does_not_overflow_huge_timeout_when_ready() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        tx.send(123u32).unwrap();

        let value = client
            .run_until_response(&mut dispatcher, &rx, Duration::MAX)
            .expect("ready response should be returned without touching timeout arithmetic");

        assert_eq!(value, 123);
    }

    #[test]
    fn run_until_response_queues_events_seen_while_waiting() {
        let mut client = Client::new(dummy_cfg());
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        client.need_connect = false;
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;

        let request_uid = 0x55AA;
        let rx = client.api_pending.register(request_uid);
        let payload = build_engine_response_payload(request_uid, EngineMethod::AuthCheck, &[]);
        send_server_packet_to_client_socket(&client, Command::API, &payload);

        let mut dispatcher = EventDispatcher::new();
        let resp = client
            .run_until_response(&mut dispatcher, &rx, Duration::from_millis(200))
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
            path: "Init".to_string(),
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
                Command::UI
                    if data.first().copied() == Some(5) && data.last().copied() == Some(1) =>
                {
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
            subscribe_trades: Some(true),
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
}

#[cfg(test)]
mod client_sender_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;

    fn make_sender() -> (
        ClientSender,
        Arc<Mutex<SubscriptionRegistry>>,
        Arc<Mutex<SendLockState>>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let subscription_registry = Arc::new(Mutex::new(SubscriptionRegistry::default()));
        let subscription_summary = Arc::new(SubscriptionRegistrySummary::default());
        let subscription_trades_scope = Arc::new(parking_lot::RwLock::new(None));
        let send_lock = Arc::new(Mutex::new(SendLockState::default()));
        let app_queue_alive = Arc::new(AtomicBool::new(true));
        let domain_ready = Arc::new(AtomicBool::new(true));
        let server_update_sent = Arc::new(AtomicBool::new(false));
        let last_trades_subscribe_request_ms = Arc::new(AtomicI64::new(i64::MIN / 2));
        let last_orderbook_subscribe_request_ms = Arc::new(AtomicI64::new(i64::MIN / 2));
        let last_orderbook_subscribe_request_uid =
            Arc::new(AtomicU64::new(NO_PENDING_ENGINE_REQUEST_UID));
        (
            ClientSender {
                shared: Arc::new(ClientSenderShared {
                    app_queue_alive: Arc::clone(&app_queue_alive),
                    domain_ready: Arc::clone(&domain_ready),
                    send_lock: Arc::clone(&send_lock),
                    subscription_registry: Arc::clone(&subscription_registry),
                    subscription_summary,
                    subscription_trades_scope,
                    server_update_sent: Arc::clone(&server_update_sent),
                    last_trades_subscribe_request_ms,
                    last_orderbook_subscribe_request_ms,
                    last_orderbook_subscribe_request_uid,
                }),
                start: Instant::now(),
            },
            subscription_registry,
            send_lock,
            app_queue_alive,
            server_update_sent,
            domain_ready,
        )
    }

    fn take_send_items(q: &Arc<Mutex<SendLockState>>) -> Vec<SendItem> {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        q.lock()
            .unwrap()
            .send_queues
            .take_into(&mut sliced, &mut high, &mut low);
        sliced.extend(high);
        sliced.extend(low);
        sliced
    }

    fn tracked_orders_for_sender(
        uid: u64,
        currency: u8,
        platform: u8,
        market_name: &str,
        status: crate::commands::trade::OrderWorkerStatus,
    ) -> crate::state::Orders {
        use crate::commands::trade::{
            BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, StopSettings,
            TradeCommand, TradeEpochHeader,
        };

        let mut orders = crate::state::Orders::new();
        let status_cmd = OrderStatus {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 4,
                        ver: 3,
                        uid,
                    },
                    currency,
                    platform,
                    market_name: market_name.to_string(),
                },
                epoch: 11,
                status,
            },
            buy_order: OrderCompact::default(),
            sell_order: OrderCompact::default(),
            stops: StopSettings::default(),
            strat_id: 0,
            is_short: false,
            db_id: 0,
            from_cache: false,
            emulator_mode: false,
            immune_for_clicks: false,
        };
        let _ = orders.apply(TradeCommand::OrderStatus(Box::new(status_cmd)));
        orders
    }

    fn command_uid(payload: &[u8]) -> Option<u64> {
        payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    }

    fn method_id(payload: &[u8]) -> Option<u8> {
        payload.get(11).copied()
    }

    fn market_names_count(payload: &[u8]) -> Option<i32> {
        let bytes: [u8; 4] = payload.get(14..18)?.try_into().ok()?;
        Some(i32::from_le_bytes(bytes))
    }

    #[test]
    fn subscribe_orderbook_updates_registry_and_sends_wire_request() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        sender.subscribe_orderbook("BTCUSDT");
        assert!(registry.lock().unwrap().orderbook_subs.contains("BTCUSDT"));
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::API.to_byte());
        assert_eq!(
            method_id(&sent[0].data),
            Some(EngineMethod::SubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn pre_init_sender_subscription_records_intent_without_wire() {
        let (sender, registry, send_q, _, _, domain_ready) = make_sender();
        domain_ready.store(false, Ordering::Relaxed);

        sender.subscribe_orderbook("BTCUSDT");
        sender.subscribe_all_trades(true);
        sender.ui_mm_subscribe(false);

        {
            let registry = registry.lock().unwrap();
            assert!(registry.orderbook_subs.contains("BTCUSDT"));
            assert_eq!(
                registry.trades_sub,
                Some(TradesSubscription { want_mm: true })
            );
            assert_eq!(registry.mm_orders_sub, Some(false));
        }
        assert!(take_send_items(&send_q).is_empty());
    }

    #[test]
    fn unsubscribe_orderbook_updates_registry_and_sends_wire_request() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        registry
            .lock()
            .unwrap()
            .orderbook_subs
            .insert("ETHUSDT".to_string());
        sender.unsubscribe_orderbook("ETHUSDT");
        assert!(!registry.lock().unwrap().orderbook_subs.contains("ETHUSDT"));
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0].data),
            Some(EngineMethod::UnsubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn subscribe_orderbooks_sends_one_batched_wire_request() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        sender.subscribe_orderbooks(["BTCUSDT", "ETHUSDT"]);
        let registry = registry.lock().unwrap();
        assert!(registry.orderbook_subs.contains("BTCUSDT"));
        assert!(registry.orderbook_subs.contains("ETHUSDT"));
        drop(registry);
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0].data),
            Some(EngineMethod::SubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn unsubscribe_orderbooks_sends_one_batched_wire_request() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        registry
            .lock()
            .unwrap()
            .orderbook_subs
            .extend(["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        sender.unsubscribe_orderbooks(["BTCUSDT", "ETHUSDT"]);
        assert!(registry.lock().unwrap().orderbook_subs.is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0].data),
            Some(EngineMethod::UnsubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn unsubscribe_all_orderbooks_clears_registry_and_sends_existing_names() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        registry
            .lock()
            .unwrap()
            .orderbook_subs
            .insert("BTCUSDT".to_string());
        sender.unsubscribe_all_orderbooks();
        assert!(registry.lock().unwrap().orderbook_subs.is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0].data),
            Some(EngineMethod::UnsubscribeOrderBook.to_byte())
        );
        assert_eq!(market_names_count(&sent[0].data), Some(1));
    }

    #[test]
    fn subscribe_all_trades_carries_want_mm_flag() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        sender.subscribe_all_trades(true);
        sender.subscribe_all_trades(false);
        let registry = registry.lock().unwrap();
        assert_eq!(
            registry.trades_sub,
            Some(TradesSubscription { want_mm: false })
        );
        assert_eq!(registry.mm_orders_sub, Some(false));
        drop(registry);
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 2);
        assert!(sent
            .iter()
            .all(|item| method_id(&item.data) == Some(EngineMethod::SubscribeAllTrades.to_byte())));
    }

    #[test]
    fn unsubscribe_all_trades_clears_registry_and_sends_wire_request() {
        let (sender, registry, send_q, _, _, _) = make_sender();
        registry.lock().unwrap().trades_sub = Some(TradesSubscription { want_mm: true });
        sender.unsubscribe_all_trades();
        assert!(registry.lock().unwrap().trades_sub.is_none());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0].data),
            Some(EngineMethod::UnsubscribeAllTrades.to_byte())
        );
    }

    #[test]
    fn try_subscribe_returns_ok() {
        let (sender, _, _, _, _, _) = make_sender();
        assert!(sender.try_subscribe_orderbook("BTC").is_ok());
        assert!(sender.try_subscribe_orderbooks(["BTC", "ETH"]).is_ok());
        assert!(sender.try_subscribe_all_trades(true).is_ok());
    }

    #[test]
    fn try_subscribe_has_no_capacity_cap() {
        let (sender, _, _, _, _, _) = make_sender();
        for i in 0..4096 {
            assert!(
                sender.try_subscribe_orderbook(&format!("M{i}")).is_ok(),
                "unbounded event queue must not fail on local capacity"
            );
        }
    }

    #[test]
    fn try_subscribe_returns_disconnected_when_receiver_dropped() {
        let (sender, _, _, alive, _, _) = make_sender();
        alive.store(false, Ordering::Relaxed);
        let err = sender.try_unsubscribe_all_trades().unwrap_err();
        assert_eq!(err, SubscribeError::Disconnected);
    }

    #[test]
    fn disconnected_sender_stateful_action_does_not_mutate_or_send() {
        use crate::commands::trade::OrderWorkerStatus;

        let (sender, _, send_q, alive, _, _) = make_sender();
        alive.store(false, Ordering::Relaxed);
        let uid = 0x7777;
        let mut orders =
            tracked_orders_for_sender(uid, 17, 9, "DOGEUSDT", OrderWorkerStatus::SellSet);

        assert!(!sender.replace_order(&mut orders, uid, 50100.0));
        assert_eq!(orders.get(uid).unwrap().sell_price, 0.0);
        assert!(take_send_items(&send_q).is_empty());
    }

    #[test]
    fn sender_try_send_cmd_keyed_queues_send_item() {
        let (sender, _, send_q, _, _, _) = make_sender();
        let payload = vec![1, 2, 3, 4];
        let key = UniqueKey::order_move(42);

        sender
            .try_send_cmd_keyed(
                payload.clone(),
                Command::Order,
                SendPriority::High,
                true,
                3,
                key,
            )
            .expect("send command should enqueue");

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].data, payload);
        assert_eq!(sent[0].cmd, Command::Order.to_byte());
        assert_eq!(sent[0].priority, SendPriority::High);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 3);
        assert_eq!(sent[0].retry_left, 2);
        assert_eq!(sent[0].u_key, key);
    }

    #[test]
    fn sender_retry_left_clamps_zero_like_delphi() {
        let (sender, _, send_q, _, _, _) = make_sender();

        sender
            .try_send_cmd_keyed(
                vec![1, 2, 3, 4],
                Command::Order,
                SendPriority::High,
                true,
                0,
                UniqueKey::order_move(42),
            )
            .expect("send command should enqueue");

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].max_retries, 0);
        assert_eq!(
            sent[0].retry_left, 0,
            "Delphi clamps RetryLeft with Max(0, MaxRetryCount - 1)"
        );
    }

    #[test]
    fn sender_try_send_api_request_uses_sliced_api_defaults() {
        let (sender, _, send_q, _, _, _) = make_sender();
        let payload = crate::commands::engine_request::base_check();

        sender
            .try_send_api_request(payload.clone())
            .expect("api request should enqueue");

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].data, payload);
        assert_eq!(sent[0].cmd, Command::API.to_byte());
        assert_eq!(sent[0].priority, SendPriority::Sliced);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 6);
        assert_eq!(sent[0].retry_left, 5);
        assert_eq!(sent[0].u_key, UniqueKey::none());
    }

    #[test]
    fn pre_init_raw_sender_send_cmd_is_gated() {
        let (sender, _, send_q, _, _, domain_ready) = make_sender();
        domain_ready.store(false, Ordering::Relaxed);

        let err = sender
            .try_send_cmd_keyed(
                vec![1, 2, 3, 4],
                Command::Order,
                SendPriority::High,
                true,
                3,
                UniqueKey::order_move(42),
            )
            .unwrap_err();

        assert_eq!(err, SubscribeError::DomainNotReady);
        assert!(take_send_items(&send_q).is_empty());
    }

    #[test]
    fn pre_init_raw_sender_api_allows_only_init_methods() {
        let (sender, _, send_q, _, _, domain_ready) = make_sender();
        domain_ready.store(false, Ordering::Relaxed);

        let subscribe = crate::commands::engine_request::subscribe_all_trades(false);
        let err = sender.try_send_api_request(subscribe).unwrap_err();
        assert_eq!(err, SubscribeError::DomainNotReady);
        assert!(take_send_items(&send_q).is_empty());

        let balance_full = crate::commands::engine_request::get_markets_balance_full();
        let err = sender.try_send_api_request(balance_full).unwrap_err();
        assert_eq!(err, SubscribeError::DomainNotReady);
        assert!(take_send_items(&send_q).is_empty());

        let base_check = crate::commands::engine_request::base_check();
        sender
            .try_send_api_request(base_check.clone())
            .expect("BaseCheck is an Init primitive and must pass the pre-Init gate");

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].data, base_check);
        assert_eq!(sent[0].cmd, Command::API.to_byte());
    }

    #[test]
    fn cloned_sender_updates_same_registry_and_send_queues() {
        let (sender_a, registry, send_q, _, _, _) = make_sender();
        let sender_b = sender_a.clone();
        sender_a.subscribe_orderbook("A");
        sender_b.subscribe_orderbook("B");
        let registry = registry.lock().unwrap();
        assert!(registry.orderbook_subs.contains("A"));
        assert!(registry.orderbook_subs.contains("B"));
        drop(registry);
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 2);
        assert!(sent
            .iter()
            .all(|item| method_id(&item.data) == Some(EngineMethod::SubscribeOrderBook.to_byte())));
    }

    #[test]
    fn sender_replace_order_uses_client_wrapper_wire_defaults() {
        let (sender, _, send_q, _, _, _) = make_sender();
        let uid = 42;
        let mut orders = tracked_orders_for_sender(
            uid,
            17,
            9,
            "BTCUSDT",
            crate::commands::trade::OrderWorkerStatus::SellSet,
        );

        assert!(sender.replace_order(&mut orders, uid, 50100.0));

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        let item = &sent[0];
        assert_eq!(item.cmd, Command::Order.to_byte());
        assert_eq!(item.priority, SendPriority::High);
        assert!(item.encrypted);
        assert_eq!(item.max_retries, 3);
        assert_eq!(item.retry_left, 2);
        assert_eq!(item.u_key, UniqueKey::order_move(uid));

        match crate::commands::trade::TradeCommand::parse(&item.data)
            .expect("valid replace command")
        {
            crate::commands::trade::TradeCommand::OrderReplace(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, 42);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "BTCUSDT");
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn sender_ui_switches_mark_server_update_sent_and_keep_delphi_u_key_uid() {
        let (sender, _, send_q, _, server_update_sent, _) = make_sender();

        sender.ui_switch_dex("MainDex");
        sender.ui_switch_spot(1);

        assert!(server_update_sent.load(Ordering::Relaxed));

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 2);

        let dex_uid = command_uid(&sent[0].data).expect("dex wire UID");
        assert_eq!(sent[0].cmd, Command::UI.to_byte());
        assert_eq!(sent[0].priority, SendPriority::High);
        assert_eq!(sent[0].u_key, UniqueKey::dex_switch_for(dex_uid));

        let spot_uid = command_uid(&sent[1].data).expect("spot wire UID");
        assert_eq!(sent[1].cmd, Command::UI.to_byte());
        assert_eq!(sent[1].priority, SendPriority::High);
        assert_eq!(sent[1].u_key, UniqueKey::spot_switch_for(spot_uid));
    }

    #[test]
    fn sender_strat_snapshot_payload_uses_sliced_snapshot_u_key() {
        let (sender, _, send_q, _, _, _) = make_sender();

        sender.strat_send_snapshot_payload(1, 2, true, &[1, 2, 3]);

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::Strat.to_byte());
        assert_eq!(sent[0].priority, SendPriority::Sliced);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 6);
        assert_eq!(sent[0].retry_left, 5);
        assert_eq!(sent[0].u_key, UniqueKey::strat_snapshot());
    }

    #[test]
    fn sender_balance_request_refresh_uses_balance_channel_defaults() {
        let (sender, _, send_q, _, _, _) = make_sender();

        sender.balance_request_refresh();

        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::Balance.to_byte());
        assert_eq!(sent[0].priority, SendPriority::High);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 3);
        assert_eq!(sent[0].retry_left, 2);
        assert_eq!(sent[0].data.first().copied(), Some(5));
    }

    #[test]
    fn subscribe_error_displays_with_message() {
        // Просто проверка что Display impl работает (полезно для логирования).
        assert_eq!(
            format!("{}", SubscribeError::Disconnected),
            "Client queues disconnected"
        );
    }
}

#[cfg(test)]
mod client_subscribe_integration_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn ready_client() -> Client {
        let mut client = Client::new(dummy_cfg());
        client.set_domain_ready(true);
        client
    }

    #[test]
    fn client_retry_left_clamps_zero_like_delphi() {
        let client = ready_client();

        client.send_cmd_keyed(
            vec![1, 2, 3, 4],
            Command::UI,
            SendPriority::High,
            true,
            0,
            UniqueKey::base_ui_settings_slot(),
        );

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].max_retries, 0);
        assert_eq!(
            high[0].retry_left, 0,
            "Delphi clamps RetryLeft with Max(0, MaxRetryCount - 1)"
        );
    }

    fn command_uid(payload: &[u8]) -> Option<u64> {
        payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    }

    fn method_id(payload: &[u8]) -> Option<u8> {
        payload.get(11).copied()
    }

    fn market_names_count(payload: &[u8]) -> Option<i32> {
        let bytes: [u8; 4] = payload.get(14..18)?.try_into().ok()?;
        Some(i32::from_le_bytes(bytes))
    }

    fn drain_api_requests(client: &Client) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if item.cmd == Command::API.to_byte() {
                out.push(item.data);
            }
        }
        out
    }

    fn tracked_orders(
        uid: u64,
        currency: u8,
        platform: u8,
        market_name: &str,
        status: crate::commands::trade::OrderWorkerStatus,
        is_short: bool,
        immune_for_clicks: bool,
    ) -> crate::state::Orders {
        use crate::commands::trade::{
            BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, StopSettings,
            TradeCommand, TradeEpochHeader,
        };

        let mut orders = crate::state::Orders::new();
        let status_cmd = OrderStatus {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 4,
                        ver: 3,
                        uid,
                    },
                    currency,
                    platform,
                    market_name: market_name.to_string(),
                },
                epoch: 11,
                status,
            },
            buy_order: OrderCompact::default(),
            sell_order: OrderCompact::default(),
            stops: StopSettings::default(),
            strat_id: 0,
            is_short,
            db_id: 0,
            from_cache: false,
            emulator_mode: false,
            immune_for_clicks,
        };
        let _ = orders.apply(TradeCommand::OrderStatus(Box::new(status_cmd)));
        orders
    }

    fn assert_no_queued_wire(client: &Client) {
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
    }

    #[test]
    fn pre_init_subscription_intents_update_registry_without_wire() {
        let client = Client::new(dummy_cfg());

        client.subscribe_orderbook("BTCUSDT");
        client.subscribe_all_trades(true);
        client.ui_mm_subscribe(false);

        client.with_subscription_registry(|registry| {
            assert!(registry.orderbook_subs.contains("BTCUSDT"));
            assert_eq!(
                registry.trades_sub,
                Some(TradesSubscription { want_mm: true })
            );
            assert_eq!(registry.mm_orders_sub, Some(false));
        });
        assert_no_queued_wire(&client);
    }

    #[test]
    fn pre_init_stateful_order_actions_do_not_mutate_or_send() {
        use crate::commands::trade::{OrderWorkerStatus, StopSettings};

        let client = Client::new(dummy_cfg());
        let uid = 0x5151;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );

        assert!(!client.replace_order(&mut orders, uid, 50100.0));
        assert!(!client.update_order_stops(
            &mut orders,
            uid,
            &StopSettings {
                stop_loss_on: 1,
                sl_level: 12.5,
                ..StopSettings::default()
            }
        ));

        let order = orders.get(uid).expect("local order remains");
        assert_eq!(order.sell_price, 0.0);
        assert_eq!(order.stops, StopSettings::default());
        assert_no_queued_wire(&client);
    }

    #[test]
    fn post_init_flush_sends_pre_init_registry_subscriptions_once() {
        let mut client = Client::new(dummy_cfg());

        client.subscribe_orderbook("BTCUSDT");
        client.subscribe_all_trades(true);
        assert_no_queued_wire(&client);

        client.set_domain_ready(true);
        client.send_registry_subscriptions_after_init();
        let sent = drain_api_requests(&client);
        let methods: Vec<_> = sent
            .iter()
            .filter_map(|payload| method_id(payload))
            .collect();
        assert_eq!(methods.len(), 2);
        assert!(methods.contains(&(EngineMethod::SubscribeAllTrades.to_byte())));
        assert!(methods.contains(&(EngineMethod::SubscribeOrderBook.to_byte())));

        client.subscribe_all_trades(true);
        let sent = drain_api_requests(&client);
        assert!(
            sent.is_empty(),
            "same all-trades intent after init must not duplicate the pre-init flush"
        );
    }

    #[test]
    fn client_subscribe_orderbook_updates_registry_and_wire_queue_through_sender() {
        let client = ready_client();
        client.subscribe_orderbook("BTCUSDT");
        assert!(client
            .with_subscription_registry(|registry| registry.orderbook_subs.contains("BTCUSDT")));
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn client_sender_can_be_held_independently_of_client() {
        // Sender держит clone; даже если client держится по `&` ссылке — sender
        // независим. Это база для multi-thread субскрайба без app-event backlog.
        let client = ready_client();
        let sender = client.sender();
        sender.subscribe_all_trades(true);
        assert_eq!(
            client.with_subscription_registry(|registry| registry.trades_sub),
            Some(TradesSubscription { want_mm: true })
        );
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeAllTrades.to_byte())
        );
    }

    #[test]
    fn cancel_tracked_order_uses_order_state_context() {
        use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

        let uid = 0x1122_3344_5566_7788;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        let client = ready_client();

        assert!(client.cancel_tracked_order(&mut orders, uid));

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        let item = &high[0];
        assert_eq!(item.cmd, Command::Order.to_byte());
        assert_eq!(item.priority, SendPriority::High);
        assert_eq!(item.max_retries, 3);
        assert_eq!(item.u_key, UniqueKey::order_move(uid));

        match TradeCommand::parse(&item.data).expect("valid cancel command") {
            TradeCommand::OrderCancel(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.epoch_header.epoch, 0);
                assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::SellSet);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn client_cancel_pending_order_matches_delphi_replace_then_cancel_effect() {
        use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

        let uid = 0x1188;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::None,
            false,
            false,
        );
        let client = ready_client();

        assert!(client.cancel_order(&mut orders, uid));
        assert!(
            orders.get(uid).unwrap().pending_cancel,
            "Delphi keeps vOrder.PendingCancel set on a pending order"
        );

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(
            high.len(),
            1,
            "Delphi SendCmdInt UKey-dedups the immediate replace before the cancel if the writer has not copied it"
        );
        assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
        match TradeCommand::parse(&high[0].data).expect("valid cancel command") {
            TradeCommand::OrderCancel(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::None);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn client_replace_order_uses_delphi_local_gate() {
        use crate::commands::trade::{OrderType, OrderWorkerStatus, TradeCommand};

        let uid = 0x2233;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        let client = ready_client();

        assert!(client.replace_order(&mut orders, uid, 50100.0));
        assert_eq!(orders.get(uid).unwrap().sell_price, 50100.0);
        assert!(orders.get(uid).unwrap().bulk_replace_sell);

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
        match TradeCommand::parse(&high[0].data).expect("valid replace command") {
            TradeCommand::OrderReplace(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.order_type, OrderType::Sell);
                assert_eq!(cmd.new_price, 50100.0);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }

        assert!(
            !client.replace_order(&mut orders, uid, 50200.0),
            "Delphi ReplaceSentTime gate suppresses a second replace while in flight"
        );
        assert_eq!(orders.get(uid).unwrap().sell_price, 50200.0);
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());
    }

    #[test]
    fn client_turn_order_panic_sell_uses_delphi_local_gate() {
        use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

        let uid = 0x3344;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        let client = ready_client();

        assert!(
            !client.turn_order_panic_sell(&mut orders, uid, false),
            "initial PrevPanicSell=false suppresses redundant false"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());

        assert!(client.turn_order_panic_sell(&mut orders, uid, true));
        assert!(orders.get(uid).unwrap().panic_sell);
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
        match TradeCommand::parse(&high[0].data).expect("valid panic command") {
            TradeCommand::TurnPanicSell(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert!(cmd.turn_on);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }

        assert!(!client.turn_order_panic_sell(&mut orders, uid, true));
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());
    }

    #[test]
    fn client_switch_panic_sell_by_market_matches_delphi_button_semantics() {
        use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

        let uid_a = 0x3345;
        let uid_b = 0x3346;
        let mut orders = tracked_orders(
            uid_a,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        let status_cmd = {
            use crate::commands::trade::{
                BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, StopSettings,
                TradeCommand, TradeEpochHeader,
            };
            TradeCommand::OrderStatus(Box::new(OrderStatus {
                epoch_header: TradeEpochHeader {
                    market: MarketCommandHeader {
                        base: BaseCommandHeader {
                            cmd_id: 4,
                            ver: 3,
                            uid: uid_b,
                        },
                        currency: 17,
                        platform: 9,
                        market_name: "DOGEUSDT".to_string(),
                    },
                    epoch: 11,
                    status: OrderWorkerStatus::SellSet,
                },
                buy_order: OrderCompact::default(),
                sell_order: OrderCompact::default(),
                stops: StopSettings::default(),
                strat_id: 0,
                is_short: false,
                db_id: 0,
                from_cache: false,
                emulator_mode: false,
                immune_for_clicks: false,
            }))
        };
        let _ = orders.apply(status_cmd);
        let client = ready_client();

        assert!(client.switch_panic_sell_by_market(&mut orders, "DOGEUSDT", true));
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 2);
        for item in &high {
            match TradeCommand::parse(&item.data).expect("valid panic command") {
                TradeCommand::TurnPanicSell(cmd) => {
                    assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                    assert!(cmd.turn_on);
                }
                other => panic!("unexpected trade command: {other:?}"),
            }
        }

        assert!(!client.switch_panic_sell_by_market(&mut orders, "DOGEUSDT", true));
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 2);
        for item in &high {
            match TradeCommand::parse(&item.data).expect("valid panic command") {
                TradeCommand::TurnPanicSell(cmd) => assert!(!cmd.turn_on),
                other => panic!("unexpected trade command: {other:?}"),
            }
        }
    }

    #[test]
    fn client_move_all_sells_uses_delphi_pre_send_gate() {
        use crate::commands::trade::{
            FixedPosition, MoveAllCmdType, MoveAllSellsParams, OrderWorkerStatus, PriceZone,
            ReplaceMultiKind, TradeCommand, TradeCtx,
        };

        let params = MoveAllSellsParams {
            cmd_type: MoveAllCmdType::MoveKind,
            move_kind: ReplaceMultiKind::TopVol,
            price: 50100.0,
            price_zone: PriceZone {
                min_p: 49_500.0,
                max_p: 50_500.0,
            },
            side: FixedPosition::Long,
        };
        let ctx = TradeCtx::with_route(0xCAFE, 17, 9);
        let client = ready_client();
        let empty_orders = crate::state::Orders::new();

        assert!(
            !client.move_all_sells(&empty_orders, ctx, "DOGEUSDT", params),
            "Delphi active-client branch sends nothing without a matching order"
        );
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());

        let orders = tracked_orders(
            7,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        assert!(client.move_all_sells(&orders, ctx, "DOGEUSDT", params));

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid move all sells") {
            TradeCommand::MoveAllSells(cmd) => {
                assert_eq!(cmd.market.base.uid, ctx.uid);
                assert_eq!(cmd.market.currency, 17);
                assert_eq!(cmd.market.platform, 9);
                assert_eq!(cmd.cmd_type, MoveAllCmdType::MoveKind.to_byte());
                assert_eq!(cmd.move_kind, ReplaceMultiKind::TopVol);
                assert_eq!(cmd.side, FixedPosition::Long);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn client_move_all_buys_uses_buy_only_cmd_type_and_delphi_gate() {
        use crate::commands::trade::{
            FixedPosition, MoveAllBuysCmdType, OrderWorkerStatus, ReplaceMultiKind, TradeCommand,
            TradeCtx,
        };

        let ctx = TradeCtx::with_route(0xBEEF, 17, 9);
        let client = ready_client();
        let immune_orders =
            tracked_orders(8, 17, 9, "DOGEUSDT", OrderWorkerStatus::BuySet, false, true);

        assert!(
            !client.move_all_buys(
                &immune_orders,
                ctx,
                "DOGEUSDT",
                MoveAllBuysCmdType::MoveKind,
                ReplaceMultiKind::TopVol,
                50100.0,
                FixedPosition::Long,
            ),
            "MoveKind buy overload checks not ImmuneForClicks"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());

        assert!(client.move_all_buys(
            &immune_orders,
            ctx,
            "DOGEUSDT",
            MoveAllBuysCmdType::Pers,
            ReplaceMultiKind::None,
            1.5,
            FixedPosition::Short,
        ));

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid move all buys") {
            TradeCommand::MoveAllBuys(cmd) => {
                assert_eq!(cmd.market.base.uid, ctx.uid);
                assert_eq!(cmd.cmd_type, MoveAllBuysCmdType::Pers.to_byte());
                assert_eq!(cmd.move_kind, ReplaceMultiKind::None);
                assert_eq!(cmd.side, FixedPosition::Short);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn client_set_immune_applies_local_side_effect_before_wire_send() {
        use crate::commands::trade::{ImmuneItem, OrderWorkerStatus, TradeCommand};

        let mut orders = tracked_orders(
            0x100,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        let client = ready_client();
        let items = [
            ImmuneItem {
                uid: 0x100,
                value: true,
            },
            ImmuneItem {
                uid: 0x200,
                value: true,
            },
        ];

        assert!(client.set_immune(&mut orders, &items));
        assert!(orders.get(0x100).unwrap().immune_for_clicks);

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].u_key, UniqueKey::immune_clicks(0x100));
        match TradeCommand::parse(&high[0].data).expect("valid set immune") {
            TradeCommand::SetImmune(cmd) => {
                assert_eq!(cmd.items.len(), 1);
                assert_eq!(cmd.items[0].uid, 0x100);
                assert!(cmd.items[0].value);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }

        assert!(
            !client.set_immune(
                &mut orders,
                &[ImmuneItem {
                    uid: 0x200,
                    value: false,
                }],
            ),
            "Delphi does not send if no local worker was found"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());
    }

    #[test]
    fn client_update_order_stops_uses_delphi_send_if_changed_gate() {
        use crate::commands::trade::{OrderWorkerStatus, StopSettings, TradeCommand};

        let uid = 0x4444;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::BuySet,
            false,
            false,
        );
        let client = ready_client();

        assert!(
            !client.update_order_stops(&mut orders, uid, &StopSettings::default()),
            "Delphi SendStopsIfChanged exits when Cur == FPrevStops"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());

        let stops = StopSettings {
            stop_loss_on: 1,
            sl_level: 12.5,
            use_take_profit: 1,
            take_profit: 15.0,
            ..StopSettings::default()
        };
        assert!(
            !client.update_order_stops(&mut orders, uid, &stops),
            "Delphi SendStopsIfChanged exits when worker.vOrder is nil"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());

        assert!(orders.mark_local_visual_order(uid));
        assert!(client.update_order_stops(&mut orders, uid, &stops));
        assert_eq!(orders.get(uid).unwrap().stops, stops);

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
        match TradeCommand::parse(&high[0].data).expect("valid stops update") {
            TradeCommand::OrderStopsUpdate(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.epoch_header.epoch, 0);
                assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::BuySet);
                assert_eq!(cmd.stops, stops);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }

        assert!(
            !client.update_order_stops(&mut orders, uid, &stops),
            "FPrevStops/current state was updated before queueing"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());
    }

    #[test]
    fn client_update_vstop_uses_delphi_send_if_changed_gate() {
        use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

        let uid = 0x5555;
        let mut orders = tracked_orders(
            uid,
            17,
            9,
            "DOGEUSDT",
            OrderWorkerStatus::SellSet,
            false,
            false,
        );
        let client = ready_client();

        assert!(
            !client.update_vstop(&mut orders, uid, false, false, 0.0, 0.0),
            "Delphi SendVStopIfChanged exits when fields equal FPrevVStop*"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());

        assert!(
            !client.update_vstop(&mut orders, uid, true, false, 12.5, 100.0),
            "Delphi SendVStopIfChanged exits when worker.vOrder is nil"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());

        assert!(orders.mark_local_visual_order(uid));
        assert!(client.update_vstop(&mut orders, uid, true, false, 12.5, 100.0));
        let order = orders.get(uid).unwrap();
        assert!(order.vstop_on);
        assert_eq!(order.vstop_level, 12.5);
        assert_eq!(order.vstop_vol, 100.0);

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
        match TradeCommand::parse(&high[0].data).expect("valid VStop update") {
            TradeCommand::VStopUpdate(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.epoch_header.epoch, 0);
                assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::SellSet);
                assert!(cmd.vstop_on);
                assert!(!cmd.vstop_fixed);
                assert_eq!(cmd.vstop_level, 12.5);
                assert_eq!(cmd.vstop_vol, 100.0);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }

        assert!(
            !client.update_vstop(&mut orders, uid, true, false, 12.5, 100.0),
            "FPrevVStop* current state was updated before queueing"
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert!(high.is_empty());
    }

    #[test]
    fn client_ui_mm_subscribe_updates_registry_and_pushes_keyed_send() {
        let client = ready_client();
        client.ui_mm_subscribe(true);

        assert_eq!(
            client.with_subscription_registry(|registry| registry.mm_orders_sub),
            Some(true)
        );
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        let item = &high[0];
        assert_eq!(
            Client::outgoing_mm_orders_subscribe_intent(item),
            Some(true)
        );
        let uid = command_uid(&item.data).expect("wire command UID");
        assert_eq!(item.u_key, UniqueKey::turn_mm_detection_for(uid));
    }

    #[test]
    fn ui_switches_use_delphi_command_uid_in_u_key() {
        let client = ready_client();

        client.ui_switch_dex("MainDex");
        client.ui_switch_spot(1);

        let (mut sent, mut high, mut low) = client.take_send_queues_for_test();
        sent.append(&mut high);
        sent.append(&mut low);
        assert_eq!(sent.len(), 2);

        let dex_uid = command_uid(&sent[0].data).expect("dex wire UID");
        assert_eq!(sent[0].cmd, Command::UI.to_byte());
        assert_eq!(sent[0].u_key, UniqueKey::dex_switch_for(dex_uid));

        let spot_uid = command_uid(&sent[1].data).expect("spot wire UID");
        assert_eq!(sent[1].cmd, Command::UI.to_byte());
        assert_eq!(sent[1].u_key, UniqueKey::spot_switch_for(spot_uid));
    }

    #[test]
    fn ui_single_slot_commands_use_delphi_fixed_u_key_uid() {
        let client = ready_client();

        let settings = crate::commands::ui::ClientSettingsCommand::default();
        client.ui_send_settings(&settings);

        let lev = crate::commands::ui::LevManage {
            uid: 0,
            cmd_ver: 1,
            auto_max_order: false,
            auto_lev_up: false,
            auto_isolated: false,
            auto_cross: false,
            auto_fix_lev: false,
            fix_lev: 0,
            tlg_report: false,
            lev_control: String::new(),
        };
        client.ui_lev_manage(&lev);

        let (mut sent, mut high, mut low) = client.take_send_queues_for_test();
        sent.append(&mut high);
        sent.append(&mut low);
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].u_key, UniqueKey::base_ui_settings_slot());
        assert_eq!(sent[1].u_key, UniqueKey::lev_manage_settings_slot());
    }

    #[test]
    fn subscribe_orderbook_inserts_into_registry() {
        let client = ready_client();
        client.subscribe_orderbook("BTC");
        assert!(
            client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTC"))
        );
    }

    #[test]
    fn subscribe_orderbooks_inserts_batched_orderbooks_into_registry() {
        let client = ready_client();
        client.subscribe_orderbooks(["BTC", "ETH", "BTC"]);
        client.with_subscription_registry(|registry| {
            assert_eq!(registry.orderbook_subs.len(), 2);
            assert!(registry.orderbook_subs.contains("BTC"));
            assert!(registry.orderbook_subs.contains("ETH"));
        });
    }

    #[test]
    fn unsubscribe_orderbook_removes_from_registry() {
        let client = ready_client();
        client.subscribe_orderbook("BTC");
        client.unsubscribe_orderbook("BTC");
        assert!(
            !client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTC"))
        );
    }

    #[test]
    fn batched_unsubscribe_orderbooks_removes_from_registry() {
        let client = Client::new(dummy_cfg());
        client.subscribe_orderbooks(["BTC", "ETH", "XRP"]);
        client.unsubscribe_orderbooks(["ETH", "DOGE"]);
        client.with_subscription_registry(|registry| {
            assert!(registry.orderbook_subs.contains("BTC"));
            assert!(!registry.orderbook_subs.contains("ETH"));
            assert!(registry.orderbook_subs.contains("XRP"));
        });
    }

    #[test]
    fn unsubscribe_all_orderbooks_clears_registry_and_sends_existing_names() {
        let client = ready_client();
        client.subscribe_orderbooks(["BTC", "ETH"]);
        let _ = drain_api_requests(&client);
        client.unsubscribe_all_orderbooks();
        assert!(client.with_subscription_registry(|registry| registry.orderbook_subs.is_empty()));
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::UnsubscribeOrderBook.to_byte())
        );
        assert_eq!(market_names_count(&sent[0]), Some(2));
    }

    #[test]
    fn unsubscribe_all_orderbooks_with_empty_registry_sends_no_wire() {
        let client = ready_client();
        client.unsubscribe_all_orderbooks();
        assert!(client.with_subscription_registry(|registry| registry.orderbook_subs.is_empty()));
        assert!(drain_api_requests(&client).is_empty());
    }

    #[test]
    fn subscribe_orderbook_is_idempotent() {
        // Двойной subscribe для одной пары не должен иметь побочных эффектов
        // в registry (HashSet dedup) и не должен слать второй wire-запрос.
        let client = ready_client();
        client.subscribe_orderbook("ETH");
        client.subscribe_orderbook("ETH");
        assert_eq!(
            client.with_subscription_registry(|registry| registry.orderbook_subs.len()),
            1
        );
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
    }

    #[test]
    fn subscribe_all_trades_sets_registry() {
        let client = Client::new(dummy_cfg());
        client.subscribe_all_trades(true);
        assert_eq!(
            client.with_subscription_registry(|registry| registry.trades_sub),
            Some(TradesSubscription { want_mm: true }),
        );
        assert_eq!(
            client.with_subscription_registry(|registry| registry.mm_orders_sub),
            Some(true)
        );
        assert!(
            client.with_subscription_registry(|registry| registry.trades_storage_scope.is_all())
        );
        // Повторный с другим want_mm — обновляет registry.
        client.subscribe_all_trades(false);
        assert_eq!(
            client.with_subscription_registry(|registry| registry.trades_sub),
            Some(TradesSubscription { want_mm: false }),
        );
        assert_eq!(
            client.with_subscription_registry(|registry| registry.mm_orders_sub),
            Some(false)
        );
        assert!(
            client.with_subscription_registry(|registry| registry.trades_storage_scope.is_all())
        );
    }

    #[test]
    fn subscribe_trades_for_sets_storage_scope_without_changing_wire_shape() {
        let client = ready_client();
        client.subscribe_trades_for(true, ["ETHUSDT", "BTCUSDT", "ETHUSDT"]);
        client.with_subscription_registry(|registry| {
            assert_eq!(
                registry.trades_sub,
                Some(TradesSubscription { want_mm: true })
            );
            assert_eq!(registry.mm_orders_sub, Some(true));
            assert!(registry.trades_storage_scope.contains("BTCUSDT"));
            assert!(registry.trades_storage_scope.contains("ETHUSDT"));
            assert!(!registry.trades_storage_scope.contains("SOLUSDT"));
        });
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeAllTrades.to_byte())
        );
    }

    #[test]
    fn unsubscribe_all_trades_clears_registry() {
        let client = Client::new(dummy_cfg());
        client.subscribe_all_trades(true);
        client.unsubscribe_all_trades();
        assert!(client.with_subscription_registry(|registry| registry.trades_sub.is_none()));
        assert!(client.trades_storage_scope_intent().is_none());
        assert!(crate::events::ActiveDispatchContext::from_client(&client)
            .trades_storage_scope
            .is_none());
        assert_eq!(
            client.with_subscription_registry(|registry| registry.mm_orders_sub),
            Some(true),
            "Delphi UnsubscribeAllTrades clears IsTradesSubscribed but not the MM flag",
        );
    }

    #[test]
    fn apply_mm_orders_subscribe_keeps_all_trades_want_mm() {
        let mut client = Client::new(dummy_cfg());
        client.subscribe_all_trades(false);
        let _ = client.take_send_queues_for_test(); // drain SubscribeAllTrades send command

        client.apply_mm_orders_subscribe_intent(true);

        assert_eq!(
            client.with_subscription_registry(|registry| registry.mm_orders_sub),
            Some(true)
        );
        assert_eq!(
            client.with_subscription_registry(|registry| registry.trades_sub),
            Some(TradesSubscription { want_mm: false }),
        );
    }
}

#[cfg(test)]
mod pmtu_tests {
    use super::*;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn unpack_client_packet(mac_key: &MoonKey, raw: &[u8]) -> (u8, Vec<u8>) {
        const CLIENT_HDR_SIZE: usize = 15;
        let mut buf = raw.to_vec();
        moonproto_transport::outer_light_crypt(&mut buf, mac_key);
        let hdr = moonproto_transport::ClientMsgHeader::from_bytes(&buf).unwrap();
        let saved = [buf[1], buf[2], buf[3], buf[4]];
        buf[1..5].copy_from_slice(&0u32.to_le_bytes());
        let mac = moonproto_transport::MacContext::new(mac_key).mac(&buf);
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
    fn full_reset_preserves_sending_and_api_slots_like_delphi_reset() {
        let mut client = Client::new(dummy_cfg());
        client.sending.push(sent_sliced_with_lengths(&[8], 0));
        client.pending_h.push(pending_h_item(42));
        let _rx = client.api_pending.register(0x4455);

        client.crypt_msg_counter.store(77, Ordering::Relaxed);
        client.total_sent.store(1234, Ordering::Relaxed);
        client.total_recv = 5678;
        client.rs = 0.25;
        client.used_sliced_limit = true;
        client.recvd_slider.has_new_data = true;
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
            client.api_pending.pending_count(),
            1,
            "API waiters are not part of Delphi TMoonProtoClient.Reset"
        );
        assert_eq!(client.crypt_msg_counter.load(Ordering::Relaxed), 0);
        assert_eq!(client.total_sent(), 0);
        assert_eq!(client.total_recv, 0);
        assert_eq!(client.rs, 1.0);
        assert!(!client.used_sliced_limit);
        assert!(!client.recvd_slider.has_new_data);
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
        let total_sent = writer.client.total_sent.load(Ordering::Relaxed);
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

        let delivered =
            process_ping_reader_msg(&mut client, &ping_payload_with_pmtu(8_224), 0.0, 0.0);

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
        assert!(!client.recvd_slider.has_new_data);

        ProtocolCore {
            client: &mut client,
        }
        .copy_recvd_data();
        assert!(!client.send_lock.lock().unwrap().tmp_slider.has_new_data);
        assert!(client.recvd_slider.has_new_data);

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
        assert!(client.recvd_slider.has_new_data);

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
    fn sliced_u_key_cleanup_does_not_drop_pending_h_like_delphi() {
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
    fn high_u_key_cleanup_runs_after_regular_ack_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let key = UniqueKey::order_move(42);

        let mut acked_same_key = pending_h_item(42);
        acked_same_key.u_key = key;
        client.pending_h.push(acked_same_key);
        let mut not_acked_same_key = pending_h_item(43);
        not_acked_same_key.u_key = key;
        client.pending_h.push(not_acked_same_key);

        {
            client.recvd_slider.start_num = 40;
            client.recvd_slider.bit_field[0] = 1 << 2;
            client.recvd_slider.has_new_data = true;
            client.recvd_slider.r_count = 1;
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
    fn create_sliced_object_queues_without_immediate_send_like_delphi() {
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
    fn sliced_size_check_uses_compressed_size_like_delphi() {
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
    fn encrypted_empty_sliced_is_dropped_before_crypt_like_delphi() {
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

        let wire_len =
            u16::from_le_bytes([client.tmp_send_buf[1], client.tmp_send_buf[2]]) as usize;
        assert_eq!(client.tmp_send_buf[0], Command::Crypted.to_byte());
        assert_eq!(wire_len, 60);
        assert_eq!(client.tmp_send_buf.len(), 3 + wire_len);
        assert_eq!(client.tmp_send_size, 15 + 3 + wire_len);
    }

    #[test]
    fn do_send_mp_data_sends_current_item_direct_when_buffer_is_smaller_like_delphi() {
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();

        let mut cfg = dummy_cfg();
        cfg.server_port = server_addr.port();
        let mut client = Client::new(cfg);
        client.socket = Some(client_sock);
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
    fn low_priority_items_are_split_around_sliced_retry_like_delphi() {
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();

        let mut cfg = dummy_cfg();
        cfg.server_port = server_addr.port();
        let mut client = Client::new(cfg);
        client.socket = Some(client_sock);
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
    fn sliced_retry_client_limit_is_rounded_like_delphi() {
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
    fn sliced_retry_used_limit_threshold_is_rounded_like_delphi() {
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
    fn sliced_retry_updates_trip_delay_k_before_path_delay_like_delphi() {
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
    fn sliced_retry_clock_ignores_acked_blocks_like_delphi_apply_ack_removes_them() {
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
    fn sliced_ack_applies_only_first_matching_datagram_like_delphi() {
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
    fn sliced_ack_reader_queues_writer_applies_like_delphi() {
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
    fn writer_tick_copies_ack_queues_then_check_sening_data_like_delphi() {
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
    fn send_lock_snapshot_copies_send_acks_and_tmp_slider_atomically_like_delphi() {
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
        assert!(client.recvd_slider.has_new_data);
        let send_lock = client.send_lock.lock().unwrap();
        assert!(send_lock.send_queues.is_empty());
        assert!(send_lock.incoming_sliced_acks.is_empty());
        assert!(
            !send_lock.tmp_slider.has_new_data,
            "Delphi FClient.CopyRecvdData clears TmpSlider in the same SendLock snapshot"
        );
    }
}

#[cfg(test)]
mod api_retry_tests {
    use super::*;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    #[test]
    fn engine_api_sliced_requests_use_delphi_retry_count() {
        let mut client = Client::new(dummy_cfg());
        client.set_domain_ready(true);
        let raw = crate::commands::engine_request::query_hedge_mode();

        client.send_api_request(&raw);

        let (sliced, _, _) = client.take_send_queues_for_test();
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced[0].cmd, Command::API.to_byte());
        assert_eq!(sliced[0].priority, SendPriority::Sliced);
        assert_eq!(sliced[0].max_retries, 6);
        assert_eq!(sliced[0].retry_left, 5);
    }
}

#[cfg(test)]
mod send_queue_dedup_tests {
    use super::*;

    fn item(kind: u8, uid: u64, marker: u8) -> SendItem {
        SendItem {
            data: vec![marker],
            cmd: Command::Order.to_byte(),
            encrypted: true,
            priority: SendPriority::High,
            retry_left: 2,
            max_retries: 3,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey { kind, uid },
        }
    }

    fn item_with_priority(kind: u8, uid: u64, marker: u8, priority: SendPriority) -> SendItem {
        SendItem {
            priority,
            ..item(kind, uid, marker)
        }
    }

    #[test]
    fn send_cmd_int_queue_removes_first_matching_sliced_or_high_before_append() {
        let mut queues = SendQueues::default();
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 1, SendPriority::High));
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 8, 2, SendPriority::High));
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 3, SendPriority::High));
        queues.push_send_cmd_int(item_with_priority(
            UK_ORDER_MOVE,
            7,
            4,
            SendPriority::Sliced,
        ));

        assert_eq!(
            queues
                .high
                .iter()
                .map(|item| item.data[0])
                .collect::<Vec<_>>(),
            vec![2, 3],
            "Delphi SendCmdInt removes only from the selected High queue"
        );
        assert_eq!(
            queues
                .sliced
                .iter()
                .map(|item| item.data[0])
                .collect::<Vec<_>>(),
            vec![4],
            "Sliced queue has its own UKey scope"
        );
    }

    #[test]
    fn send_cmd_int_queue_does_not_dedup_low_priority_like_delphi() {
        let mut queues = SendQueues::default();
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 1, SendPriority::Low));
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 2, SendPriority::Low));

        assert_eq!(
            queues
                .low
                .iter()
                .map(|item| item.data[0])
                .collect::<Vec<_>>(),
            vec![1, 2],
            "Delphi SendCmdInt UKey removal is only for Sliced and High"
        );
    }
}

#[cfg(test)]
mod active_library_helpers_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use std::sync::{Arc, Mutex};

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn writer(client: &mut Client) -> ProtocolCore<'_> {
        ProtocolCore { client }
    }

    #[test]
    fn bind_failed_event_waits_for_elapsed_threshold() {
        let mut client = Client::new(dummy_cfg());
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        client.on_lifecycle(Box::new(move |ev| sink.lock().unwrap().push(ev)));

        client.record_bind_failure(1_000);
        client.record_bind_failure(1_005);
        client.record_bind_failure(1_010);
        assert!(
            events.lock().unwrap().is_empty(),
            "три быстрые серии bind errors не должны сразу шуметь в UI",
        );

        client.record_bind_failure(16_000);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], LifecycleEvent::BindFailed { .. }));
    }

    #[test]
    fn bind_failed_event_repeats_only_after_throttle_window() {
        let mut client = Client::new(dummy_cfg());
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        client.on_lifecycle(Box::new(move |ev| sink.lock().unwrap().push(ev)));

        client.record_bind_failure(0);
        client.record_bind_failure(15_000);
        client.record_bind_failure(20_000);
        assert_eq!(events.lock().unwrap().len(), 1);

        client.record_bind_failure(65_000);
        assert_eq!(events.lock().unwrap().len(), 2);
    }

    #[test]
    fn bind_failure_tracking_resets_after_successful_bind() {
        let mut client = Client::new(dummy_cfg());
        client.record_bind_failure(0);
        client.record_bind_failure(15_000);
        assert!(client.bind_failure_streak > 0);

        client.reset_bind_failure_tracking();

        assert_eq!(client.bind_failure_streak, 0);
        assert_eq!(client.first_bind_failure_ms, NEVER_TIME_MS);
        assert_eq!(client.last_bind_failed_event_ms, NEVER_TIME_MS);
    }

    // =====================================================================
    //  check_indexes_fetch_timeout
    // =====================================================================

    #[test]
    fn indexes_fetch_timeout_does_nothing_when_not_in_flight() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = false;
        client.indexes_fetch_started_ms = 0;
        writer(&mut client).check_indexes_fetch_timeout(100_000_000);
        assert!(!client.indexes_fetch_in_flight);
    }

    #[test]
    fn indexes_fetch_timeout_preserves_in_flight_within_window() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        // 5 секунд прошло — меньше 12с timeout.
        writer(&mut client).check_indexes_fetch_timeout(5_000);
        assert!(
            client.indexes_fetch_in_flight,
            "в пределах timeout — флаг сохраняется"
        );
    }

    #[test]
    fn indexes_fetch_timeout_clears_in_flight_after_window() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        client.peer_app_token = 0; // не triggers re-send (нет mismatch)
        client.tracked_indexes_peer_app_token = 0;
        // 13 секунд — больше 12с timeout.
        writer(&mut client).check_indexes_fetch_timeout(13_000);
        assert!(
            !client.indexes_fetch_in_flight,
            "после timeout без peer_app_token mismatch — флаг сбрасывается"
        );
    }

    #[test]
    fn indexes_fetch_timeout_does_not_retry_without_init_intent() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        // PeerAppToken расходится, но единственный Init ещё не заказывал индексы.
        client.peer_app_token = 0xABC;
        client.tracked_indexes_peer_app_token = 0xDEF;
        client.set_domain_ready(true);
        writer(&mut client).check_indexes_fetch_timeout(13_000);
        assert!(
            !client.indexes_fetch_in_flight,
            "timeout cleanup только сбрасывает marker"
        );
        assert_eq!(
            client.indexes_fetch_started_ms, 0,
            "no re-send means started timestamp is unchanged"
        );
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
    }

    #[test]
    fn indexes_fetch_timeout_retries_after_init_intent() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        client.peer_app_token = 0xABC;
        client.tracked_indexes_peer_app_token = 0xDEF;
        client.set_domain_ready(true);
        client.domain_restore.fetch_indexes = true;

        writer(&mut client).check_indexes_fetch_timeout(13_000);

        assert!(client.indexes_fetch_in_flight);
        assert_eq!(client.indexes_fetch_started_ms, 13_000);
        let (sliced, _, _) = client.take_send_queues_for_test();
        assert_eq!(
            sliced.len(),
            1,
            "post-init timeout must retry GetMarketsIndexes"
        );
        assert_eq!(sliced[0].cmd, Command::API.to_byte());
        assert_eq!(
            sliced[0].data.get(11).copied(),
            Some(EngineMethod::GetMarketsIndexes.to_byte())
        );
    }

    #[test]
    fn indexes_fetch_timeout_zero_peer_token_does_not_re_send() {
        // Если peer_app_token = 0 (никогда не подключались) → не re-send даже если mismatch.
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        client.peer_app_token = 0;
        client.tracked_indexes_peer_app_token = 0xABC;
        writer(&mut client).check_indexes_fetch_timeout(13_000);
        assert!(
            !client.indexes_fetch_in_flight,
            "peer_app_token=0 (не подключены) → не re-send, флаг сброшен"
        );
    }
}

#[cfg(test)]
mod registry_subscription_restore_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    /// Извлекает `EngineMethod` ID из wire-payload Engine request'а.
    /// Header: CmdId(1) + ver(2) + UID(8) = 11 байт → Method на offset 11.
    fn method_id(payload: &[u8]) -> Option<u8> {
        if payload.len() < 12 {
            return None;
        }
        Some(payload[11])
    }

    fn command_uid(payload: &[u8]) -> Option<u64> {
        payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    }

    fn subscribe_all_trades_want_mm(payload: &[u8]) -> Option<bool> {
        if method_id(payload)? != EngineMethod::SubscribeAllTrades.to_byte() {
            return None;
        }
        payload.last().map(|v| *v != 0)
    }

    /// Дренирует send queues клиента, собирая wire-payload'ы отправленных API-запросов.
    fn drain_api_requests(client: &Client) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if item.cmd == Command::API.to_byte() {
                out.push(item.data);
            }
        }
        out
    }

    fn drain_send_items(client: &Client) -> Vec<SendItem> {
        let (mut sliced, mut high, mut low) = client.take_send_queues_for_test();
        sliced.append(&mut high);
        sliced.append(&mut low);
        sliced
    }

    fn mark_post_init(client: &mut Client) {
        client.set_domain_ready(true);
    }

    #[test]
    fn restore_with_empty_registry_sends_nothing() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.server_token = 0xCAFE;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert!(sent.is_empty(), "пустой registry → 0 wire-запросов");
    }

    #[test]
    fn restore_trades_only_sends_single_subscribe_all_trades() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: true });
        });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1, "только trades → 1 wire-запрос");
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeAllTrades.to_byte())
        );
        assert_eq!(subscribe_all_trades_want_mm(&sent[0]), Some(true));
    }

    #[test]
    fn restore_trades_replays_mm_orders_override_after_exact_trades_subscribe() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: false });
            registry.mm_orders_sub = Some(true);
        });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_send_items(&client);
        let api: Vec<_> = sent
            .iter()
            .filter(|item| item.cmd == Command::API.to_byte())
            .collect();
        let ui: Vec<_> = sent
            .iter()
            .filter(|item| item.cmd == Command::UI.to_byte())
            .collect();
        assert_eq!(api.len(), 1);
        assert_eq!(
            method_id(&api[0].data),
            Some(EngineMethod::SubscribeAllTrades.to_byte())
        );
        assert_eq!(
            subscribe_all_trades_want_mm(&api[0].data),
            Some(false),
            "SubscribeAllTrades replays its own stored bool"
        );
        assert_eq!(ui.len(), 1);
        assert_eq!(
            Client::outgoing_mm_orders_subscribe_intent(ui[0]),
            Some(true),
            "latest direct MMOrders flag is restored as the separate UI command"
        );
    }

    #[test]
    fn restore_mm_orders_without_trades_sends_ui_subscription() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.with_subscription_registry_mut(|registry| {
            registry.mm_orders_sub = Some(true);
        });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_send_items(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::UI.to_byte());
        assert_eq!(sent[0].priority, SendPriority::High);
        let uid = command_uid(&sent[0].data).expect("wire command UID");
        assert_eq!(sent[0].u_key, UniqueKey::turn_mm_detection_for(uid));
        assert_eq!(sent[0].data.first().copied(), Some(5));
        assert_eq!(sent[0].data.last().copied(), Some(1));
    }

    #[test]
    fn restore_orderbooks_are_batched_into_single_request() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.with_subscription_registry_mut(|registry| {
            registry.orderbook_subs.insert("BTC".to_string());
            registry.orderbook_subs.insert("ETH".to_string());
            registry.orderbook_subs.insert("XRP".to_string());
        });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        // Все три подписки должны уйти ОДНИМ batch'ем, не тремя.
        assert_eq!(sent.len(), 1, "3 orderbook подписки → 1 batch wire-запрос");
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn restore_orderbooks_dedup_by_market_name() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.with_subscription_registry_mut(|registry| {
            assert!(registry.orderbook_subs.insert("BTC".to_string()));
            assert!(!registry.orderbook_subs.insert("BTC".to_string()));
        });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1, "same market is one server-side subscription");
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeOrderBook.to_byte())
        );
    }

    #[test]
    fn restore_combined_sends_trades_plus_orderbook_batches() {
        let mut client = Client::new(dummy_cfg());
        mark_post_init(&mut client);
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: false });
            registry.orderbook_subs.insert("BTC".to_string());
            registry.orderbook_subs.insert("XRP".to_string());
        });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 2, "1 trades + 1 orderbook batch = 2 запроса");
        let methods: Vec<Option<u8>> = sent.iter().map(|p| method_id(p)).collect();
        // Один из запросов — SubscribeAllTrades.
        assert!(methods.contains(&Some(EngineMethod::SubscribeAllTrades.to_byte())));
        // Один запрос — SubscribeOrderBook batch.
        let book_count = methods
            .iter()
            .filter(|m| **m == Some(EngineMethod::SubscribeOrderBook.to_byte()))
            .count();
        assert_eq!(book_count, 1);
    }
}

#[cfg(test)]
mod refresh_tick_tests {
    use super::*;

    fn dummy_cfg(refresh: RefreshConfig) -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh,
        }
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

    fn writer(client: &mut Client) -> ProtocolCore<'_> {
        ProtocolCore { client }
    }

    #[test]
    fn refresh_config_defaults() {
        // Документированные дефолты: Delphi-worker cadence, gated by domain_ready.
        let cfg = RefreshConfig::default();
        assert_eq!(cfg.update_markets_every, Some(Duration::from_secs(2)));
        assert_eq!(cfg.check_tags_every, Some(Duration::from_secs(60)));
    }

    #[test]
    fn run_loop_does_not_refresh_between_auth_done_and_domain_init() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(1)),
            check_tags_every: Some(Duration::from_millis(1)),
        }));
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        client.need_connect = false;
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;

        let mut dispatcher = crate::events::EventDispatcher::new();
        let initial_markets_ms = client.last_update_markets_ms;
        let initial_tags_ms = client.last_check_tags_ms;

        client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);

        assert_eq!(
            client.last_update_markets_ms, initial_markets_ms,
            "AuthDone before run_init_sequence must not start UpdateMarketsList refresh"
        );
        assert_eq!(
            client.last_check_tags_ms, initial_tags_ms,
            "AuthDone before run_init_sequence must not start CheckBinanceTags refresh"
        );
        assert!(
            drain_api_methods(&client).is_empty(),
            "pre-init run loop must not enqueue background Engine API requests"
        );

        client.testing_set_domain_ready(true);
        client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);

        assert_ne!(
            client.last_update_markets_ms, initial_markets_ms,
            "after domain init the same refresh config should become active"
        );
        assert_ne!(
            client.last_check_tags_ms, initial_tags_ms,
            "after domain init the same refresh config should become active"
        );
    }

    #[test]
    fn default_refresh_starts_after_domain_init() {
        let mut client = Client::new(dummy_cfg(RefreshConfig::default()));
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        client.need_connect = false;
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;
        client.testing_set_domain_ready(true);

        let mut dispatcher = crate::events::EventDispatcher::new();
        let initial_markets_ms = client.last_update_markets_ms;
        let initial_tags_ms = client.last_check_tags_ms;

        client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);

        assert_ne!(client.last_update_markets_ms, initial_markets_ms);
        assert_ne!(client.last_check_tags_ms, initial_tags_ms);
    }

    #[test]
    fn tick_sends_first_time_immediately() {
        // last_update_markets_ms = i64::MIN/2 ("никогда") → первый тик должен сразу
        // зафиксировать timestamp (что эквивалентно отправке запроса; реальная отправка
        // в socket=None ветке log warn'ит, но логика update состоялась).
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(100)),
            check_tags_every: None,
        }));
        let before = client.last_update_markets_ms;
        assert_eq!(before, i64::MIN / 2);
        writer(&mut client).tick_periodic_refresh(0);
        assert_eq!(
            client.last_update_markets_ms, 0,
            "первый тик должен зафиксировать timestamp 0"
        );
    }

    #[test]
    fn tick_respects_interval() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(100)),
            check_tags_every: None,
        }));
        client.last_update_markets_ms = 50;

        // 50ms прошло из 100ms required — не должен слать.
        writer(&mut client).tick_periodic_refresh(100);
        assert_eq!(
            client.last_update_markets_ms, 50,
            "interval не прошёл — last_update_markets_ms не меняется"
        );

        // 100ms прошло — отправка.
        writer(&mut client).tick_periodic_refresh(150);
        assert_eq!(
            client.last_update_markets_ms, 150,
            "100ms прошло — отправка состоялась"
        );
    }

    #[test]
    fn tick_does_nothing_when_both_disabled() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        }));
        let was_markets = client.last_update_markets_ms;
        let was_tags = client.last_check_tags_ms;
        writer(&mut client).tick_periodic_refresh(1_000_000);
        assert_eq!(
            client.last_update_markets_ms, was_markets,
            "update_markets выключен — last_update_markets_ms не меняется"
        );
        assert_eq!(
            client.last_check_tags_ms, was_tags,
            "check_tags выключен — last_check_tags_ms не меняется"
        );
    }

    #[test]
    fn tick_check_tags_independent_from_update_markets() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: Some(Duration::from_millis(200)),
        }));
        client.set_domain_ready(true);
        let was_markets = client.last_update_markets_ms;
        writer(&mut client).tick_periodic_refresh(1_000_000);
        assert_eq!(
            client.last_update_markets_ms, was_markets,
            "update_markets выключен — не трогаем"
        );
        assert_eq!(
            client.last_check_tags_ms, 1_000_000,
            "check_tags включен — трогаем"
        );
    }

    #[test]
    fn first_check_tags_tick_initializes_hour_without_burst() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: Some(Duration::from_secs(60)),
        }));
        client.set_domain_ready(true);
        assert_eq!(client.check_tags_hour_slot, i64::MIN);

        writer(&mut client).tick_periodic_refresh_at(0, 42);
        assert_eq!(client.check_tags_hour_slot, 42);
        assert_eq!(client.check_tags_burst_sent, CHECK_TAGS_BURST_COUNT);
        assert_eq!(
            drain_api_methods(&client),
            vec![EngineMethod::CheckBinanceTags.to_byte()],
        );

        writer(&mut client).tick_periodic_refresh_at(200, 42);
        assert!(
            drain_api_methods(&client).is_empty(),
            "initial tick is not a burst"
        );
    }

    #[test]
    fn tick_both_intervals_independent() {
        // Оба включены, но с разными интервалами — каждый тикает по своему.
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(100)),
            check_tags_every: Some(Duration::from_millis(500)),
        }));
        client.set_domain_ready(true);
        client.last_update_markets_ms = 0;
        client.last_check_tags_ms = 0;

        // 150ms: update_markets должен сработать (100ms прошло), check_tags нет.
        writer(&mut client).tick_periodic_refresh(150);
        assert_eq!(client.last_update_markets_ms, 150);
        assert_eq!(client.last_check_tags_ms, 0);

        // 600ms: update_markets должен сработать (450ms с прошлого), check_tags тоже (600ms с прошлого).
        writer(&mut client).tick_periodic_refresh(600);
        assert_eq!(client.last_update_markets_ms, 600);
        assert_eq!(client.last_check_tags_ms, 600);
    }

    #[test]
    fn check_tags_hourly_burst_sends_four_requests_with_spacing() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: Some(Duration::from_secs(60)),
        }));
        client.set_domain_ready(true);
        client.check_tags_hour_slot = 10;
        client.last_check_tags_ms = 1_000;
        client.check_tags_burst_sent = CHECK_TAGS_BURST_COUNT;
        drain_api_methods(&client);

        writer(&mut client).tick_periodic_refresh_at(10_000, 11);
        assert_eq!(
            drain_api_methods(&client),
            vec![EngineMethod::CheckBinanceTags.to_byte()],
        );
        assert_eq!(client.check_tags_burst_sent, 1);

        writer(&mut client).tick_periodic_refresh_at(10_100, 11);
        assert!(
            drain_api_methods(&client).is_empty(),
            "200ms spacing not reached"
        );

        writer(&mut client).tick_periodic_refresh_at(10_200, 11);
        writer(&mut client).tick_periodic_refresh_at(10_400, 11);
        writer(&mut client).tick_periodic_refresh_at(10_600, 11);
        assert_eq!(
            drain_api_methods(&client),
            vec![
                EngineMethod::CheckBinanceTags.to_byte(),
                EngineMethod::CheckBinanceTags.to_byte(),
                EngineMethod::CheckBinanceTags.to_byte(),
            ],
        );
        assert_eq!(client.check_tags_burst_sent, CHECK_TAGS_BURST_COUNT);

        writer(&mut client).tick_periodic_refresh_at(10_800, 11);
        assert!(
            drain_api_methods(&client).is_empty(),
            "no fifth burst request"
        );
    }
}

#[cfg(test)]
mod server_info_tests {
    use super::*;
    use crate::commands::engine_api::{AuthCheckResponse, ServerInfo};

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None, // no NTP worker needed for this unit test
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    #[test]
    fn server_info_default_on_new_client() {
        let client = Client::new(dummy_cfg());
        assert_eq!(client.server_info(), &ServerInfo::default());
        assert!(!client.server_info().has_identity());
        assert!(client.auth_info().is_none());
    }

    #[test]
    fn set_server_info_updates_storage_and_is_retrievable_via_getter() {
        let mut client = Client::new(dummy_cfg());
        let info = ServerInfo {
            bot_id: Some(0x1234_5678),
            server_name: Some("Test Server".to_string()),
            exchange_code: Some(1),
            exchange_name: Some("Binance Futures".to_string()),
            base_currency_name: Some("USDT".to_string()),
            base_currency_code: Some(1),
            ..Default::default()
        };
        client.set_server_info(info.clone());
        assert_eq!(client.server_info(), &info);
        assert_eq!(client.server_info().bot_id, Some(0x1234_5678));
        assert_eq!(
            client.server_info().exchange_name.as_deref(),
            Some("Binance Futures")
        );
        assert!(client.server_info().has_identity());
    }

    #[test]
    fn server_info_independent_across_clients() {
        // Multi-server: два Client'а с разными server_info никак не должны
        // влиять друг на друга. Это база для multi-server терминала.
        let mut client_a = Client::new(dummy_cfg());
        let mut client_b = Client::new(dummy_cfg());

        client_a.set_server_info(ServerInfo {
            bot_id: Some(100),
            exchange_name: Some("Binance".to_string()),
            ..Default::default()
        });
        client_b.set_server_info(ServerInfo {
            bot_id: Some(200),
            exchange_name: Some("Bybit".to_string()),
            ..Default::default()
        });

        assert_eq!(client_a.server_info().bot_id, Some(100));
        assert_eq!(client_b.server_info().bot_id, Some(200));
        assert_eq!(
            client_a.server_info().exchange_name.as_deref(),
            Some("Binance")
        );
        assert_eq!(
            client_b.server_info().exchange_name.as_deref(),
            Some("Bybit")
        );
    }

    #[test]
    fn trade_ctx_requires_base_check_route_fields() {
        let client = Client::new(dummy_cfg());

        let err = client
            .trade_ctx(0x0102_0304_0506_0708)
            .expect_err("new client has no BaseCheck route");
        assert!(err.missing_exchange_code);
        assert!(err.missing_base_currency_code);
    }

    #[test]
    fn trade_ctx_uses_server_info_route_fields() {
        let mut client = Client::new(dummy_cfg());
        client.set_server_info(ServerInfo {
            exchange_code: Some(9),
            base_currency_code: Some(17),
            ..Default::default()
        });

        let ctx = client
            .trade_ctx(0x0102_0304_0506_0708)
            .expect("route fields are present");

        assert_eq!(ctx.uid, 0x0102_0304_0506_0708);
        assert_eq!(ctx.currency, 17);
        assert_eq!(ctx.platform, 9);
    }

    #[test]
    fn set_auth_info_updates_storage_and_is_retrievable_via_getter() {
        let mut client = Client::new(dummy_cfg());
        let auth = AuthCheckResponse {
            binance_account_id: 123,
            btc_address: "btc".to_string(),
            spot_ref: 7,
            is_sub_account: true,
            account_id: "acc".to_string(),
            recvd_max_payload: Some(4096),
            known_dexes: Vec::new(),
            hl_dex_market: Some(1),
            hl_spot_market: Some(0),
        };

        client.set_auth_info(auth.clone());

        assert_eq!(client.auth_info(), Some(&auth));
    }
}

#[cfg(test)]
mod subscription_registry_tests {
    use super::*;

    #[test]
    fn registry_default_is_empty() {
        let r = SubscriptionRegistry::default();
        assert!(r.orderbook_subs.is_empty());
        assert!(r.trades_sub.is_none());
    }

    #[test]
    fn registry_orderbook_insert_dedups() {
        let mut r = SubscriptionRegistry::default();
        assert!(r.orderbook_subs.insert("BTCUSDT".to_string()));
        assert!(!r.orderbook_subs.insert("BTCUSDT".to_string()));
        assert!(r.orderbook_subs.insert("ETHUSDT".to_string()));
        assert_eq!(r.orderbook_subs.len(), 2);
    }

    #[test]
    fn trades_subscription_round_trip() {
        let sub = TradesSubscription { want_mm: true };
        assert!(sub.want_mm);
        let sub_off = TradesSubscription { want_mm: false };
        assert!(!sub_off.want_mm);
    }

    /// Verify что Connected{fresh:true} срабатывает только на ПЕРВОМ Authenticated
    /// в жизни Client'а. После этого все последующие = fresh:false.
    /// Тестируем through state-machine simulation (без полного Client::new).
    #[test]
    fn lifecycle_event_connected_fresh_flag_semantics() {
        // Симулируем: при первом переходе → fresh=true. При втором → fresh=false.
        let mut was_ever_connected = false;
        let first = LifecycleEvent::Connected {
            fresh: !was_ever_connected,
        };
        was_ever_connected = true;
        let second = LifecycleEvent::Connected {
            fresh: !was_ever_connected,
        };
        assert_eq!(first, LifecycleEvent::Connected { fresh: true });
        assert_eq!(second, LifecycleEvent::Connected { fresh: false });
    }
}

#[cfg(test)]
mod event_loop_fairness_tests {
    use super::*;
    use crate::events::EventDispatcher;
    use moonproto_transport::{outer_light_crypt, MacContext, ServerMsgHeader, TRANSPORT_VER};

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
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
        client.run_with_dispatcher_queued(Duration::from_millis(50), &mut dispatcher);

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
        client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
        send_server_packet_to_client_socket(&client, Command::UI, &[0xAA]);

        let caller_thread = thread::current().id();
        let (tx, rx) = mpsc::channel();
        client.run(
            Duration::from_millis(5),
            Box::new(move |cmd, payload| {
                assert_eq!(cmd, Command::UI);
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
        client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
        send_server_packet_to_client_socket(&client, Command::UI, &[0xAA]);

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            client.run(
                Duration::from_millis(20),
                Box::new(move |cmd, payload| {
                    assert_eq!(cmd, Command::UI);
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
    fn dispatcher_event_callback_block_does_not_extend_protocol_writer_tick() {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;
        client.prev_auth_status = AuthStatus::AuthDone;
        client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
        let mut payload = Vec::new();
        payload.extend_from_slice(&0.0f64.to_le_bytes());
        payload.extend_from_slice(b"queued app event");
        send_server_packet_to_client_socket(&client, Command::LogMsg, &payload);

        let mut dispatcher = EventDispatcher::new();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            client.run_with_dispatcher(
                Duration::from_millis(20),
                &mut dispatcher,
                Box::new(move |event| {
                    assert!(matches!(event, crate::events::Event::ServerLog { .. }));
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }),
            );
            client
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("event callback started");
        thread::sleep(Duration::from_millis(80));
        release_tx.send(()).unwrap();

        let client = handle.join().expect("client run thread");
        let snapshot = client.protocol_metrics_snapshot();
        assert!(
            snapshot.writer_tick_max_ns < 50_000_000,
            "blocking event app callback leaked into protocol tick: max={}ns",
            snapshot.writer_tick_max_ns
        );
    }

    #[test]
    fn dispatcher_state_callback_block_does_not_extend_protocol_writer_tick() {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;
        client.prev_auth_status = AuthStatus::AuthDone;
        client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
        let settings = crate::commands::ui::ClientSettingsCommand {
            uid: 0x5151,
            x_sell: 3,
            ..Default::default()
        };
        send_server_packet_to_client_socket(
            &client,
            Command::UI,
            &crate::commands::ui::build_client_settings(&settings),
        );

        let mut dispatcher = EventDispatcher::new();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            client.run_with_dispatcher_state(
                Duration::from_millis(20),
                &mut dispatcher,
                Box::new(move |event, state| {
                    assert!(matches!(
                        event,
                        crate::events::Event::Settings(
                            crate::state::SettingsEvent::ClientSettingsUpdated
                        )
                    ));
                    assert_eq!(
                        state.settings().client_settings.as_ref().map(|s| s.uid),
                        Some(0x5151)
                    );
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }),
            );
            client
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("state event callback started");
        thread::sleep(Duration::from_millis(80));
        release_tx.send(()).unwrap();

        let client = handle.join().expect("client run thread");
        let snapshot = client.protocol_metrics_snapshot();
        assert!(
            snapshot.writer_tick_max_ns < 50_000_000,
            "blocking state app callback leaked into protocol tick: max={}ns",
            snapshot.writer_tick_max_ns
        );
    }

    #[test]
    fn lifecycle_callback_block_does_not_extend_protocol_writer_tick() {
        let mut client = Client::new(dummy_cfg());
        client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
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
            client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);
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
        client.socket = Some(UdpSocket::bind("127.0.0.1:0").unwrap());
        let mut dispatcher = EventDispatcher::new();

        send_server_packet_to_client_socket(&client, Command::UI, &[0xAA]);
        client.send_cmd(
            vec![1, 2, 3, 4],
            Command::UI,
            SendPriority::Sliced,
            false,
            0,
        );

        client.run_with_dispatcher_queued(Duration::from_millis(5), &mut dispatcher);

        assert!(
            !client.sending.is_empty(),
            "app/user sends must use the separate outgoing queue, not wait behind pending reader work"
        );
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
        assert_eq!(client.total_recv, 1234);
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
            client.total_recv, 91,
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
                assert_eq!(cmd, Command::UI);
                assert_eq!(payload, &[0xAA, 0xBB]);
                delivered_cb.fetch_add(1, Ordering::Relaxed);
            }),
        };
        ProtocolCore {
            client: &mut client,
        }
        .data_read_int_inline(
            Command::UI.to_byte(),
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
        assert_eq!(client.total_recv, 321);
        assert_eq!(client.last_online, 123);
    }

    #[test]
    fn data_read_grouped_payload_applies_recv_effects_once() {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        let mut grouped = Vec::new();
        grouped.push(Command::UI.to_byte());
        grouped.extend_from_slice(&1u16.to_le_bytes());
        grouped.push(0xAA);
        grouped.push(Command::Balance.to_byte());
        grouped.extend_from_slice(&1u16.to_le_bytes());
        grouped.push(0xBB);

        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered_cb = Arc::clone(&delivered);
        let mut mode = RunMode::Callback {
            on_data: Box::new(move |cmd, payload| {
                match delivered_cb.load(Ordering::Relaxed) {
                    0 => {
                        assert_eq!(cmd, Command::UI);
                        assert_eq!(payload, &[0xAA]);
                    }
                    1 => {
                        assert_eq!(cmd, Command::Balance);
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
        assert_eq!(client.total_recv, 77);
        assert_eq!(client.last_online, 456);
    }
}

#[cfg(test)]
mod service_cmd_tests {
    use super::*;
    use moonproto_transport::{
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
            mask_ver: 0,
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

    fn recv_client_packet(
        server_sock: &UdpSocket,
        client: &mut Client,
    ) -> (ClientMsgHeader, Vec<u8>) {
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
        ProtocolCore { client }.recv_drain_phase(0, &mut mode);
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
        client_id: u64,
        server_token: u64,
        peer_app_token: u64,
    ) -> Vec<u8> {
        let mut hello = handshake::Hello::new(0x1111, peer_app_token);
        hello.server_token = server_token;
        hello.app_token = peer_app_token;
        hello.timestamp = delphi_now();
        let aad = client_id.to_le_bytes();
        crypto::encrypt(master_key, &hello.to_bytes_packed(), &aad)
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

        let ((hdr, ack_payload), events) =
            recv_client_packet_with_events(&server_sock, &mut client);

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

        let who = encrypted_server_hello(
            &client.cfg.master_key,
            client.cfg.client_id,
            server_token,
            peer_app_token,
        );
        let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, &who);
        server_sock.send_to(&packet, client_addr).unwrap();

        let ((hdr1, imfriend1), events1) =
            recv_client_packet_with_events(&server_sock, &mut client);
        let (hdr2, imfriend2) = recv_client_packet(&server_sock, &mut client);

        assert_eq!(hdr1.cmd, Command::ImFriend.to_byte());
        assert_eq!(hdr2.cmd, Command::ImFriend.to_byte());
        assert!(events1.is_empty());
        assert_eq!(
            imfriend1, imfriend2,
            "Rust keeps Delphi duplicate ImFriend wire effect but removes blocking Sleep(32)"
        );
        let (encode_key, decode_key) =
            crypto::generate_sub_keys(&client.cfg.master_key, server_token);
        let aad = client.cfg.client_id.to_le_bytes();
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
        let aad = client.cfg.client_id.to_le_bytes();
        let hello = crypto::decrypt(&client.cfg.master_key, &hello_payload, &aad)
            .and_then(|payload| handshake::Hello::from_bytes(&payload))
            .expect("sent Hello decrypts with master key");
        assert_eq!(hello.mix_ts, token_before.wrapping_add(1));

        let who = encrypted_server_hello(
            &client.cfg.master_key,
            client.cfg.client_id,
            server_token,
            peer_app_token,
        );
        let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, &who);
        server_sock.send_to(&packet, client_addr).unwrap();

        let ((hdr1, imfriend1), events1) =
            recv_client_packet_with_events(&server_sock, &mut client);
        let (hdr2, _imfriend2) = recv_client_packet(&server_sock, &mut client);

        assert_eq!(hdr1.cmd, Command::ImFriend.to_byte());
        assert_eq!(hdr2.cmd, Command::ImFriend.to_byte());
        assert!(events1.is_empty());
        let (encode_key, _decode_key) =
            crypto::generate_sub_keys(&client.cfg.master_key, server_token);
        let im = crypto::decrypt(&encode_key, &imfriend1, &aad)
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
        client.waiting_hello = true;
        client.socket = Some(client_sock);
        client.start_inline_reader_session();

        let fine =
            encrypted_server_hello(&client.cfg.master_key, client.cfg.client_id, 0x2222, 0x3333);
        let packet = pack_server_packet(&client.cfg.mac_key, Command::Fine, &fine);
        server_sock.send_to(&packet, client_addr).unwrap();

        pump_inline_reader(&mut client);

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(!client.need_connect);
        assert!(!client.waiting_hello);
        assert!(!client.need_connect);
        assert!(!client.waiting_hello);
    }

    #[test]
    fn reader_clears_waiting_hello_before_invalid_who_are_you_like_delphi() {
        let _err_emu_guard = err_emu_test_guard();
        let (server_sock, client_addr, mut client) = inline_reader_test_client();
        client.waiting_hello = true;
        client.server_token = 0x1234;

        let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, b"bad");
        server_sock.send_to(&packet, client_addr).unwrap();

        pump_inline_reader(&mut client);

        assert!(!client.waiting_hello);
        assert_eq!(
            client.server_token, 0x1234,
            "invalid handshake payload must not apply WhoAreYou fields",
        );
    }

    #[test]
    fn reader_clears_waiting_hello_before_invalid_fine_like_delphi() {
        let _err_emu_guard = err_emu_test_guard();
        let (server_sock, client_addr, mut client) = inline_reader_test_client();
        client.waiting_hello = true;
        client.need_connect = true;

        let packet = pack_server_packet(&client.cfg.mac_key, Command::Fine, b"bad");
        server_sock.send_to(&packet, client_addr).unwrap();

        pump_inline_reader(&mut client);

        assert!(!client.waiting_hello);
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
        client.need_connect = false;
        client.soft_reconnect = true;
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
        client.waiting_hello = false;
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
            mask_ver: 0,
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
}

#[cfg(test)]
mod reconnect_timing_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use crate::commands::market::{
        build_markets_indexes_response, build_markets_list_response, build_markets_prices_response,
        BaseCurrency, Market, MarketPriceUpdate, MarketsListResponse, MarketsPricesResponse,
    };
    use crate::events::{Event, EventDispatcher};

    fn dummy_client() -> Client {
        Client::new(ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        })
    }

    fn test_market(name: &str) -> Market {
        Market {
            bn_market_name: name.to_string(),
            market_currency: name.to_string(),
            bn_market_currency: name.to_string(),
            base_currency: "USDT".to_string(),
            market_currency_long: name.to_string(),
            market_currency_canonic: name.to_string(),
            market_name: name.to_string(),
            market_name_mb_classic: name.to_string(),
            bn_status: "TRADING".to_string(),
            leading1000: String::new(),
            bn_price_precision: 2,
            bn_quantity_precision: 5,
            max_leverage: 50,
            k1000: 1,
            bn_iceberg_parts: 0,
            bn_margin_table_id: 0,
            bn_delivery_time: 0,
            bn_tick_size: 0.01,
            bn_step_size: 0.01,
            bn_min_qty: 0.0,
            bn_max_qty: 0.0,
            bn_min_notional: 0.0,
            bn_max_notional: 0.0,
            bn_contract_size: 0.0,
            bn_min_price: 0.0,
            bn_max_price: 0.0,
            bn_max_value: 0.0,
            bn_multiplier_up: 0.0,
            bn_multiplier_down: 0.0,
            bid_multiplier_up: 0.0,
            bid_multiplier_down: 0.0,
            ask_multiplier_up: 0.0,
            ask_multiplier_down: 0.0,
            int_bn_max_qty: 0.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            volume: 0.0,
            is_btc_market: false,
            status_trading: true,
            bn_is_fucking_shib: false,
            bn_iceberg: false,
            bn_only_isolated: false,
            futures_type: BaseCurrency::USDT,
        }
    }

    fn install_session_key(client: &mut Client) {
        client.server_token = 1;
        client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));
    }

    fn encrypted_hello(client: &Client, server_token: u64, peer_app_token: u64) -> Vec<u8> {
        let mut hello = handshake::Hello::new(client.client_token, client.app_token);
        hello.server_token = server_token;
        hello.app_token = peer_app_token;
        hello.timestamp = delphi_now();
        let aad = client.cfg.client_id.to_le_bytes();
        crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad)
    }

    fn apply_reader_handshake_payload(client: &mut Client, cmd: Command, payload: &[u8]) -> bool {
        let master_key = client.cfg.master_key;
        let client_id = client.cfg.client_id;
        let Some(hello) = Client::decode_handshake_hello(&master_key, client_id, payload) else {
            return false;
        };

        match cmd {
            Command::WhoAreYou => {
                let _encrypted_imfriend =
                    ProtocolCore { client }.apply_who_are_you_hello_and_build_imfriend(hello);
                true
            }
            Command::Fine => {
                ProtocolCore { client }.apply_fine_auth_done();
                true
            }
            _ => false,
        }
    }

    fn method_id(payload: &[u8]) -> Option<u8> {
        payload.get(11).copied()
    }

    fn request_uid(payload: &[u8]) -> Option<u64> {
        engine_request_uid(payload)
    }

    fn drain_send_items(client: &Client) -> Vec<SendItem> {
        let (mut sliced, mut high, mut low) = client.take_send_queues_for_test();
        sliced.append(&mut high);
        sliced.append(&mut low);
        sliced
    }

    fn api_methods(items: &[SendItem]) -> Vec<u8> {
        items
            .iter()
            .filter(|item| item.cmd == Command::API.to_byte())
            .filter_map(|item| method_id(&item.data))
            .collect()
    }

    fn subscribe_all_trades_want_mm(payload: &[u8]) -> Option<bool> {
        if method_id(payload)? != EngineMethod::SubscribeAllTrades.to_byte() {
            return None;
        }
        payload.last().map(|v| *v != 0)
    }

    fn build_engine_response_payload(
        request_uid: u64,
        method: EngineMethod,
        data: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8);
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(&0xAABB_CCDD_u64.to_le_bytes());
        buf.extend_from_slice(&request_uid.to_le_bytes());
        buf.push(method.to_byte());
        buf.push(1u8);
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.push(0u8);
        buf.extend_from_slice(&(data.len() as i32).to_le_bytes());
        buf.extend_from_slice(data);
        buf
    }

    #[test]
    fn want_new_hello_allows_immediate_hello_on_young_client_clock() {
        let mut client = dummy_client();

        ProtocolCore {
            client: &mut client,
        }
        .on_handshake_control_inline(Command::WantNewHello, 0, 0);

        assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
        ProtocolCore {
            client: &mut client,
        }
        .check_hello_send(100);

        assert_eq!(
            client.last_sent_hello, 100,
            "Delphi LastSentHello=0 означает немедленный retry; Rust Instant clock не должен ждать 2с",
        );
        assert!(client.waiting_hello);
    }

    #[test]
    fn early_hello_again_uses_master_key_before_whoareyou() {
        let mut client = dummy_client();
        let token_before = client.client_token;
        let payload = ProtocolCore {
            client: &mut client,
        }
        .build_hello_again_packet();
        let aad = client.cfg.client_id.to_le_bytes();
        let decrypted = crypto::decrypt(&client.cfg.master_key, &payload, &aad)
            .expect("early HelloAgain must be encrypted with MasterKey");
        let hello = handshake::Hello::from_bytes(&decrypted).expect("valid HelloAgain payload");

        assert_eq!(client.client_token, token_before + 1);
        assert_eq!(hello.mix_ts, client.client_token);
        assert_eq!(
            hello.peer_mix,
            crypto::mix_values(&hello.rnd, hello.mix_ts, 0),
            "before WhoAreYou Delphi computes PeerMix with ServerToken=0",
        );
    }

    #[test]
    fn fine_requires_master_key_hello_payload_like_delphi() {
        let mut client = dummy_client();

        assert!(!apply_reader_handshake_payload(
            &mut client,
            Command::Fine,
            b"not an encrypted hello",
        ));

        assert!(!client.authorized);
        assert_ne!(client.auth_status, AuthStatus::AuthDone);

        let mut hello = handshake::Hello::new(client.client_token, client.app_token);
        hello.timestamp = delphi_now();
        let aad = client.cfg.client_id.to_le_bytes();
        let payload = crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad);

        assert!(apply_reader_handshake_payload(
            &mut client,
            Command::Fine,
            &payload,
        ));

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(!client.need_connect);
    }

    #[test]
    fn first_fine_before_init_does_not_send_engine_api_or_restore_subscriptions() {
        let mut client = dummy_client();
        client.set_domain_ready(false);
        client.peer_app_token = 0xABCD;
        client.tracked_indexes_peer_app_token = 0;
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: true });
            registry.mm_orders_sub = Some(true);
            registry.orderbook_subs.insert("BTCUSDT".to_string());
        });

        let mut hello = handshake::Hello::new(client.client_token, client.app_token);
        hello.timestamp = delphi_now();
        let aad = client.cfg.client_id.to_le_bytes();
        let payload = crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad);

        assert!(apply_reader_handshake_payload(
            &mut client,
            Command::Fine,
            &payload,
        ));

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(
            sliced.is_empty() && high.is_empty() && low.is_empty(),
            "first Fine must not restore; restore starts only after a completed Init session",
        );
    }

    #[test]
    fn post_init_reconnect_restores_domain_without_second_init_and_reopens_stream_gate() {
        let mut client = dummy_client();

        // Simulate a Client that already connected once and completed its single Init.
        client.set_domain_ready(true);
        client.was_ever_connected = true;
        client.auth_status = AuthStatus::AuthDone;
        client.prev_auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.peer_app_token = 0x1000;
        client.tracked_indexes_peer_app_token = 0x1000;
        client.domain_restore = DomainRestoreIntent {
            fetch_indexes: true,
        };
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: false });
            registry.orderbook_subs.insert("BTCUSDT".to_string());
        });

        let who = encrypted_hello(&client, 0x2222, 0x2000);
        assert!(apply_reader_handshake_payload(
            &mut client,
            Command::WhoAreYou,
            &who,
        ));
        let fine = encrypted_hello(&client, 0x2222, 0x2000);
        assert!(apply_reader_handshake_payload(
            &mut client,
            Command::Fine,
            &fine,
        ));

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(
            client.indexes_fetch_in_flight,
            "post-init reconnect must request fresh indexes without user re-running Init"
        );

        let sent = drain_send_items(&client);
        let methods = api_methods(&sent);
        assert!(
            methods.contains(&(EngineMethod::GetMarketsIndexes.to_byte())),
            "subscriptions need fresh indexes after reconnect"
        );
        assert!(
            !methods.contains(&(EngineMethod::SubscribeAllTrades.to_byte())),
            "trades reconnect is not raw replay; Delphi runs unsubscribe + 100ms delay + subscribe from the maintenance tick"
        );
        assert!(
            !methods.contains(&(EngineMethod::SubscribeOrderBook.to_byte())),
            "orderbook subscription must wait for fresh market indexes like Delphi CheckBookTopics"
        );
        assert!(
            !methods.contains(&(EngineMethod::BaseCheck.to_byte()))
                && !methods.contains(&(EngineMethod::AuthCheck.to_byte()))
                && !methods.contains(&(EngineMethod::GetMarketsList.to_byte()))
                && !methods.contains(&(EngineMethod::GetMarketsBalanceFull.to_byte())),
            "reconnect restore is not a second Init"
        );
        assert!(
            sent.iter().all(|item| {
                item.cmd != Command::Order.to_byte()
                    && item.cmd != Command::UI.to_byte()
                    && item.cmd != Command::Balance.to_byte()
                    && item.cmd != Command::Strat.to_byte()
            }),
            "Delphi post-init resync is not repeated by the client on reconnect"
        );

        client.tick_trades_reconnect_sequence(10_000, 0);
        let trades_reconnect_sent = drain_send_items(&client);
        let trades_reconnect_methods = api_methods(&trades_reconnect_sent);
        assert_eq!(
            trades_reconnect_methods,
            vec![EngineMethod::UnsubscribeAllTrades.to_byte()],
            "NeedReconnectAllTrades starts with UnSubscribeAllTrades"
        );
        let unsubscribe_uid =
            request_uid(&trades_reconnect_sent[0].data).expect("unsubscribe request uid");

        client.tick_trades_reconnect_sequence(10_050, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "Delphi SendAndWait: SubscribeAllTrades must not be sent before UnSubscribeAllTrades response"
        );

        client.tick_trades_reconnect_sequence(10_100, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "100ms alone is not enough; the sleep starts after UnSubscribeAllTrades completes"
        );

        let unsubscribe_response =
            build_engine_response_payload(unsubscribe_uid, EngineMethod::UnsubscribeAllTrades, &[]);
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                unsubscribe_response,
                false,
                false,
                &mut sink,
            );
        }
        let subscribe_due = client
            .pending_trades_resubscribe_after_ms
            .expect("unsubscribe response starts the Delphi Sleep(100) window");
        client.tick_trades_reconnect_sequence(subscribe_due - 1, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "Delphi Sleep(100): SubscribeAllTrades must not be immediate after UnSubscribeAllTrades response"
        );

        client.tick_trades_reconnect_sequence(subscribe_due, 0);
        let trades_subscribe_sent = drain_send_items(&client);
        let trades_subscribe_methods = api_methods(&trades_subscribe_sent);
        assert_eq!(
            trades_subscribe_methods,
            vec![EngineMethod::SubscribeAllTrades.to_byte()],
            "after 100ms delay reconnect sends DoSubscribeAllTrades(false)"
        );

        client.tick_trades_reconnect_sequence(10_200, client.server_token);
        assert!(
            drain_send_items(&client).is_empty(),
            "once TradesStream has observed the current ServerToken, reconnect retry stops"
        );

        let response_data = build_markets_indexes_response(&["BTCUSDT".to_string()]);
        let response_payload =
            build_engine_response_payload(0x7777, EngineMethod::GetMarketsIndexes, &response_data);
        let mut buffered = Vec::new();
        {
            let mut sink = DispatchSink::Buffer(&mut buffered);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                response_payload,
                false,
                false,
                &mut sink,
            );
        }
        assert!(!client.indexes_fetch_in_flight);
        assert!(client.market_indexes_current_for_peer());
        let after_indexes_sent = drain_send_items(&client);
        let after_indexes_methods = api_methods(&after_indexes_sent);
        assert!(
            after_indexes_methods.contains(&(EngineMethod::UpdateMarketsList.to_byte())),
            "after reconnect index sync, library must refresh market prices like Delphi UpdateMarketsList"
        );
        assert!(
            after_indexes_methods.contains(&(EngineMethod::SubscribeOrderBook.to_byte())),
            "after reconnect index sync, library must replay orderbook subscriptions"
        );

        let mut dispatcher = EventDispatcher::new();
        let mut out = Vec::new();
        let mut actions = Vec::new();
        let (cmd, payload) = buffered.pop().expect("API response must reach dispatcher");
        let ctx = crate::events::ActiveDispatchContext::from_client(&client);
        dispatcher.dispatch_into_active_actions(
            cmd,
            &payload,
            client.now_ms(),
            &mut out,
            &ctx,
            &mut actions,
        );
        client.apply_active_actions(actions.drain(..));
        assert!(
            dispatcher.markets().indexes_synchronized,
            "fresh GetMarketsIndexes response reopens indexed stream gate"
        );

        out.clear();
        actions.clear();
        let ctx = crate::events::ActiveDispatchContext::from_client(&client);
        dispatcher.dispatch_into_active_actions(
            Command::OrderBook,
            &[],
            client.now_ms(),
            &mut out,
            &ctx,
            &mut actions,
        );
        client.apply_active_actions(actions.drain(..));
        assert!(
            out.is_empty(),
            "Delphi ProcessOrderBookPacket still gates packets until SubscribeOrderBook success confirms FSubscribedBookServerToken"
        );

        let subscribe_uid = after_indexes_sent
            .iter()
            .find(|item| {
                item.cmd == Command::API.to_byte()
                    && method_id(&item.data) == Some(EngineMethod::SubscribeOrderBook.to_byte())
            })
            .and_then(|item| request_uid(&item.data))
            .expect("SubscribeOrderBook replay uid");
        let response_payload =
            build_engine_response_payload(subscribe_uid, EngineMethod::SubscribeOrderBook, &[]);
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                response_payload,
                false,
                false,
                &mut sink,
            );
        }
        assert_eq!(client.subscribed_book_server_token, client.server_token);

        out.clear();
        actions.clear();
        let ctx = crate::events::ActiveDispatchContext::from_client(&client);
        dispatcher.dispatch_into_active_actions(
            Command::OrderBook,
            &[],
            client.now_ms(),
            &mut out,
            &ctx,
            &mut actions,
        );
        client.apply_active_actions(actions.drain(..));
        assert!(
            out.iter().any(|ev| matches!(
                ev,
                Event::ParseFailed {
                    cmd: Command::OrderBook,
                    ..
                }
            )),
            "after SubscribeOrderBook success confirms current ServerToken, OrderBook packets reach parser"
        );
    }

    #[test]
    fn trades_reconnect_restores_distinct_mm_orders_override_after_delayed_subscribe() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.server_token = 0x2222;
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: false });
            registry.mm_orders_sub = Some(true);
        });

        client.tick_trades_reconnect_sequence(10_000, 0);
        let unsubscribe_sent = drain_send_items(&client);
        assert_eq!(
            api_methods(&unsubscribe_sent),
            vec![EngineMethod::UnsubscribeAllTrades.to_byte()]
        );
        let unsubscribe_uid = request_uid(&unsubscribe_sent[0].data).expect("request uid");

        client.tick_trades_reconnect_sequence(10_100, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "SubscribeAllTrades waits for UnSubscribeAllTrades response"
        );
        let unsubscribe_response =
            build_engine_response_payload(unsubscribe_uid, EngineMethod::UnsubscribeAllTrades, &[]);
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                unsubscribe_response,
                false,
                false,
                &mut sink,
            );
        }
        let subscribe_due = client.pending_trades_resubscribe_after_ms.unwrap();
        client.tick_trades_reconnect_sequence(subscribe_due, 0);
        let sent = drain_send_items(&client);
        let api: Vec<_> = sent
            .iter()
            .filter(|item| item.cmd == Command::API.to_byte())
            .collect();
        let ui: Vec<_> = sent
            .iter()
            .filter(|item| item.cmd == Command::UI.to_byte())
            .collect();

        assert_eq!(api.len(), 1);
        assert_eq!(
            method_id(&api[0].data),
            Some(EngineMethod::SubscribeAllTrades.to_byte())
        );
        assert_eq!(
            subscribe_all_trades_want_mm(&api[0].data),
            Some(false),
            "delayed reconnect SubscribeAllTrades keeps its exact stored bool"
        );
        assert_eq!(ui.len(), 1);
        assert_eq!(
            Client::outgoing_mm_orders_subscribe_intent(ui[0]),
            Some(true),
            "direct MMOrders intent is restored after the delayed all-trades subscribe"
        );
    }

    #[test]
    fn orderbook_reconnect_retries_until_full_batch_response_confirms_server_token() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.server_token = 0x2222;
        client.peer_app_token = 0x3333;
        client.tracked_indexes_peer_app_token = 0x3333;
        client.subscribed_book_server_token = 0x1111;
        client.with_subscription_registry_mut(|registry| {
            registry.orderbook_subs.insert("BTCUSDT".to_string());
            registry.orderbook_subs.insert("ETHUSDT".to_string());
        });

        assert!(
            client.tick_orderbook_reconnect_sequence(10_000),
            "NeedResubscribeOrderBooks must send full BookSubbed batch when ServerToken changed"
        );
        let first_sent = drain_send_items(&client);
        let first_methods = api_methods(&first_sent);
        assert_eq!(
            first_methods,
            vec![EngineMethod::SubscribeOrderBook.to_byte()],
            "reconnect retry sends one batched SubscribeOrderBook"
        );
        let first_uid = request_uid(&first_sent[0].data).expect("request uid");
        assert_eq!(client.pending_orderbook_resubscribe_uid, Some(first_uid));
        assert_eq!(client.last_book_reconnect_check_ms, 10_000);

        assert!(
            !client.tick_orderbook_reconnect_sequence(21_999),
            "Delphi DoSubscribeOrderBooks SendAndWait window blocks retry before FTimeout"
        );
        assert!(drain_send_items(&client).is_empty());

        let normal_subscribe_response =
            build_engine_response_payload(0xABCD, EngineMethod::SubscribeOrderBook, &[]);
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                normal_subscribe_response,
                false,
                false,
                &mut sink,
            );
        }
        assert_eq!(
            client.subscribed_book_server_token, 0x1111,
            "non-reconnect SubscribeOrderBook success must not stop a pending full replay"
        );
        assert_ne!(
            client
                .last_orderbook_subscribe_request_ms
                .load(Ordering::Relaxed),
            NEVER_TIME_MS,
            "non-matching SubscribeOrderBook response must not close the pending batch SendAndWait gate"
        );

        assert!(
            client.tick_orderbook_reconnect_sequence(22_000),
            "after FTimeout and the 5s throttle, Delphi allows the next retry"
        );
        let second_sent = drain_send_items(&client);
        let second_uid = request_uid(&second_sent[0].data).expect("request uid");
        assert_eq!(client.pending_orderbook_resubscribe_uid, Some(second_uid));

        let response_payload =
            build_engine_response_payload(second_uid, EngineMethod::SubscribeOrderBook, &[]);
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                response_payload,
                false,
                false,
                &mut sink,
            );
        }
        assert_eq!(client.subscribed_book_server_token, client.server_token);
        assert_eq!(client.pending_orderbook_resubscribe_uid, None);
        assert!(
            !client.tick_orderbook_reconnect_sequence(20_000),
            "after confirmed current ServerToken, NeedResubscribeOrderBooks stops"
        );
        assert!(drain_send_items(&client).is_empty());
    }

    #[test]
    fn orderbook_reconnect_first_tick_is_immediate_without_inflight_subscribe() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.server_token = 0x2222;
        client.peer_app_token = 0x3333;
        client.tracked_indexes_peer_app_token = 0x3333;
        client.subscribed_book_server_token = 0x1111;
        client.with_subscription_registry_mut(|registry| {
            registry.orderbook_subs.insert("BTCUSDT".to_string());
        });

        assert!(
            client.tick_orderbook_reconnect_sequence(1),
            "Delphi LastBookReconnectCheck=0 against GetTickCount64 allows the first reconnect check immediately"
        );
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::SubscribeOrderBook.to_byte()]
        );
    }

    #[test]
    fn queued_orderbook_subscribe_blocks_pre_response_reconnect_like_delphi_sendandwait() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.server_token = 0x2222;
        client.peer_app_token = 0x3333;
        client.tracked_indexes_peer_app_token = 0x3333;
        client.subscribed_book_server_token = 0x1111;

        client.subscribe_orderbook("BTCUSDT");
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::SubscribeOrderBook.to_byte()]
        );
        let requested_at = client
            .last_orderbook_subscribe_request_ms
            .load(Ordering::Relaxed);
        assert!(requested_at >= 0);

        assert!(
            !client.tick_orderbook_reconnect_sequence(
                requested_at + crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS - 1,
            ),
            "DoSubscribeOrderBooks is still inside Delphi FTimeout"
        );
        assert!(drain_send_items(&client).is_empty());

        assert!(
            client.tick_orderbook_reconnect_sequence(
                requested_at + crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS,
            ),
            "after FTimeout, NeedResubscribeOrderBooks may send the full BookSubbed batch"
        );
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::SubscribeOrderBook.to_byte()]
        );
    }

    #[test]
    fn first_successful_orderbook_subscribe_sets_initial_book_server_token_like_delphi() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.server_token = 0x2222;
        client.subscribed_book_server_token = 0;

        let response_payload =
            build_engine_response_payload(0xABCD, EngineMethod::SubscribeOrderBook, &[]);
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                response_payload,
                false,
                false,
                &mut sink,
            );
        }

        assert_eq!(client.subscribed_book_server_token, 0x2222);
    }

    #[test]
    fn malformed_get_markets_indexes_response_does_not_reopen_stream_gate() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.peer_app_token = 0x2000;
        client.tracked_indexes_peer_app_token = 0x1000;
        client.indexes_fetch_in_flight = true;
        client.update_markets_after_indexes = true;
        client.restore_orderbooks_after_indexes = true;
        client.with_subscription_registry_mut(|registry| {
            registry.orderbook_subs.insert("BTCUSDT".to_string());
        });

        // count=1, first string declares len=3 but only one byte follows.
        let mut malformed_indexes = Vec::new();
        malformed_indexes.extend_from_slice(&1_i32.to_le_bytes());
        malformed_indexes.extend_from_slice(&3_u16.to_le_bytes());
        malformed_indexes.push(b'B');
        let response_payload = build_engine_response_payload(
            0x7777,
            EngineMethod::GetMarketsIndexes,
            &malformed_indexes,
        );

        let mut buffered = Vec::new();
        {
            let mut sink = DispatchSink::Buffer(&mut buffered);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                response_payload,
                false,
                false,
                &mut sink,
            );
        }

        assert!(
            !client.indexes_fetch_in_flight,
            "malformed response still finishes this in-flight attempt"
        );
        assert!(
            !client.market_indexes_current_for_peer(),
            "Delphi does not treat malformed GetMarketsIndexes as synchronized"
        );
        let sent = drain_send_items(&client);
        let methods = api_methods(&sent);
        assert!(
            !methods.contains(&(EngineMethod::UpdateMarketsList.to_byte())),
            "UpdateMarketsList must wait for valid indexes payload"
        );
        assert!(
            !methods.contains(&(EngineMethod::SubscribeOrderBook.to_byte())),
            "orderbook restore must wait for valid indexes payload"
        );
        assert!(
            client.update_markets_after_indexes,
            "retry after a later valid indexes response must still refresh markets"
        );
        assert!(
            client.restore_orderbooks_after_indexes,
            "retry after a later valid indexes response must still replay orderbooks"
        );

        let mut dispatcher = EventDispatcher::new();
        let mut out = Vec::new();
        let mut actions = Vec::new();
        let (cmd, payload) = buffered.pop().expect("API response must reach dispatcher");
        let ctx = crate::events::ActiveDispatchContext::from_client(&client);
        dispatcher.dispatch_into_active_actions(
            cmd,
            &payload,
            client.now_ms(),
            &mut out,
            &ctx,
            &mut actions,
        );
        assert!(
            !dispatcher.markets().indexes_synchronized,
            "dispatcher must also keep stream gate closed on malformed indexes"
        );
    }

    #[test]
    fn unknown_indexed_market_price_requests_markets_list_like_delphi_new_market_found() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.peer_app_token = 0x2000;
        client.tracked_indexes_peer_app_token = 0x2000;

        let mut dispatcher = EventDispatcher::new();
        dispatcher.markets.apply_markets_list(MarketsListResponse {
            markets: vec![test_market("BTCUSDT")],
            corr_markets: vec![],
        });
        dispatcher
            .markets
            .apply_markets_indexes(vec!["DOGEUSDT".to_string()]);

        let prices = build_markets_prices_response(&MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 0.1,
                ask: 0.2,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 0.15,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        });
        let response_payload =
            build_engine_response_payload(0x7777, EngineMethod::UpdateMarketsList, &prices);

        let mut out = Vec::new();
        let mut actions = Vec::new();
        let ctx = crate::events::ActiveDispatchContext::from_client(&client);
        dispatcher.dispatch_into_active_actions(
            Command::API,
            &response_payload,
            client.now_ms(),
            &mut out,
            &ctx,
            &mut actions,
        );

        assert!(
            actions
                .iter()
                .any(|action| matches!(action, crate::events::ActiveAction::RequestMarketsList)),
            "Delphi NewMarketFound path must become active GetMarketsList refresh"
        );
        client.apply_active_actions(actions.drain(..));
        let sent = drain_send_items(&client);
        let methods = api_methods(&sent);
        assert!(
            methods.contains(&(EngineMethod::GetMarketsList.to_byte())),
            "active action must enqueue emk_GetMarketsList"
        );
    }

    #[test]
    fn new_market_list_refresh_requests_immediate_prices_like_delphi_new_markets() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.auth_status = AuthStatus::AuthDone;
        client.authorized = true;

        let mut dispatcher = EventDispatcher::new();
        dispatcher.markets.apply_markets_list(MarketsListResponse {
            markets: vec![test_market("BTCUSDT")],
            corr_markets: vec![],
        });
        dispatcher.markets.markets_list_refresh_needed = true;

        let list = build_markets_list_response(
            &MarketsListResponse {
                markets: vec![test_market("BTCUSDT"), test_market("DOGEUSDT")],
                corr_markets: vec![],
            },
            2,
        );
        let response_payload =
            build_engine_response_payload(0x8888, EngineMethod::GetMarketsList, &list);

        let mut out = Vec::new();
        let mut actions = Vec::new();
        let ctx = crate::events::ActiveDispatchContext::from_client(&client);
        dispatcher.dispatch_into_active_actions(
            Command::API,
            &response_payload,
            client.now_ms(),
            &mut out,
            &ctx,
            &mut actions,
        );

        assert!(dispatcher.markets().get("DOGEUSDT").is_some());
        assert!(
            actions.iter().any(|action| {
                matches!(
                    action,
                    crate::events::ActiveAction::RequestUpdateMarketsList
                )
            }),
            "Delphi runs UpdateMarketsList immediately when NewMarkets.Count > 0"
        );
        client.apply_active_actions(actions.drain(..));
        let sent = drain_send_items(&client);
        let methods = api_methods(&sent);
        assert!(
            methods.contains(&(EngineMethod::UpdateMarketsList.to_byte())),
            "active action must enqueue emk_UpdateMarketsList"
        );
    }

    #[test]
    fn trades_reconnect_retries_every_five_seconds_until_stream_token_is_seen() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.server_token = 0x2222;
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: true });
        });

        client.tick_trades_reconnect_sequence(10_000, 0);
        let first_unsubscribe = drain_send_items(&client);
        assert_eq!(
            api_methods(&first_unsubscribe),
            vec![EngineMethod::UnsubscribeAllTrades.to_byte()]
        );
        let first_unsubscribe_uid = request_uid(&first_unsubscribe[0].data).expect("request uid");
        client.tick_trades_reconnect_sequence(10_100, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "SubscribeAllTrades waits for UnSubscribeAllTrades response, not only Sleep(100)"
        );

        let first_unsubscribe_response = build_engine_response_payload(
            first_unsubscribe_uid,
            EngineMethod::UnsubscribeAllTrades,
            &[],
        );
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                first_unsubscribe_response,
                false,
                false,
                &mut sink,
            );
        }
        let first_subscribe_due = client.pending_trades_resubscribe_after_ms.unwrap();
        client.tick_trades_reconnect_sequence(first_subscribe_due, 0);
        let first_subscribe_sent = drain_send_items(&client);
        assert_eq!(
            api_methods(&first_subscribe_sent),
            vec![EngineMethod::SubscribeAllTrades.to_byte()]
        );
        let first_subscribe_uid = request_uid(&first_subscribe_sent[0].data).expect("request uid");

        let first_subscribe_response = build_engine_response_payload(
            first_subscribe_uid,
            EngineMethod::SubscribeAllTrades,
            &[],
        );
        {
            let mut ignored = Vec::new();
            let mut sink = DispatchSink::Buffer(&mut ignored);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                first_subscribe_response,
                false,
                false,
                &mut sink,
            );
        }
        let refreshed_at = client.last_trades_reconnect_check_ms;
        client.tick_trades_reconnect_sequence(refreshed_at + TRADES_RECONNECT_THROTTLE_MS - 1, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "Delphi LastReconnectCheck blocks retry for 5s after SubscribeAllTrades success"
        );
        client.tick_trades_reconnect_sequence(refreshed_at + TRADES_RECONNECT_THROTTLE_MS, 0);
        let second_unsubscribe = drain_send_items(&client);
        assert_eq!(
            api_methods(&second_unsubscribe),
            vec![EngineMethod::UnsubscribeAllTrades.to_byte()]
        );

        let unsubscribe_timeout = refreshed_at
            + TRADES_RECONNECT_THROTTLE_MS
            + crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS;
        client.tick_trades_reconnect_sequence(unsubscribe_timeout, client.server_token);
        assert!(
            drain_send_items(&client).is_empty(),
            "UnSubscribeAllTrades timeout starts the paired Sleep(100), it does not send Subscribe immediately"
        );
        client.tick_trades_reconnect_sequence(
            unsubscribe_timeout + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS,
            client.server_token,
        );
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::SubscribeAllTrades.to_byte()],
            "after UnSubscribeAllTrades SendAndWait timeout, paired delayed SubscribeAllTrades still completes"
        );

        client.tick_trades_reconnect_sequence(
            unsubscribe_timeout + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS + 100,
            client.server_token,
        );
        assert!(
            drain_send_items(&client).is_empty(),
            "observed current FTradesServerToken stops further retries"
        );
    }

    #[test]
    fn successful_subscribe_all_trades_response_refreshes_reconnect_gate_like_delphi() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.server_token = 0x2222;
        client.last_trades_reconnect_check_ms = -TRADES_RECONNECT_THROTTLE_MS;
        client.with_subscription_registry_mut(|registry| {
            registry.trades_sub = Some(TradesSubscription { want_mm: true });
        });

        let response_payload =
            build_engine_response_payload(0x7777, EngineMethod::SubscribeAllTrades, &[]);
        let mut buffered = Vec::new();
        {
            let mut sink = DispatchSink::Buffer(&mut buffered);
            client.client_new_data_decoded(
                Command::API.to_byte(),
                response_payload,
                false,
                false,
                &mut sink,
            );
        }

        let refreshed_at = client.last_trades_reconnect_check_ms;
        assert!(
            refreshed_at >= 0,
            "Delphi SubscribeAllTrades success updates LastReconnectCheck"
        );

        client.tick_trades_reconnect_sequence(refreshed_at + TRADES_RECONNECT_THROTTLE_MS - 1, 0);
        assert!(
            drain_send_items(&client).is_empty(),
            "until the 5s gate expires, FTradesServerToken=0 must not cause immediate reconnect"
        );

        client.tick_trades_reconnect_sequence(refreshed_at + TRADES_RECONNECT_THROTTLE_MS, 0);
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::UnsubscribeAllTrades.to_byte()],
            "after the Delphi gate expires, missing TradesStream token starts reconnect"
        );
    }

    #[test]
    fn queued_subscribe_all_trades_request_blocks_pre_response_reconnect_like_delphi_sendandwait() {
        let mut client = dummy_client();
        client.set_domain_ready(true);
        client.server_token = 0x2222;
        client.last_trades_reconnect_check_ms = -TRADES_RECONNECT_THROTTLE_MS;

        client.subscribe_all_trades(true);
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::SubscribeAllTrades.to_byte()]
        );

        let requested_at = client
            .last_trades_subscribe_request_ms
            .load(Ordering::Relaxed);
        assert!(
            requested_at >= 0,
            "queued SubscribeAllTrades must arm the Delphi SendAndWait gate"
        );

        client.tick_trades_reconnect_sequence(
            requested_at + crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS - 1,
            0,
        );
        assert!(
            drain_send_items(&client).is_empty(),
            "NeedReconnectAllTrades must not enqueue UnsubscribeAllTrades while the subscribe request is still inside the Delphi-equivalent gate"
        );

        client.tick_trades_reconnect_sequence(
            requested_at + crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS,
            0,
        );
        assert_eq!(
            api_methods(&drain_send_items(&client)),
            vec![EngineMethod::UnsubscribeAllTrades.to_byte()]
        );
    }

    #[test]
    fn waiting_hello_retries_hello_again_like_delphi_even_before_fine() {
        let mut client = dummy_client();
        let token_before = client.client_token;

        ProtocolCore {
            client: &mut client,
        }
        .check_hello_send(100);
        assert_eq!(client.last_sent_hello, 100);
        assert!(client.waiting_hello);

        ProtocolCore {
            client: &mut client,
        }
        .check_offline_reconnect(350);

        assert_eq!(client.auth_status, AuthStatus::Offline);
        assert_eq!(client.last_sent_hello, 350);
        assert_eq!(
            client.client_token,
            token_before + 2,
            "Delphi retries HelloAgain while FWaitingHello; a dropped Fine must not stall auth",
        );
    }

    #[test]
    fn soft_reconnect_waiting_hello_still_retries_hello_again() {
        let mut client = dummy_client();
        install_session_key(&mut client);
        client.server_token = 0x1234;
        client.soft_reconnect = true;
        client.need_connect = true;
        let token_before = client.client_token;

        ProtocolCore {
            client: &mut client,
        }
        .check_hello_send(100);
        assert_eq!(client.last_sent_hello, 100);
        assert!(client.waiting_hello);
        assert_eq!(client.client_token, token_before + 1);

        ProtocolCore {
            client: &mut client,
        }
        .check_offline_reconnect(350);

        assert_eq!(client.auth_status, AuthStatus::Offline);
        assert_eq!(client.last_sent_hello, 350);
        assert_eq!(
            client.client_token,
            token_before + 2,
            "soft reconnect keeps the Delphi HelloAgain retry behavior",
        );
    }

    #[test]
    fn need_hello_again_allows_immediate_retry_on_young_client_clock() {
        let mut client = dummy_client();
        install_session_key(&mut client);

        ProtocolCore {
            client: &mut client,
        }
        .on_handshake_control_inline(Command::NeedHelloAgain, 0, 1000);

        assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
        ProtocolCore {
            client: &mut client,
        }
        .check_offline_reconnect(100);

        assert_eq!(
            client.last_sent_hello, 100,
            "NeedHelloAgain должен обходить минимум 200мс после Delphi-сброса LastSentHello в ноль",
        );
        assert!(client.waiting_hello);
    }

    #[test]
    fn ping_before_fine_does_not_stop_connect_retry_after_lost_fine() {
        let mut client = dummy_client();
        client.auth_status = AuthStatus::Connected;
        client.need_connect = true;
        client.waiting_hello = false;

        let ping_payload = vec![0u8; control::PING_SIZE];
        client
            .apply_ping_and_build_response(&ping_payload, 0.0, 0.0, 0, ping_payload.len() as u64)
            .expect("valid ping");

        assert!(
            client.need_connect,
            "Ping before AuthDone proves server liveness, not a completed Fine; connect retry must stay armed",
        );

        ProtocolCore {
            client: &mut client,
        }
        .check_hello_send(100);
        assert_eq!(client.last_sent_hello, 100);
        assert!(client.waiting_hello);
    }
}
