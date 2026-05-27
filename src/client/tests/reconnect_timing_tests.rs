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
        initial_balance: 0.0,
        locked_balance: 0.0,
        pos_size: 0.0,
        pos_price: 0.0,
        liq_price: 0.0,
        pos_dir: 0,
        long_pos_size: 0.0,
        long_pos_price: 0.0,
        long_liq_price: 0.0,
        long_position_type: 0,
        short_pos_size: 0.0,
        short_pos_price: 0.0,
        short_liq_price: 0.0,
        short_position_type: 0,
        asset_balance: 0.0,
        asset_balance_full: 0.0,
        total_profit_b: 0.0,
        total_profit_l: 0.0,
        total_profit_s: 0.0,
        leverage_x: 1,
        position_type: 0,
        balance_hash: 0,
        last_balance_epoch: 0,
        arb_slots: std::collections::HashMap::new(),
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

fn build_engine_response_payload(request_uid: u64, method: EngineMethod, data: &[u8]) -> Vec<u8> {
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
    let response_payload =
        build_engine_response_payload(0x7777, EngineMethod::GetMarketsIndexes, &malformed_indexes);

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
        actions
            .iter()
            .any(|action| { matches!(action, crate::events::ActiveAction::RequestOrderSnapshot) }),
        "Delphi AddNewMarket queues TAllStatusesReq after local market creation"
    );
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
        sent.iter().any(|item| {
            Command::from_byte(item.cmd) == Command::Order && item.data.first() == Some(&9)
        }),
        "active action must enqueue TAllStatusesReq"
    );
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

    let first_subscribe_response =
        build_engine_response_payload(first_subscribe_uid, EngineMethod::SubscribeAllTrades, &[]);
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
