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
        transport_mode: TransportMode::V0,
        client_id: 0,
        ntp_host: None,
        refresh: RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        },
        market_history: crate::state::MarketHistorySizing::default(),
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
        has_1000_prefix_alias: false,
        bn_iceberg: false,
        bn_only_isolated: false,
        futures_type: BaseCurrency::USDT,
        initial_balance: 0.0,
        locked_balance: 0.0,
        pos_size: 0.0,
        pos_price: 0.0,
        liq_price: 0.0,
        pos_dir: crate::commands::trade::OrderType::Sell,
        long_pos_size: 0.0,
        long_pos_price: 0.0,
        long_liq_price: 0.0,
        long_position_type: crate::commands::market::PositionType::Cross,
        short_pos_size: 0.0,
        short_pos_price: 0.0,
        short_liq_price: 0.0,
        short_position_type: crate::commands::market::PositionType::Cross,
        asset_balance: 0.0,
        asset_balance_full: 0.0,
        total_profit_b: 0.0,
        total_profit_l: 0.0,
        total_profit_s: 0.0,
        leverage_x: 1,
        position_type: crate::commands::market::PositionType::Cross,
        balance_hash: 0,
        last_balance_epoch: 0,
        trade_tail: Default::default(),
        price: Default::default(),
        delta_state: Default::default(),
        market_blacklisted_cfg: false,
        arb_slots: std::collections::HashMap::new(),
    }
}

fn install_session_key(client: &mut Client) {
    client.server_token = 1;
    client.session_rnd = client.handshake_rnd;
    let (encode_key, decode_key) = crypto::generate_session_sub_keys(
        &client.cfg.master_key,
        client.cfg.client_id,
        client.server_token,
        &client.session_rnd,
    );
    client.encode_key = encode_key;
    client.decode_key = decode_key;
    client.encode_cipher = Some(crypto::cipher_from_key(&encode_key));
    client
        .recv
        .data_read_state
        .set_decode_cipher(crypto::cipher_from_key(&decode_key));
    client.refresh_ack_session32();
}

fn encrypted_hello(
    client: &Client,
    cmd: Command,
    server_token: u64,
    peer_app_token: u64,
) -> Vec<u8> {
    let mix_ts = client.client_token.wrapping_add(1);
    let mut hello = handshake::Hello::new(mix_ts, client.app_token);
    hello.rnd = client.handshake_rnd;
    hello.server_token = server_token;
    hello.app_token = peer_app_token;
    if matches!(cmd, Command::Fine | Command::WantNewHello) {
        hello.peer_mix = 0;
    }
    hello.timestamp = delphi_now();
    let aad = handshake::handshake_aad(client.cfg.client_id, cmd.to_byte());
    if cmd == Command::Fine {
        let cipher = client
            .recv
            .data_read_state
            .decode_cipher
            .as_ref()
            .expect("test session decode cipher");
        crypto::encrypt_with_cipher(cipher, &hello.to_bytes_packed(), &aad)
    } else {
        crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad)
    }
}

fn apply_reader_handshake_payload(client: &mut Client, cmd: Command, payload: &[u8]) -> bool {
    match cmd {
        Command::WhoAreYou => {
            let master_key = client.cfg.master_key;
            let client_id = client.cfg.client_id;
            let Some(hello) =
                Client::decode_handshake_hello(&master_key, client_id, cmd.to_byte(), payload)
            else {
                return false;
            };
            if !client.same_handshake_rnd(&hello.rnd) {
                return false;
            }
            let _encrypted_imfriend = ProtocolCore { client }.apply_hello_and_build_imfriend(hello);
            true
        }
        Command::Fine => {
            let aad = handshake::handshake_aad(client.cfg.client_id, Command::Fine.to_byte());
            let Some(cipher) = client.recv.data_read_state.decode_cipher.as_ref() else {
                return false;
            };
            let Some(decrypted) = crypto::decrypt_with_cipher(cipher, payload, &aad) else {
                return false;
            };
            let Some(hello) = handshake::Hello::from_bytes(&decrypted) else {
                return false;
            };
            if !client.same_handshake_rnd(&hello.rnd) || hello.peer_mix != 0 {
                return false;
            }
            client.accepted_server_mix_ts(hello.mix_ts);
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
fn want_new_hello_outside_rebind_is_dropped_without_clearing_wait() {
    let mut client = dummy_client();
    client.start_hello_wait(HelloWaitState::PrimaryHelloCold, 0);
    client.last_sent_hello = 123;
    let need_connect_before = client.need_connect;

    ProtocolCore {
        client: &mut client,
    }
    .on_handshake_control(Command::WantNewHello, &[], 0, 0);

    assert_eq!(client.last_sent_hello, 123);
    assert!(client.waiting_hello);
    assert!(!client.next_primary_hello_new_session);
    assert_eq!(client.need_connect, need_connect_before);
}

#[test]
fn want_new_hello_makes_late_fine_invalid_until_new_who_are_you() {
    let mut client = dummy_client();
    client.handshake_rnd = [0xC1; 16];
    install_session_key(&mut client);
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.lifecycle.was_ever_connected = true;
    client.set_hello_wait_state(HelloWaitState::RebindHelloAgain);
    let fine = encrypted_hello(&client, Command::Fine, client.server_token, 0x3333);
    let want = encrypted_hello(&client, Command::WantNewHello, 0, 0);

    ProtocolCore {
        client: &mut client,
    }
    .on_handshake_control(Command::WantNewHello, &want, 0, 0);

    ProtocolCore {
        client: &mut client,
    }
    .on_fine(&fine, 0, 10);

    assert!(!client.authorized);
    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert!(client.need_connect);
    assert!(
        client.next_primary_hello_new_session,
        "after WantNewHello the next valid path is a new hard Hello, not old Fine",
    );
}

#[test]
fn late_want_new_hello_does_not_reset_fresh_authorized_session() {
    let mut client = dummy_client();
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.need_connect = false;
    client.server_token = 0x1111;
    client.clear_hello_wait_state();

    ProtocolCore {
        client: &mut client,
    }
    .on_handshake_control(Command::WantNewHello, &[], 0, 10);

    assert!(client.authorized);
    assert_eq!(client.auth_status, AuthStatus::AuthDone);
    assert!(!client.need_connect);
    assert_eq!(client.server_token, 0x1111);
    assert!(!client.next_primary_hello_new_session);
}

#[test]
fn want_new_hello_is_accepted_during_rebind_hello_again() {
    let mut client = dummy_client();
    client.authorized = true;
    client.auth_status = AuthStatus::Offline;
    client.need_connect = false;
    client.server_token = 0x1111;
    client.handshake_rnd = [0xA5; 16];
    client.set_hello_wait_state(HelloWaitState::RebindHelloAgain);
    let want = encrypted_hello(&client, Command::WantNewHello, 0, 0);

    ProtocolCore {
        client: &mut client,
    }
    .on_handshake_control(Command::WantNewHello, &want, 0, 10);

    assert!(!client.authorized);
    assert_eq!(client.auth_status, AuthStatus::Connected);
    assert!(client.need_connect);
    assert!(client.next_primary_hello_new_session);
}

#[test]
fn hello_again_without_session_is_not_built() {
    let mut client = dummy_client();
    let token_before = client.client_token;
    let payload = ProtocolCore {
        client: &mut client,
    }
    .build_hello_again_packet();

    assert!(payload.is_none());
    assert_eq!(client.client_token, token_before);
}

#[test]
fn fine_requires_session_key_hello_payload() {
    let mut client = dummy_client();
    client.start_hello_wait(HelloWaitState::PrimaryImFriendSent, 0);
    client.handshake_rnd = [0xA5; 16];

    assert!(!apply_reader_handshake_payload(
        &mut client,
        Command::Fine,
        b"not an encrypted hello",
    ));

    assert!(!client.authorized);
    assert_ne!(client.auth_status, AuthStatus::AuthDone);

    let mut hello = handshake::Hello::new(client.client_token, client.app_token);
    hello.rnd = client.handshake_rnd;
    hello.timestamp = delphi_now();
    let aad = handshake::handshake_aad(client.cfg.client_id, Command::Fine.to_byte());
    let master_payload = crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad);

    assert!(apply_reader_handshake_payload(&mut client, Command::Fine, &master_payload,) == false);

    install_session_key(&mut client);
    client.authorized = false;
    client.auth_status = AuthStatus::Connected;
    let payload = encrypted_hello(
        &client,
        Command::Fine,
        client.server_token,
        client.app_token,
    );

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
    client.start_hello_wait(HelloWaitState::PrimaryImFriendSent, 0);
    client.handshake_rnd = [0xB1; 16];
    install_session_key(&mut client);
    client.authorized = false;
    client.auth_status = AuthStatus::Connected;
    client.reconnect.tracked_indexes_peer_app_token = 0;
    client.with_subscription_registry_mut(|registry| {
        registry.trades_sub = Some(TradesSubscription { want_mm: true });
        registry.mm_orders_sub = Some(true);
        registry.orderbook_subs.insert("BTCUSDT".to_string());
    });

    let payload = encrypted_hello(
        &client,
        Command::Fine,
        client.server_token,
        client.app_token,
    );

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
    client.lifecycle.was_ever_connected = true;
    client.auth_status = AuthStatus::AuthDone;
    client.prev_auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.peer_app_token = 0x1000;
    client.reconnect.tracked_indexes_peer_app_token = 0x1000;
    client.subscriptions.domain_restore = DomainRestoreIntent {
        fetch_indexes: true,
    };
    client.with_subscription_registry_mut(|registry| {
        registry.trades_sub = Some(TradesSubscription { want_mm: false });
        registry.orderbook_subs.insert("BTCUSDT".to_string());
    });

    let who = encrypted_hello(&client, Command::WhoAreYou, 0x2222, 0x2000);
    assert!(apply_reader_handshake_payload(
        &mut client,
        Command::WhoAreYou,
        &who,
    ));
    let fine = encrypted_hello(&client, Command::Fine, 0x2222, 0x2000);
    assert!(apply_reader_handshake_payload(
        &mut client,
        Command::Fine,
        &fine,
    ));

    assert!(client.authorized);
    assert_eq!(client.auth_status, AuthStatus::AuthDone);
    assert!(
        client.reconnect.indexes_fetch_in_flight,
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
        .reconnect
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
    assert!(!client.reconnect.indexes_fetch_in_flight);
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
    assert_eq!(
        client.reconnect.subscribed_book_server_token,
        client.server_token
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
fn post_init_reconnect_same_peer_app_token_does_not_refetch_indexes() {
    let mut client = dummy_client();

    client.set_domain_ready(true);
    client.lifecycle.was_ever_connected = true;
    client.auth_status = AuthStatus::AuthDone;
    client.prev_auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.peer_app_token = 0x1000;
    client.reconnect.tracked_indexes_peer_app_token = 0x1000;
    client.subscriptions.domain_restore = DomainRestoreIntent {
        fetch_indexes: true,
    };
    client.with_subscription_registry_mut(|registry| {
        registry.orderbook_subs.insert("BTCUSDT".to_string());
    });

    let who = encrypted_hello(&client, Command::WhoAreYou, 0x2222, 0x1000);
    assert!(apply_reader_handshake_payload(
        &mut client,
        Command::WhoAreYou,
        &who,
    ));
    let fine = encrypted_hello(&client, Command::Fine, 0x2222, 0x1000);
    assert!(apply_reader_handshake_payload(
        &mut client,
        Command::Fine,
        &fine,
    ));

    let methods = api_methods(&drain_send_items(&client));
    assert!(
        !methods.contains(&(EngineMethod::GetMarketsIndexes.to_byte())),
        "Delphi GetMarketsIndexes repair is only for changed PeerAppToken"
    );
    assert!(
        methods.contains(&(EngineMethod::SubscribeOrderBook.to_byte())),
        "same-token reconnect can replay orderbook subscription immediately"
    );
    assert!(
        !methods.contains(&(EngineMethod::UpdateMarketsList.to_byte())),
        "do not synthesize an indexes->update chain when indexes are already current"
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
    let subscribe_due = client
        .reconnect
        .pending_trades_resubscribe_after_ms
        .unwrap();
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
    client.reconnect.tracked_indexes_peer_app_token = 0x3333;
    client.reconnect.subscribed_book_server_token = 0x1111;
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
    assert_eq!(
        client.reconnect.pending_orderbook_resubscribe_uid,
        Some(first_uid)
    );
    assert_eq!(client.reconnect.last_book_reconnect_check_ms, 10_000);

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
        client.reconnect.subscribed_book_server_token, 0x1111,
        "non-reconnect SubscribeOrderBook success must not stop a pending full replay"
    );
    assert_ne!(
            client
                .reconnect.last_orderbook_subscribe_request_ms
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
    assert_eq!(
        client.reconnect.pending_orderbook_resubscribe_uid,
        Some(second_uid)
    );

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
    assert_eq!(
        client.reconnect.subscribed_book_server_token,
        client.server_token
    );
    assert_eq!(client.reconnect.pending_orderbook_resubscribe_uid, None);
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
    client.reconnect.tracked_indexes_peer_app_token = 0x3333;
    client.reconnect.subscribed_book_server_token = 0x1111;
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
// parity: MoonBot MoonProtoEngine.pas:SendAndWait
fn queued_orderbook_subscribe_blocks_pre_response_reconnect() {
    let mut client = dummy_client();
    client.set_domain_ready(true);
    client.auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.server_token = 0x2222;
    client.peer_app_token = 0x3333;
    client.reconnect.tracked_indexes_peer_app_token = 0x3333;
    client.reconnect.subscribed_book_server_token = 0x1111;

    client.subscribe_orderbook("BTCUSDT");
    assert_eq!(
        api_methods(&drain_send_items(&client)),
        vec![EngineMethod::SubscribeOrderBook.to_byte()]
    );
    let requested_at = client
        .reconnect
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
// parity: MoonBot MoonProtoEngine.pas:NeedResubscribeOrderBooks
fn first_successful_orderbook_subscribe_sets_initial_book_server_token() {
    let mut client = dummy_client();
    client.set_domain_ready(true);
    client.auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.server_token = 0x2222;
    client.reconnect.subscribed_book_server_token = 0;

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

    assert_eq!(client.reconnect.subscribed_book_server_token, 0x2222);
}

#[test]
fn malformed_get_markets_indexes_response_does_not_reopen_stream_gate() {
    let mut client = dummy_client();
    client.set_domain_ready(true);
    client.auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.peer_app_token = 0x2000;
    client.reconnect.tracked_indexes_peer_app_token = 0x1000;
    client.reconnect.indexes_fetch_in_flight = true;
    client.reconnect.update_markets_after_indexes = true;
    client.reconnect.restore_orderbooks_after_indexes = true;
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
        !client.reconnect.indexes_fetch_in_flight,
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
        client.reconnect.update_markets_after_indexes,
        "retry after a later valid indexes response must still refresh markets"
    );
    assert!(
        client.reconnect.restore_orderbooks_after_indexes,
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
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (NewMarketFound)
fn unknown_indexed_market_price_requests_markets_list() {
    let mut client = dummy_client();
    client.set_domain_ready(true);
    client.auth_status = AuthStatus::AuthDone;
    client.authorized = true;
    client.peer_app_token = 0x2000;
    client.reconnect.tracked_indexes_peer_app_token = 0x2000;

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
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (NewMarkets)
fn new_market_list_refresh_requests_immediate_prices() {
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
    let first_subscribe_due = client
        .reconnect
        .pending_trades_resubscribe_after_ms
        .unwrap();
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
    let refreshed_at = client.reconnect.last_trades_reconnect_check_ms;
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
// parity: MoonBot MoonProtoEngine.pas:SubscribeAllTrades
fn successful_subscribe_all_trades_response_refreshes_reconnect_gate() {
    let mut client = dummy_client();
    client.set_domain_ready(true);
    client.server_token = 0x2222;
    client.reconnect.last_trades_reconnect_check_ms = -TRADES_RECONNECT_THROTTLE_MS;
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

    let refreshed_at = client.reconnect.last_trades_reconnect_check_ms;
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
// parity: MoonBot MoonProtoEngine.pas:SendAndWait
fn queued_subscribe_all_trades_request_blocks_pre_response_reconnect() {
    let mut client = dummy_client();
    client.set_domain_ready(true);
    client.server_token = 0x2222;
    client.reconnect.last_trades_reconnect_check_ms = -TRADES_RECONNECT_THROTTLE_MS;

    client.subscribe_all_trades(true);
    assert_eq!(
        api_methods(&drain_send_items(&client)),
        vec![EngineMethod::SubscribeAllTrades.to_byte()]
    );

    let requested_at = client
        .reconnect
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
fn primary_hello_wait_does_not_retry_hello_again_before_session_exists() {
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

    assert_eq!(
        client.last_sent_hello, 100,
        "primary handshake wait must not produce session HelloAgain",
    );
    assert_eq!(
        client.client_token,
        token_before + 1,
        "only the original hard Hello is sent before WhoAreYou/session state exists",
    );
    assert_ne!(client.auth_status, AuthStatus::Offline);
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
    client.lifecycle.was_ever_connected = true;

    ProtocolCore {
        client: &mut client,
    }
    .apply_need_hello_again(1000);

    assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
    ProtocolCore {
        client: &mut client,
    }
    .check_hello_send(100);

    assert_eq!(
        client.last_sent_hello, 100,
        "NeedHelloAgain must bypass the 200ms minimum after the Delphi reset of LastSentHello to zero",
    );
    assert!(client.waiting_hello);
}

#[test]
fn primary_handshake_timeout_does_not_turn_into_soft_hello_again() {
    let mut client = dummy_client();
    client.server_token = 0x1234;
    client.need_connect = true;
    client.start_hello_wait(HelloWaitState::PrimaryImFriendSent, 0);

    ProtocolCore {
        client: &mut client,
    }
    .check_reconnect_timeout(RECONNECT_WAITING_MS + 1);

    assert!(client.force_disconnect);
    assert!(client.need_connect);
    assert!(
        !client.soft_reconnect,
        "primary/ImFriend timeout must retry hard Hello, not HelloAgain",
    );
    assert!(client.next_primary_hello_new_session);
    assert!(!client.waiting_hello);
}

#[test]
fn rebind_handshake_timeout_keeps_soft_hello_again_path() {
    let mut client = dummy_client();
    install_session_key(&mut client);
    client.server_token = 0x1234;
    client.need_connect = true;
    client.start_hello_wait(HelloWaitState::RebindHelloAgain, 0);

    ProtocolCore {
        client: &mut client,
    }
    .check_reconnect_timeout(RECONNECT_WAITING_MS + 1);

    assert!(client.force_disconnect);
    assert!(client.need_connect);
    assert!(
        client.soft_reconnect,
        "rebind timeout preserves Delphi soft reconnect / HelloAgain path",
    );
    assert!(!client.waiting_hello);
}

#[test]
fn ping_before_fine_does_not_stop_connect_retry_after_lost_fine() {
    let mut client = dummy_client();
    client.auth_status = AuthStatus::Connected;
    client.need_connect = true;
    client.clear_hello_wait_state();

    let ping_payload = vec![0u8; control::PING_SIZE];
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_cb = std::sync::Arc::clone(&events);
    let mut mode = RunMode::Callback {
        on_data: Box::new(move |cmd, payload| {
            events_cb.lock().unwrap().push((cmd, payload.to_vec()));
        }),
    };
    let sent_before = client.take_send_queues_for_test();
    let sent_before_len = sent_before.0.len() + sent_before.1.len() + sent_before.2.len();
    let mut protocol_wait = Duration::ZERO;

    ProtocolCore {
        client: &mut client,
    }
    .route_command(
        Command::Ping.to_byte(),
        &ping_payload,
        ping_payload.len() as u64,
        ping_payload.len() as u64,
        10,
        &mut mode,
        &mut protocol_wait,
    );
    drop(mode);
    let sent_after_ping = client.take_send_queues_for_test();
    let sent_after_ping_len =
        sent_after_ping.0.len() + sent_after_ping.1.len() + sent_after_ping.2.len();

    assert!(
            client.need_connect,
            "Ping before AuthDone proves server liveness, not a completed Fine; connect retry must stay armed",
        );
    assert_eq!(
        client.round_trip_delay, 0,
        "pre-AuthDone Ping must not update RTT/PMTU fields",
    );
    assert!(
        events.lock().unwrap().is_empty(),
        "pre-AuthDone Ping must not reach app/domain callback",
    );
    assert_eq!(
        sent_before_len, sent_after_ping_len,
        "pre-AuthDone Ping must not send a Ping response",
    );

    ProtocolCore {
        client: &mut client,
    }
    .check_hello_send(100);
    assert_eq!(client.last_sent_hello, 100);
    assert!(client.waiting_hello);
}
