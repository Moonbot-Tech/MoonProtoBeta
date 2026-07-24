use super::*;
use crate::commands::engine_api::EngineMethod;

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

fn ready_client() -> Client {
    let mut client = Client::new(dummy_cfg());
    client.set_domain_ready(true);
    client
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoDataToSend.Create
fn client_retry_left_clamps_zero() {
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
    assert!(
        client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTCUSDT"))
    );
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::SubscribeOrderBook.to_byte())
    );
}

#[test]
fn client_sender_can_be_held_independently_of_client() {
    // Sender holds a clone; even if client is held by a `&` reference, the sender
    // is independent. This is the basis for multi-thread subscribing without an app-event backlog.
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
    assert!(client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTC")));
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
    assert!(!client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTC")));
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
    // A double subscribe for the same pair must have no side effects
    // on the registry (HashSet dedup) and must not send a second wire request.
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
    assert!(client.with_subscription_registry(|registry| registry.trades_storage_scope.is_all()));
    // A repeat with a different want_mm updates the registry.
    client.subscribe_all_trades(false);
    assert_eq!(
        client.with_subscription_registry(|registry| registry.trades_sub),
        Some(TradesSubscription { want_mm: false }),
    );
    assert_eq!(
        client.with_subscription_registry(|registry| registry.mm_orders_sub),
        Some(false)
    );
    assert!(client.with_subscription_registry(|registry| registry.trades_storage_scope.is_all()));
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
    assert!(client.trade_storage_intent().is_none());
    assert!(crate::events::ActiveDispatchContext::from_client(&client)
        .trade_storage_intent
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
