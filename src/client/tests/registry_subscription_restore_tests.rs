use super::*;
use crate::commands::candles::DeepHistoryKind;
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

fn method_id(payload: &[u8]) -> Option<u8> {
    payload.get(11).copied()
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

fn market_names_count(payload: &[u8]) -> Option<i32> {
    let mut pos = 12usize;
    let market_len = u16::from_le_bytes(payload.get(pos..pos + 2)?.try_into().ok()?) as usize;
    pos += 2 + market_len;
    Some(i32::from_le_bytes(
        payload.get(pos..pos + 4)?.try_into().ok()?,
    ))
}

fn engine_request_tf_param(payload: &[u8]) -> Option<u8> {
    let mut pos = 12usize;
    let market_len = u16::from_le_bytes(payload.get(pos..pos + 2)?.try_into().ok()?) as usize;
    pos += 2 + market_len;
    let count = i32::from_le_bytes(payload.get(pos..pos + 4)?.try_into().ok()?);
    pos += 4;
    for _ in 0..count {
        let len = u16::from_le_bytes(payload.get(pos..pos + 2)?.try_into().ok()?) as usize;
        pos += 2 + len;
    }
    let params_size = i32::from_le_bytes(payload.get(pos..pos + 4)?.try_into().ok()?);
    pos += 4;
    if params_size != 1 {
        return None;
    }
    payload.get(pos).copied()
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
fn shared_state_preserves_subscription_intent_across_runtime_rebuild() {
    let shared = ClientSharedState::new();
    let first = Client::new_with_shared(dummy_cfg(), shared.clone());
    first.with_subscription_registry_mut(|registry| {
        registry.trades_sub = Some(TradesSubscription { want_mm: true });
        registry.mm_orders_sub = Some(false);
        registry.orderbook_subs.insert("BTCUSDT".to_string());
    });
    drop(first);

    let mut second = Client::new_with_shared(dummy_cfg(), shared);
    second.with_subscription_registry(|registry| {
        assert_eq!(
            registry.trades_sub,
            Some(TradesSubscription { want_mm: true })
        );
        assert_eq!(registry.mm_orders_sub, Some(false));
        assert!(registry.orderbook_subs.contains("BTCUSDT"));
    });

    mark_post_init(&mut second);
    second.server_token = 1;
    second.restore_registry_subscriptions();
    let sent = drain_send_items(&second);
    assert!(
        sent.iter().any(|item| item.cmd == Command::API.to_byte()
            && method_id(&item.data) == Some(EngineMethod::SubscribeAllTrades.to_byte())),
        "rebuilt client must replay the retained trades intent"
    );
    assert!(
        sent.iter().any(|item| item.cmd == Command::API.to_byte()
            && method_id(&item.data) == Some(EngineMethod::SubscribeOrderBook.to_byte())),
        "rebuilt client must replay the retained orderbook intent"
    );
}

#[test]
fn restore_with_empty_registry_sends_nothing() {
    let mut client = Client::new(dummy_cfg());
    mark_post_init(&mut client);
    client.server_token = 0xCAFE;
    client.restore_registry_subscriptions();
    let sent = drain_api_requests(&client);
    assert!(sent.is_empty(), "empty registry → 0 wire requests");
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
    assert_eq!(sent.len(), 1, "trades only → 1 wire request");
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
    assert_eq!(
        sent.len(),
        1,
        "3 orderbook subscriptions → 1 batch wire request"
    );
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::SubscribeOrderBook.to_byte())
    );
}

#[test]
fn restore_candle_subscriptions_are_batched_with_tf_kind() {
    let mut client = Client::new(dummy_cfg());
    mark_post_init(&mut client);
    client.with_subscription_registry_mut(|registry| {
        registry.candle_subs.insert("BTCUSDT".to_string());
        registry.candle_subs.insert("ETHUSDT".to_string());
        registry.candle_tf = Some(DeepHistoryKind::Hour4);
    });
    client.server_token = 1;
    client.restore_registry_subscriptions();
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::SubscribeCandles.to_byte())
    );
    assert_eq!(market_names_count(&sent[0]), Some(2));
    assert_eq!(
        engine_request_tf_param(&sent[0]),
        Some(DeepHistoryKind::Hour4.to_byte())
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
    assert_eq!(sent.len(), 2, "1 trades + 1 orderbook batch = 2 requests");
    let methods: Vec<Option<u8>> = sent.iter().map(|p| method_id(p)).collect();
    assert!(methods.contains(&Some(EngineMethod::SubscribeAllTrades.to_byte())));
    let book_count = methods
        .iter()
        .filter(|m| **m == Some(EngineMethod::SubscribeOrderBook.to_byte()))
        .count();
    assert_eq!(book_count, 1);
}
