use super::*;
use crate::client::subscriptions::PendingCandleSubscribes;
use crate::commands::candles::DeepHistoryKind;
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
    let last_candle_subscribe_request_ms = Arc::new(AtomicI64::new(NEVER_TIME_MS));
    let pending_candle_subscribes = Arc::new(Mutex::new(PendingCandleSubscribes::default()));
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
                last_candle_subscribe_request_ms,
                pending_candle_subscribes,
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
        .send_queues
        .take_into(&mut sliced, &mut high, &mut low);
    sliced.extend(high);
    sliced.extend(low);
    sliced
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

#[test]
fn subscribe_orderbook_updates_registry_and_sends_wire_request() {
    let (sender, registry, send_q, _, _, _) = make_sender();
    sender.subscribe_orderbook("BTCUSDT");
    assert!(registry.lock().orderbook_subs.contains("BTCUSDT"));
    let sent = take_send_items(&send_q);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].cmd, Command::API.to_byte());
    assert_eq!(
        method_id(&sent[0].data),
        Some(EngineMethod::SubscribeOrderBook.to_byte())
    );
}

#[test]
fn subscribe_candles_tf_change_preserves_existing_markets_and_sends_request() {
    let (sender, registry, send_q, _, _, _) = make_sender();
    sender.subscribe_candles(["BTCUSDT"], DeepHistoryKind::Hour4);
    let first = take_send_items(&send_q);
    assert_eq!(first.len(), 1);
    assert_eq!(
        method_id(&first[0].data),
        Some(EngineMethod::SubscribeCandles.to_byte())
    );
    assert_eq!(
        engine_request_tf_param(&first[0].data),
        Some(DeepHistoryKind::Hour4.to_byte())
    );

    sender.subscribe_candles(["BTCUSDT"], DeepHistoryKind::Hour1);
    let second = take_send_items(&send_q);
    assert_eq!(
        second.len(),
        1,
        "per-market TF changes are idempotent subscribe updates; no unsubscribe is sent"
    );
    assert_eq!(
        method_id(&second[0].data),
        Some(EngineMethod::SubscribeCandles.to_byte())
    );
    assert_eq!(market_names_count(&second[0].data), Some(1));
    assert_eq!(
        engine_request_tf_param(&second[0].data),
        Some(DeepHistoryKind::Hour1.to_byte())
    );

    let registry = registry.lock();
    assert_eq!(
        registry.candle_subs.get("BTCUSDT"),
        Some(&DeepHistoryKind::Hour1)
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
        let registry = registry.lock();
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
    registry.lock().orderbook_subs.insert("ETHUSDT".to_string());
    sender.unsubscribe_orderbook("ETHUSDT");
    assert!(!registry.lock().orderbook_subs.contains("ETHUSDT"));
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
    let registry = registry.lock();
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
        .orderbook_subs
        .extend(["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    sender.unsubscribe_orderbooks(["BTCUSDT", "ETHUSDT"]);
    assert!(registry.lock().orderbook_subs.is_empty());
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
    registry.lock().orderbook_subs.insert("BTCUSDT".to_string());
    sender.unsubscribe_all_orderbooks();
    assert!(registry.lock().orderbook_subs.is_empty());
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
    let registry = registry.lock();
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
    registry.lock().trades_sub = Some(TradesSubscription { want_mm: true });
    sender.unsubscribe_all_trades();
    assert!(registry.lock().trades_sub.is_none());
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
fn sender_try_send_cmd_keyed_queues_send_item() {
    let (sender, _, send_q, _, _, _) = make_sender();
    let payload = vec![1, 2, 3, 4];
    let key = UniqueKey::order_command(crate::commands::registry::UK_ORDER_CMD_BUY, 42);

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
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoDataToSend.Create
fn sender_retry_left_clamps_zero() {
    let (sender, _, send_q, _, _, _) = make_sender();

    sender
        .try_send_cmd_keyed(
            vec![1, 2, 3, 4],
            Command::Order,
            SendPriority::High,
            true,
            0,
            UniqueKey::order_command(crate::commands::registry::UK_ORDER_CMD_BUY, 42),
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
            UniqueKey::order_command(crate::commands::registry::UK_ORDER_CMD_BUY, 42),
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
    let registry = registry.lock();
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
    // Just a check that the Display impl works (useful for logging).
    assert_eq!(
        format!("{}", SubscribeError::Disconnected),
        "Client queues disconnected"
    );
}
