use super::*;

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

#[test]
fn engine_api_sliced_requests_use_registry_retry_count() {
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

#[test]
fn rejected_pre_init_subscription_does_not_update_reconnect_clocks() {
    let client = Client::new(dummy_cfg());
    let raw = crate::commands::engine_request::subscribe_all_trades(false);

    client.send_api_request_at(&raw, 1234);

    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty());
    assert!(high.is_empty());
    assert!(low.is_empty());
    assert_eq!(
        client
            .reconnect
            .last_trades_subscribe_request_ms
            .load(Ordering::Relaxed),
        NEVER_TIME_MS,
        "a request rejected by the domain gate must not look like an in-flight reconnect subscribe"
    );
}
