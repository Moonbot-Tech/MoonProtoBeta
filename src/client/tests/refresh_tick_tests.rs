use super::*;

fn dummy_cfg(refresh: RefreshConfig) -> ClientConfig {
    ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        mask_ver: TransportMode::V0,
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
    client.transport.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    client.need_connect = false;
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;

    let mut dispatcher = crate::events::EventDispatcher::new();
    let initial_markets_ms = client.last_update_markets_ms;
    let initial_tags_ms = client.last_check_tags_ms;

    client.run_dispatcher_steps_for_test(1, &mut dispatcher);

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
    client.run_dispatcher_steps_for_test(1, &mut dispatcher);

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
    client.transport.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    client.need_connect = false;
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;
    client.testing_set_domain_ready(true);

    let mut dispatcher = crate::events::EventDispatcher::new();
    let initial_markets_ms = client.last_update_markets_ms;
    let initial_tags_ms = client.last_check_tags_ms;

    client.run_dispatcher_steps_for_test(1, &mut dispatcher);

    assert_ne!(client.last_update_markets_ms, initial_markets_ms);
    assert_ne!(client.last_check_tags_ms, initial_tags_ms);
}

#[test]
fn tick_sends_first_time_immediately() {
    let mut client = Client::new(dummy_cfg(RefreshConfig {
        update_markets_every: Some(Duration::from_millis(100)),
        check_tags_every: None,
    }));
    let before = client.last_update_markets_ms;
    assert_eq!(before, i64::MIN / 2);
    writer(&mut client).tick_periodic_refresh(0);
    assert_eq!(
        client.last_update_markets_ms, 0,
        "the first tick must record timestamp 0"
    );
}

#[test]
fn tick_respects_interval() {
    let mut client = Client::new(dummy_cfg(RefreshConfig {
        update_markets_every: Some(Duration::from_millis(100)),
        check_tags_every: None,
    }));
    client.last_update_markets_ms = 50;

    writer(&mut client).tick_periodic_refresh(100);
    assert_eq!(
        client.last_update_markets_ms, 50,
        "interval not elapsed — last_update_markets_ms does not change"
    );

    writer(&mut client).tick_periodic_refresh(150);
    assert_eq!(
        client.last_update_markets_ms, 150,
        "100ms elapsed — the send happened"
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
        "update_markets disabled — last_update_markets_ms does not change"
    );
    assert_eq!(
        client.last_check_tags_ms, was_tags,
        "check_tags disabled — last_check_tags_ms does not change"
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
        "update_markets disabled — leave it alone"
    );
    assert_eq!(
        client.last_check_tags_ms, 1_000_000,
        "check_tags enabled — touch it"
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
    let mut client = Client::new(dummy_cfg(RefreshConfig {
        update_markets_every: Some(Duration::from_millis(100)),
        check_tags_every: Some(Duration::from_millis(500)),
    }));
    client.set_domain_ready(true);
    client.last_update_markets_ms = 0;
    client.last_check_tags_ms = 0;

    writer(&mut client).tick_periodic_refresh(150);
    assert_eq!(client.last_update_markets_ms, 150);
    assert_eq!(client.last_check_tags_ms, 0);

    writer(&mut client).tick_periodic_refresh(600);
    assert_eq!(client.last_update_markets_ms, 600);
    assert_eq!(client.last_check_tags_ms, 600);
}

#[test]
fn tick_stale_peer_app_token_sends_indexes_before_update_markets() {
    let mut client = Client::new(dummy_cfg(RefreshConfig {
        update_markets_every: Some(Duration::from_millis(100)),
        check_tags_every: Some(Duration::from_millis(100)),
    }));
    client.set_domain_ready(true);
    client.domain_restore = DomainRestoreIntent {
        fetch_indexes: true,
    };
    client.peer_app_token = 0x2222;
    client.tracked_indexes_peer_app_token = 0x1111;
    client.last_update_markets_ms = 0;
    client.last_check_tags_ms = 0;

    writer(&mut client).tick_periodic_refresh_at(150, 42);

    let methods = drain_api_methods(&client);
    assert!(
        methods.contains(&EngineMethod::GetMarketsIndexes.to_byte()),
        "Delphi UpdateMarketsList first synchronously refreshes SrvMarkets when PeerAppToken changed"
    );
    assert!(
        !methods.contains(&EngineMethod::UpdateMarketsList.to_byte()),
        "UpdateMarketsList must wait until GetMarketsIndexes is valid for current PeerAppToken"
    );
    assert!(
        methods.contains(&EngineMethod::CheckBinanceTags.to_byte()),
        "token tag refresh is independent from server-index mapping"
    );
    assert_eq!(
        client.last_update_markets_ms, 0,
        "skipped price refresh must not consume its periodic interval"
    );
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
