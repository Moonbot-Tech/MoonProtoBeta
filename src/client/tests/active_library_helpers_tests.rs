use super::*;
use crate::commands::engine_api::EngineMethod;
use std::sync::{Arc, Mutex};

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

fn writer(client: &mut Client) -> ProtocolCore<'_> {
    ProtocolCore { client }
}

#[test]
fn connect_and_init_installs_initial_strategies_before_waiting_for_auth() {
    let mut client = Client::new(dummy_cfg());
    let mut dispatcher = crate::events::EventDispatcher::new();
    let cfg = ConnectConfig::new(InitConfig {
        initial_strategies: Some(InitialStrategies::new(42, Vec::new())),
        ..Default::default()
    })
    .with_connect_timeout(Duration::ZERO);

    let result = connect_and_init(&mut client, &mut dispatcher, cfg);

    assert!(matches!(result, Err(ConnectError::ConnectTimedOut { .. })));
    assert_eq!(dispatcher.local_strategy_epoch(), 42);
    assert_eq!(dispatcher.strategy_snapshot_vec().len(), 0);
}

#[test]
fn connect_and_init_stops_waiting_as_soon_as_auth_done() {
    let mut client = Client::new(dummy_cfg().without_ntp());
    let mut dispatcher = crate::events::EventDispatcher::new();
    client.authorized = true;
    client.auth_status = AuthStatus::AuthDone;

    let started = Instant::now();
    let result = connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(InitConfig {
            step_timeout: Some(Duration::ZERO),
            ..Default::default()
        })
        .with_connect_timeout(Duration::from_secs(30)),
    );
    assert!(result.is_err(), "init should fail without server responses");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "connect_and_init waited for connect_timeout despite already authorized: {:?}",
        started.elapsed()
    );
}

#[test]
fn only_strategy_handshake_commands_are_allowed_before_domain_ready() {
    let client = Client::new(dummy_cfg());

    assert!(engine_method_allowed_before_domain_ready(
        EngineMethod::BaseCheck
    ));
    assert!(engine_method_allowed_before_domain_ready(
        EngineMethod::AuthCheck
    ));
    assert!(engine_method_allowed_before_domain_ready(
        EngineMethod::GetMarketsList
    ));
    assert!(engine_method_allowed_before_domain_ready(
        EngineMethod::UpdateMarketsList
    ));
    assert!(
        !engine_method_allowed_before_domain_ready(EngineMethod::GetMarketsIndexes),
        "Delphi cold InitInt does not send GetMarketsIndexes; it is post-init stale-token repair"
    );

    assert!(incoming_allowed_before_domain_ready(
        Command::Strat,
        &crate::commands::strat::build_snapshot_request(7)
    ));
    assert!(
        incoming_allowed_before_domain_ready(Command::Strat, &[10]),
        "TStratRuntimeState is an initial server state fact and must not be dropped before domain_ready"
    );
    assert!(
        incoming_allowed_before_domain_ready(Command::UI, &[20, 1, 1]),
        "TRuntimeStateCommand is sent from SrvConnect and must not be dropped before domain_ready"
    );
    assert!(
        incoming_allowed_before_domain_ready(Command::UI, &[22]),
        "TKernelLicenseStateCommand is an initial server state fact and must not be dropped before domain_ready"
    );
    assert!(!incoming_allowed_before_domain_ready(
        Command::Strat,
        &crate::commands::strat::build_delete(7, 42, "")
    ));

    client.strat_schema_request();
    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].cmd, Command::Strat.to_byte());
    assert!(crate::commands::strat::is_schema_request_payload(
        &high[0].data
    ));

    client.strat_send_snapshot_payload(1, 0, true, &[]);
    let (sliced, _, _) = client.take_send_queues_for_test();
    assert!(
        sliced.is_empty(),
        "snapshot replies are latched before domain_ready and sent by post-init resync"
    );

    client.strat_delete(42, "");
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(
        high.is_empty(),
        "regular Strat commands must stay gated until domain_ready"
    );
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
        "three quick series of bind errors must not immediately spam the UI",
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
    assert!(client.transport.bind_failure_streak > 0);

    client.reset_bind_failure_tracking();

    assert_eq!(client.transport.bind_failure_streak, 0);
    assert_eq!(client.transport.first_bind_failure_ms, NEVER_TIME_MS);
    assert_eq!(client.transport.last_bind_failed_event_ms, NEVER_TIME_MS);
}

#[test]
fn indexes_fetch_timeout_does_nothing_when_not_in_flight() {
    let mut client = Client::new(dummy_cfg());
    client.reconnect.indexes_fetch_in_flight = false;
    client.reconnect.indexes_fetch_started_ms = 0;
    writer(&mut client).check_indexes_fetch_timeout(100_000_000);
    assert!(!client.reconnect.indexes_fetch_in_flight);
}

#[test]
fn indexes_fetch_timeout_preserves_in_flight_within_window() {
    let mut client = Client::new(dummy_cfg());
    client.reconnect.indexes_fetch_in_flight = true;
    client.reconnect.indexes_fetch_started_ms = 0;
    writer(&mut client).check_indexes_fetch_timeout(5_000);
    assert!(
        client.reconnect.indexes_fetch_in_flight,
        "within the timeout — the flag is preserved"
    );
}

#[test]
fn indexes_fetch_timeout_clears_in_flight_after_window() {
    let mut client = Client::new(dummy_cfg());
    client.reconnect.indexes_fetch_in_flight = true;
    client.reconnect.indexes_fetch_started_ms = 0;
    client.peer_app_token = 0;
    client.reconnect.tracked_indexes_peer_app_token = 0;
    writer(&mut client).check_indexes_fetch_timeout(13_000);
    assert!(
        !client.reconnect.indexes_fetch_in_flight,
        "after the timeout without a peer_app_token mismatch — the flag is cleared"
    );
}

#[test]
fn indexes_fetch_timeout_does_not_retry_without_init_intent() {
    let mut client = Client::new(dummy_cfg());
    client.reconnect.indexes_fetch_in_flight = true;
    client.reconnect.indexes_fetch_started_ms = 0;
    client.peer_app_token = 0xABC;
    client.reconnect.tracked_indexes_peer_app_token = 0xDEF;
    client.set_domain_ready(true);
    writer(&mut client).check_indexes_fetch_timeout(13_000);
    assert!(
        !client.reconnect.indexes_fetch_in_flight,
        "timeout cleanup only clears the marker"
    );
    assert_eq!(
        client.reconnect.indexes_fetch_started_ms, 0,
        "no re-send means started timestamp is unchanged"
    );
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
}

#[test]
fn indexes_fetch_timeout_retries_after_init_intent() {
    let mut client = Client::new(dummy_cfg());
    client.reconnect.indexes_fetch_in_flight = true;
    client.reconnect.indexes_fetch_started_ms = 0;
    client.peer_app_token = 0xABC;
    client.reconnect.tracked_indexes_peer_app_token = 0xDEF;
    client.set_domain_ready(true);
    client.subscriptions.domain_restore.fetch_indexes = true;

    writer(&mut client).check_indexes_fetch_timeout(13_000);

    assert!(client.reconnect.indexes_fetch_in_flight);
    assert_eq!(client.reconnect.indexes_fetch_started_ms, 13_000);
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
    let mut client = Client::new(dummy_cfg());
    client.reconnect.indexes_fetch_in_flight = true;
    client.reconnect.indexes_fetch_started_ms = 0;
    client.peer_app_token = 0;
    client.reconnect.tracked_indexes_peer_app_token = 0xABC;
    writer(&mut client).check_indexes_fetch_timeout(13_000);
    assert!(
        !client.reconnect.indexes_fetch_in_flight,
        "peer_app_token=0 (not connected) → no re-send, flag cleared"
    );
}
