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
    client.peer_app_token = 0;
    client.tracked_indexes_peer_app_token = 0;
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
