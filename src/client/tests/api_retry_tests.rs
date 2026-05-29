use super::*;

fn dummy_cfg() -> ClientConfig {
    ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        mask_ver: TransportMode::V0,
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
