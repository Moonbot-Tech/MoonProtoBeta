use super::*;
use crate::commands::engine_api::{AuthCheckResponse, ServerInfo};

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

#[test]
fn server_info_default_on_new_client() {
    let client = Client::new(dummy_cfg());
    assert_eq!(client.server_info(), &ServerInfo::default());
    assert!(!client.server_info().has_identity());
    assert!(client.auth_info().is_none());
}

#[test]
fn set_server_info_updates_storage_and_is_retrievable_via_getter() {
    let mut client = Client::new(dummy_cfg());
    let info = ServerInfo {
        bot_id: Some(0x1234_5678),
        server_name: Some("Test Server".to_string()),
        exchange_code: Some(1),
        exchange_name: Some("Binance Futures".to_string()),
        base_currency_name: Some("USDT".to_string()),
        base_currency_code: Some(1),
        ..Default::default()
    };
    client.set_server_info(info.clone());
    assert_eq!(client.server_info(), &info);
    assert_eq!(client.server_info().bot_id, Some(0x1234_5678));
    assert_eq!(
        client.server_info().exchange_name.as_deref(),
        Some("Binance Futures")
    );
    assert!(client.server_info().has_identity());
}

#[test]
fn server_info_independent_across_clients() {
    let mut client_a = Client::new(dummy_cfg());
    let mut client_b = Client::new(dummy_cfg());

    client_a.set_server_info(ServerInfo {
        bot_id: Some(100),
        exchange_name: Some("Binance".to_string()),
        ..Default::default()
    });
    client_b.set_server_info(ServerInfo {
        bot_id: Some(200),
        exchange_name: Some("Bybit".to_string()),
        ..Default::default()
    });

    assert_eq!(client_a.server_info().bot_id, Some(100));
    assert_eq!(client_b.server_info().bot_id, Some(200));
    assert_eq!(
        client_a.server_info().exchange_name.as_deref(),
        Some("Binance")
    );
    assert_eq!(
        client_b.server_info().exchange_name.as_deref(),
        Some("Bybit")
    );
}

#[test]
fn trade_ctx_requires_base_check_route_fields() {
    let client = Client::new(dummy_cfg());

    let err = client
        .trade_ctx(0x0102_0304_0506_0708)
        .expect_err("new client has no BaseCheck route");
    assert!(err.missing_exchange_code);
    assert!(err.missing_base_currency_code);
}

#[test]
fn trade_ctx_uses_server_info_route_fields() {
    let mut client = Client::new(dummy_cfg());
    client.set_server_info(ServerInfo {
        exchange_code: Some(9),
        base_currency_code: Some(17),
        ..Default::default()
    });

    let ctx = client
        .trade_ctx(0x0102_0304_0506_0708)
        .expect("route fields are present");

    assert_eq!(ctx.uid, 0x0102_0304_0506_0708);
    assert_eq!(ctx.currency, 17);
    assert_eq!(ctx.platform, 9);
}

#[test]
fn set_auth_info_updates_storage_and_is_retrievable_via_getter() {
    let mut client = Client::new(dummy_cfg());
    let auth = AuthCheckResponse {
        binance_account_id: 123,
        btc_address: "btc".to_string(),
        spot_ref: 7,
        is_sub_account: true,
        account_id: "acc".to_string(),
        recvd_max_payload: Some(4096),
        known_dexes: Vec::new(),
        hl_dex_market: Some(1),
        hl_spot_market: Some(0),
    };

    client.set_auth_info(auth.clone());

    assert_eq!(client.auth_info(), Some(&auth));
}
