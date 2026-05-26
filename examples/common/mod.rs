#![allow(dead_code)]

use std::time::Duration;

use moonproto::{
    parse_key_info, ClientConfig, ConnectConfig, ImportedKeyInfo, InitConfig, InitialStrategies,
    MoonClient, MoonClientError, RefreshConfig,
};

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

pub fn client_config(
    key_b64: &str,
    endpoint_arg: Option<&String>,
) -> Result<(ClientConfig, ImportedKeyInfo), String> {
    let info = parse_key_info(key_b64).ok_or_else(|| "invalid MoonBot key".to_string())?;
    let (host, port) = endpoint(endpoint_arg, &info);
    let mask_ver = info.network.map(|network| network.mask_ver).unwrap_or(0);
    let cfg = ClientConfig::new(host, port, info.keys.master_key, info.keys.mac_key)
        .with_transport_mode(mask_ver);
    Ok((cfg, info))
}

pub fn connect(
    key_b64: &str,
    endpoint_arg: Option<&String>,
    init: InitConfig,
) -> Result<MoonClient, MoonClientError> {
    let (cfg, _) = client_config(key_b64, endpoint_arg).expect("invalid MoonBot key");
    MoonClient::connect(
        cfg,
        ConnectConfig::new(init).with_connect_timeout(DEFAULT_CONNECT_TIMEOUT),
    )
}

pub fn connect_with_refresh(
    key_b64: &str,
    endpoint_arg: Option<&String>,
    init: InitConfig,
    refresh: RefreshConfig,
) -> Result<MoonClient, MoonClientError> {
    let (cfg, _) = client_config(key_b64, endpoint_arg).expect("invalid MoonBot key");
    MoonClient::connect(
        cfg.with_refresh(refresh),
        ConnectConfig::new(init).with_connect_timeout(DEFAULT_CONNECT_TIMEOUT),
    )
}

pub fn init_config() -> InitConfig {
    InitConfig {
        initial_strategies: Some(InitialStrategies::new(0, Vec::new())),
        step_timeout: None,
        ..Default::default()
    }
}

fn endpoint(endpoint_arg: Option<&String>, info: &ImportedKeyInfo) -> (String, u16) {
    if let Some(value) = endpoint_arg {
        return parse_endpoint_arg(value);
    }

    if let Some(network) = info.network {
        let host = network
            .address
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        return (host, network.port);
    }

    ("127.0.0.1".to_string(), 3000)
}

fn parse_endpoint_arg(value: &str) -> (String, u16) {
    let Some((host, port)) = value.rsplit_once(':') else {
        return (value.to_string(), 3000);
    };
    if host.is_empty() {
        return (value.to_string(), 3000);
    }
    (host.to_string(), port.parse().unwrap_or(3000))
}
