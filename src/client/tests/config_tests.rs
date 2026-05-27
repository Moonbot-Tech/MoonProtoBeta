use super::*;

fn zero_key() -> MoonKey {
    [0u8; 16]
}

#[test]
fn client_config_defaults_to_v0() {
    let cfg = ClientConfig::new("127.0.0.1", 3000, zero_key(), zero_key());
    assert_eq!(cfg.mask_ver, 0);
}

#[test]
fn transport_builder_keeps_v1() {
    let cfg = ClientConfig::new("127.0.0.1", 3000, zero_key(), zero_key()).with_transport_mode(1);

    assert_eq!(cfg.mask_ver, 1);
}

#[test]
fn transport_builder_keeps_v2() {
    let cfg = ClientConfig::new("127.0.0.1", 3000, zero_key(), zero_key()).with_transport_mode(2);

    assert_eq!(cfg.mask_ver, 2);
}

#[test]
fn transport_builder_rejects_unsupported_mode_values() {
    let cfg = ClientConfig::new("127.0.0.1", 3000, zero_key(), zero_key()).with_transport_mode(7);

    assert_eq!(cfg.mask_ver, 0);
}
