use super::records::STOP_SETTINGS_SIZE;
use super::*;
use crate::commands::registry::{write_string, CURRENT_PROTO_CMD_VER};

#[test]
fn stop_settings_wire_layout_matches_fixed_record() {
    let settings = StopSettings::disabled()
        .with_stop_loss_percent(1.25, 0.1)
        .with_trailing_percent(2.5, 0.2)
        .with_take_profit_price(123.0);
    let mut bytes = Vec::new();
    settings.write_to(&mut bytes);
    assert_eq!(bytes.len(), STOP_SETTINGS_SIZE);
    let mut input = bytes.as_slice();
    assert_eq!(StopSettings::read_from_delphi_stream(&mut input), settings);
    assert!(input.is_empty());
}

#[test]
fn closed_sell_order_report_parses_dbid_and_sql() {
    let mut bytes = Vec::new();
    BaseCommandHeader {
        cmd_id: 31,
        ver: CURRENT_PROTO_CMD_VER,
        uid: 77,
    }
    .write(&mut bytes);
    bytes.extend_from_slice(&123i64.to_le_bytes());
    write_string(&mut bytes, "UPDATE Orders SET Status=1 WHERE ID=123");

    let TradeCommand::ClosedSellOrderReport(report) = TradeCommand::parse(&bytes).unwrap() else {
        panic!("expected closed-sell report");
    };
    assert_eq!(report.db_id, 123);
    assert_eq!(report.sql, "UPDATE Orders SET Status=1 WHERE ID=123");
}

#[test]
fn order_type_preserves_unknown_wire_ordinal() {
    let unknown = OrderType::from_byte(250);
    assert_eq!(unknown.to_byte(), 250);
    assert!(!unknown.is_known());
}
