use super::*;
use crate::commands::registry::{
    UK_ORDER_CMD_BUY, UK_ORDER_CMD_IMMUNE, UK_ORDER_CMD_PANIC, UK_ORDER_CMD_SELL,
    UK_ORDER_CMD_STOPS, UK_ORDER_CMD_VSTOP,
};
use crate::commands::trade::{build_order_command, OrderCommandPayload, StopSettings};

fn item(kind: u8, uid: u64, marker: u8) -> SendItem {
    SendItem {
        data: vec![marker],
        cmd: Command::Order.to_byte(),
        encrypted: true,
        priority: SendPriority::High,
        retry_left: 2,
        max_retries: 3,
        msg_num: 0,
        last_sent_at: 0,
        u_key: UniqueKey { kind, uid },
    }
}

fn item_with_priority(kind: u8, uid: u64, marker: u8, priority: SendPriority) -> SendItem {
    SendItem {
        priority,
        ..item(kind, uid, marker)
    }
}

#[test]
fn send_cmd_int_queue_removes_first_matching_sliced_or_high_before_append() {
    let mut queues = SendQueues::default();
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_CMD_BUY,
        7,
        1,
        SendPriority::High,
    ));
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_CMD_BUY,
        8,
        2,
        SendPriority::High,
    ));
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_CMD_BUY,
        7,
        3,
        SendPriority::High,
    ));
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_CMD_BUY,
        7,
        4,
        SendPriority::Sliced,
    ));

    assert_eq!(
        queues
            .high
            .iter()
            .map(|item| item.data[0])
            .collect::<Vec<_>>(),
        vec![2, 3],
        "Delphi SendCmdInt removes only from the selected High queue"
    );
    assert_eq!(
        queues
            .sliced
            .iter()
            .map(|item| item.data[0])
            .collect::<Vec<_>>(),
        vec![4],
        "Sliced queue has its own UKey scope"
    );
}

#[test]
// parity: MoonBot MoonProtoCommon.pas:TMoonProtoBaseNet.SendCmdInt
fn send_cmd_int_queue_does_not_dedup_low_priority() {
    let mut queues = SendQueues::default();
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_CMD_BUY,
        7,
        1,
        SendPriority::Low,
    ));
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_CMD_BUY,
        7,
        2,
        SendPriority::Low,
    ));

    assert_eq!(
        queues
            .low
            .iter()
            .map(|item| item.data[0])
            .collect::<Vec<_>>(),
        vec![1, 2],
        "Delphi SendCmdInt UKey removal is only for Sliced and High"
    );
}

#[test]
fn buy_stops_and_vstop_for_one_order_do_not_evict_each_other() {
    let mut queues = SendQueues::default();
    queues.push_send_cmd_int(item(UK_ORDER_CMD_BUY, 7, 1));
    queues.push_send_cmd_int(item(UK_ORDER_CMD_STOPS, 7, 2));
    queues.push_send_cmd_int(item(UK_ORDER_CMD_VSTOP, 7, 3));

    assert_eq!(
        queues
            .high
            .iter()
            .map(|item| (item.u_key.kind, item.data[0]))
            .collect::<Vec<_>>(),
        vec![
            (UK_ORDER_CMD_BUY, 1),
            (UK_ORDER_CMD_STOPS, 2),
            (UK_ORDER_CMD_VSTOP, 3),
        ]
    );
}

#[test]
fn all_six_order_command_groups_use_distinct_per_order_ukeys() {
    let order_id = 0x1122_3344_5566_7788;
    let grouped = [
        (
            OrderCommandPayload::TargetBuy {
                order_id,
                price: 100.0,
                size: 1.0,
            },
            UK_ORDER_CMD_BUY,
        ),
        (
            OrderCommandPayload::TargetSell {
                order_id,
                price: 101.0,
            },
            UK_ORDER_CMD_SELL,
        ),
        (
            OrderCommandPayload::Stops {
                order_id,
                stops: StopSettings::default(),
            },
            UK_ORDER_CMD_STOPS,
        ),
        (
            OrderCommandPayload::VStop {
                order_id,
                enabled: true,
                fixed: false,
                level: 2.0,
                volume: 3.0,
            },
            UK_ORDER_CMD_VSTOP,
        ),
        (
            OrderCommandPayload::Panic {
                order_id,
                enabled: true,
            },
            UK_ORDER_CMD_PANIC,
        ),
        (
            OrderCommandPayload::Immune {
                order_id,
                enabled: true,
            },
            UK_ORDER_CMD_IMMUNE,
        ),
    ];

    for (payload, expected_kind) in grouped {
        assert_eq!(
            crate::client::domain_send::order_command_u_key(&payload),
            UniqueKey::order_command(expected_kind, order_id),
        );
    }
}

#[test]
fn one_shot_order_commands_keep_their_delphi_retry_budget() {
    let close = build_order_command(
        1,
        OrderCommandPayload::ClosePosition {
            market_name: "BTCUSDT".to_owned(),
            mode: 0,
            flag: false,
        },
    );
    let manual_sell = build_order_command(
        2,
        OrderCommandPayload::ManualSell {
            market_name: "BTCUSDT".to_owned(),
            price: 100.0,
            size: 1.0,
        },
    );
    let start = build_order_command(
        3,
        OrderCommandPayload::Start {
            market_name: "BTCUSDT".to_owned(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 0,
            size: 1.0,
            price: 100.0,
            planned_sell_price: 0.0,
        },
    );

    for payload in [&close, &manual_sell] {
        let meta = typed_send_metadata(Command::Order, payload, Some(UniqueKey::none()))
            .expect("valid order command metadata");
        assert_eq!(meta.max_retries, 1);
        assert_eq!(meta.u_key, UniqueKey::none());
    }

    let start_meta = typed_send_metadata(Command::Order, &start, Some(UniqueKey::none()))
        .expect("valid start metadata");
    assert_eq!(start_meta.max_retries, 4);
    assert_eq!(start_meta.u_key, UniqueKey::none());
}
