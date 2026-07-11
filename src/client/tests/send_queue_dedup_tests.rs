use super::*;
use crate::commands::registry::{UK_STOP_MOVE, UK_VSTOP_MOVE};

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
    queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 1, SendPriority::High));
    queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 8, 2, SendPriority::High));
    queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 3, SendPriority::High));
    queues.push_send_cmd_int(item_with_priority(
        UK_ORDER_MOVE,
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
    queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 1, SendPriority::Low));
    queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 2, SendPriority::Low));

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
fn move_stops_and_vstop_for_one_order_do_not_evict_each_other() {
    let mut queues = SendQueues::default();
    queues.push_send_cmd_int(item(UK_ORDER_MOVE, 7, 1));
    queues.push_send_cmd_int(item(UK_STOP_MOVE, 7, 2));
    queues.push_send_cmd_int(item(UK_VSTOP_MOVE, 7, 3));

    assert_eq!(
        queues
            .high
            .iter()
            .map(|item| (item.u_key.kind, item.data[0]))
            .collect::<Vec<_>>(),
        vec![(UK_ORDER_MOVE, 1), (UK_STOP_MOVE, 2), (UK_VSTOP_MOVE, 3)]
    );
}
