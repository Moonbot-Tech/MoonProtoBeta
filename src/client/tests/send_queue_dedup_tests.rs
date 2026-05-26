use super::*;

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
fn send_cmd_int_queue_does_not_dedup_low_priority_like_delphi() {
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
