use super::*;

#[test]
fn slice_header_and_ack_use_private_wire_structs() {
    assert_eq!(std::mem::size_of::<WireSliceHeader>(), 4);
    assert_eq!(SLICE_HEADER_SIZE, 4);
    assert_eq!(std::mem::size_of::<WireSlicedAck>(), 34);
    assert_eq!(ACK256_WIRE_SIZE, 34);

    let header = SliceHeader {
        datagram_num: 0x1234,
        block_num: 5,
        max_block_num: 9,
    };
    let mut header_bytes = Vec::new();
    header.write_to(&mut header_bytes);
    assert_eq!(header_bytes, vec![0x34, 0x12, 5, 9]);
    let parsed = SliceHeader::from_bytes(&header_bytes).expect("valid TMoonProtoSliceHeader");
    assert_eq!(parsed.datagram_num, 0x1234);
    assert_eq!(parsed.block_num, 5);
    assert_eq!(parsed.max_block_num, 9);

    let mut flags = [0u8; 32];
    flags[0] = 0b0000_0011;
    flags[31] = 0x80;
    let ack = build_ack_bytes(&flags, 0xABCD);
    assert_eq!(&ack[0..32], &flags);
    assert_eq!(&ack[32..34], &0xABCDu16.to_le_bytes());
    let (parsed_flags, parsed_datagram) = parse_ack_bytes(&ack).expect("valid ACK256");
    assert_eq!(parsed_flags, flags);
    assert_eq!(parsed_datagram, 0xABCD);
}

#[test]
fn single_block_datagram() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    // Single block: SliceHeader(dgram=1, block=0, max=0) + cmd(0x0A) + data
    let payload = vec![
        0x01, 0x00, // datagram_num = 1
        0x00, // block_num = 0
        0x00, // max_block_num = 0 (1 block total)
        0x0A, // cmd byte
        0xDE, 0xAD, // data
    ];

    let (assembled, _ack) = recv.on_new_sliced(&payload);
    let (datagram_num, cmd, data, _, _) = assembled.unwrap();
    assert_eq!(datagram_num, 1);
    assert_eq!(cmd, 0x0A);
    assert_eq!(data, vec![0xDE, 0xAD]);
    assert!(
            recv.receiving.contains_key(&datagram_num),
            "TMoonProtoClient.OnNewSliced returns the completed object; BaseNet.OnNewSliced removes it after DataReadInt"
        );
    recv.receiving.remove(&datagram_num);
}

#[test]
fn multi_block_datagram() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    // Block 1 arrives first
    let block1 = vec![
        0x05, 0x00, // datagram_num = 5
        0x01, // block_num = 1
        0x01, // max_block_num = 1 (2 blocks total)
        0xBB, 0xCC, // data
    ];
    let (assembled, _) = recv.on_new_sliced(&block1);
    assert!(assembled.is_none()); // not complete yet

    // Block 0 arrives
    let block0 = vec![
        0x05, 0x00, // datagram_num = 5
        0x00, // block_num = 0
        0x01, // max_block_num = 1
        0x1C, // cmd byte
        0xAA, // data
    ];
    let (assembled, _) = recv.on_new_sliced(&block0);
    let (datagram_num, cmd, data, _, _) = assembled.unwrap();
    assert_eq!(datagram_num, 5);
    assert_eq!(cmd, 0x1C);
    assert_eq!(data, vec![0xAA, 0xBB, 0xCC]);
    assert!(recv.receiving.contains_key(&datagram_num));
    recv.receiving.remove(&datagram_num);
}

#[test]
fn completed_datagram_is_returned_only_once_before_caller_removes_it() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let payload = vec![
        0x09, 0x00, // datagram_num = 9
        0x00, // block_num = 0
        0x00, // max_block_num = 0
        0x0A, // cmd byte
        0xDE, 0xAD,
    ];

    let (assembled, ack) = recv.on_new_sliced(&payload);
    assert!(assembled.is_some());
    assert_eq!(ack[0] & 0x01, 0x01);
    assert!(recv.receiving.contains_key(&9));

    let (duplicate_assembled, duplicate_ack) = recv.on_new_sliced(&payload);
    assert!(
            duplicate_assembled.is_none(),
            "after a complete datagram was returned once, duplicate pieces must not queue a second DataReadInt"
        );
    assert_eq!(duplicate_ack[0] & 0x01, 0x01);
    assert!(recv.receiving.contains_key(&9));
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.OnNewSliced
fn duplicate_after_completed_datagram_gets_full_ack() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let block0 = vec![
        0x09, 0x00, // datagram_num = 9
        0x00, // block_num = 0
        0x03, // max_block_num = 3
        0x0A, // cmd byte
        0xAA,
    ];
    let block1 = vec![0x09, 0x00, 0x01, 0x03, 0xBB];
    let block2 = vec![0x09, 0x00, 0x02, 0x03, 0xCC];
    let block3 = vec![0x09, 0x00, 0x03, 0x03, 0xDD];

    assert!(recv.on_new_sliced(&block0).0.is_none());
    assert!(recv.on_new_sliced(&block1).0.is_none());
    assert!(recv.on_new_sliced(&block2).0.is_none());
    assert!(recv.on_new_sliced(&block3).0.is_some());
    assert!(
        recv.receiving.contains_key(&9),
        "Rust keeps the completed datagram until caller runs DataReadInt and removes it"
    );

    let (duplicate_assembled, duplicate_ack) = recv.on_new_sliced(&block1);

    assert!(duplicate_assembled.is_none());
    assert!(
        duplicate_ack[..32].iter().all(|byte| *byte == 0xFF),
        "Delphi would already have removed Receiving, so the next duplicate hits ACK.SetAllFlags"
    );
    assert_eq!(&duplicate_ack[32..34], &9u16.to_le_bytes());
}

#[test]
fn non_duplicate_block_after_completed_datagram_does_not_mutate_receiver() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let complete_block0 = vec![
        0x37, 0x00, // datagram_num = 55
        0x00, // block_num = 0
        0x00, // max_block_num = 0, so one received block completes the datagram
        0xAA, 0xBB,
    ];
    let later_block_same_datagram = vec![
        0x37, 0x00, // datagram_num = 55
        0x08, // a different block before Rust caller removed Receiving
        0x00, 0xBB,
    ];

    assert!(recv.on_new_sliced(&complete_block0).0.is_some());
    let before_blocks = recv
        .receiving
        .get(&55)
        .expect("completed datagram stays until caller removal")
        .received_count;

    let (assembled, ack) = recv.on_new_sliced(&later_block_same_datagram);

    assert!(assembled.is_none());
    assert!(ack[..32].iter().all(|byte| *byte == 0xFF));
    assert_eq!(
        recv.receiving.get(&55).unwrap().received_count,
        before_blocks,
        "after completion Rust must emulate Delphi post-removal path without adding later pieces"
    );
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.OnNewSliced
fn block_num_above_max_is_dropped_without_ack_bit() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let payload = vec![
        0x21, 0x00, // datagram_num = 33
        0x07, // block_num = 7 (malformed: greater than MaxBlockNum)
        0x00, // max_block_num = 0 (one block total)
        0xAA, 0xBB,
    ];

    let (assembled, ack) = recv.on_new_sliced(&payload);
    assert!(assembled.is_none());
    assert_eq!(ack[0] & (1 << 7), 0);
    assert!(
        recv.receiving.contains_key(&33),
        "OnNewSliced creates the datagram entry before Delphi ReceivedPiece drops the invalid block"
    );
    assert_eq!(
        recv.receiving.get(&33).unwrap().received_count,
        0,
        "invalid BlockNum must not mutate piece state"
    );
}

#[test]
fn accepts_full_256_block_datagram() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let datagram = 0x1234u16;
    for block_num in 1u8..=255 {
        let payload = vec![
            (datagram & 0xFF) as u8,
            (datagram >> 8) as u8,
            block_num,
            255,
            block_num,
        ];
        let (assembled, ack) = recv.on_new_sliced(&payload);
        assert!(assembled.is_none());
        if block_num == 255 {
            assert_eq!(ack[31] & 0x80, 0x80);
        }
    }

    let block0 = vec![
        (datagram & 0xFF) as u8,
        (datagram >> 8) as u8,
        0,
        255,
        0x1C,
        0,
    ];
    let (assembled, ack) = recv.on_new_sliced(&block0);
    let (datagram_num, cmd, data, _dup_count, blocks_count) = assembled.unwrap();

    assert_eq!(datagram_num, datagram);
    assert_eq!(cmd, 0x1C);
    assert_eq!(blocks_count, 256);
    assert_eq!(data.len(), 256);
    assert_eq!(data[0], 0);
    assert_eq!(data[255], 255);
    assert!(ack[..32].iter().all(|byte| *byte == 0xFF));
    assert_eq!(&ack[32..34], &datagram.to_le_bytes());
    assert!(recv.receiving.contains_key(&datagram_num));
    recv.receiving.remove(&datagram_num);
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.OnNewSliced
fn first_block_num_above_max_is_not_received() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let payload = vec![
        0x37, 0x00, // datagram_num = 55
        0x01, // block_num = 1
        0x00, // max_block_num = 0 (BlocksCount = 1)
        0xAA, 0xBB,
    ];

    let (assembled, ack) = recv.on_new_sliced(&payload);
    assert!(
        assembled.is_none(),
        "Delphi ReceivedPiece exits before adding BlockNum >= BlocksCount"
    );
    assert_eq!(ack[0] & 0b0000_0010, 0);
    assert_eq!(&ack[32..34], &55u16.to_le_bytes());
    assert!(recv.receiving.contains_key(&55));
    assert_eq!(recv.receiving.get(&55).unwrap().received_count, 0);
    recv.receiving.remove(&55);
}

#[test]
fn first_datagram_before_duplicate_window_is_new() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(5000);

    let payload = vec![
        0x04, 0x00, // datagram_num = 4
        0x00, // block_num = 0
        0x00, // max_block_num = 0
        0x1F, // cmd byte
        0xAA, 0xBB,
    ];

    let (assembled, _ack) = recv.on_new_sliced(&payload);
    let (datagram_num, cmd, data, _dup_count, blocks_count) = assembled
        .expect("first ever datagram must be accepted even during first 9s after Client::new");

    assert_eq!(datagram_num, 4);
    assert_eq!(cmd, 0x1F);
    assert_eq!(data, vec![0xAA, 0xBB]);
    assert_eq!(blocks_count, 1);
    assert!(recv.receiving.contains_key(&datagram_num));
    recv.receiving.remove(&datagram_num);
}

#[test]
fn incoming_sliced_datagrams_are_not_capped() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    for datagram in 0u16..300 {
        let payload = vec![
            (datagram & 0xFF) as u8,
            (datagram >> 8) as u8,
            1,
            1,
            datagram as u8,
        ];
        let (assembled, _) = recv.on_new_sliced(&payload);
        assert!(assembled.is_none());
    }

    assert_eq!(recv.receiving.len(), 300);

    let block0 = vec![0, 0, 0, 1, 0x1C, 0xAA];
    let (assembled, _) = recv.on_new_sliced(&block0);
    let (datagram_num, cmd, data, _dup_count, blocks_count) =
        assembled.expect("oldest incomplete datagram must not be evicted by a Rust-only cap");

    assert_eq!(datagram_num, 0);
    assert_eq!(cmd, 0x1C);
    assert_eq!(blocks_count, 2);
    assert_eq!(data, vec![0xAA, 0x00]);
    assert!(recv.receiving.contains_key(&datagram_num));
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.ClearOldReceiving
fn clear_old_refreshes_duplicate_window() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);

    let stale_block1 = vec![42, 0, 1, 1, 0xBB];
    let (assembled, _) = recv.on_new_sliced(&stale_block1);
    assert!(assembled.is_none());
    assert_eq!(recv.receiving.len(), 1);

    recv.set_last_online(20000);
    recv.clear_old();
    assert!(recv.receiving.is_empty());

    let late_block0 = vec![42, 0, 0, 1, 0x1C, 0xAA];
    let (assembled, ack) = recv.on_new_sliced(&late_block0);

    assert!(assembled.is_none());
    assert!(recv.receiving.is_empty());
    assert!(ack[..32].iter().all(|byte| *byte == 0xFF));
    assert_eq!(&ack[32..34], &42u16.to_le_bytes());
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoClient.DoCleanUp
fn do_cleanup_runs_on_reader_packet_cadence() {
    let mut recv = SlicingReceiver::new();
    recv.set_last_online(10000);
    recv.do_cleanup();

    let stale_block1 = vec![42, 0, 1, 1, 0xBB];
    let (assembled, _) = recv.on_new_sliced(&stale_block1);
    assert!(assembled.is_none());
    assert_eq!(recv.receiving.len(), 1);

    recv.set_last_online(14999);
    recv.do_cleanup();
    assert_eq!(
        recv.receiving.len(),
        1,
        "Delphi DoCleanUp is throttled by abs(LastCleanedReceived - LastOnline) > 5000"
    );

    recv.set_last_online(20000);
    recv.do_cleanup();
    assert!(
        recv.receiving.is_empty(),
        "accepted reader packets drive ClearOldReceiving before command-specific handling"
    );
}
