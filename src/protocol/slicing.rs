/// Slicing engine — receive side.
/// Byte-exact port of TMoonProtoSlicedData / OnNewSliced from MoonProtoIntStruct.pas.
///
/// Wire format of MPC_Sliced payload (after header strip):
///   SliceHeader (4 bytes): DatagramNum:u16 + BlockNum:u8 + MaxBlockNum:u8
///   Block data: variable length
///
/// Block 0 additionally contains: cmd:u8 before the data.
/// Reassembly: sort by BlockNum, strip SliceHeader from each, extract cmd from block 0.
use std::collections::HashMap;
#[cfg(feature = "diagnostic-trace")]
use std::sync::OnceLock;
use zerocopy::byteorder::little_endian::U16 as LeU16;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[cfg(feature = "diagnostic-trace")]
pub(crate) fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("MOONPROTO_TRACE_SLICES")
            .map(|v| {
                let v = v.to_string_lossy();
                !(v.is_empty() || v == "0" || v.eq_ignore_ascii_case("false"))
            })
            .unwrap_or(false)
    })
}

#[cfg(not(feature = "diagnostic-trace"))]
#[inline(always)]
pub(crate) fn trace_enabled() -> bool {
    false
}

pub(crate) fn acked_count(flags: &[u8; 32], blocks_count: usize) -> usize {
    (0..blocks_count)
        .filter(|block| flags[block / 8] & (1 << (block % 8)) != 0)
        .count()
}

pub(crate) fn missing_preview(flags: &[u8; 32], blocks_count: usize) -> String {
    let mut out = String::new();
    let mut shown = 0usize;
    for block in 0..blocks_count {
        if flags[block / 8] & (1 << (block % 8)) == 0 {
            if shown > 0 {
                out.push(',');
            }
            if shown >= 24 {
                out.push_str("...");
                break;
            }
            out.push_str(&block.to_string());
            shown += 1;
        }
    }
    if out.is_empty() {
        "none".to_string()
    } else {
        out
    }
}

/// TMoonProtoSliceHeader — 4 bytes packed
#[derive(Debug, Clone, Copy)]
pub struct SliceHeader {
    pub datagram_num: u16,
    pub block_num: u8,
    pub max_block_num: u8,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireSliceHeader {
    datagram_num: LeU16,
    block_num: u8,
    max_block_num: u8,
}

pub const SLICE_HEADER_SIZE: usize = std::mem::size_of::<WireSliceHeader>();
const _: [(); 4] = [(); SLICE_HEADER_SIZE];

impl SliceHeader {
    fn from_wire(wire: WireSliceHeader) -> Self {
        Self {
            datagram_num: wire.datagram_num.get(),
            block_num: wire.block_num,
            max_block_num: wire.max_block_num,
        }
    }

    fn to_wire(self) -> WireSliceHeader {
        WireSliceHeader {
            datagram_num: LeU16::new(self.datagram_num),
            block_num: self.block_num,
            max_block_num: self.max_block_num,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < SLICE_HEADER_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireSliceHeader::read_from_bytes(&data[..SLICE_HEADER_SIZE]).ok()?,
        ))
    }

    pub fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

// `Slice` (тип одного блока с header'ом) представлен парой `(BlockNum, payload)`.

/// Tracks all blocks of one datagram being received
#[derive(Debug)]
pub struct SlicedData {
    pub datagram_num: u16,
    pub blocks_count: usize, // MaxBlockNum + 1
    // Delphi keeps received slices in a sorted list and does not reject
    // BlockNum > MaxBlockNum. Keep the same machine effect: ACK the actual
    // BlockNum, insert by BlockNum if not a duplicate, and use Count ==
    // BlocksCount as the completion test.
    blocks: Vec<(u8, Vec<u8>)>,
    received_count: usize,
    completion_returned: bool,
    pub ack_flags: [u8; 32], // TMoonProtoFlag256 = set of byte = 32 bytes
    pub dup_count: u8,       // DupCount (matches IntStruct.pas:539)
}

impl SlicedData {
    pub fn new(datagram_num: u16, max_block_num: u8) -> Self {
        let count = (max_block_num as usize) + 1;
        Self {
            datagram_num,
            blocks_count: count,
            blocks: Vec::with_capacity(count),
            received_count: 0,
            completion_returned: false,
            ack_flags: [0u8; 32],
            dup_count: 0,
        }
    }

    /// Receive a piece. Returns true if this completes the datagram.
    pub fn receive_piece(&mut self, block_num: u8, payload: Vec<u8>) -> bool {
        let idx = block_num as usize;

        // Set ACK flag (set of byte semantics: byte index = block_num / 8, bit = block_num % 8)
        self.ack_flags[idx / 8] |= 1 << (idx % 8);

        match self
            .blocks
            .binary_search_by_key(&block_num, |(block, _)| *block)
        {
            Ok(_) => {
                self.dup_count = self.dup_count.saturating_add(1);
            }
            Err(insert_at) => {
                self.blocks.insert(insert_at, (block_num, payload));
                self.received_count += 1;
            }
        }

        self.received_count == self.blocks_count
    }

    pub fn is_complete(&self) -> bool {
        self.received_count == self.blocks_count
    }

    /// Reassemble the complete message. Returns (cmd, data).
    /// Block 0: SliceHeader already stripped by caller, first byte = cmd, rest = data.
    /// Block N>0: SliceHeader already stripped, all = data.
    pub fn assemble(&self) -> Option<(u8, Vec<u8>)> {
        if !self.is_complete() {
            return None;
        }
        // B-V2-09 fix: prealloc capacity по сумме block sizes — избегаем re-alloc'ов
        // в extend_from_slice. На больших Sliced сообщениях (~50KB) это экономит ~10
        // re-alloc'ов с растущей capacity до финального размера.
        let total: usize = self.blocks.iter().map(|(_, b)| b.len()).sum();
        let mut cmd = 0u8;
        let mut saw_block_zero = false;
        let mut result = Vec::with_capacity(total.saturating_sub(1));

        for (block_num, data) in &self.blocks {
            if *block_num == 0 {
                saw_block_zero = true;
                if let Some((&first, rest)) = data.split_first() {
                    cmd = first; // TMoonProtoCommand byte
                    result.extend_from_slice(rest);
                }
            } else {
                result.extend_from_slice(data);
            }
        }
        if !saw_block_zero {
            // Delphi `TMoonProtoSlicedData.GetReceivedStream` only updates Fcmd
            // while iterating a BlockNum=0 slice. If malformed blocks complete a
            // datagram without block 0, Fcmd stays at constructor default
            // MPC_None, but BaseNet.OnNewSliced still calls DataReadInt and then
            // removes the datagram from Receiving.
            cmd = 0;
        }
        Some((cmd, result))
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireSlicedAck {
    flags: [u8; 32],
    datagram_num: LeU16,
}

/// ACK256 wire format: 32 bytes flags + 2 bytes DatagramNum = 34 bytes
pub const ACK256_WIRE_SIZE: usize = std::mem::size_of::<WireSlicedAck>();
const _: [(); 34] = [(); ACK256_WIRE_SIZE];
pub type SlicedPayloadResult = Option<(u16, u8, Vec<u8>, u8, usize)>;
pub type SlicedProcessResult = (SlicedPayloadResult, [u8; ACK256_WIRE_SIZE]);

pub fn build_ack_bytes(flags: &[u8; 32], datagram_num: u16) -> [u8; ACK256_WIRE_SIZE] {
    let mut buf = [0u8; ACK256_WIRE_SIZE];
    let wire = WireSlicedAck {
        flags: *flags,
        datagram_num: LeU16::new(datagram_num),
    };
    buf.copy_from_slice(wire.as_bytes());
    buf
}

pub fn parse_ack_bytes(payload: &[u8]) -> Option<([u8; 32], u16)> {
    if payload.len() < ACK256_WIRE_SIZE {
        return None;
    }
    let wire = WireSlicedAck::read_from_bytes(&payload[..ACK256_WIRE_SIZE]).ok()?;
    Some((wire.flags, wire.datagram_num.get()))
}

/// Receiving state: tracks all in-progress datagrams.
/// Matches TMoonProtoClient.Receiving: TDictionary<TDatagramNum, TMoonProtoSlicedData>
pub struct SlicingReceiver {
    pub receiving: HashMap<u16, SlicedData>,
    /// B-09 fix: фиксированный размер LAST_RECVD_BUF_SIZE — типизирован как массив,
    /// `Box<[..; N]>` чтобы не паковать 16KB на stack (создание Client не падает по стеку),
    /// но размер известен compile-time → bounds checks eliminate'ятся.
    last_recvd_ts: Box<[i64; LAST_RECVD_BUF_SIZE]>,
    last_online: i64,
    last_cleaned_received: i64,
}

const LAST_RECVD_BUF_SIZE: usize = 2048;
const TIME_WHEN_CAN_RECEIVE_RPT: i64 = 9000; // ms
                                             // Client time is monotonic milliseconds since `Client::new`, so `0` is a valid
                                             // early timestamp. A never-seen slot must sit outside the duplicate window.
const NEVER_RECEIVED_MS: i64 = -TIME_WHEN_CAN_RECEIVE_RPT - 1;

impl Default for SlicingReceiver {
    fn default() -> Self {
        Self::new()
    }
}

impl SlicingReceiver {
    pub fn new() -> Self {
        Self {
            receiving: HashMap::new(),
            last_recvd_ts: Box::new([NEVER_RECEIVED_MS; LAST_RECVD_BUF_SIZE]),
            last_online: 0,
            last_cleaned_received: 0,
        }
    }

    pub fn set_last_online(&mut self, ms: i64) {
        self.last_online = ms;
    }

    /// Matches `TMoonProtoClient.DoCleanUp`: reader-side cleanup is driven by
    /// accepted incoming packets and runs before command-specific handling.
    pub fn do_cleanup(&mut self) {
        if (self.last_cleaned_received - self.last_online).abs() > 5000 {
            self.clear_old();
            self.last_cleaned_received = self.last_online;
        }
    }

    /// Check if this is a "new" datagram (not recently seen).
    /// Matches TMoonProtoClient.IsNewDatagram.
    fn is_new_datagram(&mut self, num: u16) -> bool {
        let idx = (num as usize) % LAST_RECVD_BUF_SIZE;
        let is_new = (self.last_online - self.last_recvd_ts[idx]).abs() > TIME_WHEN_CAN_RECEIVE_RPT;
        if is_new {
            self.last_recvd_ts[idx] = self.last_online;
        }
        is_new
    }

    /// Process an incoming MPC_Sliced packet payload (after outer header strip).
    /// Returns: (Option<(datagram_num, cmd, data, dup_count, blocks_count)>, ack_bytes).
    ///
    /// This matches `TMoonProtoClient.OnNewSliced`: it updates the receiving
    /// dictionary and returns the completed `TMoonProtoSlicedData` equivalent.
    /// The caller must run `DataReadInt` first and only then remove the datagram
    /// from `Receiving`, like `TMoonProtoBaseNet.OnNewSliced`.
    pub fn on_new_sliced(&mut self, payload: &[u8]) -> SlicedProcessResult {
        let trace = trace_enabled();
        let hdr = match SliceHeader::from_bytes(payload) {
            Some(h) => h,
            None => {
                if trace {
                    eprintln!(
                        "[slice-rx] t={} malformed len={} action=drop-no-header",
                        self.last_online,
                        payload.len()
                    );
                }
                return (None, [0u8; ACK256_WIRE_SIZE]);
            }
        };

        let block_data = payload[SLICE_HEADER_SIZE..].to_vec();
        let datagram_num = hdr.datagram_num;
        let mut action = "existing";

        // Check if this is a new datagram number
        if self.is_new_datagram(datagram_num) {
            action = "new";
            // Remove any old entry with same number
            self.receiving.remove(&datagram_num);
            // Create new SlicedData
            self.receiving.insert(
                datagram_num,
                SlicedData::new(datagram_num, hdr.max_block_num),
            );
        } else if !self.receiving.contains_key(&datagram_num) {
            // Not new, not in receiving → already completed, send full ACK
            let flags = [0xFFu8; 32]; // SetAllFlags
            let ack = build_ack_bytes(&flags, datagram_num);
            if trace {
                eprintln!(
                    "[slice-rx] t={} d={} b={}/{} len={} action=already-complete-full-ack",
                    self.last_online,
                    datagram_num,
                    hdr.block_num,
                    hdr.max_block_num,
                    block_data.len()
                );
            }
            return (None, ack);
        } else {
            // Existing entry — check if MaxBlockNum matches (recreate if mismatch)
            let existing = self.receiving.get(&datagram_num).unwrap();
            // D-V2-13 fix: saturating_sub защита от theoretical underflow если blocks_count=0.
            // Логически blocks_count = max_block_num+1, минимум 1 — но защита defensive
            // на случай code change ниже по стеку.
            if existing.blocks_count.saturating_sub(1) != hdr.max_block_num as usize {
                action = "recreate-maxblock-change";
                self.receiving.remove(&datagram_num);
                self.receiving.insert(
                    datagram_num,
                    SlicedData::new(datagram_num, hdr.max_block_num),
                );
            }
        }

        // Add the piece
        let sliced = self.receiving.get_mut(&datagram_num).unwrap();
        if sliced.completion_returned {
            // Delphi BaseNet.OnNewSliced removes the completed datagram from
            // Receiving immediately after DataReadInt. Because Rust still
            // removes it from the main-loop side, any later block that arrives
            // in this short gap must behave like Delphi's post-removal branch:
            // no state mutation, no second assembled payload, ACK.SetAllFlags.
            let full_ack = build_ack_bytes(&[0xFFu8; 32], datagram_num);
            if trace {
                eprintln!(
                    "[slice-rx-complete] t={} d={} block_after_complete=true full_ack=true blocks={}",
                    self.last_online, datagram_num, sliced.blocks_count
                );
            }
            return (None, full_ack);
        }
        let complete = sliced.receive_piece(hdr.block_num, block_data);
        let ack = build_ack_bytes(&sliced.ack_flags, datagram_num);
        if trace {
            let got = sliced.received_count;
            let total = sliced.blocks_count;
            let acked = acked_count(&sliced.ack_flags, total);
            eprintln!(
                "[slice-rx] t={} d={} b={}/{} action={} got={}/{} acked={} dup={} complete={} missing={}",
                self.last_online,
                datagram_num,
                hdr.block_num,
                hdr.max_block_num,
                action,
                got,
                total,
                acked,
                sliced.dup_count,
                complete,
                missing_preview(&sliced.ack_flags, total)
            );
        }

        if complete {
            let dup_count = sliced.dup_count;
            let blocks_count = sliced.blocks_count;
            let assembled = sliced
                .assemble()
                .map(|(cmd, data)| (datagram_num, cmd, data, dup_count, blocks_count));
            if assembled.is_some() {
                sliced.completion_returned = true;
            }
            if trace {
                match &assembled {
                    Some((_, cmd, data, dup_count, blocks_count)) => eprintln!(
                        "[slice-rx-complete] t={} d={} inner_cmd={} len={} dup={} blocks={}",
                        self.last_online,
                        datagram_num,
                        cmd,
                        data.len(),
                        dup_count,
                        blocks_count
                    ),
                    None => eprintln!(
                        "[slice-rx-complete] t={} d={} assemble_failed=true blocks={}",
                        self.last_online, datagram_num, blocks_count
                    ),
                }
            }
            (assembled, ack)
        } else {
            (None, ack)
        }
    }

    /// Clean old incomplete datagrams (called periodically).
    /// Matches TMoonProtoClient.ClearOldReceiving: for every removed datagram Delphi calls
    /// `IsNewDatagram`, which also refreshes the timestamp bucket.
    pub fn clear_old(&mut self) {
        let last_online = self.last_online;
        let last_recvd_ts = &mut self.last_recvd_ts;
        self.receiving.retain(|&k, _| {
            let idx = (k as usize) % LAST_RECVD_BUF_SIZE;
            let is_old = (last_online - last_recvd_ts[idx]).abs() > TIME_WHEN_CAN_RECEIVE_RPT;
            if is_old {
                last_recvd_ts[idx] = last_online;
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Command;

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
    fn duplicate_after_completed_datagram_gets_full_ack_like_delphi_post_removal() {
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

        let malformed_complete = vec![
            0x37, 0x00, // datagram_num = 55
            0x07, // block_num = 7
            0x00, // max_block_num = 0, so one received block completes the datagram
            0xAA,
        ];
        let later_block_same_datagram = vec![
            0x37, 0x00, // datagram_num = 55
            0x08, // a different block before Rust caller removed Receiving
            0x00, 0xBB,
        ];

        assert!(recv.on_new_sliced(&malformed_complete).0.is_some());
        let before_blocks = recv
            .receiving
            .get(&55)
            .expect("completed datagram stays until caller removal")
            .blocks
            .len();

        let (assembled, ack) = recv.on_new_sliced(&later_block_same_datagram);

        assert!(assembled.is_none());
        assert!(ack[..32].iter().all(|byte| *byte == 0xFF));
        assert_eq!(
            recv.receiving.get(&55).unwrap().blocks.len(),
            before_blocks,
            "after completion Rust must emulate Delphi post-removal path without adding later pieces"
        );
    }

    #[test]
    fn completed_datagram_without_block_zero_is_delivered_as_none_cmd_like_delphi() {
        let mut recv = SlicingReceiver::new();
        recv.set_last_online(10000);

        let payload = vec![
            0x21, 0x00, // datagram_num = 33
            0x07, // block_num = 7 (malformed: no block 0)
            0x00, // max_block_num = 0 (one block total)
            0xAA, 0xBB, // payload copied as data, not command byte
        ];

        let (assembled, ack) = recv.on_new_sliced(&payload);
        let (datagram_num, cmd, data, _, _) = assembled.unwrap();

        assert_eq!(datagram_num, 33);
        assert_eq!(
            cmd,
            Command::None as u8,
            "Delphi leaves TMoonProtoSlicedData.Fcmd at MPC_None when no BlockNum=0 was seen"
        );
        assert_eq!(data, vec![0xAA, 0xBB]);
        assert_eq!(ack[0] & (1 << 7), 1 << 7);
        assert!(
            recv.receiving.contains_key(&33),
            "BaseNet.OnNewSliced removes Receiving only after DataReadInt"
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
    fn block_num_above_max_is_received_like_delphi() {
        let mut recv = SlicingReceiver::new();
        recv.set_last_online(10000);

        let payload = vec![
            0x37, 0x00, // datagram_num = 55
            0x01, // block_num = 1
            0x00, // max_block_num = 0 (BlocksCount = 1)
            0xAA, 0xBB,
        ];

        let (assembled, ack) = recv.on_new_sliced(&payload);
        let (datagram_num, cmd, data, _dup_count, blocks_count) = assembled
            .expect("Delphi ReceivedPiece inserts the slice even when BlockNum > MaxBlockNum");

        assert_eq!(datagram_num, 55);
        assert_eq!(cmd, 0, "without block 0 Delphi leaves Fcmd at MPC_None");
        assert_eq!(data, vec![0xAA, 0xBB]);
        assert_eq!(blocks_count, 1);
        assert_eq!(ack[0] & 0b0000_0010, 0b0000_0010);
        assert_eq!(&ack[32..34], &55u16.to_le_bytes());
        assert!(recv.receiving.contains_key(&datagram_num));
        recv.receiving.remove(&datagram_num);
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
    fn clear_old_refreshes_duplicate_window_like_delphi() {
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
    fn do_cleanup_runs_on_reader_packet_cadence_like_delphi() {
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
}
