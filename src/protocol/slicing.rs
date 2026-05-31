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
pub(crate) struct SliceHeader {
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

pub(crate) const SLICE_HEADER_SIZE: usize = std::mem::size_of::<WireSliceHeader>();
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

    pub(crate) fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < SLICE_HEADER_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireSliceHeader::read_from_bytes(&data[..SLICE_HEADER_SIZE]).ok()?,
        ))
    }

    pub(crate) fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BlockSpan {
    offset: u32,
    len: u32,
    present: bool,
}

/// Tracks all blocks of one datagram being received
#[derive(Debug)]
pub(crate) struct SlicedData {
    // Parsed from the wire and set at construction, but not read downstream;
    // kept to mirror TMoonProtoSlicedData's DatagramNum field. Do not delete.
    #[allow(dead_code)]
    pub datagram_num: u16,
    pub(crate) blocks_count: usize, // MaxBlockNum + 1
    // Delphi keeps received slices in a sorted list and does not reject
    // BlockNum > MaxBlockNum. Keep the same machine effect: ACK the actual
    // BlockNum, insert by BlockNum if not a duplicate, and use Count ==
    // BlocksCount as the completion test.
    block_spans: [BlockSpan; 256],
    block_payloads: Vec<u8>,
    max_present_block_num: u8,
    received_count: usize,
    completion_returned: bool,
    pub(crate) ack_flags: [u8; 32], // TMoonProtoFlag256 = set of byte = 32 bytes
    pub(crate) dup_count: u8,       // DupCount (matches IntStruct.pas:539)
}

impl SlicedData {
    pub(crate) fn new(datagram_num: u16, max_block_num: u8) -> Self {
        let count = (max_block_num as usize) + 1;
        Self {
            datagram_num,
            blocks_count: count,
            block_spans: [BlockSpan::default(); 256],
            block_payloads: Vec::new(),
            max_present_block_num: 0,
            received_count: 0,
            completion_returned: false,
            ack_flags: [0u8; 32],
            dup_count: 0,
        }
    }

    /// Receive a piece. Returns true if this completes the datagram.
    pub(crate) fn receive_piece(&mut self, block_num: u8, payload: &[u8]) -> bool {
        let idx = block_num as usize;

        // Set ACK flag (set of byte semantics: byte index = block_num / 8, bit = block_num % 8)
        self.ack_flags[idx / 8] |= 1 << (idx % 8);

        let span = &mut self.block_spans[idx];
        if span.present {
            self.dup_count = self.dup_count.saturating_add(1);
        } else {
            self.max_present_block_num = self.max_present_block_num.max(block_num);
            let offset = self.block_payloads.len();
            self.block_payloads.extend_from_slice(payload);
            *span = BlockSpan {
                offset: offset as u32,
                len: payload.len() as u32,
                present: true,
            };
            self.received_count += 1;
        }

        self.received_count == self.blocks_count
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.received_count == self.blocks_count
    }

    /// Reassemble the complete message. Returns (cmd, data).
    /// Block 0: SliceHeader already stripped by caller, first byte = cmd, rest = data.
    /// Block N>0: SliceHeader already stripped, all = data.
    pub(crate) fn assemble(&self) -> Option<(u8, Vec<u8>)> {
        if !self.is_complete() {
            return None;
        }
        // Receive pieces live in one dense buffer plus BlockNum->span metadata.
        // Iterating only to the highest received BlockNum preserves the same
        // sorted-by-BlockNum effect as Delphi's sorted slice list, including
        // malformed BlockNum > MaxBlockNum cases, without scanning unused slots.
        let total = self.block_payloads.len();
        let mut cmd = 0u8;
        let mut saw_block_zero = false;
        let mut result = Vec::with_capacity(total.saturating_sub(1));

        for block_num in 0..=self.max_present_block_num {
            let span = self.block_spans[block_num as usize];
            if !span.present {
                continue;
            }
            let offset = span.offset as usize;
            let len = span.len as usize;
            let data = &self.block_payloads[offset..offset + len];
            if block_num == 0 {
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
pub(crate) const ACK256_WIRE_SIZE: usize = std::mem::size_of::<WireSlicedAck>();
const _: [(); 34] = [(); ACK256_WIRE_SIZE];
pub(crate) type SlicedPayloadResult = Option<(u16, u8, Vec<u8>, u8, usize)>;
pub(crate) type SlicedProcessResult = (SlicedPayloadResult, [u8; ACK256_WIRE_SIZE]);

pub(crate) fn build_ack_bytes(flags: &[u8; 32], datagram_num: u16) -> [u8; ACK256_WIRE_SIZE] {
    let mut buf = [0u8; ACK256_WIRE_SIZE];
    let wire = WireSlicedAck {
        flags: *flags,
        datagram_num: LeU16::new(datagram_num),
    };
    buf.copy_from_slice(wire.as_bytes());
    buf
}

pub(crate) fn parse_ack_bytes(payload: &[u8]) -> Option<([u8; 32], u16)> {
    if payload.len() < ACK256_WIRE_SIZE {
        return None;
    }
    let wire = WireSlicedAck::read_from_bytes(&payload[..ACK256_WIRE_SIZE]).ok()?;
    Some((wire.flags, wire.datagram_num.get()))
}

/// Receiving state: tracks all in-progress datagrams.
/// Matches TMoonProtoClient.Receiving: TDictionary<TDatagramNum, TMoonProtoSlicedData>
pub(crate) struct SlicingReceiver {
    pub(crate) receiving: HashMap<u16, SlicedData>,
    /// B-09 fix: fixed LAST_RECVD_BUF_SIZE — typed as an array,
    /// `Box<[..; N]>` so we don't put 16KB on the stack (Client creation doesn't blow the stack),
    /// but the size is known at compile-time → bounds checks are eliminated.
    last_recvd_ts: Box<[i64; LAST_RECVD_BUF_SIZE]>,
    last_online: i64,
    last_cleaned_received: i64,
}

// Delphi `LastRecvdBufSize` (MoonProtoIntStruct.pas). Bumped 2048 -> 4096 to
// match the эталон: x2 headroom against Sliced-dedup slot collisions (ceiling
// ~455 datagrams/s within the 9s window; real Sliced-rate is units-tens/s).
const LAST_RECVD_BUF_SIZE: usize = 4096;
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
    pub(crate) fn new() -> Self {
        Self {
            receiving: HashMap::new(),
            last_recvd_ts: Box::new([NEVER_RECEIVED_MS; LAST_RECVD_BUF_SIZE]),
            last_online: 0,
            last_cleaned_received: 0,
        }
    }

    pub(crate) fn set_last_online(&mut self, ms: i64) {
        self.last_online = ms;
    }

    /// Matches `TMoonProtoClient.DoCleanUp`: reader-side cleanup is driven by
    /// accepted incoming packets and runs before command-specific handling.
    pub(crate) fn do_cleanup(&mut self) {
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
    pub(crate) fn on_new_sliced(&mut self, payload: &[u8]) -> SlicedProcessResult {
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

        let block_data = &payload[SLICE_HEADER_SIZE..];
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
            // D-V2-13 fix: saturating_sub guards against theoretical underflow if blocks_count=0.
            // Logically blocks_count = max_block_num+1, minimum 1 — but the guard is defensive
            // in case of a code change further down the stack.
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
    pub(crate) fn clear_old(&mut self) {
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
mod tests;
