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

pub const SLICE_HEADER_SIZE: usize = 4;

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

fn acked_count(flags: &[u8; 32], blocks_count: usize) -> usize {
    (0..blocks_count)
        .filter(|block| flags[block / 8] & (1 << (block % 8)) != 0)
        .count()
}

fn missing_preview(flags: &[u8; 32], blocks_count: usize) -> String {
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

impl SliceHeader {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < SLICE_HEADER_SIZE {
            return None;
        }
        Some(Self {
            datagram_num: u16::from_le_bytes(data[0..2].try_into().unwrap()),
            block_num: data[2],
            max_block_num: data[3],
        })
    }
}

// `Slice` (тип одного блока с header'ом) объединён в HashMap значения SlicedData.blocks
// — отдельный тип не используется.

/// Tracks all blocks of one datagram being received
#[derive(Debug)]
pub struct SlicedData {
    pub datagram_num: u16,
    pub blocks_count: usize,      // MaxBlockNum + 1
    blocks: Vec<Option<Vec<u8>>>, // indexed by BlockNum, payload after SliceHeader
    received_count: usize,
    pub ack_flags: [u8; 32], // TMoonProtoFlag256 = set of byte = 32 bytes
    pub dup_count: u8,       // DupCount (matches IntStruct.pas:539)
}

impl SlicedData {
    pub fn new(datagram_num: u16, max_block_num: u8) -> Self {
        let count = (max_block_num as usize) + 1;
        Self {
            datagram_num,
            blocks_count: count,
            blocks: vec![None; count],
            received_count: 0,
            ack_flags: [0u8; 32],
            dup_count: 0,
        }
    }

    /// Receive a piece. Returns true if this completes the datagram.
    pub fn receive_piece(&mut self, block_num: u8, payload: Vec<u8>) -> bool {
        let idx = block_num as usize;
        if idx >= self.blocks_count {
            return false;
        }

        // Set ACK flag (set of byte semantics: byte index = block_num / 8, bit = block_num % 8)
        self.ack_flags[idx / 8] |= 1 << (idx % 8);

        if self.blocks[idx].is_none() {
            self.blocks[idx] = Some(payload);
            self.received_count += 1;
        } else {
            self.dup_count = self.dup_count.saturating_add(1);
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
        let total: usize = self
            .blocks
            .iter()
            .filter_map(|b| b.as_ref())
            .map(|b| b.len())
            .sum();
        let mut cmd = 0u8;
        let mut result = Vec::with_capacity(total.saturating_sub(1));

        for (i, block) in self.blocks.iter().enumerate() {
            let data = block.as_ref()?;
            if i == 0 {
                if data.is_empty() {
                    return None;
                }
                cmd = data[0]; // TMoonProtoCommand byte
                result.extend_from_slice(&data[1..]);
            } else {
                result.extend_from_slice(data);
            }
        }
        Some((cmd, result))
    }
}

/// ACK256 wire format: 32 bytes flags + 2 bytes DatagramNum = 34 bytes
pub const ACK256_WIRE_SIZE: usize = 34;
pub type SlicedPayloadResult = Option<(u8, Vec<u8>, u8, usize)>;
pub type SlicedProcessResult = (SlicedPayloadResult, [u8; ACK256_WIRE_SIZE]);

pub fn build_ack_bytes(flags: &[u8; 32], datagram_num: u16) -> [u8; ACK256_WIRE_SIZE] {
    let mut buf = [0u8; ACK256_WIRE_SIZE];
    buf[0..32].copy_from_slice(flags);
    buf[32..34].copy_from_slice(&datagram_num.to_le_bytes());
    buf
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
        }
    }

    pub fn set_last_online(&mut self, ms: i64) {
        self.last_online = ms;
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
    /// Returns: (Option<(cmd, assembled_data)>, ack_to_send)
    /// Matches TMoonProtoClient.OnNewSliced byte-for-byte.
    /// Returns: (Option<(cmd, data, dup_count, blocks_count)>, ack_bytes)
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

        // Delphi accepts the full byte range: MaxBlockNum is u8, and
        // MaxSlicedDataSize is computed as PTMU * 256 minus headers. Large
        // chunked-candles responses legitimately use close to 256 blocks.
        // Также reject block_num за пределами max_block_num (явная corruption / attack).
        if hdr.block_num > hdr.max_block_num {
            log::warn!(target: "moonproto::slicing",
                "Sliced dgram={} block_num={} > max_block_num={} — rejecting",
                hdr.datagram_num, hdr.block_num, hdr.max_block_num);
            if trace {
                eprintln!(
                    "[slice-rx] t={} d={} b={}/{} len={} action=drop-bad-block",
                    self.last_online,
                    hdr.datagram_num,
                    hdr.block_num,
                    hdr.max_block_num,
                    payload.len()
                );
            }
            return (None, [0u8; ACK256_WIRE_SIZE]);
        }

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
                .map(|(cmd, data)| (cmd, data, dup_count, blocks_count));
            if trace {
                match &assembled {
                    Some((cmd, data, dup_count, blocks_count)) => eprintln!(
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
            self.receiving.remove(&datagram_num);
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
        let (cmd, data, _, _) = assembled.unwrap();
        assert_eq!(cmd, 0x0A);
        assert_eq!(data, vec![0xDE, 0xAD]);
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
        let (cmd, data, _, _) = assembled.unwrap();
        assert_eq!(cmd, 0x1C);
        assert_eq!(data, vec![0xAA, 0xBB, 0xCC]);
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
        let (cmd, data, _dup_count, blocks_count) = assembled.unwrap();

        assert_eq!(cmd, 0x1C);
        assert_eq!(blocks_count, 256);
        assert_eq!(data.len(), 256);
        assert_eq!(data[0], 0);
        assert_eq!(data[255], 255);
        assert!(ack[..32].iter().all(|byte| *byte == 0xFF));
        assert_eq!(&ack[32..34], &datagram.to_le_bytes());
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
        let (cmd, data, _dup_count, blocks_count) = assembled
            .expect("first ever datagram must be accepted even during first 9s after Client::new");

        assert_eq!(cmd, 0x1F);
        assert_eq!(data, vec![0xAA, 0xBB]);
        assert_eq!(blocks_count, 1);
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
        let (cmd, data, _dup_count, blocks_count) =
            assembled.expect("oldest incomplete datagram must not be evicted by a Rust-only cap");

        assert_eq!(cmd, 0x1C);
        assert_eq!(blocks_count, 2);
        assert_eq!(data, vec![0xAA, 0x00]);
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
}
