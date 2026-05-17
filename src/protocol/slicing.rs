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

pub const SLICE_HEADER_SIZE: usize = 4;

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

/// One received slice (block)
#[derive(Debug)]
struct Slice {
    header: SliceHeader,
    data: Vec<u8>, // full slice payload INCLUDING header bytes
}

/// Tracks all blocks of one datagram being received
#[derive(Debug)]
pub struct SlicedData {
    pub datagram_num: u16,
    pub blocks_count: usize, // MaxBlockNum + 1
    blocks: Vec<Option<Vec<u8>>>, // indexed by BlockNum, payload after SliceHeader
    received_count: usize,
    pub ack_flags: [u8; 32], // TMoonProtoFlag256 = set of byte = 32 bytes
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
        }
        // else: duplicate, ignore (DupCount++ in Delphi)

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
        let mut cmd = 0u8;
        let mut result = Vec::new();

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
    last_recvd_ts: Vec<i64>, // for duplicate datagram detection
    last_online: i64,
}

const LAST_RECVD_BUF_SIZE: usize = 2048;
const TIME_WHEN_CAN_RECEIVE_RPT: i64 = 9000; // ms

impl SlicingReceiver {
    pub fn new() -> Self {
        Self {
            receiving: HashMap::new(),
            last_recvd_ts: vec![0i64; LAST_RECVD_BUF_SIZE],
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
    pub fn on_new_sliced(&mut self, payload: &[u8]) -> (Option<(u8, Vec<u8>)>, [u8; ACK256_WIRE_SIZE]) {
        let hdr = match SliceHeader::from_bytes(payload) {
            Some(h) => h,
            None => return (None, [0u8; ACK256_WIRE_SIZE]),
        };

        let block_data = payload[SLICE_HEADER_SIZE..].to_vec();
        let datagram_num = hdr.datagram_num;

        // Check if this is a new datagram number
        if self.is_new_datagram(datagram_num) {
            // Remove any old entry with same number
            self.receiving.remove(&datagram_num);
            // Create new SlicedData
            self.receiving.insert(datagram_num, SlicedData::new(datagram_num, hdr.max_block_num));
        } else if !self.receiving.contains_key(&datagram_num) {
            // Not new, not in receiving → already completed, send full ACK
            let mut flags = [0xFFu8; 32]; // SetAllFlags
            let ack = build_ack_bytes(&flags, datagram_num);
            return (None, ack);
        } else {
            // Existing entry — check if MaxBlockNum matches (recreate if mismatch)
            let existing = self.receiving.get(&datagram_num).unwrap();
            if existing.blocks_count - 1 != hdr.max_block_num as usize {
                self.receiving.remove(&datagram_num);
                self.receiving.insert(datagram_num, SlicedData::new(datagram_num, hdr.max_block_num));
            }
        }

        // Add the piece
        let sliced = self.receiving.get_mut(&datagram_num).unwrap();
        let complete = sliced.receive_piece(hdr.block_num, block_data);
        let ack = build_ack_bytes(&sliced.ack_flags, datagram_num);

        if complete {
            let assembled = sliced.assemble();
            self.receiving.remove(&datagram_num);
            (assembled, ack)
        } else {
            (None, ack)
        }
    }

    /// Clean old incomplete datagrams (called periodically).
    /// Matches TMoonProtoClient.ClearOldReceiving.
    pub fn clear_old(&mut self) {
        let to_remove: Vec<u16> = self.receiving.keys()
            .filter(|&&k| {
                let idx = (k as usize) % LAST_RECVD_BUF_SIZE;
                (self.last_online - self.last_recvd_ts[idx]).abs() > TIME_WHEN_CAN_RECEIVE_RPT
            })
            .copied()
            .collect();

        for k in to_remove {
            self.receiving.remove(&k);
        }
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
        let mut payload = vec![
            0x01, 0x00, // datagram_num = 1
            0x00,       // block_num = 0
            0x00,       // max_block_num = 0 (1 block total)
            0x0A,       // cmd byte
            0xDE, 0xAD, // data
        ];

        let (assembled, ack) = recv.on_new_sliced(&payload);
        let (cmd, data) = assembled.unwrap();
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
            0x01,       // block_num = 1
            0x01,       // max_block_num = 1 (2 blocks total)
            0xBB, 0xCC, // data
        ];
        let (assembled, _) = recv.on_new_sliced(&block1);
        assert!(assembled.is_none()); // not complete yet

        // Block 0 arrives
        let block0 = vec![
            0x05, 0x00, // datagram_num = 5
            0x00,       // block_num = 0
            0x01,       // max_block_num = 1
            0x1C,       // cmd byte
            0xAA,       // data
        ];
        let (assembled, _) = recv.on_new_sliced(&block0);
        let (cmd, data) = assembled.unwrap();
        assert_eq!(cmd, 0x1C);
        assert_eq!(data, vec![0xAA, 0xBB, 0xCC]);
    }
}
