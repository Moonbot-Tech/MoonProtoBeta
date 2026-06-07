//! Fixed-size MoonProto service records: Ping, SizeTest, and ProbeMTU.

use zerocopy::byteorder::little_endian::{
    F64 as LeF64, I32 as LeI32, U16 as LeU16, U32 as LeU32, U64 as LeU64,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub(crate) const PING_SIZE: usize = std::mem::size_of::<WirePing>();
const _: [(); 54] = [(); PING_SIZE];
pub(crate) const SIZE_TEST_SIZE: usize = std::mem::size_of::<WireSizeTestData>();
const _: [(); 6] = [(); SIZE_TEST_SIZE];
pub(crate) const PROBE_MTU_SIZE: usize = std::mem::size_of::<WireProbeMtu>();
const _: [(); 5] = [(); PROBE_MTU_SIZE];
pub(crate) const PROBE_MTU_ACK_SIZE: usize = std::mem::size_of::<WireProbeMtuAck>();
const _: [(); 5] = [(); PROBE_MTU_ACK_SIZE];

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WirePing {
    time: LeF64,
    initial_time: LeF64,
    trip_delay: LeI32,
    pmtu: LeU16,
    global_timing_orders: LeU16,
    overheat: u8,
    total_sent_bytes: LeU64,
    total_recv_bytes: LeU64,
    rsq: u8,
    ack_start: LeU64,
    ack_session: LeU32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PingFrame {
    pub time: f64,
    pub initial_time: f64,
    pub trip_delay: i32,
    pub pmtu: u16,
    pub global_timing_orders: u16,
    pub overheat: u8,
    pub rsq: u8,
    pub ack_session: u32,
}

impl PingFrame {
    pub(crate) fn read(data: &[u8]) -> Option<Self> {
        if data.len() < PING_SIZE {
            return None;
        }
        let wire = WirePing::read_from_bytes(&data[..PING_SIZE]).ok()?;
        Some(Self {
            time: wire.time.get(),
            initial_time: wire.initial_time.get(),
            trip_delay: wire.trip_delay.get(),
            pmtu: wire.pmtu.get(),
            global_timing_orders: wire.global_timing_orders.get(),
            overheat: wire.overheat,
            rsq: wire.rsq,
            ack_session: wire.ack_session.get(),
        })
    }

    pub(crate) fn rs(self) -> f64 {
        self.rsq as f64 * (1.0 / 255.0)
    }

    pub(crate) fn response_bytes(
        self,
        corrected_now_dt: f64,
        total_sent_bytes: u64,
        total_recv_bytes: u64,
        ack_start: u64,
        ack_session: u32,
    ) -> Vec<u8> {
        let wire = WirePing {
            time: LeF64::new(corrected_now_dt),
            initial_time: LeF64::new(self.initial_time),
            trip_delay: LeI32::new(self.trip_delay),
            pmtu: LeU16::new(self.pmtu),
            global_timing_orders: LeU16::new(self.global_timing_orders),
            overheat: self.overheat,
            total_sent_bytes: LeU64::new(total_sent_bytes),
            total_recv_bytes: LeU64::new(total_recv_bytes),
            rsq: self.rsq,
            ack_start: LeU64::new(ack_start),
            ack_session: LeU32::new(ack_session),
        };
        wire.as_bytes().to_vec()
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireSizeTestData {
    size: LeU16,
    packet_num: LeU16,
    series_num: LeU16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SizeTestData {
    pub size: u16,
    pub series_num: u16,
}

impl SizeTestData {
    pub(crate) fn read(data: &[u8]) -> Option<Self> {
        if data.len() < SIZE_TEST_SIZE {
            return None;
        }
        let wire = WireSizeTestData::read_from_bytes(&data[..SIZE_TEST_SIZE]).ok()?;
        Some(Self {
            size: wire.size.get(),
            series_num: wire.series_num.get(),
        })
    }

    pub(crate) fn ack_bytes(size: u16, series_num: u16) -> Vec<u8> {
        let mut out = vec![0u8; size as usize];
        let wire = WireSizeTestData {
            size: LeU16::new(size),
            packet_num: LeU16::new(0),
            series_num: LeU16::new(series_num),
        };
        out[..SIZE_TEST_SIZE].copy_from_slice(wire.as_bytes());
        out
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireProbeMtu {
    probe_id: LeU16,
    probe_index: u8,
    test_size: LeU16,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireProbeMtuAck {
    probe_id: LeU16,
    probe_index: u8,
    received_size: LeU16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProbeMtu {
    pub probe_id: u16,
    pub probe_index: u8,
    pub test_size: u16,
}

impl ProbeMtu {
    pub(crate) fn read(data: &[u8]) -> Option<Self> {
        if data.len() < PROBE_MTU_SIZE {
            return None;
        }
        let wire = WireProbeMtu::read_from_bytes(&data[..PROBE_MTU_SIZE]).ok()?;
        Some(Self {
            probe_id: wire.probe_id.get(),
            probe_index: wire.probe_index,
            test_size: wire.test_size.get(),
        })
    }

    pub(crate) fn ack_bytes(self) -> Vec<u8> {
        let mut out = vec![0u8; self.test_size as usize];
        let wire = WireProbeMtuAck {
            probe_id: LeU16::new(self.probe_id),
            probe_index: self.probe_index,
            received_size: LeU16::new(self.test_size),
        };
        out[..PROBE_MTU_ACK_SIZE].copy_from_slice(wire.as_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_records_have_delphi_sizes() {
        assert_eq!(std::mem::size_of::<WirePing>(), 54);
        assert_eq!(PING_SIZE, 54);
        assert_eq!(std::mem::size_of::<WireSizeTestData>(), 6);
        assert_eq!(SIZE_TEST_SIZE, 6);
        assert_eq!(std::mem::size_of::<WireProbeMtu>(), 5);
        assert_eq!(PROBE_MTU_SIZE, 5);
        assert_eq!(std::mem::size_of::<WireProbeMtuAck>(), 5);
        assert_eq!(PROBE_MTU_ACK_SIZE, 5);
    }

    #[test]
    fn size_test_ack_writes_fixed_header_and_zero_packet_num() {
        let ack = SizeTestData::ack_bytes(8, 0x1234);

        assert_eq!(ack.len(), 8);
        assert_eq!(&ack[0..2], &8u16.to_le_bytes());
        assert_eq!(&ack[2..4], &0u16.to_le_bytes());
        assert_eq!(&ack[4..6], &0x1234u16.to_le_bytes());
    }

    #[test]
    fn probe_mtu_ack_writes_received_size() {
        let probe = ProbeMtu {
            probe_id: 0xAABB,
            probe_index: 1,
            test_size: 7,
        };
        let ack = probe.ack_bytes();

        assert_eq!(ack.len(), 7);
        assert_eq!(&ack[0..2], &0xAABBu16.to_le_bytes());
        assert_eq!(ack[2], 1);
        assert_eq!(&ack[3..5], &7u16.to_le_bytes());
    }
}
