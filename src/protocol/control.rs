//! Fixed-size MoonProto service records: Ping, SizeTest, and ProbeMTU.

use zerocopy::byteorder::little_endian::{
    F64 as LeF64, I32 as LeI32, U16 as LeU16, U32 as LeU32, U64 as LeU64,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::transport::CLIENT_HDR_SIZE;

pub(crate) const PING_SIZE: usize = std::mem::size_of::<WirePing>();
const _: [(); 57] = [(); PING_SIZE];
pub(crate) const PING_MEMORY_INFO_SIZE: usize = std::mem::size_of::<WirePingMemoryInfo>();
const _: [(); 5] = [(); PING_MEMORY_INFO_SIZE];
pub(crate) const PING_FLAG_MEMORY_INFO: u8 = 1;
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
    moment_cpu: u8,
    total_cpu: u8,
    flags: u8,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WirePingMemoryInfo {
    used_memory_mb: LeU16,
    free_physical_memory_mb: LeU16,
    cores: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PingMemoryInfo {
    pub(crate) used_memory_mb: u16,
    pub(crate) free_physical_memory_mb: u16,
    pub(crate) cores: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PingTelemetry {
    pub(crate) moment_cpu_percent: u8,
    pub(crate) total_cpu_percent: u8,
    pub(crate) memory: Option<PingMemoryInfo>,
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
    pub moment_cpu_percent: u8,
    pub total_cpu_percent: u8,
    pub memory: Option<PingMemoryInfo>,
    pub ack_words_offset: usize,
}

impl PingFrame {
    pub(crate) fn read(data: &[u8]) -> Option<Self> {
        if data.len() < PING_SIZE {
            return None;
        }
        let wire = WirePing::read_from_bytes(&data[..PING_SIZE]).ok()?;
        let mut ack_words_offset = PING_SIZE;
        let memory = if wire.flags & PING_FLAG_MEMORY_INFO != 0 {
            if data.len() >= ack_words_offset + PING_MEMORY_INFO_SIZE {
                let memory = WirePingMemoryInfo::read_from_bytes(
                    &data[ack_words_offset..ack_words_offset + PING_MEMORY_INFO_SIZE],
                )
                .ok()?;
                ack_words_offset += PING_MEMORY_INFO_SIZE;
                Some(PingMemoryInfo {
                    used_memory_mb: memory.used_memory_mb.get(),
                    free_physical_memory_mb: memory.free_physical_memory_mb.get(),
                    cores: memory.cores,
                })
            } else {
                ack_words_offset = data.len();
                None
            }
        } else {
            None
        };
        Some(Self {
            time: wire.time.get(),
            initial_time: wire.initial_time.get(),
            trip_delay: wire.trip_delay.get(),
            pmtu: wire.pmtu.get(),
            global_timing_orders: wire.global_timing_orders.get(),
            overheat: wire.overheat,
            rsq: wire.rsq,
            ack_session: wire.ack_session.get(),
            moment_cpu_percent: wire.moment_cpu,
            total_cpu_percent: wire.total_cpu,
            memory,
            ack_words_offset,
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
        telemetry: PingTelemetry,
    ) -> Vec<u8> {
        let flags = if telemetry.memory.is_some() {
            PING_FLAG_MEMORY_INFO
        } else {
            0
        };
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
            moment_cpu: telemetry.moment_cpu_percent,
            total_cpu: telemetry.total_cpu_percent,
            flags,
        };
        let mut out = wire.as_bytes().to_vec();
        if let Some(memory) = telemetry.memory {
            let memory = WirePingMemoryInfo {
                used_memory_mb: LeU16::new(memory.used_memory_mb),
                free_physical_memory_mb: LeU16::new(memory.free_physical_memory_mb),
                cores: memory.cores,
            };
            out.extend_from_slice(memory.as_bytes());
        }
        out
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

    pub(crate) fn ack_bytes(size: u16, series_num: u16) -> Option<Vec<u8>> {
        let mut out = padded_client_payload(size, SIZE_TEST_SIZE)?;
        let wire = WireSizeTestData {
            size: LeU16::new(size),
            packet_num: LeU16::new(0),
            series_num: LeU16::new(series_num),
        };
        out[..SIZE_TEST_SIZE].copy_from_slice(wire.as_bytes());
        Some(out)
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

    pub(crate) fn ack_bytes(self) -> Option<Vec<u8>> {
        let mut out = padded_client_payload(self.test_size, PROBE_MTU_ACK_SIZE)?;
        let wire = WireProbeMtuAck {
            probe_id: LeU16::new(self.probe_id),
            probe_index: self.probe_index,
            received_size: LeU16::new(self.test_size),
        };
        out[..PROBE_MTU_ACK_SIZE].copy_from_slice(wire.as_bytes());
        Some(out)
    }
}

fn padded_client_payload(total_datagram_size: u16, record_size: usize) -> Option<Vec<u8>> {
    let payload_len = usize::from(total_datagram_size).checked_sub(CLIENT_HDR_SIZE)?;
    if payload_len < record_size {
        return None;
    }
    Some(vec![0u8; payload_len])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_records_have_delphi_sizes() {
        assert_eq!(std::mem::size_of::<WirePing>(), 57);
        assert_eq!(PING_SIZE, 57);
        assert_eq!(std::mem::size_of::<WirePingMemoryInfo>(), 5);
        assert_eq!(std::mem::size_of::<WireSizeTestData>(), 6);
        assert_eq!(SIZE_TEST_SIZE, 6);
        assert_eq!(std::mem::size_of::<WireProbeMtu>(), 5);
        assert_eq!(PROBE_MTU_SIZE, 5);
        assert_eq!(std::mem::size_of::<WireProbeMtuAck>(), 5);
        assert_eq!(PROBE_MTU_ACK_SIZE, 5);
    }

    #[test]
    fn size_test_ack_writes_fixed_header_and_zero_packet_num() {
        let total_size = (CLIENT_HDR_SIZE + 8) as u16;
        let ack = SizeTestData::ack_bytes(total_size, 0x1234).unwrap();

        assert_eq!(ack.len(), 8);
        assert_eq!(&ack[0..2], &total_size.to_le_bytes());
        assert_eq!(&ack[2..4], &0u16.to_le_bytes());
        assert_eq!(&ack[4..6], &0x1234u16.to_le_bytes());
    }

    #[test]
    fn probe_mtu_ack_writes_received_size() {
        let probe = ProbeMtu {
            probe_id: 0xAABB,
            probe_index: 1,
            test_size: (CLIENT_HDR_SIZE + 7) as u16,
        };
        let ack = probe.ack_bytes().unwrap();

        assert_eq!(ack.len(), 7);
        assert_eq!(&ack[0..2], &0xAABBu16.to_le_bytes());
        assert_eq!(ack[2], 1);
        assert_eq!(&ack[3..5], &((CLIENT_HDR_SIZE + 7) as u16).to_le_bytes());
    }

    #[test]
    fn ping_memory_profile_precedes_ack_words() {
        let wire = WirePing {
            time: LeF64::new(1.0),
            initial_time: LeF64::new(2.0),
            trip_delay: LeI32::new(3),
            pmtu: LeU16::new(1200),
            global_timing_orders: LeU16::new(4),
            overheat: 5,
            total_sent_bytes: LeU64::new(6),
            total_recv_bytes: LeU64::new(7),
            rsq: 8,
            ack_start: LeU64::new(9),
            ack_session: LeU32::new(10),
            moment_cpu: 11,
            total_cpu: 12,
            flags: PING_FLAG_MEMORY_INFO,
        };
        let memory = WirePingMemoryInfo {
            used_memory_mb: LeU16::new(1234),
            free_physical_memory_mb: LeU16::new(5678),
            cores: 24,
        };
        let ack_word = 0x8877_6655_4433_2211u64;
        let mut payload = wire.as_bytes().to_vec();
        payload.extend_from_slice(memory.as_bytes());
        payload.extend_from_slice(&ack_word.to_le_bytes());

        let ping = PingFrame::read(&payload).expect("valid v4 Ping");
        assert_eq!(ping.ack_words_offset, PING_SIZE + PING_MEMORY_INFO_SIZE);
        assert_eq!(
            ping.memory,
            Some(PingMemoryInfo {
                used_memory_mb: 1234,
                free_physical_memory_mb: 5678,
                cores: 24,
            })
        );
        assert_eq!(
            u64::from_le_bytes(
                payload[ping.ack_words_offset..ping.ack_words_offset + 8]
                    .try_into()
                    .unwrap()
            ),
            ack_word
        );
    }

    #[test]
    fn ping_response_overwrites_peer_telemetry_with_local_profile() {
        let wire = WirePing {
            time: LeF64::new(1.0),
            initial_time: LeF64::new(2.0),
            trip_delay: LeI32::new(3),
            pmtu: LeU16::new(1200),
            global_timing_orders: LeU16::new(4),
            overheat: 5,
            total_sent_bytes: LeU64::new(6),
            total_recv_bytes: LeU64::new(7),
            rsq: 8,
            ack_start: LeU64::new(9),
            ack_session: LeU32::new(10),
            moment_cpu: 90,
            total_cpu: 91,
            flags: 0,
        };
        let ping = PingFrame::read(wire.as_bytes()).unwrap();
        let response = ping.response_bytes(
            3.0,
            100,
            200,
            300,
            400,
            PingTelemetry {
                moment_cpu_percent: 17,
                total_cpu_percent: 23,
                memory: Some(PingMemoryInfo {
                    used_memory_mb: 345,
                    free_physical_memory_mb: 678,
                    cores: 16,
                }),
            },
        );

        assert_eq!(response.len(), PING_SIZE + PING_MEMORY_INFO_SIZE);
        let echoed = PingFrame::read(&response).unwrap();
        assert_eq!(echoed.moment_cpu_percent, 17);
        assert_eq!(echoed.total_cpu_percent, 23);
        assert_eq!(echoed.memory.unwrap().cores, 16);
    }
}
