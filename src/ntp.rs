/// SNTP time synchronization — byte-exact port of TMoonProtoTymeSyncer + TMySNTP.GetBestNTP.
/// Source: IndyUDPHelper.pas:459-522, MoonProtoIntStruct.pas:1246-1303
///
/// NTP packet format (RFC 4330): 48 bytes.
/// Client sends: LI=0, VN=4, Mode=3 (client), rest zeros.
/// Server responds with timestamps in NTP format (seconds since 1900-01-01, 32.32 fixed).
///
/// Offset = ((T2 - T1) + (T3 - T4)) / 2  (in seconds as f64)
/// RoundTrip = (T4 - T1) - (T3 - T2)

use std::net::UdpSocket;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const NTP_PORT: u16 = 123;
const NTP_PACKET_SIZE: usize = 48;
const NTP_EPOCH_OFFSET: u64 = 2_208_988_800; // seconds between 1900-01-01 and 1970-01-01

pub struct NtpSyncResult {
    pub time_offset: f64,    // seconds (add to SystemTime to get corrected time)
    pub round_trip_ms: i64,  // milliseconds
    pub synced: bool,
}

/// One SNTP request. Returns (offset_seconds, roundtrip_seconds) or None on failure.
fn sntp_request(host: &str, timeout_ms: u64) -> Option<(f64, f64)> {
    let addr = format!("{}:{}", host, NTP_PORT);
    let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.set_read_timeout(Some(Duration::from_millis(timeout_ms))).ok()?;
    sock.set_write_timeout(Some(Duration::from_millis(1000))).ok()?;

    // Build NTP request: LI=0, VN=4, Mode=3 → first byte = 0b00_100_011 = 0x23
    let mut request = [0u8; NTP_PACKET_SIZE];
    request[0] = 0x23; // LI=0, VN=4, Mode=3

    // T1 = client transmit timestamp (stored in bytes 40..47)
    let t1 = system_time_to_ntp();
    request[40..44].copy_from_slice(&t1.0.to_be_bytes());
    request[44..48].copy_from_slice(&t1.1.to_be_bytes());

    let t1_f = ntp_to_seconds(t1.0, t1.1);

    sock.send_to(&request, &addr).ok()?;

    let mut response = [0u8; NTP_PACKET_SIZE];
    let (n, _) = sock.recv_from(&mut response).ok()?;
    if n < NTP_PACKET_SIZE { return None; }

    let t4_ntp = system_time_to_ntp();
    let t4 = ntp_to_seconds(t4_ntp.0, t4_ntp.1);

    // T2 = server receive timestamp (bytes 32..39)
    let t2_sec = u32::from_be_bytes(response[32..36].try_into().unwrap());
    let t2_frac = u32::from_be_bytes(response[36..40].try_into().unwrap());
    let t2 = ntp_to_seconds(t2_sec, t2_frac);

    // T3 = server transmit timestamp (bytes 40..47)
    let t3_sec = u32::from_be_bytes(response[40..44].try_into().unwrap());
    let t3_frac = u32::from_be_bytes(response[44..48].try_into().unwrap());
    let t3 = ntp_to_seconds(t3_sec, t3_frac);

    // Offset and round-trip
    let offset = ((t2 - t1_f) + (t3 - t4)) / 2.0;
    let roundtrip = (t4 - t1_f) - (t3 - t2);

    Some((offset, roundtrip))
}

/// Get best NTP sync (matches TMySNTP.GetBestNTP with TryCount=4).
/// Returns offset in seconds and best round-trip in ms.
pub fn get_best_ntp(host: &str, try_count: usize) -> NtpSyncResult {
    let mut best_delay_ms: i64 = i64::MAX;
    let mut best_offset: f64 = 0.0;
    let mut timeout_ms: u64 = 500;

    for attempt in 0..try_count {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(50));
        }

        if let Some((offset, roundtrip)) = sntp_request(host, timeout_ms) {
            let rt_ms = (roundtrip * 1000.0).round() as i64;

            if rt_ms < best_delay_ms {
                best_delay_ms = rt_ms;
                best_offset = offset;
            }
        } else {
            timeout_ms = (timeout_ms + 100).min(2000);
        }
    }

    if best_delay_ms == i64::MAX {
        NtpSyncResult { time_offset: 0.0, round_trip_ms: 0, synced: false }
    } else {
        NtpSyncResult { time_offset: best_offset, round_trip_ms: best_delay_ms, synced: true }
    }
}

/// Convert NTP timestamp (seconds + fraction since 1900) to seconds as f64.
fn ntp_to_seconds(sec: u32, frac: u32) -> f64 {
    sec as f64 + (frac as f64 / 4_294_967_296.0) // 2^32
}

/// Get current system time as NTP timestamp (seconds, fraction) since 1900-01-01.
fn system_time_to_ntp() -> (u32, u32) {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let ntp_sec = since_epoch.as_secs() + NTP_EPOCH_OFFSET;
    let ntp_frac = ((since_epoch.subsec_nanos() as u64) << 32) / 1_000_000_000;
    (ntp_sec as u32, ntp_frac as u32)
}

/// Convert NTP time offset to Delphi TDateTime offset.
/// Delphi: GlobalMPTimeOffset used as `Now + GlobalMPTimeOffset` to get corrected time.
/// Since TDateTime is in days: offset_days = offset_seconds / 86400.
pub fn offset_to_delphi_days(offset_seconds: f64) -> f64 {
    offset_seconds / 86400.0
}

/// Background NTP sync thread — byte-exact port of `TMoonProtoTymeSyncer.Execute`
/// (MoonProtoIntStruct.pas:1246-1303).
///
/// Поведение:
/// 1. Init: `get_best_ntp(host, 4)` → `MinDelay` + apply offset через `apply_fn`.
/// 2. Loop с шагом ~500ms:
///    - `GetTimeTryCount++`
///    - Если `GetTimeTryCount < 4` → ещё один `get_best_ntp(host, 2)`; если `NewDelay < MinDelay` → обновить offset.
///    - Если `GetTimeTryCount > 1000` (~500с) → reset cycle (`MinDelay *= 1.1`), повтор уточнения.
///
/// `apply_fn` вызывается с offset в **секундах** при каждом улучшении. Обычно передают
/// `client::set_ntp_offset` чтобы атомарно обновить глобальный offset.
///
/// Поток daemon-style: умирает при завершении программы (нет explicit stop).
pub fn spawn_sync_thread<F>(host: String, apply_fn: F)
    where F: Fn(f64) + Send + 'static
{
    std::thread::Builder::new()
        .name("moonproto-ntp-sync".into())
        .spawn(move || {
            // Initial sync (try_count=4)
            let first = get_best_ntp(&host, 4);
            let mut min_delay_ms: i64 = if first.synced {
                apply_fn(first.time_offset);
                first.round_trip_ms
            } else {
                i64::MAX
            };
            let mut try_count: u32 = 1;

            loop {
                // Sleep 5 × 100ms = 500ms (как Delphi pas:1273-1275)
                for _ in 0..5 {
                    std::thread::sleep(Duration::from_millis(100));
                }

                try_count += 1;
                if try_count > 1000 {
                    try_count = 2;
                    // Расширяем приёмное окно — позволим худший RTT перебить (Delphi pas:1281)
                    min_delay_ms = ((min_delay_ms as f64 * 1.1) as i64) + 10;
                }

                if try_count < 4 {
                    let r = get_best_ntp(&host, 2);
                    if r.synced && r.round_trip_ms < min_delay_ms {
                        min_delay_ms = r.round_trip_ms;
                        apply_fn(r.time_offset);
                    }
                }
                // try_count >= 4 и <= 1000 — idle (тишина, как Delphi)
            }
        })
        .expect("failed to spawn NTP sync thread");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntp_sync_works() {
        let result = get_best_ntp("pool.ntp.org", 3);
        if result.synced {
            println!("NTP offset: {:.3}ms, RTT: {}ms", result.time_offset * 1000.0, result.round_trip_ms);
            assert!(result.time_offset.abs() < 60.0); // offset < 60 seconds = sane
            assert!(result.round_trip_ms < 5000); // RTT < 5 seconds
        } else {
            println!("NTP sync failed (network unavailable?)");
        }
    }

    #[test]
    fn ntp_offset_to_delphi_days() {
        assert_eq!(offset_to_delphi_days(86400.0), 1.0);
        assert_eq!(offset_to_delphi_days(0.0), 0.0);
        assert!((offset_to_delphi_days(3600.0) - (1.0 / 24.0)).abs() < 1e-9);
    }
}
