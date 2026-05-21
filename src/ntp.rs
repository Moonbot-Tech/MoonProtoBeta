/// SNTP time synchronization — byte-exact port of TMoonProtoTymeSyncer + TMySNTP.GetBestNTP.
/// Source: IndyUDPHelper.pas:459-522, MoonProtoIntStruct.pas:1246-1303
///
/// NTP packet format (RFC 4330): 48 bytes.
/// Client sends: LI=0, VN=4, Mode=3 (client), rest zeros.
/// Server responds with timestamps in NTP format (seconds since 1900-01-01, 32.32 fixed).
///
/// Offset = ((T2 - T1) + (T3 - T4)) / 2  (in seconds as f64)
/// RoundTrip = (T4 - T1) - (T3 - T2)

use log::{debug, warn};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex, OnceLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const NTP_PORT: u16 = 123;
const NTP_PACKET_SIZE: usize = 48;
const NTP_EPOCH_OFFSET: u64 = 2_208_988_800; // seconds between 1900-01-01 and 1970-01-01

#[derive(Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, Clone)]
struct NtpState {
    best_delay_ms: i64,
    receive_timeout_ms: u64,
    synced_once: bool,
}

impl Default for NtpState {
    fn default() -> Self {
        Self {
            best_delay_ms: 0,
            receive_timeout_ms: 500,
            synced_once: false,
        }
    }
}

fn get_best_ntp_with_state<F>(
    state: &mut NtpState,
    try_count: usize,
    mut request: F,
) -> NtpSyncResult
where
    F: FnMut(u64) -> Option<(f64, f64)>,
{
    let force_sync = state.best_delay_ms == 0;
    state.best_delay_ms = ((state.best_delay_ms as f64 * 1.05).round() as i64) + 1;
    let mut large_offset_retry_count = 0;
    let mut attempts = 0usize;
    let mut effective_try_count = try_count;

    while attempts < effective_try_count {
        attempts += 1;

        if let Some((offset, roundtrip)) = request(state.receive_timeout_ms) {
            let rt_ms = (roundtrip.abs() * 1000.0).round() as i64;

            if rt_ms < 50 || force_sync || rt_ms < state.best_delay_ms {
                if offset.abs() > 60.0 && state.synced_once && large_offset_retry_count < 2 {
                    large_offset_retry_count += 1;
                    effective_try_count = 6.min(effective_try_count + 1);
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }

                state.best_delay_ms = rt_ms;
                state.synced_once = true;
                return NtpSyncResult {
                    time_offset: offset,
                    round_trip_ms: rt_ms,
                    synced: true,
                };
            }
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    if !state.synced_once {
        state.receive_timeout_ms = (state.receive_timeout_ms + 100).min(2000);
    }

    NtpSyncResult {
        time_offset: 0.0,
        round_trip_ms: 0,
        synced: false,
    }
}

/// Get best NTP sync (matches TMySNTP.GetBestNTP with TryCount=4).
/// Returns offset in seconds and accepted round-trip in ms.
pub fn get_best_ntp(host: &str, try_count: usize) -> NtpSyncResult {
    let mut state = NtpState::default();
    let result = get_best_ntp_with_state(&mut state, try_count, |timeout_ms| {
        sntp_request(host, timeout_ms)
    });
    if result.synced {
        debug!(
            "NTP sync ok: host={host} offset={:.1}ms rtt={}ms",
            result.time_offset * 1000.0,
            result.round_trip_ms
        );
    } else {
        warn!("NTP sync failed: all {try_count} attempts to {host} returned no valid response");
    }
    result
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

struct ProcessNtpState {
    host: String,
    shutdown: Arc<AtomicBool>,
    refs: usize,
}

static PROCESS_NTP: OnceLock<Mutex<Option<ProcessNtpState>>> = OnceLock::new();

fn process_ntp_state() -> &'static Mutex<Option<ProcessNtpState>> {
    PROCESS_NTP.get_or_init(|| Mutex::new(None))
}

/// Process-level guard for the Delphi-style global MoonProto SNTP syncer.
///
/// Delphi stores `GlobalMPTimeOffset` in process-global state and starts a
/// single `TMoonProtoTymeSyncer` thread from the application bootstrap. Rust
/// keeps the same global offset, so clients in the same process must share the
/// worker instead of racing several per-client workers against the same value.
pub(crate) struct ProcessNtpGuard;

impl Drop for ProcessNtpGuard {
    fn drop(&mut self) {
        release_process_sync();
    }
}

/// Acquire the shared process-level NTP syncer.
///
/// While at least one guard is alive, the same background worker is reused by
/// every `Client`. If a later client asks for another host, the existing worker
/// is kept: Delphi has only one process-wide SNTP host/offset, so mixing hosts
/// inside one process would reintroduce last-writer-wins timing.
pub(crate) fn acquire_process_sync(host: String, apply_fn: fn(f64)) -> Option<ProcessNtpGuard> {
    acquire_process_sync_with(host, apply_fn, |host, apply_fn| {
        spawn_sync_thread(host, apply_fn)
    })
}

fn acquire_process_sync_with<S>(
    host: String,
    apply_fn: fn(f64),
    spawn: S,
) -> Option<ProcessNtpGuard>
where
    S: FnOnce(String, fn(f64)) -> Arc<AtomicBool>,
{
    let mut state = process_ntp_state().lock().ok()?;
    if let Some(active) = state.as_mut() {
        if active.host != host {
            warn!(
                target: "moonproto::ntp",
                "NTP sync already runs for host {}; requested {}; sharing the existing process-level syncer",
                active.host,
                host
            );
        }
        active.refs = active.refs.saturating_add(1);
        return Some(ProcessNtpGuard);
    }

    let shutdown = spawn(host.clone(), apply_fn);
    if shutdown.load(Ordering::Relaxed) {
        return None;
    }

    *state = Some(ProcessNtpState {
        host,
        shutdown,
        refs: 1,
    });
    Some(ProcessNtpGuard)
}

fn release_process_sync() {
    if let Ok(mut state) = process_ntp_state().lock() {
        if let Some(active) = state.as_mut() {
            active.refs = active.refs.saturating_sub(1);
            if active.refs == 0 {
                active.shutdown.store(true, Ordering::Relaxed);
                *state = None;
            }
        }
    }
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
/// Возвращает `Arc<AtomicBool>` shutdown flag. Установка `true` приведёт к выходу из
/// loop'а при следующей итерации (max ~500ms задержки до выхода). Если spawn не
/// удался (mobile memory pressure / thread limits) — возвращает `Arc<AtomicBool::new(true)>`
/// сразу (значит "уже выключен", остановить нечего).
///
/// audit_responsibility A6: возможность остановить thread нужна для:
/// - mobile suspend (iOS Background App Refresh — экономия батареи)
/// - graceful shutdown Client (через Drop)
/// - переподключение к другому серверу (создаётся новый Client → старый NTP не нужен)
pub fn spawn_sync_thread<F>(host: String, apply_fn: F) -> std::sync::Arc<std::sync::atomic::AtomicBool>
    where F: Fn(f64) + Send + 'static
{
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);

    // C-V2-05 fix: graceful обработка spawn'а NTP thread'а. На iOS / Android при
    // memory pressure / thread limits ОС может отказать в создании потока. Long-running
    // mobile клиент не должен паниковать — без NTP timestamps будут с системным временем
    // (хуже точность, но клиент остаётся работоспособным).
    if let Err(e) = std::thread::Builder::new()
        .name("moonproto-ntp-sync".into())
        .spawn(move || {
            let mut ntp_state = NtpState::default();

            // Initial sync (try_count=4) — пропускаем если уже shutdown
            if shutdown_thread.load(Ordering::Relaxed) { return; }
            let first = get_best_ntp_with_state(&mut ntp_state, 4, |timeout_ms| {
                sntp_request(&host, timeout_ms)
            });
            let mut min_delay_ms: i64 = if first.synced {
                apply_fn(first.time_offset);
                first.round_trip_ms
            } else {
                i64::MAX
            };
            let mut try_count: u32 = 1;

            loop {
                // Sleep 5 × 100ms = 500ms (как Delphi pas:1273-1275) с проверкой shutdown
                // каждые 100ms — выход в течение ~100ms после `store(true)`.
                for _ in 0..5 {
                    if shutdown_thread.load(Ordering::Relaxed) { return; }
                    std::thread::sleep(Duration::from_millis(100));
                }

                try_count += 1;
                if try_count > 1000 {
                    try_count = 2;
                    // Расширяем приёмное окно — позволим худший RTT перебить (Delphi pas:1281)
                    min_delay_ms = ((min_delay_ms as f64 * 1.1) as i64) + 10;
                }

                if try_count < 4 {
                    let r = get_best_ntp_with_state(&mut ntp_state, 2, |timeout_ms| {
                        sntp_request(&host, timeout_ms)
                    });
                    if r.synced && r.round_trip_ms < min_delay_ms {
                        min_delay_ms = r.round_trip_ms;
                        apply_fn(r.time_offset);
                    }
                }
                // try_count >= 4 и <= 1000 — idle (тишина, как Delphi)
            }
        })
    {
        log::error!(target: "moonproto::ntp",
            "Не удалось запустить NTP sync thread: {e}. NTP отключён — timestamps будут с системным временем.");
        // Возвращаем flag уже-в-shutdown — caller'у нечего останавливать.
        shutdown.store(true, Ordering::Relaxed);
    }
    shutdown
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Mutex, MutexGuard, OnceLock};

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

    // ===== Delphi GetBestNTP selection =====

    #[test]
    fn first_sync_accepts_large_offset_like_delphi() {
        let mut state = NtpState::default();
        let result = get_best_ntp_with_state(&mut state, 1, |_| {
            Some((31_536_000.0, 0.120))
        });

        assert!(result.synced);
        assert_eq!(result.time_offset, 31_536_000.0);
        assert_eq!(result.round_trip_ms, 120);
    }

    #[test]
    fn already_synced_large_offset_gets_two_extra_tries() {
        let mut state = NtpState {
            best_delay_ms: 10,
            receive_timeout_ms: 500,
            synced_once: true,
        };
        let mut calls = 0usize;

        let result = get_best_ntp_with_state(&mut state, 2, |_| {
            calls += 1;
            Some((120.0 + calls as f64, 0.020))
        });

        assert!(result.synced);
        assert_eq!(calls, 3);
        assert_eq!(result.time_offset, 123.0);
    }

    #[test]
    fn no_sync_increases_receive_timeout_until_delphi_cap() {
        let mut state = NtpState::default();

        let result = get_best_ntp_with_state(&mut state, 2, |_| None);

        assert!(!result.synced);
        assert_eq!(state.receive_timeout_ms, 600);
    }

    static PROCESS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static PROCESS_SPAWN_COUNT: AtomicUsize = AtomicUsize::new(0);
    static PROCESS_LAST_SHUTDOWN: OnceLock<Mutex<Option<Arc<AtomicBool>>>> = OnceLock::new();

    fn lock_process_tests() -> MutexGuard<'static, ()> {
        PROCESS_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap()
    }

    fn process_last_shutdown() -> &'static Mutex<Option<Arc<AtomicBool>>> {
        PROCESS_LAST_SHUTDOWN.get_or_init(|| Mutex::new(None))
    }

    fn reset_process_sync_for_test() {
        let mut state = process_ntp_state().lock().unwrap();
        if let Some(active) = state.take() {
            active.shutdown.store(true, Ordering::Relaxed);
        }
        PROCESS_SPAWN_COUNT.store(0, Ordering::Relaxed);
        *process_last_shutdown().lock().unwrap() = None;
    }

    fn process_sync_snapshot() -> Option<(String, usize, bool)> {
        process_ntp_state().lock().unwrap().as_ref().map(|active| {
            (
                active.host.clone(),
                active.refs,
                active.shutdown.load(Ordering::Relaxed),
            )
        })
    }

    fn noop_apply(_: f64) {}

    fn fake_process_spawn(_: String, _: fn(f64)) -> Arc<AtomicBool> {
        PROCESS_SPAWN_COUNT.fetch_add(1, Ordering::Relaxed);
        let shutdown = Arc::new(AtomicBool::new(false));
        *process_last_shutdown().lock().unwrap() = Some(Arc::clone(&shutdown));
        shutdown
    }

    fn failed_process_spawn(_: String, _: fn(f64)) -> Arc<AtomicBool> {
        PROCESS_SPAWN_COUNT.fetch_add(1, Ordering::Relaxed);
        Arc::new(AtomicBool::new(true))
    }

    #[test]
    fn process_sync_is_shared_by_clients() {
        let _lock = lock_process_tests();
        reset_process_sync_for_test();

        let first = acquire_process_sync_with(
            "pool.ntp.org".to_string(),
            noop_apply,
            fake_process_spawn,
        )
        .expect("first client should start process NTP");
        assert_eq!(PROCESS_SPAWN_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(
            process_sync_snapshot(),
            Some(("pool.ntp.org".to_string(), 1, false))
        );

        let shutdown = process_last_shutdown()
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .unwrap();
        let second = acquire_process_sync_with(
            "pool.ntp.org".to_string(),
            noop_apply,
            fake_process_spawn,
        )
        .expect("second client should share process NTP");

        assert_eq!(PROCESS_SPAWN_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(
            process_sync_snapshot(),
            Some(("pool.ntp.org".to_string(), 2, false))
        );

        drop(first);
        assert_eq!(
            process_sync_snapshot(),
            Some(("pool.ntp.org".to_string(), 1, false))
        );
        assert!(!shutdown.load(Ordering::Relaxed));

        drop(second);
        assert_eq!(process_sync_snapshot(), None);
        assert!(shutdown.load(Ordering::Relaxed));

        reset_process_sync_for_test();
    }

    #[test]
    fn process_sync_keeps_first_host_until_last_guard_drops() {
        let _lock = lock_process_tests();
        reset_process_sync_for_test();

        let first = acquire_process_sync_with(
            "ntp-a.example".to_string(),
            noop_apply,
            fake_process_spawn,
        )
        .unwrap();
        let second = acquire_process_sync_with(
            "ntp-b.example".to_string(),
            noop_apply,
            fake_process_spawn,
        )
        .unwrap();

        assert_eq!(PROCESS_SPAWN_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(
            process_sync_snapshot(),
            Some(("ntp-a.example".to_string(), 2, false))
        );

        drop(first);
        drop(second);
        reset_process_sync_for_test();
    }

    #[test]
    fn process_sync_spawn_failure_does_not_register_global_worker() {
        let _lock = lock_process_tests();
        reset_process_sync_for_test();

        let guard = acquire_process_sync_with(
            "pool.ntp.org".to_string(),
            noop_apply,
            failed_process_spawn,
        );

        assert!(guard.is_none());
        assert_eq!(PROCESS_SPAWN_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(process_sync_snapshot(), None);

        reset_process_sync_for_test();
    }
}
