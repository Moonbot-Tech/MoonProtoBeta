//! Process time helpers used by the client runtime.

use std::sync::atomic::{AtomicU64, Ordering};

/// Global NTP time offset (days). Set once at startup by ntp::get_best_ntp.
/// Matches Delphi GlobalMPTimeOffset.
static NTP_OFFSET_DAYS: AtomicU64 = AtomicU64::new(0);

/// Set the process-global NTP correction in seconds.
///
/// `ClientConfig::new` normally starts the managed NTP syncer automatically.
/// Public tools that intentionally manage NTP directly should use the `ntp`
/// module and provide their own callback/storage.
pub(crate) fn set_ntp_offset(offset_seconds: f64) {
    let bits = (offset_seconds / 86400.0).to_bits();
    NTP_OFFSET_DAYS.store(bits, Ordering::Relaxed);
}

pub(crate) fn current_utc_hour_slot() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .checked_div(3600)
        .unwrap_or(0) as i64
}

fn get_ntp_offset_days() -> f64 {
    f64::from_bits(NTP_OFFSET_DAYS.load(Ordering::Relaxed))
}

/// Process-global fallback for low-level `EventDispatcher::dispatch_into` callers
/// that have not linked a per-client `ServerTimeDelta` source. The recommended
/// active path auto-links `EventDispatcher` to `Client::server_time_delta_handle`
/// via `MoonClient` / the low-level active pump and **does not use** this global value.
///
/// Multi-client sessions do not share this value in the active path: each
/// `Client` has its own `Arc<AtomicU64>` handle.
static SERVER_TIME_DELTA_DAYS: AtomicU64 = AtomicU64::new(0);

/// Set the fallback server_time_delta (in days, as TDateTime).
/// Called from `MPC_Ping` handling; consumers must NOT call this
/// directly — use `client.server_time_delta_handle()` for multi-Client.
pub(crate) fn set_server_time_delta_global(delta_days: f64) {
    SERVER_TIME_DELTA_DAYS.store(delta_days.to_bits(), Ordering::Relaxed);
}

/// Get the fallback server_time_delta (days). Used by `EventDispatcher` when
/// the per-Client source is not linked.
pub(crate) fn get_server_time_delta_global() -> f64 {
    f64::from_bits(SERVER_TIME_DELTA_DAYS.load(Ordering::Relaxed))
}

/// Delphi raw `Now` as UTC TDateTime (days since 1899-12-30), without NTP offset.
/// Used for `ServerTimeDelta := Ping.InitialTime - Now`.
pub(crate) fn delphi_now_raw() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25569.0 + secs / 86400.0
}

/// Delphi TDateTime corrected by the NTP offset.
/// Matches: `Now - GlobalMPTimeZoneOffset + GlobalMPTimeOffset`.
/// We use UTC directly (no timezone offset needed — TDateTime in MoonProto = UTC).
///
/// The NTP offset (optional, on by default) reaches the protocol only here. It is
/// soft — it never sets the OS clock — and the client gates nothing on it: it
/// feeds only outgoing handshake/Ping timestamps and the `net_lag_ping`
/// diagnostic. `server_time_delta` (order times) uses [`delphi_now_raw`] with no
/// offset, anti-replay is the `msg_num` slider, rate/PMTU use Ping payload fields,
/// and the client validates no incoming timestamp. A spoofed NTP offset therefore
/// cannot affect confidentiality, integrity or authentication; its only reachable
/// effect is availability — a server that checks handshake-timestamp freshness
/// could reject a skewed Hello (reconnect) — plus a wrong lag readout. The offset
/// is intentionally not clamped here (parity with the Delphi reference); disable
/// the source entirely with `ClientConfig::without_ntp`.
pub(crate) fn delphi_now() -> f64 {
    delphi_now_raw() + get_ntp_offset_days()
}
