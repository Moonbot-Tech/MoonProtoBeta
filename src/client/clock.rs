//! Process time helpers used by the client runtime.

use std::sync::atomic::{AtomicU64, Ordering};

/// Global NTP time offset (days). Set once at startup by ntp::get_best_ntp.
/// Matches Delphi GlobalMPTimeOffset.
static NTP_OFFSET_DAYS: AtomicU64 = AtomicU64::new(0);

/// Set the process-global NTP correction in seconds.
///
/// `ClientConfig::new` normally starts the managed NTP syncer automatically.
/// This function is exposed for tests and custom tools that manage time sync
/// outside the client.
pub fn set_ntp_offset(offset_seconds: f64) {
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

/// Process-global fallback для low-level `EventDispatcher::dispatch_into` callers
/// которые не привязали per-client `ServerTimeDelta` source. Рекомендуемый
/// active path auto-link'ает `EventDispatcher` к `Client::server_time_delta_handle`
/// через `Client::run_with_dispatcher` и **не использует** это global значение.
///
/// DEVIATION #23 закрыт: multi-Client больше не страдает от перезаписи —
/// каждый Client имеет свой `Arc<AtomicU64>` handle.
static SERVER_TIME_DELTA_DAYS: AtomicU64 = AtomicU64::new(0);

/// Установить fallback server_time_delta (в днях, как TDateTime).
/// Вызывается из обработки `MPC_Ping`; потребитель НЕ должен
/// вызывать напрямую — используй `client.server_time_delta_handle()` для multi-Client.
pub(crate) fn set_server_time_delta_global(delta_days: f64) {
    SERVER_TIME_DELTA_DAYS.store(delta_days.to_bits(), Ordering::Relaxed);
}

/// Получить fallback server_time_delta (дни). Используется `EventDispatcher` когда
/// per-Client source не привязан.
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

/// Delphi TDateTime corrected by NTP offset.
/// Matches: `Now - GlobalMPTimeZoneOffset + GlobalMPTimeOffset`.
/// We use UTC directly (no timezone offset needed — TDateTime in MoonProto = UTC).
pub(crate) fn delphi_now() -> f64 {
    delphi_now_raw() + get_ntp_offset_days()
}
