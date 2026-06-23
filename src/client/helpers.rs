//! Small pure helpers shared by client submodules.

use std::time::{Duration, Instant};

use crate::commands::engine_api::EngineMethod;
use crate::protocol::Command;

#[inline]
pub(super) fn is_domain_push_command(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::Order
            | Command::Strat
            | Command::Balance
            | Command::TradesStream
            | Command::TradesResendResponse
            | Command::OrderBook
            | Command::UI
    )
}

#[inline]
pub(super) fn is_trades_stream_command(cmd: Command) -> bool {
    matches!(cmd, Command::TradesStream | Command::TradesResendResponse)
}

#[inline]
pub(super) fn is_datagram_too_large_error(e: &std::io::Error) -> bool {
    match e.raw_os_error() {
        Some(90) => true,    // Linux EMSGSIZE
        Some(10040) => true, // Windows WSAEMSGSIZE
        Some(40)
            if cfg!(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
            )) =>
        {
            true
        }
        _ => false,
    }
}

#[inline]
pub(super) fn is_pmtu_probe_ack_command(cmd: u8) -> bool {
    matches!(
        Command::from_byte(cmd),
        Command::SizeAck | Command::ProbeMTUAck
    )
}

#[inline]
pub(super) fn engine_request_uid(request_payload: &[u8]) -> Option<u64> {
    request_payload
        .get(3..11)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
}

#[inline]
pub(super) fn engine_request_method(request_payload: &[u8]) -> Option<EngineMethod> {
    request_payload
        .get(11)
        .copied()
        .map(EngineMethod::from_byte)
}

#[inline]
pub(super) fn engine_method_allowed_before_domain_ready(method: EngineMethod) -> bool {
    matches!(
        method,
        EngineMethod::BaseCheck
            | EngineMethod::AuthCheck
            | EngineMethod::GetMarketsList
            | EngineMethod::UpdateMarketsList
    )
}

#[inline]
pub(super) fn outgoing_allowed_before_domain_ready(cmd: u8, data: &[u8]) -> bool {
    matches!(
        Command::from_byte(cmd),
        Command::API
            if engine_request_method(data)
                .is_some_and(engine_method_allowed_before_domain_ready)
    ) || matches!(
        Command::from_byte(cmd),
        Command::Strat
            if crate::commands::strat::is_schema_request_payload(data)
    )
}

#[inline]
pub(super) fn incoming_allowed_before_domain_ready(cmd: Command, data: &[u8]) -> bool {
    matches!(
        cmd,
        Command::Strat
            if crate::commands::strat::is_schema_payload(data)
                || crate::commands::strat::is_snapshot_request_payload(data)
                || crate::commands::strat::is_runtime_state_payload(data)
    ) || matches!(
        cmd,
        Command::UI
            if crate::commands::ui::is_runtime_state_payload(data)
                || crate::commands::ui::is_kernel_license_state_payload(data)
    )
}

#[inline]
pub(super) fn timeout_remaining(start: Instant, timeout: Duration) -> Option<Duration> {
    let elapsed = start.elapsed();
    if elapsed >= timeout {
        None
    } else {
        Some(timeout.saturating_sub(elapsed))
    }
}

#[inline]
#[cfg(test)]
pub(super) fn queued_client_settings_updated_since(
    dispatcher: &crate::events::EventDispatcher,
    first_new_event: usize,
) -> bool {
    dispatcher
        .queued_events()
        .get(first_new_event..)
        .unwrap_or(&[])
        .iter()
        .any(|event| {
            matches!(
                event,
                crate::events::Event::Settings(crate::state::SettingsEvent::ClientSettingsUpdated)
            )
        })
}
