//! Periodic refresh timers, the Delphi `ServerUpdateSent` marker, and the
//! in-flight Engine API / chunked-candles response collectors.

use crate::api_pending::ApiPending;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use super::candles::CandlesParseQueue;
use super::config::CHECK_TAGS_BURST_COUNT;
use super::PartialCandles;

/// Periodic refresh clocks carved out of [`super::Client`].
///
/// Groups the F6/F7 periodic-refresh timers driven from the main loop
/// (`last_update_markets_ms`, `last_check_tags_ms`, `check_tags_hour_slot`,
/// `check_tags_burst_sent`, `last_check_tags_burst_ms`) and the Delphi
/// `ServerUpdateSent` marker (`server_update_sent`), which the UI/restart path
/// sets and BaseCheck init consumes. Field names, types, and meaning are
/// unchanged from when they lived directly on `Client`.
///
/// `server_update_sent` is an `Arc<AtomicBool>` cloned into `ClientSender` via
/// `Arc::clone`; the timers are owned outright.
pub(crate) struct RefreshClocks {
    /// F6/F7: timestamps of the last periodic refresh commands. `i64::MIN/2` =
    /// "never" -> the first tick fires immediately after Connected (if the matching
    /// interval is set in `cfg.refresh`). After that — every
    /// `update_markets_every` / `check_tags_every`.
    pub(crate) last_update_markets_ms: i64,
    pub(crate) last_check_tags_ms: i64,
    /// Delphi `BHeavyApiWorker` issues up to 4 quick `CheckBinanceTags` after
    /// the hour changes. These fields hold the current wall-clock hour slot and burst progress.
    pub(crate) check_tags_hour_slot: i64,
    pub(crate) check_tags_burst_sent: u8,
    pub(crate) last_check_tags_burst_ms: i64,

    /// Delphi `cfg.MoonProtoConfig.ServerUpdateSent`: set by UI commands that
    /// can make the server restart/change routing; consumed by BaseCheck init.
    pub(crate) server_update_sent: Arc<AtomicBool>,
}

impl RefreshClocks {
    pub(crate) fn new(server_update_sent: Arc<AtomicBool>) -> Self {
        Self {
            last_update_markets_ms: i64::MIN / 2,
            last_check_tags_ms: i64::MIN / 2,
            check_tags_hour_slot: i64::MIN,
            check_tags_burst_sent: CHECK_TAGS_BURST_COUNT,
            last_check_tags_burst_ms: i64::MIN / 2,
            server_update_sent,
        }
    }
}

/// In-flight Engine API response collectors carved out of [`super::Client`].
///
/// Groups the pending-Engine-API request registry (`api_pending`) and the
/// internal chunked full-candles snapshot collectors keyed by `request_uid`
/// (`pending_candles`). Both are about responses still in flight; neither is a
/// refresh clock, so they live in their own struct. Field names, types, and
/// meaning are unchanged from when they lived directly on `Client`.
///
/// `api_pending` is an `Arc<ApiPending>` cloned into the runtime loop via
/// `Arc::clone`; `pending_candles` is owned outright.
pub(crate) struct PendingApi {
    /// Registry of pending Engine API requests.
    /// On receiving a `Command::API` packet, `dispatch` delivers the response
    /// to the registered receiver if the UID is found.
    pub(crate) api_pending: Arc<ApiPending>,

    /// Internal full-candles snapshot collectors by `request_uid`. Filled by
    /// the automatic Active Lib snapshot request and cleared when the
    /// aggregator completes or times out.
    ///
    /// Application code does not see this packet-shaped layer; it gets retained
    /// candles through snapshots/events.
    pub(crate) pending_candles: HashMap<u64, PartialCandles>,

    /// Persistent parser worker for completed full-candles snapshots.
    ///
    /// The protocol reader must never spawn a thread on the final Sliced block:
    /// thread creation showed up as a 20ms+ reader stall on Linux/VPS. Reader
    /// path only queues a completed zipped stream here; zlib parse/apply stays
    /// outside UDP receive.
    pub(crate) candles_parse: CandlesParseQueue,
}

impl PendingApi {
    pub(crate) fn new() -> Self {
        Self {
            api_pending: ApiPending::new_arc(),
            pending_candles: HashMap::new(),
            candles_parse: CandlesParseQueue::new(),
        }
    }
}
