use crate::commands::strategy_serializer::strategy_last_date_to_moon_time;
use crate::MoonTime;

/// Cached serialized strategy payload used when the core asks this client for
/// its current local strategy list.
#[derive(Debug, Clone)]
pub(crate) struct StrategySnapshotPayloadCache {
    pub client_max_last_date: u64,
    pub data: Vec<u8>,
}

/// Lightweight strategy row kept by the active client.
#[derive(Debug, Clone)]
pub struct StrategyInfo {
    /// Server strategy identifier. `0` is not a valid live strategy id.
    pub strategy_id: u64,
    /// Strategy version from the serialized strategy header.
    pub strategy_ver: i32,
    /// Unix epoch milliseconds used by strategy edit/rollback guards.
    ///
    /// UI code should use [`Self::last_edit_time`] for labels.
    pub last_date: u64,
    /// Sell price copied from decoded snapshot field `SellPrice`, when present.
    pub sell_price: f64,
    /// Current checked-state used by strategy start/stop UI.
    pub checked: bool,
    /// Last server-acknowledged checked-state.
    pub prev_checked: bool,
    /// Folder path in the strategy tree.
    ///
    /// `Arc<str>` shared with the decoded snapshot `path` — refcount bump on
    /// apply instead of a per-strategy heap copy.
    pub folder_path: std::sync::Arc<str>,
}

impl StrategyInfo {
    pub(super) fn new(strategy_id: u64) -> Self {
        Self {
            strategy_id,
            strategy_ver: 0,
            last_date: 0,
            sell_price: 0.0,
            checked: false,
            prev_checked: false,
            folder_path: std::sync::Arc::from(""),
        }
    }

    /// Last edit timestamp as the normal public MoonProto time type.
    pub fn last_edit_time(&self) -> MoonTime {
        strategy_last_date_to_moon_time(self.last_date)
    }
}

#[derive(Debug, Clone)]
pub enum StratEvent {
    /// Full snapshot (`Full=true`) was decoded and applied.
    SnapshotFull {
        server_epoch: u64,
        /// Compressed snapshot payload length. The raw bytes are kept only in
        /// diagnostics builds for FireTest/custom decoder dumps.
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        raw_len: usize,
        #[cfg(feature = "diagnostics")]
        #[doc(hidden)]
        raw_data: Vec<u8>,
    },
    /// Partial snapshot (`Full=false`) was decoded and applied.
    SnapshotPartial {
        server_epoch: u64,
        /// Compressed snapshot payload length. The raw bytes are kept only in
        /// diagnostics builds for FireTest/custom decoder dumps.
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        raw_len: usize,
        #[cfg(feature = "diagnostics")]
        #[doc(hidden)]
        raw_data: Vec<u8>,
    },
    /// Result of a strategy/folder delete command.
    ///
    /// The core can request both a strategy-id delete and a folder-path delete
    /// in one command. The event is emitted only when at least one part changed
    /// state.
    Deleted {
        strategy_id: u64,
        folder_path: String,
        strategy_deleted: bool,
        folder_deleted: bool,
    },
    /// Checked flags were synchronized, either by full replace or by delta.
    CheckedSynced { changed: usize, is_delta: bool },
    /// Server echo for a checked-state sync sent by this client.
    CheckedEcho { count: usize },
    /// Strategy schema was received and parsed.
    SchemaApplied {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        raw_len: usize,
        format_version: u8,
        kind_count: usize,
        field_count: usize,
    },
    /// Server sent a strategy schema, but the compressed body parse failed.
    SchemaParseFailed {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        raw_len: usize,
    },
    /// Server reported whether the global strategy engine is currently running.
    RuntimeState { strategies_running: bool },
}

impl StratEvent {
    /// Server epoch for full/partial strategy snapshots.
    pub fn snapshot_server_epoch(&self) -> Option<u64> {
        match self {
            StratEvent::SnapshotFull { server_epoch, .. }
            | StratEvent::SnapshotPartial { server_epoch, .. } => Some(*server_epoch),
            _ => None,
        }
    }

    /// Raw snapshot payload length for diagnostics without touching the bytes.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn snapshot_raw_len(&self) -> Option<usize> {
        match self {
            StratEvent::SnapshotFull { raw_len, .. }
            | StratEvent::SnapshotPartial { raw_len, .. } => Some(*raw_len),
            StratEvent::SchemaApplied { raw_len, .. }
            | StratEvent::SchemaParseFailed { raw_len } => Some(*raw_len),
            _ => None,
        }
    }
}
