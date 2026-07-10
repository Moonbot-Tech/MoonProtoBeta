//! Public event/read-model types.

use super::*;
use crate::commands::engine_api::EngineMethod;
use crate::commands::market::PositionType;
use crate::commands::strat::{
    DetectSignalCommand, DETECT_KIND_ALERT, DETECT_KIND_CHART_ONLY, DETECT_KIND_ROW,
};
use crate::state::{AccountEvent, ExchangeKind};
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::time::MoonTime;

#[doc(hidden)]
/// Fresh strategy snapshot override returned by internal runtime tooling for a
/// server strategy-snapshot request.
///
/// Normal active-library flow: the application gives strategies to
/// [`crate::InitialStrategies`] before init, and the runtime uses its owned
/// `StratsState` for the post-init snapshot and request replies.
/// This provider is an internal test/runtime hook, not terminal API.
pub(crate) struct StrategySnapshotReply {
    pub server_epoch: u64,
    pub client_max_last_date: u64,
    pub full: bool,
    pub data: Vec<u8>,
}

impl StrategySnapshotReply {
    /// Build a reply from an already serialized strategy-list payload.
    ///
    /// Empty `data` is treated as an empty strategy list and normalized to a
    /// valid non-empty serializer payload. This prevents a normal provider from
    /// sending malformed `Size=0` snapshot data.
    pub(crate) fn from_payload(
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: Vec<u8>,
    ) -> Self {
        let data = if data.is_empty() {
            crate::commands::strategy_serializer::StrategyBatchBuilder::empty_payload()
        } else {
            data
        };
        Self {
            server_epoch,
            client_max_last_date,
            full,
            data,
        }
    }
}

/// Follow-up order-status request target produced after a full order-status
/// snapshot did not mention a locally tracked worker.
///
/// Active `MoonClient` sends these automatically after applying the snapshot.
/// The type is kept crate-private because terminal code should see the updated
/// order state/events, not build follow-up protocol requests manually.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MissingOrderStatusRequest {
    pub ctx: TradeCtx,
    pub market_name: String,
}

/// One watcher fill after trades-stream time-shift application.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillEvent {
    /// Protocol-native shifted timestamp in milliseconds.
    ///
    /// This is diagnostics-only because it is not Unix milliseconds. Terminal UI
    /// should use [`Self::time`] or [`Self::unix_millis`].
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub time_ms: i64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) time_ms: i64,
    /// Shifted protocol-native day value for diagnostics.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub time: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) time: f64,
    pub price: f32,
    pub qty: f32,
    pub z_btc: f32,
    pub position: f32,
    /// Order type byte; unknown raw bytes are preserved for forward compatibility.
    pub order_type: OrderType,
    pub is_short: bool,
    pub is_open: bool,
    pub is_taker: bool,
}

impl WatcherFillEvent {
    #[inline]
    pub fn time(&self) -> MoonTime {
        MoonTime::from_delphi_days(self.time).unwrap_or(MoonTime::ZERO)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn time_delphi(&self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(&self) -> i64 {
        self.time().unix_millis()
    }
}

/// Typed watcher fills from one `TradesStream` WatcherFills section.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillsEvent {
    pub(crate) market_index: u16,
    pub market_name: Arc<str>,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub user: [u8; 20],
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) user: [u8; 20],
    pub fills: Vec<WatcherFillEvent>,
}

impl WatcherFillsEvent {
    /// HyperDex user address bytes.
    pub fn user(&self) -> &[u8; 20] {
        &self.user
    }

    /// HyperDex user address formatted as lowercase `0x...` hex.
    pub fn user_hex(&self) -> String {
        crate::state::hl_address_hex(&self.user)
    }

    /// Server-local market index retained for protocol diagnostics.
    ///
    /// Normal UI code should use [`Self::market_name`] and market handles from
    /// the snapshot/read model.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn market_index(&self) -> u16 {
        self.market_index
    }
}

/// Server-side log line mirrored by MoonProto.
///
/// The transport stores protocol-native time plus UTF-8 text, but terminal code
/// should treat this as a typed log event and convert time through helpers
/// instead of carrying raw wire-day values around the UI.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerLogEvent {
    time: f64,
    pub msg: String,
}

impl ServerLogEvent {
    pub(crate) fn new(time: f64, msg: String) -> Self {
        Self { time, msg }
    }

    #[inline]
    pub fn time(&self) -> MoonTime {
        MoonTime::from_delphi_days(self.time).unwrap_or(MoonTime::ZERO)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn time_delphi(&self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(&self) -> i64 {
        self.time().unix_millis()
    }
}

/// One watcher row fact relayed by the MoonProto core.
#[derive(Debug, Clone, PartialEq)]
pub struct DetectWatcherRow {
    pub pos_val: f64,
    pub val: f64,
    pub row_flags: u8,
}

impl DetectWatcherRow {
    pub fn is_open(&self) -> bool {
        (self.row_flags & 0x01) != 0
    }

    pub fn is_taker(&self) -> bool {
        (self.row_flags & 0x02) != 0
    }
}

/// Server-side detect/UI fact.
///
/// The core already performed detect/watcher/alert logic. Rust terminal code
/// should display this fact using local UI settings and retained strategy/market
/// state; it must not recompute the detect itself.
#[derive(Debug, Clone, PartialEq)]
pub struct DetectEvent {
    pub market_name: String,
    pub strategy_id: u64,
    pub is_short: bool,
    pub kind_bits: u8,
    pub msg: String,
    pub watcher_row: Option<DetectWatcherRow>,
    pub alert_obj_uid: Option<u64>,
}

impl DetectEvent {
    pub(crate) fn from_command(cmd: DetectSignalCommand) -> Self {
        let watcher_row = cmd.has_row().then_some(DetectWatcherRow {
            pos_val: cmd.pos_val,
            val: cmd.val,
            row_flags: cmd.row_flags,
        });
        let alert_obj_uid = cmd.has_alert().then_some(cmd.obj_uid);
        Self {
            market_name: cmd.market_name,
            strategy_id: cmd.strategy_id,
            is_short: cmd.is_short,
            kind_bits: cmd.kind,
            msg: cmd.msg,
            watcher_row,
            alert_obj_uid,
        }
    }

    pub fn is_regular_detect(&self) -> bool {
        self.kind_bits == 0
    }

    pub fn has_watcher_row(&self) -> bool {
        (self.kind_bits & DETECT_KIND_ROW) != 0
    }

    pub fn is_chart_only(&self) -> bool {
        (self.kind_bits & DETECT_KIND_CHART_ONLY) != 0
    }

    pub fn is_alert_fire(&self) -> bool {
        (self.kind_bits & DETECT_KIND_ALERT) != 0
    }
}

/// User-facing asynchronous Engine API action kind.
///
/// These are non-blocking runtime intents. UI code queues the action and then
/// observes retained state or completion events instead of waiting on a raw
/// request/response slot.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineActionKind {
    CancelAllOrders,
    SetLeverage {
        market: String,
        new_leverage: i32,
    },
    SetHedgeMode {
        hedge_mode: bool,
    },
    ChangePositionType {
        market: String,
        position_type: PositionType,
    },
    ConvertDustBnb,
    ConfirmRiskLimit {
        market: String,
    },
    SetMaMode {
        ma_mode: bool,
    },
    TransferAsset {
        asset: String,
        qty: f64,
        from: ExchangeKind,
        to: ExchangeKind,
    },
    ReloadOrderBook,
}

/// Completion of an asynchronous user-facing Engine API action.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineActionEvent {
    pub kind: EngineActionKind,
    #[doc(hidden)]
    pub(crate) request_uid: Option<u64>,
    pub(crate) method: EngineMethod,
    pub success: bool,
    pub error_code: i32,
    pub error_msg: String,
}

impl EngineActionEvent {
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn request_uid(&self) -> Option<u64> {
        self.request_uid
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn method(&self) -> EngineMethod {
        self.method
    }
}

/// Exact SQL report for a closed sell order written by the MoonBot core.
///
/// The core sends the same expanded SQL text it uses for its Orders database
/// writer. Active Lib does not parse it into a second order model; clients that
/// need external DB/report sync receive the canonical SQL text and the MoonBot
/// Orders DB row id. Use `db_id` as the stable mirror key: later SQL for price
/// changes, partial fills, or final execution updates the same DB record.
///
/// This is a legacy compatibility path. New report databases should use
/// [`crate::MoonClient::reports`] and [`Event::Report`] for typed, resumable
/// replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedSellOrderReportEvent {
    /// MoonBot Orders database row id. This is not the order worker UID.
    pub db_id: i64,
    /// Expanded SQL for the MoonBot Orders database row.
    pub sql: String,
}

/// Live TF candle update pushed by the core for a subscribed market.
///
/// `applied_to_history` is true only when Active Lib already had a loaded
/// demand-history ring for the same market/TF and the row passed the core's
/// candle-window checks. The raw pushed candle is always included so UI code
/// can still react without guessing why retained history did not move.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveCandleEvent {
    pub market_name: String,
    pub kind: crate::commands::candles::DeepHistoryKind,
    pub candle: crate::commands::candles::DeepPrice,
    pub applied_to_history: bool,
    pub history_count: usize,
    pub history_revision: u64,
}

/// Arbitrage relay was applied to retained market state.
///
/// Active Lib applies compact arbitrage payloads directly to retained market
/// slots. This event is a signal for UI code to refresh selected market
/// handles, not a raw packet surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArbEvent {
    PricesApplied {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        uid: u64,
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        version: u8,
        market_blocks: usize,
        price_items: usize,
        applied_prices: usize,
    },
    IsolationApplied {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        uid: u64,
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        version: u8,
        entries: usize,
        applied_entries: usize,
    },
}

/// All typed events emitted by the [`crate::MoonClient`] runtime.
#[derive(Debug)]
pub enum Event {
    /// Order channel event: order creation, update, removal, or snapshot
    /// follow-up.
    Order(OrderEvent),
    /// OrderBook channel: applied updates/low-level cache control events.
    OrderBook(OrderBookEvent),
    /// TradesStream channel event. A packet can produce several
    /// [`TradesEvent`] values, so each sub-event is delivered as a separate
    /// `Event::Trade` instead of a nested vector.
    Trade(TradesEvent),
    /// Typed HyperDex watcher fills from the trades stream. Active Lib exposes
    /// the domain rows directly instead of dropping the section as opaque bytes.
    WatcherFills(WatcherFillsEvent),
    /// Balance read-model event: full snapshots and incremental updates.
    /// Internal/base/request balance packets are consumed without a public event.
    Balance(BalanceEvent),
    /// Account-level async refresh state: hedge mode, API-key expiration, and
    /// similar scalar account metadata.
    Account(AccountEvent),
    /// Transferable wallet assets refreshed through Engine API.
    TransferAssets(TransferAssetsEvent),
    /// Demand-driven CoinCard candles for one market/history kind.
    CoinCardCandles(crate::state::CoinCardCandlesEvent),
    /// Live TF candle update for a subscribed market.
    LiveCandle(LiveCandleEvent),
    /// Initial full 5m candles snapshot for retained Active Lib history.
    ///
    /// This is emitted only after the history worker acknowledges that the
    /// snapshot command has been processed, so readers already see the candles.
    CandlesSnapshot(crate::state::CandlesSnapshotEvent),
    /// Completion of a non-blocking user-facing Engine API action.
    EngineAction(EngineActionEvent),
    /// Legacy closed-sell SQL compatibility stream.
    ///
    /// New report databases should use [`Event::Report`].
    ClosedSellOrderReport(ClosedSellOrderReportEvent),
    /// Typed report-database replication stream.
    Report(crate::state::ReportEvent),
    /// Compact arbitrage relay applied to retained market state.
    Arb(ArbEvent),
    /// Strat channel: snapshot/delete/sell-price update.
    Strat(StratEvent),
    /// Detect/watcher/chart-alert fact produced by the core.
    Detect(DetectEvent),
    /// UI channel receive branch: settings snapshot, leverage snapshot, remote
    /// update request, or arbitrage activation notification.
    Settings(SettingsEvent),
    /// Authoritative chart-alert object state changed.
    ChartAlert(crate::state::ChartAlertEvent),
    /// Ready chart text rows for one market were replaced by the core.
    ChartText(crate::state::ChartTextSnapshot),
    /// Markets state was updated after an Engine API response.
    Markets(MarketsEvent),
    /// Engine API response that was not consumed by the pending-response
    /// registry.
    ///
    /// Normal applications use typed domain events and `EngineAction`. This raw
    /// response is kept for diagnostics/custom tooling only.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    EngineResponse(EngineResponse),
    /// Authenticated server-side log line.
    ServerLog(ServerLogEvent),
    /// Raw payload for channels the dispatcher does not parse.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    Raw { cmd: Command, payload: Vec<u8> },
    /// Payload parsing failed.
    ///
    /// `payload` is cloned only on failure so live diagnostics can dump the
    /// exact bytes that failed to parse without adding work to the normal path.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    ParseFailed {
        cmd: Command,
        len: usize,
        payload: Vec<u8>,
    },
}

impl Event {
    pub fn server_log_time(&self) -> Option<MoonTime> {
        match self {
            Self::ServerLog(log) => Some(log.time()),
            _ => None,
        }
    }
}
