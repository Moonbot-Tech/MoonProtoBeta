//! Public event/read-model types.

use super::*;
use crate::commands::engine_api::EngineMethod;
use crate::commands::market::PositionType;
use crate::state::{AccountEvent, ExchangeKind};
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::time::MoonTime;

#[doc(hidden)]
/// Fresh strategy snapshot override returned by internal runtime tooling for a
/// server `TStratSnapshotRequest`.
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
    /// Build a reply from an already serialized `TStrategySerializer` payload.
    ///
    /// Empty `data` is treated as an empty strategy list and normalized to a
    /// valid non-empty serializer payload. This matches Delphi
    /// `TStratSnapshot.CreateFromStrats([])` and prevents a normal provider from
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

/// Follow-up `TOrderStatusRequest` target produced after a `TAllStatuses`
/// snapshot did not mention a locally tracked Delphi `WCache` worker.
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

/// One watcher fill after Delphi `ProcessTradesStream` time-shift application.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillEvent {
    /// Delphi `Round(TDateTime * MSecsPerDay)` timestamp used by `TWSFill.Time`.
    ///
    /// This is diagnostics-only because it is not Unix milliseconds. Terminal UI
    /// should use [`Self::time`] or [`Self::unix_millis`].
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub time_ms: i64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) time_ms: i64,
    /// Shifted Delphi `TDateTime` value for consumers that work in days.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub time: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) time: f64,
    pub price: f32,
    pub qty: f32,
    pub z_btc: f32,
    pub position: f32,
    /// Delphi `TOrderType`; unknown raw bytes are preserved like Delphi enum bytes.
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
    pub user: [u8; 20],
    pub fills: Vec<WatcherFillEvent>,
}

impl WatcherFillsEvent {
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
/// The wire packet stores `time:TDateTime + UTF-8 text`, but terminal code
/// should treat this as a typed log event and convert time through helpers
/// instead of carrying raw Delphi day values around the UI.
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

/// User-facing asynchronous Engine API action kind.
///
/// Delphi low-level `TMoonProtoEngine` often implements these commands through
/// `SendAndWait`, but UI code wraps them in `TThread.CreateAnonymousThread`.
/// Active Lib exposes that same user effect as non-blocking intents.
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

/// Arbitrage relay was applied to retained market state.
///
/// Delphi applies compact arb payloads directly to `TMarket.ArbSlots` /
/// `TMarket.ArbNow`. Active Lib follows that model: this event is a signal for
/// UI code to refresh selected market handles, not a raw packet surface.
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
    /// Typed HyperDex watcher fills. Delphi decodes these inside
    /// `ProcessTradesStream` and calls `ProcessWatcherFillsDetect`; Active Lib
    /// exposes the same domain data instead of dropping the section as opaque
    /// bytes.
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
    /// Initial full 5m candles snapshot for retained Active Lib history.
    ///
    /// This is emitted only after the history worker acknowledges that the
    /// snapshot command has been processed, so readers already see the candles.
    CandlesSnapshot(crate::state::CandlesSnapshotEvent),
    /// Completion of a non-blocking user-facing Engine API action.
    EngineAction(EngineActionEvent),
    /// Compact arbitrage relay applied to retained market state.
    Arb(ArbEvent),
    /// Strat channel: snapshot/delete/sell-price update.
    Strat(StratEvent),
    /// UI channel receive branch: settings snapshot, leverage snapshot, remote
    /// update request, or arbitrage activation notification.
    Settings(SettingsEvent),
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
    /// Authenticated server-side log line (`MPC_LogMsg`).
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
