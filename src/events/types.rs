//! Public event/read-model types.

use super::*;
use crate::commands::strategy_schema::StrategySchema;
use crate::commands::strategy_serializer::StrategySnapshot;
use crate::commands::EngineMethod;
use crate::state::{AccountEvent, ExchangeKind};
use crate::time::DelphiTime;

/// Fresh strategy snapshot override returned by the application for a server
/// `TStratSnapshotRequest`.
///
/// Normal active-library flow: the application gives strategies to
/// [`EventDispatcher::set_local_strategies`] before init, and the dispatcher
/// uses its owned `StratsState` for the post-init snapshot and request replies.
/// This provider is only an advanced escape hatch for callers that need to
/// rebuild payload bytes themselves.
pub struct StrategySnapshotReply {
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
    pub fn from_payload(
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

    /// Build a reply from decoded strategy snapshots.
    ///
    /// This is the provider-side counterpart of Delphi
    /// `TStratSnapshot.CreateFromStrats`: it serializes the current application
    /// strategy list, computes `ClientMaxLastDate`, and marks the packet as a
    /// full snapshot by default. Pass the live `TStratSchema` fetched during
    /// Init; Rust does not carry a static Delphi field/default table.
    pub fn from_strategies(
        server_epoch: u64,
        full: bool,
        schema: &StrategySchema,
        strategies: &[StrategySnapshot],
    ) -> Self {
        let mut builder = crate::commands::strategy_serializer::StrategyBatchBuilder::new(schema);
        let mut client_max_last_date = 0u64;
        for strategy in strategies {
            client_max_last_date = client_max_last_date.max(strategy.last_date);
            builder.write_strategy(strategy);
        }
        Self {
            server_epoch,
            client_max_last_date,
            full,
            data: builder.finalize(),
        }
    }
}

/// Follow-up `TOrderStatusRequest` target produced after a `TAllStatuses`
/// snapshot did not mention a locally tracked Delphi `WCache` worker.
///
/// Active `MoonClient` / custom active runtimes send these automatically. Raw
/// `EventDispatcher::dispatch_into` users can call
/// [`EventDispatcher::missing_order_status_requests_after_snapshot`] after
/// `OrderEvent::Snapshot` and send the returned requests through
/// `Client::request_order_status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingOrderStatusRequest {
    pub ctx: TradeCtx,
    pub market_name: String,
}

/// One watcher fill after Delphi `ProcessTradesStream` time-shift application.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillEvent {
    /// Delphi `Round(TDateTime * MSecsPerDay)` timestamp used by `TWSFill.Time`.
    pub time_ms: i64,
    /// Shifted Delphi `TDateTime` value for consumers that work in days.
    pub time: f64,
    pub price: f32,
    pub qty: f32,
    pub z_btc: f32,
    pub position: f32,
    /// Raw `TOrderType` ordinal. Unknown values are preserved like Delphi enum bytes.
    pub order_type: OrderType,
    pub is_short: bool,
    pub is_open: bool,
    pub is_taker: bool,
}

impl WatcherFillEvent {
    #[inline]
    pub fn time_delphi(&self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(&self) -> Option<i64> {
        self.time_delphi().unix_millis()
    }
}

/// Typed watcher fills from one `TradesStream` WatcherFills section.
#[derive(Debug, Clone, PartialEq)]
pub struct WatcherFillsEvent {
    pub market_index: u16,
    pub market_name: String,
    pub user: [u8; 20],
    pub fills: Vec<WatcherFillEvent>,
}

/// User-facing asynchronous Engine API action kind.
///
/// Delphi low-level `TMoonProtoEngine` often implements these commands through
/// `SendAndWait`, but UI code wraps them in `TThread.CreateAnonymousThread`.
/// Active Lib exposes that same user effect as non-blocking intents.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineActionKind {
    MarketsBalanceFullRefresh,
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
        position_type: u8,
        new_market: bool,
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
    pub request_uid: Option<u64>,
    pub method: EngineMethod,
    pub success: bool,
    pub error_code: i32,
    pub error_msg: String,
}

/// Arbitrage relay was applied to retained market state.
///
/// Delphi applies compact arb payloads directly to `TMarket.ArbSlots` /
/// `TMarket.ArbNow`. Active Lib follows that model: this event is a signal for
/// UI code to refresh selected market handles, not a raw packet surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArbEvent {
    PricesApplied {
        uid: u64,
        version: u8,
        market_blocks: usize,
        price_items: usize,
        applied_prices: usize,
    },
    IsolationApplied {
        uid: u64,
        version: u8,
        entries: usize,
        applied_entries: usize,
    },
}

/// All typed events emitted by [`EventDispatcher`].
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
    /// UI channel: settings updated, MM subscribe changed, etc.
    Settings(SettingsEvent),
    /// Markets state was updated after an Engine API response.
    Markets(MarketsEvent),
    /// Engine API response that was not consumed by the pending-response
    /// registry.
    EngineResponse(EngineResponse),
    /// Server-side log message (`MPC_LogMsg`): `time:TDateTime + msg:UTF-8 rest`.
    ServerLog { time: f64, msg: String },
    /// Raw payload for channels the dispatcher does not parse.
    Raw { cmd: Command, payload: Vec<u8> },
    /// Payload parsing failed.
    ///
    /// `payload` is cloned only on failure so live diagnostics can dump the
    /// exact bytes that failed to parse without adding work to the normal path.
    ParseFailed {
        cmd: Command,
        len: usize,
        payload: Vec<u8>,
    },
}

impl Event {
    pub fn server_log_time(&self) -> Option<DelphiTime> {
        match self {
            Self::ServerLog { time, .. } => Some(DelphiTime::from_days(*time)),
            _ => None,
        }
    }
}
