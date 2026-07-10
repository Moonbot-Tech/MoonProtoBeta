//! Public high-level Active Lib runtime types.

use super::*;
use parking_lot::MutexGuard;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Event emitted by the high-level [`MoonClient`](super::MoonClient) runtime.
///
/// The runtime never calls UI code directly. It pushes these values into a
/// user-provided [`MoonEventSink`]. GUI integrations can post them to their own
/// event loop; immediate-mode UIs can use [`MoonEventQueue`].
#[derive(Debug)]
pub enum MoonClientEvent {
    Domain(crate::events::Event),
    Lifecycle(LifecycleEvent),
}

/// Versioned read-model snapshot published by [`MoonClient`](super::MoonClient).
///
/// The revision increases each time the runtime publishes a new snapshot.
/// Immediate-mode UIs can keep the last seen revision and skip expensive redraw
/// preparation when it has not changed, while still using the cheap `Arc`
/// snapshot for actual reads.
#[derive(Clone, Debug)]
pub struct MoonClientSnapshot {
    revision: u64,
    state: Arc<crate::events::MoonStateSnapshot>,
}

impl MoonClientSnapshot {
    pub(crate) fn new(revision: u64, state: Arc<crate::events::MoonStateSnapshot>) -> Self {
        Self { revision, state }
    }

    /// Monotonic revision local to this `MoonClient` runtime.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Clone the underlying immutable state snapshot.
    pub fn state_arc(&self) -> Arc<crate::events::MoonStateSnapshot> {
        Arc::clone(&self.state)
    }
}

impl std::ops::Deref for MoonClientSnapshot {
    type Target = crate::events::MoonStateSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

/// Non-blocking event delivery bridge for [`MoonClient`](super::MoonClient).
///
/// `emit` on the runtime side only sends into the sink's internal queue; the
/// user callback is executed by a tiny delivery worker. The callback should
/// still be quick so event delivery itself does not build a backlog. For UI
/// frameworks, it should usually enqueue/post the event into the framework event
/// loop rather than render or mutate heavy UI state directly.
#[derive(Clone)]
pub struct MoonEventSink {
    emit: Arc<dyn Fn(MoonClientEvent) + Send + Sync>,
}

impl MoonEventSink {
    /// Build a sink from a framework callback/poster.
    pub fn callback<F>(emit: F) -> Self
    where
        F: Fn(MoonClientEvent) + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let _ = thread::Builder::new()
            .name("moonproto-event-sink".to_string())
            .spawn(move || {
                while let Ok(event) = rx.recv() {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| emit(event))) {
                        log::error!(
                            target: "moonproto::runtime",
                            "moonproto-event-sink callback panicked: {}",
                            panic_payload_message(payload.as_ref())
                        );
                    }
                }
            })
            .expect("spawn moonproto event sink worker");
        Self {
            emit: Arc::new(move |event| {
                let _ = tx.send(event);
            }),
        }
    }

    /// Default queue adapter for immediate-mode UI, CLI, and tests.
    pub fn queue() -> (Self, Arc<MoonEventQueue>) {
        Self::queue_with_waker(|| {})
    }

    /// Queue adapter that also wakes the host UI/event loop after each event.
    pub fn queue_with_waker<F>(wake: F) -> (Self, Arc<MoonEventQueue>)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let (events_tx, events_rx) = mpsc::channel();
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel();
        let wake = Arc::new(wake);
        let sink = Self {
            emit: Arc::new(move |event| {
                match event {
                    MoonClientEvent::Domain(event) => {
                        let _ = events_tx.send(event);
                    }
                    MoonClientEvent::Lifecycle(event) => {
                        let _ = lifecycle_tx.send(event);
                    }
                }
                wake();
            }),
        };
        (
            sink,
            Arc::new(MoonEventQueue {
                events_rx: Mutex::new(events_rx),
                lifecycle_rx: Mutex::new(lifecycle_rx),
            }),
        )
    }

    pub(crate) fn emit(&self, event: MoonClientEvent) {
        (self.emit)(event);
    }

    pub(crate) fn emit_domain(&self, event: crate::events::Event) {
        self.emit(MoonClientEvent::Domain(event));
    }

    pub(crate) fn emit_lifecycle(&self, event: LifecycleEvent) {
        self.emit(MoonClientEvent::Lifecycle(event));
    }
}

#[cfg(test)]
mod event_sink_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn callback_sink_does_not_run_user_callback_inline() {
        let (started_tx, started_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let sink = MoonEventSink::callback(move |_event| {
            let _ = started_tx.send(());
            thread::sleep(Duration::from_millis(200));
            let _ = finish_tx.send(());
        });

        let started = Instant::now();
        sink.emit_lifecycle(LifecycleEvent::Connecting);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "runtime-side sink emit must not wait for user callback work"
        );

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("callback worker should receive event");
        finish_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("callback worker should finish callback");
    }

    #[test]
    fn callback_sink_survives_user_callback_panic() {
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let (ok_tx, ok_rx) = mpsc::channel();
        let sink = MoonEventSink::callback(move |_event| {
            let call = callback_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                panic!("synthetic callback panic");
            }
            let _ = ok_tx.send(call + 1);
        });

        sink.emit_lifecycle(LifecycleEvent::Connecting);
        sink.emit_lifecycle(LifecycleEvent::Reconnecting);

        let delivered = ok_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("callback worker should continue after one callback panic");
        assert_eq!(delivered, 2);
    }
}

/// Queue adapter returned by [`MoonEventSink::queue`].
///
/// This is the natural adapter for egui/eframe-style immediate-mode apps:
/// runtime pushes events, wakes the UI if configured, and the UI drains pending
/// events during its normal update pass.
pub struct MoonEventQueue {
    events_rx: Mutex<mpsc::Receiver<crate::events::Event>>,
    lifecycle_rx: Mutex<mpsc::Receiver<LifecycleEvent>>,
}

impl MoonEventQueue {
    /// Drain typed domain events into a caller-owned buffer.
    ///
    /// This is the allocation-free hot-path form for UI update loops. The
    /// buffer is not cleared first, so callers can batch several sources if
    /// needed.
    pub fn drain_events_into(&self, out: &mut Vec<crate::events::Event>) {
        let rx = lock_queue_mutex(&self.events_rx, "events");
        while let Ok(event) = rx.try_recv() {
            out.push(event);
        }
    }

    /// Drain typed domain events without blocking.
    pub fn drain_events(&self) -> Vec<crate::events::Event> {
        let mut out = Vec::new();
        self.drain_events_into(&mut out);
        out
    }

    /// Drain lifecycle events into a caller-owned buffer.
    pub fn drain_lifecycle_events_into(&self, out: &mut Vec<LifecycleEvent>) {
        let rx = lock_queue_mutex(&self.lifecycle_rx, "lifecycle");
        while let Ok(event) = rx.try_recv() {
            out.push(event);
        }
    }

    /// Drain lifecycle events without blocking.
    pub fn drain_lifecycle_events(&self) -> Vec<LifecycleEvent> {
        let mut out = Vec::new();
        self.drain_lifecycle_events_into(&mut out);
        out
    }

    /// Try to receive one typed domain event without blocking.
    pub fn try_recv_event(&self) -> Option<crate::events::Event> {
        lock_queue_mutex(&self.events_rx, "events").try_recv().ok()
    }

    /// Try to receive one lifecycle event without blocking.
    pub fn try_recv_lifecycle_event(&self) -> Option<LifecycleEvent> {
        lock_queue_mutex(&self.lifecycle_rx, "lifecycle")
            .try_recv()
            .ok()
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(value) = payload.downcast_ref::<&'static str>() {
        (*value).to_string()
    } else if let Some(value) = payload.downcast_ref::<String>() {
        value.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn lock_queue_mutex<'a, T>(mutex: &'a Mutex<T>, _name: &'static str) -> MutexGuard<'a, T> {
    mutex.lock()
}

/// Ticket returned after Active Lib has queued a non-blocking Engine API action.
///
/// The server result arrives later as [`crate::events::Event::EngineAction`].
/// Retained state is updated through the matching domain events/snapshots.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineActionTicket {
    pub kind: crate::events::EngineActionKind,
    #[doc(hidden)]
    pub(crate) request_uid: Option<u64>,
    #[doc(hidden)]
    pub(crate) method: crate::commands::engine_api::EngineMethod,
}

impl EngineActionTicket {
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn request_uid(&self) -> Option<u64> {
        self.request_uid
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn method(&self) -> crate::commands::engine_api::EngineMethod {
        self.method
    }
}

/// Ticket returned after a demand-driven CoinCard candles request is queued.
///
/// Completion arrives as [`crate::events::Event::CoinCardCandles`]; the candles
/// are then readable from `snapshot().coin_card_candles()`.
#[derive(Debug, Clone, PartialEq)]
pub struct CoinCardCandlesTicket {
    pub market: String,
    pub kind: crate::commands::candles::DeepHistoryKind,
    #[doc(hidden)]
    pub(crate) request_uid: Option<u64>,
}

impl CoinCardCandlesTicket {
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn request_uid(&self) -> Option<u64> {
        self.request_uid
    }
}

/// User-facing VStop settings for one tracked order.
///
/// The runtime derives the current order status and route from live `Orders`
/// before sending. This type only describes the UI intent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VStopParams {
    pub(crate) enabled: bool,
    pub(crate) fixed: bool,
    pub(crate) level: f64,
    pub(crate) volume: f64,
}

impl VStopParams {
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            fixed: false,
            level: 0.0,
            volume: 0.0,
        }
    }

    /// Enabled VStop with percentage-style level semantics.
    pub const fn percent(level: f64, volume: f64) -> Self {
        Self {
            enabled: true,
            fixed: false,
            level,
            volume,
        }
    }

    /// Enabled VStop with fixed-price level semantics.
    pub const fn fixed(level: f64, volume: f64) -> Self {
        Self {
            enabled: true,
            fixed: true,
            level,
            volume,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClosePositionParams, SplitOrderParams, VStopParams};

    #[test]
    fn vstop_params_semantic_constructors_set_delphi_flags() {
        assert_eq!(
            VStopParams::disabled(),
            VStopParams {
                enabled: false,
                fixed: false,
                level: 0.0,
                volume: 0.0,
            }
        );
        assert_eq!(
            VStopParams::percent(12.5, 3.0),
            VStopParams {
                enabled: true,
                fixed: false,
                level: 12.5,
                volume: 3.0,
            }
        );
        assert_eq!(
            VStopParams::fixed(50_000.0, 8.0),
            VStopParams {
                enabled: true,
                fixed: true,
                level: 50_000.0,
                volume: 8.0,
            }
        );
    }

    #[test]
    fn split_order_params_hide_wire_flags_behind_intents() {
        let normal = SplitOrderParams::new("BTCUSDT", 3);
        assert_eq!(normal.market, "BTCUSDT");
        assert_eq!(normal.parts, 3);
        assert!(!normal.is_strategy_piece());
        assert!(!normal.sells_strategy_piece());

        let piece = SplitOrderParams::strategy_piece("ETHUSDT", 2);
        assert_eq!(piece.market, "ETHUSDT");
        assert_eq!(piece.parts, 2);
        assert!(piece.is_strategy_piece());
        assert!(!piece.sells_strategy_piece());

        let piece_sell = SplitOrderParams::strategy_piece_and_sell("SOLUSDT", 2);
        assert_eq!(piece_sell.market, "SOLUSDT");
        assert_eq!(piece_sell.parts, 2);
        assert!(piece_sell.is_strategy_piece());
        assert!(piece_sell.sells_strategy_piece());
    }

    #[test]
    fn close_position_default_matches_delphi_limit_close() {
        let limit = ClosePositionParams::new("BTCUSDT");
        assert_eq!(limit.market, "BTCUSDT");
        assert!(!limit.uses_market_order());

        let explicit_limit = ClosePositionParams::limit_orders("ETHUSDT");
        assert_eq!(explicit_limit.market, "ETHUSDT");
        assert!(!explicit_limit.uses_market_order());

        let market = ClosePositionParams::market_order("SOLUSDT");
        assert_eq!(market.market, "SOLUSDT");
        assert!(market.uses_market_order());
    }
}

/// Error returned by the high-level [`MoonClient`](super::MoonClient) runtime API.
#[derive(Debug)]
pub enum MoonClientError {
    /// Connect/init failed before the runtime became usable.
    ///
    /// Carries the typed [`ConnectError`], including background non-blocking
    /// startup failures surfaced through the ready channel.
    Connect(ConnectError),
    /// A one-shot runtime request timed out.
    RequestTimeout,
    /// A one-shot runtime request channel was closed.
    RequestDisconnected,
    /// Session route fields required by market-level trade actions are missing.
    TradeContext(TradeContextError),
    /// Retained state required to build a high-level command is not ready yet.
    StateUnavailable(&'static str),
    /// A user-facing market name could not be resolved to the active market map.
    UnknownMarket(String),
    /// A UI emulator command cannot fit the wire `Word Count` field.
    TooManyEmuTradePoints(usize),
    /// Report replication request has a negative cursor or an invalid depth.
    InvalidReportSyncRequest,
    /// The runtime thread stopped, panicked, or its command channel is closed.
    RuntimeStopped,
}

impl std::fmt::Display for MoonClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(err) => write!(f, "{err}"),
            Self::RequestTimeout => write!(f, "MoonProto request timed out"),
            Self::RequestDisconnected => write!(f, "MoonProto request channel disconnected"),
            Self::TradeContext(err) => write!(f, "{err}"),
            Self::StateUnavailable(reason) => write!(f, "MoonProto state is unavailable: {reason}"),
            Self::UnknownMarket(market) => write!(f, "MoonProto market is unknown: {market}"),
            Self::TooManyEmuTradePoints(count) => {
                write!(
                    f,
                    "MoonProto emulated trade command has too many points: {count}"
                )
            }
            Self::InvalidReportSyncRequest => write!(
                f,
                "MoonProto report sync request has an invalid cursor or history depth"
            ),
            Self::RuntimeStopped => write!(f, "MoonProto runtime is stopped"),
        }
    }
}

impl std::error::Error for MoonClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(err) => Some(err),
            Self::TradeContext(err) => Some(err),
            Self::RequestTimeout
            | Self::RequestDisconnected
            | Self::StateUnavailable(_)
            | Self::UnknownMarket(_)
            | Self::TooManyEmuTradePoints(_)
            | Self::InvalidReportSyncRequest => None,
            Self::RuntimeStopped => None,
        }
    }
}

impl From<ConnectError> for MoonClientError {
    fn from(err: ConnectError) -> Self {
        Self::Connect(err)
    }
}

impl From<mpsc::RecvTimeoutError> for MoonClientError {
    fn from(err: mpsc::RecvTimeoutError) -> Self {
        match err {
            mpsc::RecvTimeoutError::Timeout => Self::RequestTimeout,
            mpsc::RecvTimeoutError::Disconnected => Self::RequestDisconnected,
        }
    }
}

impl From<TradeContextError> for MoonClientError {
    fn from(err: TradeContextError) -> Self {
        Self::TradeContext(err)
    }
}

/// User-facing trades stream content selection.
///
/// Low-level wire helpers still use the historical boolean because that is the
/// packet field. `MoonClient` uses this enum so application code does not have
/// to remember what `true` means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradesStreamMode {
    TradesOnly,
    TradesAndMarketMakers,
}

impl TradesStreamMode {
    pub const fn want_market_makers(self) -> bool {
        matches!(self, Self::TradesAndMarketMakers)
    }
}

/// Long/short side for user-facing market trade intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Long,
    Short,
}

impl OrderSide {
    pub const fn is_short(self) -> bool {
        matches!(self, Self::Short)
    }
}

/// User-facing parameters for opening a new order.
#[derive(Debug, Clone)]
pub struct NewOrderParams {
    pub market: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    /// `None` sends `StratID=0`.
    pub strategy_id: Option<u64>,
}

impl NewOrderParams {
    pub fn new(market: impl Into<String>, side: OrderSide, price: f64, size: f64) -> Self {
        Self {
            market: market.into(),
            side,
            price,
            size,
            strategy_id: None,
        }
    }

    /// Build new-order params for a retained selected market.
    ///
    /// Terminal UI normally keeps `MarketHandle` after search/chart selection,
    /// so this constructor avoids turning that selected object back into a
    /// user-visible lookup problem.
    pub fn for_market(
        market: &crate::state::MarketHandle,
        side: OrderSide,
        price: f64,
        size: f64,
    ) -> Self {
        Self::new(market.name(), side, price, size)
    }

    pub fn with_strategy_id(mut self, strategy_id: u64) -> Self {
        self.strategy_id = Some(strategy_id);
        self
    }
}

/// Client-side ticket returned when a new-order intent is queued.
///
/// `client_order_id` is only the client-generated outbound label written into
/// the new-order command. The server does not echo it in the typed order
/// stream, and the created order is identified only by its server `uid`.
/// Applications must not attach fills/cancels/PnL to an optimistic row by this
/// value; once the server creates an order, read it from `snapshot().orders()`
/// and use the server `uid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NewOrderTicket {
    pub client_order_id: u64,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub request_uid: u64,
}

/// User-facing parameters for splitting the selected sell order.
#[derive(Debug, Clone)]
pub struct SplitOrderParams {
    pub market: String,
    pub parts: i32,
    split_small: bool,
    split_small_sell: bool,
}

impl SplitOrderParams {
    /// Split the selected sell order into `parts`.
    pub fn new(market: impl Into<String>, parts: i32) -> Self {
        Self::equal_parts(market, parts)
    }

    /// Split the selected sell order into `parts`.
    pub fn equal_parts(market: impl Into<String>, parts: i32) -> Self {
        Self {
            market: market.into(),
            parts,
            split_small: false,
            split_small_sell: false,
        }
    }

    /// Split a strategy-defined small piece.
    ///
    /// MoonBot may replace `parts` with the strategy piece size before creating
    /// the new order.
    pub fn strategy_piece(market: impl Into<String>, parts: i32) -> Self {
        Self {
            market: market.into(),
            parts,
            split_small: true,
            split_small_sell: false,
        }
    }

    /// Split a strategy-defined small piece and route that piece into sell.
    pub fn strategy_piece_and_sell(market: impl Into<String>, parts: i32) -> Self {
        Self {
            market: market.into(),
            parts,
            split_small: true,
            split_small_sell: true,
        }
    }

    /// Build equal-parts split params for a retained selected market.
    pub fn for_market(market: &crate::state::MarketHandle, parts: i32) -> Self {
        Self::new(market.name(), parts)
    }

    /// Build equal-parts split params for a retained selected market.
    pub fn equal_parts_for_market(market: &crate::state::MarketHandle, parts: i32) -> Self {
        Self::equal_parts(market.name(), parts)
    }

    /// Build small-piece split params for a retained selected market.
    pub fn strategy_piece_for_market(market: &crate::state::MarketHandle, parts: i32) -> Self {
        Self::strategy_piece(market.name(), parts)
    }

    /// Build small-piece-and-sell split params for a retained selected market.
    pub fn strategy_piece_and_sell_for_market(
        market: &crate::state::MarketHandle,
        parts: i32,
    ) -> Self {
        Self::strategy_piece_and_sell(market.name(), parts)
    }

    pub const fn is_strategy_piece(&self) -> bool {
        self.split_small
    }

    pub const fn sells_strategy_piece(&self) -> bool {
        self.split_small_sell
    }
}

/// User-facing parameters for market close.
#[derive(Debug, Clone)]
pub struct ClosePositionParams {
    pub market: String,
    market_sell: bool,
}

impl ClosePositionParams {
    /// Close the current position by placing closing limit orders.
    pub fn new(market: impl Into<String>) -> Self {
        Self::limit_orders(market)
    }

    /// Close the current position by placing closing limit orders.
    pub fn limit_orders(market: impl Into<String>) -> Self {
        Self {
            market: market.into(),
            market_sell: false,
        }
    }

    /// Force market-order close semantics.
    pub fn market_order(market: impl Into<String>) -> Self {
        Self {
            market: market.into(),
            market_sell: true,
        }
    }

    /// Build limit-close params for a retained selected market.
    pub fn for_market(market: &crate::state::MarketHandle) -> Self {
        Self::new(market.name())
    }

    /// Build limit-close params for a retained selected market.
    pub fn limit_orders_for_market(market: &crate::state::MarketHandle) -> Self {
        Self::limit_orders(market.name())
    }

    /// Build market-close params for a retained selected market.
    pub fn market_order_for_market(market: &crate::state::MarketHandle) -> Self {
        Self::market_order(market.name())
    }

    pub const fn uses_market_order(&self) -> bool {
        self.market_sell
    }
}

/// User-facing parameters for placing a sell order.
#[derive(Debug, Clone)]
pub struct SellOrderParams {
    pub market: String,
    pub price: f64,
    pub size: f64,
}

impl SellOrderParams {
    pub fn new(market: impl Into<String>, price: f64, size: f64) -> Self {
        Self {
            market: market.into(),
            price,
            size,
        }
    }

    /// Build sell-order params for a retained selected market.
    pub fn for_market(market: &crate::state::MarketHandle, price: f64, size: f64) -> Self {
        Self::new(market.name(), price, size)
    }
}
