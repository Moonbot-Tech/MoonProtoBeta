//! Public high-level Active Lib runtime types.

use super::*;

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
                    emit(event);
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
        let rx = self.events_rx.lock().unwrap();
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
        let rx = self.lifecycle_rx.lock().unwrap();
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
        self.events_rx.lock().unwrap().try_recv().ok()
    }

    /// Try to receive one lifecycle event without blocking.
    pub fn try_recv_lifecycle_event(&self) -> Option<LifecycleEvent> {
        self.lifecycle_rx.lock().unwrap().try_recv().ok()
    }
}

/// Ticket returned after Active Lib has queued a non-blocking Engine API action.
///
/// The server result arrives later as [`crate::events::Event::EngineAction`] and
/// as the underlying [`crate::events::Event::EngineResponse`].
#[derive(Debug, Clone, PartialEq)]
pub struct EngineActionTicket {
    pub kind: crate::events::EngineActionKind,
    pub request_uid: Option<u64>,
    pub method: crate::commands::EngineMethod,
}

/// Ticket returned after a demand-driven CoinCard candles request is queued.
///
/// Completion arrives as [`crate::events::Event::CoinCardCandles`]; the candles
/// are then readable from `snapshot().coin_card_candles()`.
#[derive(Debug, Clone, PartialEq)]
pub struct CoinCardCandlesTicket {
    pub market: String,
    pub kind: crate::commands::candles::DeepHistoryKind,
    pub request_uid: Option<u64>,
}

/// User-facing VStop settings for one tracked order.
///
/// The runtime derives the current order status and route from live `Orders`
/// before sending. This type only describes the UI intent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VStopParams {
    pub enabled: bool,
    pub fixed: bool,
    pub level: f64,
    pub volume: f64,
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

    /// Positional compatibility constructor.
    ///
    /// Prefer a struct literal so the two booleans stay named at the call site:
    /// `VStopParams { enabled: true, fixed: false, level, volume }`.
    #[doc(hidden)]
    pub const fn new(enabled: bool, fixed: bool, level: f64, volume: f64) -> Self {
        Self {
            enabled,
            fixed,
            level,
            volume,
        }
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
            Self::RuntimeStopped => write!(f, "MoonProto runtime is stopped"),
        }
    }
}

impl std::error::Error for MoonClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(err) => Some(err),
            Self::TradeContext(err) => Some(err),
            Self::RequestTimeout | Self::RequestDisconnected => None,
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
    /// `None` sends Delphi `StratID=0`.
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

    pub fn with_strategy_id(mut self, strategy_id: u64) -> Self {
        self.strategy_id = Some(strategy_id);
        self
    }
}

/// Client-side ticket returned when a new-order intent is queued.
///
/// This is the UID written into `TNewOrderCommand`. The server echoes it in
/// its "request -> order uid" logs/events, so UI/test code can correlate the
/// user intent with the server-created order without guessing by market name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NewOrderTicket {
    pub request_uid: u64,
}

/// User-facing parameters for `TSplitOrderCommand`.
#[derive(Debug, Clone)]
pub struct SplitOrderParams {
    pub market: String,
    pub parts: i32,
    pub split_small: bool,
    pub split_small_sell: bool,
}

impl SplitOrderParams {
    pub fn new(market: impl Into<String>, parts: i32) -> Self {
        Self {
            market: market.into(),
            parts,
            split_small: false,
            split_small_sell: false,
        }
    }
}

/// User-facing parameters for market close.
#[derive(Debug, Clone)]
pub struct ClosePositionParams {
    pub market: String,
    pub market_sell: bool,
}

impl ClosePositionParams {
    pub fn new(market: impl Into<String>) -> Self {
        Self {
            market: market.into(),
            market_sell: true,
        }
    }
}

/// User-facing parameters for `TDoSellOrderCommand`.
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
}
