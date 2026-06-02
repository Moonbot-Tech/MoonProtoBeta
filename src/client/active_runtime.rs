//! High-level Active Lib runtime handle.
//!
//! This is the public happy-path layer: applications start one MoonProto runtime
//! and stop/drop it explicitly.

use super::*;
use parking_lot::{MutexGuard, RwLock, RwLockReadGuard};
use std::any::Any;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};

mod commands;
mod handles;
mod runtime_loop;
mod types;

use crate::commands::market::PositionType;
use crate::commands::ui::SpotMarketKind;
use commands::{RuntimeCommand, StratRuntimeCommand, UiRuntimeCommand};
pub use handles::{
    MoonAccount, MoonBalances, MoonCandles, MoonEmulator, MoonOrders, MoonSettings, MoonStrategies,
    MoonStreams, MoonTrade, OrderTarget,
};
use runtime_loop::runtime_loop;
pub use types::{
    ClosePositionParams, CoinCardCandlesTicket, EngineActionTicket, MoonClientError,
    MoonClientEvent, MoonClientSnapshot, MoonEventQueue, MoonEventSink, NewOrderParams,
    NewOrderTicket, OrderSide, SellOrderParams, SplitOrderParams, TradesStreamMode, VStopParams,
};

/// High-level Active Lib client for regular applications.
///
/// `MoonClient::connect` owns the protocol/runtime thread. It runs until
/// [`Self::stop`] or drop, keeps reconnect/subscriptions/gap recovery alive, and
/// exposes read snapshots plus user-intent commands. Applications do not choose
/// a protocol-loop duration.
pub struct MoonClient {
    tx: mpsc::Sender<RuntimeCommand>,
    shutdown: Arc<AtomicBool>,
    event_queue: Option<Arc<MoonEventQueue>>,
    snapshot: Arc<RwLock<Option<MoonClientSnapshot>>>,
    #[cfg(any(test, feature = "diagnostics"))]
    err_emu_diagnostics: Arc<Mutex<super::diagnostics::ErrEmuDiagnosticsState>>,
    #[cfg(any(test, feature = "diagnostics"))]
    protocol_metrics: Arc<ProtocolMetrics>,
    /// Shared subscription registry, mirrored from the runtime client so
    /// `active_subscriptions()` can read it without a channel round-trip.
    subscription_registry: Arc<Mutex<SubscriptionRegistry>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    lifecycle_join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl MoonClient {
    /// Start the Active Lib runtime and return immediately.
    ///
    /// The one-time connect/init sequence runs inside the owned runtime thread.
    /// Readiness arrives as [`LifecycleEvent::Ready`]; startup failure arrives
    /// as [`LifecycleEvent::ConnectFailed`]. Events are delivered through the
    /// default queue adapter and can be drained with [`Self::drain_events`] and
    /// [`Self::drain_lifecycle_events`].
    pub fn connect(cfg: ClientConfig, connect: ConnectConfig) -> Result<Self, MoonClientError> {
        let (sink, queue) = MoonEventSink::queue();
        Self::start_inner(cfg, connect, sink, Some(queue), None)
    }

    /// Start the Active Lib runtime with a custom event sink and return immediately.
    pub fn connect_with_sink(
        cfg: ClientConfig,
        connect: ConnectConfig,
        sink: MoonEventSink,
    ) -> Result<Self, MoonClientError> {
        Self::start_inner(cfg, connect, sink, None, None)
    }

    /// Start the runtime and block until the one-time init sequence reaches
    /// [`LifecycleEvent::Ready`], or fail.
    ///
    /// This is a convenience for command-line tools, scripts, and tests that do
    /// one-shot work after connect. It is **not** the canonical UI path: a long
    /// running application should use [`Self::connect`] (which returns at once)
    /// and react to [`LifecycleEvent::Ready`] / [`LifecycleEvent::ConnectFailed`]
    /// from the event sink, exactly like the Delphi client gates work on its
    /// async `InitDone` flag instead of blocking a thread on readiness.
    ///
    /// The wait is a single channel receive (no busy polling). `timeout` bounds
    /// the whole connect+init wait; pick a value larger than the connect/init
    /// timeout in `connect` so a precise [`ConnectError`] surfaces before this
    /// outer bound trips. The default queue adapter stays attached, so events can
    /// still be drained after this returns.
    pub fn connect_blocking(
        cfg: ClientConfig,
        connect: ConnectConfig,
        timeout: Duration,
    ) -> Result<Self, MoonClientError> {
        let (sink, queue) = MoonEventSink::queue();
        let (ready_tx, ready_rx) = mpsc::channel();
        let client = Self::start_inner(cfg, connect, sink, Some(queue), Some(ready_tx))?;
        match ready_rx.recv_timeout(timeout) {
            Ok(Ok(())) => Ok(client),
            Ok(Err(err)) => {
                let _ = client.disconnect();
                let _ = client.wait_finished();
                Err(MoonClientError::from(err))
            }
            Err(err) => {
                let _ = client.disconnect();
                let _ = client.wait_finished();
                Err(MoonClientError::from(err))
            }
        }
    }

    fn start_inner(
        cfg: ClientConfig,
        connect: ConnectConfig,
        event_sink: MoonEventSink,
        event_queue: Option<Arc<MoonEventQueue>>,
        ready_tx: Option<mpsc::Sender<Result<(), ConnectError>>>,
    ) -> Result<Self, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let runtime_shutdown = Arc::clone(&shutdown);
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel();
        let snapshot = Arc::new(RwLock::new(None));
        let runtime_snapshot = Arc::clone(&snapshot);
        let shared_state = ClientSharedState::new();
        let lifecycle_sink = event_sink.clone();
        let lifecycle_join = thread::spawn(move || {
            while let Ok(event) = lifecycle_rx.recv() {
                if let Err(payload) =
                    catch_unwind(AssertUnwindSafe(|| lifecycle_sink.emit_lifecycle(event)))
                {
                    log::error!(
                        target: "moonproto::runtime",
                        "moonproto-lifecycle-dispatcher panicked: {}",
                        panic_payload_message(payload.as_ref())
                    );
                }
            }
        });

        let thread_shared_state = shared_state.clone();
        let join = thread::spawn(move || {
            supervise_runtime_loop(
                cfg,
                connect,
                event_sink,
                runtime_snapshot,
                rx,
                lifecycle_tx,
                runtime_shutdown,
                ready_tx,
                thread_shared_state,
            );
        });
        #[cfg(any(test, feature = "diagnostics"))]
        let err_emu_diagnostics = Arc::clone(&shared_state.err_emu_diagnostics);
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = &shared_state;
        #[cfg(any(test, feature = "diagnostics"))]
        let protocol_metrics = Arc::clone(&shared_state.protocol_metrics);
        let subscription_registry = Arc::clone(&shared_state.subscription_registry);

        Ok(Self {
            tx,
            shutdown,
            event_queue,
            snapshot,
            #[cfg(any(test, feature = "diagnostics"))]
            err_emu_diagnostics,
            #[cfg(any(test, feature = "diagnostics"))]
            protocol_metrics,
            subscription_registry,
            join: Mutex::new(Some(join)),
            lifecycle_join: Mutex::new(Some(lifecycle_join)),
        })
    }

    fn runtime_restart_delay(panic_count: u32) -> Duration {
        let capped = panic_count.min(5);
        Duration::from_millis(100 * (1_u64 << capped))
    }

    /// Latest immutable read-model snapshot, cheap to clone and safe to keep in
    /// UI state.
    pub fn snapshot(&self) -> Option<Arc<crate::events::MoonStateSnapshot>> {
        read_snapshot_lock(&self.snapshot)
            .as_ref()
            .map(MoonClientSnapshot::state_arc)
    }

    /// Server identity (bot id, base-currency name, exchange code, server
    /// build/flags) from the latest published snapshot. `None` until the first
    /// snapshot is published; the value is the all-empty default until BaseCheck
    /// resolves. Convenience over `snapshot()?.server_info().clone()`.
    pub fn server_info(&self) -> Option<crate::commands::engine_api::ServerInfo> {
        self.snapshot().map(|s| s.server_info().clone())
    }

    /// Per-account metadata from the last successful AuthCheck, taken from the
    /// latest published snapshot. `None` before the client authenticates.
    pub fn auth_info(&self) -> Option<crate::commands::engine_api::AuthCheckResponse> {
        self.snapshot().and_then(|s| s.auth_info().cloned())
    }

    /// Whether the latest published snapshot's server route has the fields
    /// required for market-level trade commands (`exchange_code` and
    /// `base_currency_code` from BaseCheck). Mirrors
    /// Equivalent to the low-level route check, but reads the snapshot, so it
    /// reflects the state after Init. Returns the all-missing error before the
    /// first snapshot.
    pub fn trade_route_status(&self) -> Result<(), TradeContextError> {
        match self.snapshot() {
            Some(snapshot) => match TradeContextError::from_server_info(snapshot.server_info()) {
                Some(err) => Err(err),
                None => Ok(()),
            },
            None => Err(TradeContextError {
                missing_exchange_code: true,
                missing_base_currency_code: true,
            }),
        }
    }

    /// `true` when [`Self::trade_route_status`] is `Ok`: the session is ready to
    /// send market-level trade commands. Convenient for gating a UI trade button.
    pub fn is_ready_to_trade(&self) -> bool {
        self.trade_route_status().is_ok()
    }

    /// Read the streams this session currently has subscribed (orderbooks,
    /// all-trades, market-maker orders).
    ///
    /// This reflects the subscription registry — the intent the active library
    /// maintains and replays across reconnect — so it stays correct even after a
    /// link loss restored the session automatically.
    pub fn active_subscriptions(&self) -> crate::client::ActiveSubscriptions {
        self.subscription_registry.lock().active_subscriptions()
    }

    /// Latest immutable read-model snapshot with a monotonic runtime-local
    /// revision.
    ///
    /// This is the UI-friendly variant of [`Self::snapshot`]: keep the last
    /// revision in your view model and skip expensive redraw preparation when
    /// it has not changed.
    pub fn snapshot_versioned(&self) -> Option<MoonClientSnapshot> {
        read_snapshot_lock(&self.snapshot).clone()
    }

    /// Revision of the latest published snapshot.
    pub fn snapshot_revision(&self) -> Option<u64> {
        self.snapshot
            .read()
            .as_ref()
            .map(MoonClientSnapshot::revision)
    }

    /// Snapshot client-side ErrEmu packet-loss counters collected while
    /// `set_err_emu` is enabled (see the "Packet Loss Test Hook" guide).
    ///
    /// This is a test/diagnostic facility — production applications should not
    /// enable ErrEmu at all. Mirrors the low-level diagnostic counters on the
    /// high-level runtime path so health/stress tests built on `MoonClient` can
    /// read the same counters.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn err_emu_diagnostics_snapshot(&self) -> crate::client::ErrEmuDiagnostics {
        let configured_rate = ERR_EMU_RATE.load(Ordering::Relaxed);
        self.err_emu_diagnostics.lock().snapshot(configured_rate)
    }

    /// Snapshot protocol/runtime CPU counters for tests and diagnostics.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn protocol_metrics_snapshot(&self) -> ProtocolMetricsSnapshot {
        self.protocol_metrics.snapshot(0)
    }

    /// Hidden FireTest hook: drop outgoing datagrams inside the runtime owner.
    ///
    /// Normal applications must not use this. FireTest uses it to emulate a NAT
    /// blackhole and verify reconnect/subscription recovery on the public
    /// `MoonClient` path.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn debug_set_outgoing_blackhole(&self, enabled: bool) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::DebugOutgoingBlackhole(enabled))
    }

    /// Hidden FireTest hook: reset client-side ErrEmu counters inside the
    /// runtime owner.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn debug_reset_err_emu_diagnostics(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::DebugResetErrEmuDiagnostics)
    }

    /// Drain typed events produced by the Active Lib runtime.
    pub fn drain_events_into(&self, out: &mut Vec<crate::events::Event>) {
        if let Some(queue) = &self.event_queue {
            queue.drain_events_into(out);
        }
    }

    /// Drain typed events produced by the Active Lib runtime.
    pub fn drain_events(&self) -> Vec<crate::events::Event> {
        self.event_queue
            .as_ref()
            .map(|queue| queue.drain_events())
            .unwrap_or_default()
    }

    /// Try to receive one event without blocking.
    pub fn try_recv_event(&self) -> Option<crate::events::Event> {
        self.event_queue
            .as_ref()
            .and_then(|queue| queue.try_recv_event())
    }

    /// Drain lifecycle events observed by the runtime.
    pub fn drain_lifecycle_events_into(&self, out: &mut Vec<LifecycleEvent>) {
        if let Some(queue) = &self.event_queue {
            queue.drain_lifecycle_events_into(out);
        }
    }

    /// Drain lifecycle events observed by the runtime.
    pub fn drain_lifecycle_events(&self) -> Vec<LifecycleEvent> {
        self.event_queue
            .as_ref()
            .map(|queue| queue.drain_lifecycle_events())
            .unwrap_or_default()
    }

    /// Try to receive one lifecycle event without blocking.
    pub fn try_recv_lifecycle_event(&self) -> Option<LifecycleEvent> {
        self.event_queue
            .as_ref()
            .and_then(|queue| queue.try_recv_lifecycle_event())
    }

    /// Order intent API. The live `Orders` state remains owned by the runtime.
    pub fn orders(&self) -> MoonOrders {
        MoonOrders {
            tx: self.tx.clone(),
        }
    }

    /// Market-level trade intent API. The runtime derives `TradeCtx` from the
    /// active session route learned during Init/BaseCheck.
    pub fn trade(&self) -> MoonTrade {
        MoonTrade {
            tx: self.tx.clone(),
        }
    }

    /// Stream subscription API for orderbooks and trades.
    pub fn streams(&self) -> MoonStreams<'_> {
        MoonStreams { client: self }
    }

    /// Balance, position, and transferable-assets refresh API.
    pub fn balances(&self) -> MoonBalances<'_> {
        MoonBalances { client: self }
    }

    /// Account-level Engine API actions and account metadata refreshes.
    pub fn account(&self) -> MoonAccount<'_> {
        MoonAccount { client: self }
    }

    /// UI/settings command API.
    pub fn settings(&self) -> MoonSettings<'_> {
        MoonSettings { client: self }
    }

    /// Chart-trade emulator command API.
    pub fn emulator(&self) -> MoonEmulator<'_> {
        MoonEmulator { client: self }
    }

    /// Demand-driven candle request API.
    pub fn candles(&self) -> MoonCandles<'_> {
        MoonCandles { client: self }
    }

    /// Strategy-state command API.
    pub fn strategies(&self) -> MoonStrategies<'_> {
        MoonStrategies { client: self }
    }

    /// Subscribe to one orderbook by market name.
    pub(crate) fn subscribe_orderbook(
        &self,
        market_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::SubscribeOrderBook(market_name.into()))
    }

    /// Subscribe to several orderbooks by market name.
    pub(crate) fn subscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.send_no_reply(RuntimeCommand::SubscribeOrderBooks(
            market_names.into_iter().map(Into::into).collect(),
        ))
    }

    /// Unsubscribe from one orderbook by market name.
    pub(crate) fn unsubscribe_orderbook(
        &self,
        market_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeOrderBook(market_name.into()))
    }

    /// Unsubscribe from several orderbooks by market name.
    pub(crate) fn unsubscribe_orderbooks<I, S>(
        &self,
        market_names: I,
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.send_no_reply(RuntimeCommand::UnsubscribeOrderBooks(
            market_names.into_iter().map(Into::into).collect(),
        ))
    }

    /// Unsubscribe from all orderbooks remembered in the reconnect registry.
    pub(crate) fn unsubscribe_all_orderbooks(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeAllOrderBooks)
    }

    /// Subscribe to all trades and retain Active Lib data for all markets.
    pub(crate) fn subscribe_all_trades(
        &self,
        mode: TradesStreamMode,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::SubscribeAllTrades(
            mode.want_market_makers(),
        ))
    }

    /// Subscribe to all trades on the wire while retaining Active Lib data for
    /// all markets when `market_names` is empty, or for the given markets.
    pub(crate) fn subscribe_trades_for<I, S>(
        &self,
        mode: TradesStreamMode,
        market_names: I,
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.send_no_reply(RuntimeCommand::SubscribeTradesFor {
            want_mm: mode.want_market_makers(),
            markets: market_names.into_iter().map(Into::into).collect(),
        })
    }

    /// Unsubscribe from all trades and clear the reconnect registry intent.
    pub(crate) fn unsubscribe_all_trades(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeAllTrades)
    }

    /// Request a fresh balance snapshot through the active runtime.
    pub(crate) fn refresh_balances(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::BalanceRefresh)
    }

    /// Request a fresh hedge-mode value and return immediately.
    ///
    /// Completion arrives through `Event::Account`; read the current value from
    /// `snapshot().account().hedge_mode()`.
    pub(crate) fn refresh_hedge_mode(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::AccountHedgeModeRefresh)
    }

    /// Request fresh API-key expiration metadata and return immediately.
    ///
    /// Completion arrives through `Event::Account`; read the current value from
    /// `snapshot().account().api_expiration()`.
    pub(crate) fn refresh_api_expiration_time(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::AccountApiExpirationRefresh)
    }

    /// Request transferable asset refresh for Spot, Futures, and Quarterly.
    ///
    /// This returns as soon as the requests are queued. The runtime applies each
    /// response to `snapshot().transfer_assets()`, emits per-wallet
    /// `Event::TransferAssets`, and emits `TransferAssetsEvent::RefreshCompleted`
    /// after all wallet kinds have answered.
    pub(crate) fn refresh_transfer_assets(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::TransferAssetsRefresh)
    }

    /// Request transferable asset refresh for one wallet kind.
    pub(crate) fn refresh_transfer_assets_kind(
        &self,
        kind: crate::state::ExchangeKind,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::TransferAssetsRefreshKind(kind))
    }

    /// Cancel all exchange orders through Engine API and return immediately.
    pub(crate) fn cancel_all_orders(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::CancelAllOrders,
            crate::commands::engine_request::cancel_all_orders(),
        )
    }

    /// Set leverage for a market through Engine API and return immediately.
    pub(crate) fn set_leverage(
        &self,
        market: impl AsRef<str>,
        new_leverage: i32,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let market = market.as_ref();
        self.queue_engine_action(
            crate::events::EngineActionKind::SetLeverage {
                market: market.to_string(),
                new_leverage,
            },
            crate::commands::engine_request::set_leverage(market, new_leverage),
        )
    }

    /// Set account hedge mode through Engine API and return immediately.
    pub(crate) fn set_hedge_mode(
        &self,
        hedge_mode: bool,
    ) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::SetHedgeMode { hedge_mode },
            crate::commands::engine_request::set_hedge_mode(hedge_mode),
        )
    }

    /// Change position type for a market through Engine API and return immediately.
    pub(crate) fn change_position_type(
        &self,
        market: impl AsRef<str>,
        position_type: PositionType,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let market = market.as_ref();
        self.queue_engine_action(
            crate::events::EngineActionKind::ChangePositionType {
                market: market.to_string(),
                position_type,
            },
            crate::commands::engine_request::change_position_type(market, position_type, false),
        )
    }

    /// Convert dust to BNB through Engine API and return immediately.
    pub(crate) fn convert_dust_bnb(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::ConvertDustBnb,
            crate::commands::engine_request::convert_dust_bnb(),
        )
    }

    /// Confirm risk limit for a market through Engine API and return immediately.
    pub(crate) fn confirm_risk_limit(
        &self,
        market: impl AsRef<str>,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let market = market.as_ref();
        self.queue_engine_action(
            crate::events::EngineActionKind::ConfirmRiskLimit {
                market: market.to_string(),
            },
            crate::commands::engine_request::confirm_risk_limit(market),
        )
    }

    /// Set MA mode through Engine API and return immediately.
    pub(crate) fn set_ma_mode(&self, ma_mode: bool) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::SetMaMode { ma_mode },
            crate::commands::engine_request::set_ma_mode(ma_mode),
        )
    }

    /// Transfer an asset between exchange wallets through Engine API and return immediately.
    pub(crate) fn transfer_asset(
        &self,
        asset: impl AsRef<str>,
        qty: f64,
        from: crate::state::ExchangeKind,
        to: crate::state::ExchangeKind,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let asset = asset.as_ref();
        self.queue_engine_action(
            crate::events::EngineActionKind::TransferAsset {
                asset: asset.to_string(),
                qty,
                from,
                to,
            },
            crate::commands::engine_request::do_transfer_asset(
                asset,
                qty,
                from.to_byte(),
                to.to_byte(),
            ),
        )
    }

    /// Reload orderbook data through Engine API and return immediately.
    pub(crate) fn reload_order_book(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::ReloadOrderBook,
            crate::commands::engine_request::reload_order_book(),
        )
    }

    /// Request CoinCard deep-history candles and return immediately.
    ///
    /// These are demand-driven candles such as Delphi `hk_4h` for CoinCard UI.
    /// They are separate from the retained 5m candles that Active Lib loads and
    /// maintains from trades. Completion arrives as `Event::CoinCardCandles`;
    /// read the latest rows through `snapshot().coin_card_candles()`.
    pub(crate) fn request_coin_card_candles(
        &self,
        market: impl Into<String>,
        ticks: crate::commands::candles::DeepHistoryKind,
    ) -> Result<CoinCardCandlesTicket, MoonClientError> {
        let market = market.into();
        let payload = crate::commands::candles::get_coin_card_candles(&market, ticks);
        let ticket = CoinCardCandlesTicket {
            market,
            kind: ticks,
            request_uid: engine_request_uid(&payload),
        };
        self.send_no_reply(RuntimeCommand::CoinCardCandles {
            ticket: ticket.clone(),
            payload,
        })?;
        Ok(ticket)
    }

    fn queue_engine_action(
        &self,
        kind: crate::events::EngineActionKind,
        payload: Vec<u8>,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let ticket = EngineActionTicket {
            kind: kind.clone(),
            request_uid: engine_request_uid(&payload),
            method: engine_request_method(&payload)
                .unwrap_or(crate::commands::engine_api::EngineMethod::None),
        };
        self.send_no_reply(RuntimeCommand::EngineAction {
            kind,
            ticket: ticket.clone(),
            payload,
        })?;
        Ok(ticket)
    }

    /// Request a fresh UI/settings snapshot through the active runtime.
    ///
    /// The command returns after being queued. Completion arrives as
    /// `Event::Settings(SettingsEvent::ClientSettingsUpdated)`, and the latest
    /// value is readable through `snapshot().settings().client_settings`.
    pub(crate) fn request_client_settings(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SettingsRequest))
    }

    /// Set the market-maker orders subscription flag.
    pub(crate) fn set_mm_orders_subscription(
        &self,
        subscribe: bool,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::MmSubscribe(subscribe)))
    }

    /// Send a full client-settings snapshot.
    pub(crate) fn send_settings(
        &self,
        settings: crate::commands::ui::ClientSettingsCommand,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SendSettings(settings)))
    }

    /// Request a MoonBot version update.
    pub(crate) fn request_version_update(
        &self,
        version_name: impl Into<String>,
        is_release: bool,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::UpdateVersion {
            version_name: version_name.into(),
            is_release,
        }))
    }

    /// Switch DEX mode.
    pub(crate) fn switch_dex(&self, dex_name: impl Into<String>) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SwitchDex(
            dex_name.into(),
        )))
    }

    /// Switch spot mode.
    pub(crate) fn switch_spot(&self, spot: SpotMarketKind) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SwitchSpot(spot)))
    }

    /// Send a leverage-management command (`TLevManageCommand`).
    pub(crate) fn manage_leverage(
        &self,
        cmd: crate::commands::ui::LevManage,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::LevManage(cmd)))
    }

    /// Send emulated chart trades (`TEmuTradesCommand`).
    pub(crate) fn send_emulated_trades(
        &self,
        market_index: u16,
        base_time: f64,
        points: Vec<crate::commands::ui::EmuTradePoint>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::EmuTrades {
            market_index,
            base_time,
            points,
        }))
    }

    /// Send a trigger-management command (`TTriggerManageCommand`).
    pub(crate) fn manage_triggers(
        &self,
        action: u8,
        all_markets: bool,
        markets: Vec<u16>,
        keys: Vec<u16>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::TriggerManage {
            action,
            all_markets,
            markets,
            keys,
        }))
    }

    /// Send a reset-profit command (`TResetProfitCommand`).
    pub(crate) fn reset_profit(&self, kind: u8) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::ResetProfit(kind)))
    }

    /// Send an arb-activation notify (`TArbActivateNotify`); `valid_days` is a
    /// Delphi `TDateTime` (days).
    pub(crate) fn notify_arb_activation(&self, valid_days: f64) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::ArbActivateNotify(
            valid_days,
        )))
    }

    /// Send a strategy sell-price update.
    pub(crate) fn strat_sell_price_update(
        &self,
        strategy_id: u64,
        sell_price: f64,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Strat(
            StratRuntimeCommand::SellPriceUpdate {
                strategy_id,
                sell_price,
            },
        ))
    }

    /// Delete one strategy or folder.
    pub(crate) fn strat_delete(
        &self,
        strategy_id: u64,
        folder_path: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Strat(StratRuntimeCommand::Delete {
            strategy_id,
            folder_path: folder_path.into(),
        }))
    }

    /// Synchronize the Active Lib local strategy list after a terminal edit and
    /// send a Delphi `TStratSnapshot` batch to the server.
    ///
    /// The runtime uses the live strategy schema fetched during Init, so callers
    /// do not carry serializer field hardcode. The call only queues the intent;
    /// server echo/update arrives later through `Event::Strat`.
    pub(crate) fn send_strategy_snapshot_batch(
        &self,
        strategies: Vec<crate::commands::strategy_serializer::StrategySnapshot>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::StrategySnapshotBatch(strategies))
    }

    /// Change a local strategy checked flag in the active runtime state.
    pub(crate) fn set_strategy_checked(
        &self,
        strategy_id: u64,
        checked: bool,
    ) -> Result<(), MoonClientError> {
        self.tx
            .send(RuntimeCommand::StrategySetChecked {
                strategy_id,
                checked,
            })
            .map_err(|_| MoonClientError::RuntimeStopped)
    }

    /// Send Delphi checked-state delta if any local strategy changed.
    pub(crate) fn send_strategy_checked_delta(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::StrategySendCheckedDelta)
    }

    /// Start or stop strategies with Delphi V2 checked-delta semantics.
    pub(crate) fn strategy_start_stop(&self, is_start: bool) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::StrategyStartStop { is_start })
    }

    /// Request runtime shutdown and return immediately.
    pub fn disconnect(&self) -> Result<(), MoonClientError> {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.tx.send(RuntimeCommand::Stop);
        Ok(())
    }

    /// Alias for [`Self::disconnect`].
    pub fn stop(&self) -> Result<(), MoonClientError> {
        self.disconnect()
    }

    /// Wait until the runtime thread exits.
    pub fn wait_finished(&self) -> Result<(), MoonClientError> {
        if let Some(join) = lock_runtime_mutex(&self.join, "runtime join").take() {
            join.join().map_err(|_| MoonClientError::RuntimeStopped)?;
        }
        if let Some(join) = lock_runtime_mutex(&self.lifecycle_join, "lifecycle join").take() {
            join.join().map_err(|_| MoonClientError::RuntimeStopped)?;
        }
        Ok(())
    }

    fn send_no_reply(&self, cmd: RuntimeCommand) -> Result<(), MoonClientError> {
        self.tx
            .send(cmd)
            .map_err(|_| MoonClientError::RuntimeStopped)
    }
}

#[allow(clippy::too_many_arguments)]
fn supervise_runtime_loop(
    cfg: ClientConfig,
    connect: ConnectConfig,
    event_sink: MoonEventSink,
    snapshot: Arc<RwLock<Option<MoonClientSnapshot>>>,
    rx: mpsc::Receiver<RuntimeCommand>,
    lifecycle_tx: mpsc::Sender<LifecycleEvent>,
    runtime_shutdown: Arc<AtomicBool>,
    ready_tx: Option<mpsc::Sender<Result<(), ConnectError>>>,
    shared_state: ClientSharedState,
) {
    let market_history_sizing = cfg.market_history;
    let mut panic_count = 0_u32;
    let mut deferred_commands = VecDeque::new();

    while !runtime_shutdown.load(Ordering::Relaxed) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut client = Client::new_with_shared(cfg.clone(), shared_state.clone());
            client.set_runtime_shutdown_flag(Arc::clone(&runtime_shutdown));
            client.set_lifecycle_event_sender(Some(lifecycle_tx.clone()));
            let mut dispatcher = crate::events::EventDispatcher::new();
            dispatcher.set_market_history_sizing(market_history_sizing);

            runtime_loop(
                client,
                dispatcher,
                &rx,
                event_sink.clone(),
                Arc::clone(&snapshot),
                connect.clone(),
                ready_tx.clone(),
                &mut deferred_commands,
            );
        }));

        match result {
            Ok(()) => break,
            Err(payload) => {
                panic_count = panic_count.saturating_add(1);
                let message = panic_payload_message(payload.as_ref());
                log::error!(
                    target: "moonproto::runtime",
                    "moonproto-runtime panicked; rebuilding protocol owner and reconnecting: {message}"
                );
                if runtime_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let _ = lifecycle_tx.send(LifecycleEvent::Reconnecting);
                thread::sleep(MoonClient::runtime_restart_delay(panic_count));
            }
        }
    }
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(value) = payload.downcast_ref::<&'static str>() {
        (*value).to_string()
    } else if let Some(value) = payload.downcast_ref::<String>() {
        value.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn lock_runtime_mutex<'a, T>(mutex: &'a Mutex<T>, _name: &'static str) -> MutexGuard<'a, T> {
    mutex.lock()
}

fn read_snapshot_lock(
    lock: &RwLock<Option<MoonClientSnapshot>>,
) -> RwLockReadGuard<'_, Option<MoonClientSnapshot>> {
    lock.read()
}

impl Drop for MoonClient {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.tx.send(RuntimeCommand::Stop);
        if let Some(join) = self.join.get_mut().take() {
            let _ = join.join();
        }
        if let Some(join) = self.lifecycle_join.get_mut().take() {
            let _ = join.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnect_wait_finished_interrupts_startup_wait() {
        let cfg = ClientConfig::new("127.0.0.1", 9, [0; 16], [0; 16]).without_ntp();
        let client = MoonClient::connect(
            cfg,
            ConnectConfig::new(InitConfig::default()).with_connect_timeout(Duration::from_secs(30)),
        )
        .expect("runtime should start");

        let started = Instant::now();
        client.disconnect().expect("shutdown should be queued");
        client.wait_finished().expect("runtime should exit");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown waited for startup timeout instead of interrupting it: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn connect_blocking_returns_error_on_failed_startup() {
        let cfg = ClientConfig::new("127.0.0.1", 9, [0; 16], [0; 16]).without_ntp();
        let started = Instant::now();
        let result = MoonClient::connect_blocking(
            cfg,
            ConnectConfig::new(InitConfig::default())
                .with_connect_timeout(Duration::from_millis(50)),
            Duration::from_secs(5),
        );
        assert!(
            result.is_err(),
            "connect_blocking must surface startup failure instead of returning Ready"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "connect_blocking should return on ConnectFailed via the ready channel well \
             before the outer timeout: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn connect_with_sink_delivers_lifecycle_without_default_queue() {
        let cfg = ClientConfig::new("127.0.0.1", 9, [0; 16], [0; 16]).without_ntp();
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        let sink = MoonEventSink::callback(move |event| {
            if let MoonClientEvent::Lifecycle(event) = event {
                let _ = tx.lock().send(event);
            }
        });

        let client = MoonClient::connect_with_sink(
            cfg,
            ConnectConfig::new(InitConfig::default())
                .with_connect_timeout(Duration::from_millis(50)),
            sink,
        )
        .expect("runtime should start");

        assert!(
            client.drain_lifecycle_events().is_empty(),
            "connect_with_sink must not secretly install the default queue adapter"
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut saw_connect_failed = false;
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(LifecycleEvent::ConnectFailed { .. }) => {
                    saw_connect_failed = true;
                    break;
                }
                Ok(_) => {}
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        assert!(
            saw_connect_failed,
            "callback sink should receive ConnectFailed from unreachable test endpoint"
        );

        client.disconnect().expect("shutdown should be queued");
        client.wait_finished().expect("runtime should exit");
    }
}
