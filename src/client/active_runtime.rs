//! High-level Active Lib runtime handle.
//!
//! This is the public happy-path layer: applications start one MoonProto runtime
//! and stop/drop it explicitly. The finite-duration pump remains an internal
//! implementation detail for tests and protocol tools.

use super::*;
use std::sync::RwLock;

mod handles;
mod types;

pub use handles::{MoonOrders, MoonTrade};
pub use types::{
    ClosePositionParams, MoonClientError, NewOrderParams, OrderSide, SellOrderParams,
    SplitOrderParams,
};

const ACTIVE_RUNTIME_TICK: Duration = Duration::from_millis(20);

/// High-level Active Lib client for regular applications.
///
/// `MoonClient::connect` owns the protocol/runtime thread. It runs until
/// [`Self::stop`] or drop, keeps reconnect/subscriptions/gap recovery alive, and
/// exposes read snapshots plus user-intent commands. Applications do not choose
/// a protocol-loop duration.
pub struct MoonClient {
    tx: mpsc::Sender<RuntimeCommand>,
    events_rx: Mutex<mpsc::Receiver<crate::events::Event>>,
    lifecycle_rx: Mutex<mpsc::Receiver<LifecycleEvent>>,
    snapshot: Arc<RwLock<Option<Arc<crate::events::EventDispatcherSnapshot>>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl MoonClient {
    /// Connect, run the one-time Init sequence, then start the Active Lib
    /// runtime thread.
    pub fn connect(cfg: ClientConfig, connect: ConnectConfig) -> Result<Self, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        let (events_tx, events_rx) = mpsc::channel();
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel();
        let (init_tx, init_rx) = mpsc::channel();
        let snapshot = Arc::new(RwLock::new(None));
        let runtime_snapshot = Arc::clone(&snapshot);

        let join = thread::spawn(move || {
            let mut client = Client::new(cfg);
            client.set_lifecycle_event_sender(Some(lifecycle_tx));
            let mut dispatcher = crate::events::EventDispatcher::new();

            let init_result = connect_and_init(&mut client, &mut dispatcher, connect);
            match init_result {
                Ok(result) => {
                    publish_snapshot(&dispatcher, &runtime_snapshot);
                    publish_queued_events(&mut dispatcher, &events_tx);
                    let _ = init_tx.send(Ok(result));
                }
                Err(err) => {
                    let _ = init_tx.send(Err(err));
                    return;
                }
            }

            runtime_loop(client, dispatcher, rx, events_tx, runtime_snapshot);
        });

        match init_rx.recv() {
            Ok(Ok(_)) => Ok(Self {
                tx,
                events_rx: Mutex::new(events_rx),
                lifecycle_rx: Mutex::new(lifecycle_rx),
                snapshot,
                join: Mutex::new(Some(join)),
            }),
            Ok(Err(err)) => {
                let _ = join.join();
                Err(MoonClientError::Connect(err))
            }
            Err(_) => {
                let _ = join.join();
                Err(MoonClientError::RuntimeStopped)
            }
        }
    }

    /// Latest immutable read-model snapshot, cheap to clone and safe to keep in
    /// UI state.
    pub fn snapshot(&self) -> Option<Arc<crate::events::EventDispatcherSnapshot>> {
        self.snapshot.read().unwrap().clone()
    }

    /// Drain typed events produced by the Active Lib runtime.
    pub fn drain_events(&self) -> Vec<crate::events::Event> {
        let rx = self.events_rx.lock().unwrap();
        let mut out = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(event) => out.push(event),
                Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }
        out
    }

    /// Try to receive one event without blocking.
    pub fn try_recv_event(&self) -> Option<crate::events::Event> {
        self.events_rx.lock().unwrap().try_recv().ok()
    }

    /// Receive one event with an application-selected timeout.
    pub fn recv_event_timeout(
        &self,
        timeout: Duration,
    ) -> Result<crate::events::Event, mpsc::RecvTimeoutError> {
        self.events_rx.lock().unwrap().recv_timeout(timeout)
    }

    /// Drain lifecycle events observed by the runtime.
    pub fn drain_lifecycle_events(&self) -> Vec<LifecycleEvent> {
        let rx = self.lifecycle_rx.lock().unwrap();
        let mut out = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(event) => out.push(event),
                Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }
        out
    }

    /// Try to receive one lifecycle event without blocking.
    pub fn try_recv_lifecycle_event(&self) -> Option<LifecycleEvent> {
        self.lifecycle_rx.lock().unwrap().try_recv().ok()
    }

    /// Receive one lifecycle event with an application-selected timeout.
    pub fn recv_lifecycle_event_timeout(
        &self,
        timeout: Duration,
    ) -> Result<LifecycleEvent, mpsc::RecvTimeoutError> {
        self.lifecycle_rx.lock().unwrap().recv_timeout(timeout)
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

    /// Subscribe to one orderbook by market name.
    pub fn subscribe_orderbook(
        &self,
        market_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::SubscribeOrderBook(market_name.into()))
    }

    /// Subscribe to several orderbooks by market name.
    pub fn subscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.send_no_reply(RuntimeCommand::SubscribeOrderBooks(
            market_names.into_iter().map(Into::into).collect(),
        ))
    }

    /// Unsubscribe from one orderbook by market name.
    pub fn unsubscribe_orderbook(
        &self,
        market_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeOrderBook(market_name.into()))
    }

    /// Unsubscribe from several orderbooks by market name.
    pub fn unsubscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.send_no_reply(RuntimeCommand::UnsubscribeOrderBooks(
            market_names.into_iter().map(Into::into).collect(),
        ))
    }

    /// Unsubscribe from all orderbooks remembered in the reconnect registry.
    pub fn unsubscribe_all_orderbooks(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeAllOrderBooks)
    }

    /// Subscribe to all trades and retain Active Lib data for all markets.
    pub fn subscribe_all_trades(&self, want_mm: bool) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::SubscribeAllTrades(want_mm))
    }

    /// Subscribe to all trades on the wire while retaining Active Lib data for
    /// all markets when `market_names` is empty, or for the given markets.
    pub fn subscribe_trades_for<I, S>(
        &self,
        want_mm: bool,
        market_names: I,
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.send_no_reply(RuntimeCommand::SubscribeTradesFor {
            want_mm,
            markets: market_names.into_iter().map(Into::into).collect(),
        })
    }

    /// Unsubscribe from all trades and clear the reconnect registry intent.
    pub fn unsubscribe_all_trades(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeAllTrades)
    }

    /// Request a fresh balance snapshot through the active runtime.
    pub fn refresh_balances(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::BalanceRefresh)
    }

    /// Request a fresh full balance snapshot and return the applied read model.
    pub fn request_balance_snapshot(
        &self,
        timeout: Duration,
    ) -> Result<crate::state::BalancesState, MoonClientError> {
        self.send_request(RuntimeCommandRequest::BalanceSnapshot { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::BalanceSnapshot(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request a fresh order snapshot and return the applied order list.
    pub fn request_order_snapshot(
        &self,
        timeout: Duration,
    ) -> Result<Vec<crate::state::Order>, MoonClientError> {
        self.send_request(RuntimeCommandRequest::OrderSnapshot { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::OrderSnapshot(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request one asset balance through Engine API.
    pub fn request_balance(
        &self,
        asset: impl Into<String>,
        timeout: Duration,
    ) -> Result<f64, MoonClientError> {
        self.send_request(RuntimeCommandRequest::Balance {
            asset: asset.into(),
            timeout,
        })
        .and_then(|reply| match reply {
            RuntimeReply::Balance(result) => result.map_err(MoonClientError::from),
            _ => Err(MoonClientError::RuntimeStopped),
        })
    }

    /// Request hedge-mode state through Engine API.
    pub fn request_hedge_mode(&self, timeout: Duration) -> Result<bool, MoonClientError> {
        self.send_request(RuntimeCommandRequest::HedgeMode { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::HedgeMode(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request API-key expiration metadata through Engine API.
    pub fn request_api_expiration_time(
        &self,
        timeout: Duration,
    ) -> Result<crate::commands::engine_api::ApiExpirationTime, MoonClientError> {
        self.send_request(RuntimeCommandRequest::ApiExpirationTime { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::ApiExpirationTime(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request transferable assets through Engine API.
    pub fn request_transfer_assets(
        &self,
        balance_type: u8,
        timeout: Duration,
    ) -> Result<Vec<crate::commands::engine_api::TransferAsset>, MoonClientError> {
        self.send_request(RuntimeCommandRequest::TransferAssets {
            balance_type,
            timeout,
        })
        .and_then(|reply| match reply {
            RuntimeReply::TransferAssets(result) => result.map_err(MoonClientError::from),
            _ => Err(MoonClientError::RuntimeStopped),
        })
    }

    /// Request server-side full balance refresh. The current server normally
    /// returns an empty successful EngineResponse; the balance state arrives
    /// through the normal balance channel.
    pub fn request_markets_balance_full(
        &self,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::get_markets_balance_full(),
            timeout,
        )
    }

    /// Cancel all exchange orders through Engine API.
    pub fn cancel_all_orders(&self, timeout: Duration) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::cancel_all_orders(),
            timeout,
        )
    }

    /// Set leverage for a market through Engine API.
    pub fn set_leverage(
        &self,
        market: impl AsRef<str>,
        new_leverage: i32,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::set_leverage(market.as_ref(), new_leverage),
            timeout,
        )
    }

    /// Set account hedge mode through Engine API.
    pub fn set_hedge_mode(
        &self,
        hedge_mode: bool,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::set_hedge_mode(hedge_mode),
            timeout,
        )
    }

    /// Change position type for a market through Engine API.
    pub fn change_position_type(
        &self,
        market: impl AsRef<str>,
        position_type: u8,
        new_market: bool,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::change_position_type(
                market.as_ref(),
                position_type,
                new_market,
            ),
            timeout,
        )
    }

    /// Convert dust to BNB through Engine API.
    pub fn convert_dust_bnb(&self, timeout: Duration) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(crate::commands::engine_request::convert_dust_bnb(), timeout)
    }

    /// Confirm risk limit for a market through Engine API.
    pub fn confirm_risk_limit(
        &self,
        market: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::confirm_risk_limit(market.as_ref()),
            timeout,
        )
    }

    /// Set MA mode through Engine API.
    pub fn set_ma_mode(
        &self,
        ma_mode: bool,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::set_ma_mode(ma_mode),
            timeout,
        )
    }

    /// Transfer an asset between exchange wallets through Engine API.
    pub fn do_transfer_asset(
        &self,
        asset: impl AsRef<str>,
        qty: f64,
        from: u8,
        to: u8,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::do_transfer_asset(asset.as_ref(), qty, from, to),
            timeout,
        )
    }

    /// Reload orderbook data through Engine API.
    pub fn reload_order_book(&self, timeout: Duration) -> Result<EngineResponse, MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::reload_order_book(),
            timeout,
        )
    }

    /// Request chunked candles and return the merged response.
    pub fn request_candles_data(
        &self,
        timeout: Duration,
    ) -> Result<MergedCandles, MoonClientError> {
        self.send_request(RuntimeCommandRequest::CandlesData { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::CandlesData(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request one market's historical candles through Engine API.
    pub fn request_coin_card_candles(
        &self,
        market: impl Into<String>,
        ticks: crate::commands::candles::DeepHistoryKind,
        timeout: Duration,
    ) -> Result<Vec<crate::commands::candles::DeepPrice>, MoonClientError> {
        self.send_request(RuntimeCommandRequest::CoinCardCandles {
            market: market.into(),
            ticks,
            timeout,
        })
        .and_then(|reply| match reply {
            RuntimeReply::CoinCardCandles(result) => result.map_err(MoonClientError::from),
            _ => Err(MoonClientError::RuntimeStopped),
        })
    }

    fn request_engine_success(
        &self,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<EngineResponse, MoonClientError> {
        let response = self
            .send_request(RuntimeCommandRequest::EngineRaw { payload, timeout })
            .and_then(|reply| match reply {
                RuntimeReply::EngineRaw(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })?;
        if response.success {
            Ok(response)
        } else {
            Err(MoonClientError::EngineRequest(EngineRequestError::Server {
                method: response.method,
                code: response.error_code,
                message: response.error_msg,
            }))
        }
    }

    /// Request a fresh UI/settings snapshot through the active runtime.
    pub fn refresh_settings(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SettingsRequest))
    }

    /// Request the current UI/settings snapshot and return the applied value.
    pub fn request_client_settings(
        &self,
        timeout: Duration,
    ) -> Result<crate::commands::ui::ClientSettingsCommand, MoonClientError> {
        self.send_request(RuntimeCommandRequest::ClientSettings { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::ClientSettings(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Set the market-maker orders subscription flag.
    pub fn ui_mm_subscribe(&self, subscribe: bool) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::MmSubscribe(subscribe)))
    }

    /// Send a full client-settings snapshot.
    pub fn ui_send_settings(
        &self,
        settings: crate::commands::ui::ClientSettingsCommand,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SendSettings(settings)))
    }

    /// Request a MoonBot version update.
    pub fn ui_update_version(
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
    pub fn ui_switch_dex(&self, dex_name: impl Into<String>) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SwitchDex(
            dex_name.into(),
        )))
    }

    /// Switch spot mode.
    pub fn ui_switch_spot(&self, spot_index: u8) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SwitchSpot(spot_index)))
    }

    /// Send a strategy sell-price update.
    pub fn strat_sell_price_update(
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
    pub fn strat_delete(
        &self,
        strategy_id: u64,
        folder_path: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Strat(StratRuntimeCommand::Delete {
            strategy_id,
            folder_path: folder_path.into(),
        }))
    }

    /// Change a local strategy checked flag in the active runtime state.
    pub fn set_strategy_checked(
        &self,
        strategy_id: u64,
        checked: bool,
    ) -> Result<bool, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::StrategySetChecked {
                strategy_id,
                checked,
                reply: tx,
            })
            .map_err(|_| MoonClientError::RuntimeStopped)?;
        rx.recv().map_err(|_| MoonClientError::RuntimeStopped)
    }

    /// Send Delphi checked-state delta if any local strategy changed.
    pub fn send_strategy_checked_delta(&self) -> Result<usize, MoonClientError> {
        self.send_usize(RuntimeCommand::StrategySendCheckedDelta)
    }

    /// Start or stop strategies with Delphi V2 checked-delta semantics.
    pub fn strategy_start_stop(&self, is_start: bool) -> Result<usize, MoonClientError> {
        self.send_usize(RuntimeCommand::StrategyStartStop { is_start })
    }

    /// Stop the runtime thread and wait until it exits.
    pub fn stop(&self) -> Result<(), MoonClientError> {
        let _ = self.tx.send(RuntimeCommand::Stop);
        if let Some(join) = self.join.lock().unwrap().take() {
            join.join().map_err(|_| MoonClientError::RuntimeStopped)?;
        }
        Ok(())
    }

    fn send_no_reply(&self, cmd: RuntimeCommand) -> Result<(), MoonClientError> {
        self.tx
            .send(cmd)
            .map_err(|_| MoonClientError::RuntimeStopped)
    }

    fn send_usize(&self, cmd: RuntimeCommand) -> Result<usize, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::WithUsizeReply {
                cmd: Box::new(cmd),
                reply: tx,
            })
            .map_err(|_| MoonClientError::RuntimeStopped)?;
        rx.recv().map_err(|_| MoonClientError::RuntimeStopped)
    }

    fn send_request(
        &self,
        request: RuntimeCommandRequest,
    ) -> Result<RuntimeReply, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::Request { request, reply: tx })
            .map_err(|_| MoonClientError::RuntimeStopped)?;
        rx.recv().map_err(|_| MoonClientError::RuntimeStopped)
    }
}

impl Drop for MoonClient {
    fn drop(&mut self) {
        let _ = self.tx.send(RuntimeCommand::Stop);
        if let Some(join) = self.join.get_mut().unwrap().take() {
            let _ = join.join();
        }
    }
}

enum RuntimeCommand {
    Stop,
    SubscribeOrderBook(String),
    SubscribeOrderBooks(Vec<String>),
    UnsubscribeOrderBook(String),
    UnsubscribeOrderBooks(Vec<String>),
    UnsubscribeAllOrderBooks,
    SubscribeAllTrades(bool),
    SubscribeTradesFor {
        want_mm: bool,
        markets: Vec<String>,
    },
    UnsubscribeAllTrades,
    BalanceRefresh,
    Ui(UiRuntimeCommand),
    Strat(StratRuntimeCommand),
    StrategySetChecked {
        strategy_id: u64,
        checked: bool,
        reply: mpsc::Sender<bool>,
    },
    StrategySendCheckedDelta,
    StrategyStartStop {
        is_start: bool,
    },
    WithUsizeReply {
        cmd: Box<RuntimeCommand>,
        reply: mpsc::Sender<usize>,
    },
    Request {
        request: RuntimeCommandRequest,
        reply: mpsc::Sender<RuntimeReply>,
    },
    OrderAction {
        kind: RuntimeCommandKind,
        reply: mpsc::Sender<bool>,
    },
    TradeAction {
        kind: RuntimeTradeCommandKind,
        reply: mpsc::Sender<Result<bool, TradeContextError>>,
    },
}

enum RuntimeCommandRequest {
    OrderSnapshot {
        timeout: Duration,
    },
    BalanceSnapshot {
        timeout: Duration,
    },
    Balance {
        asset: String,
        timeout: Duration,
    },
    HedgeMode {
        timeout: Duration,
    },
    ApiExpirationTime {
        timeout: Duration,
    },
    TransferAssets {
        balance_type: u8,
        timeout: Duration,
    },
    CandlesData {
        timeout: Duration,
    },
    CoinCardCandles {
        market: String,
        ticks: crate::commands::candles::DeepHistoryKind,
        timeout: Duration,
    },
    ClientSettings {
        timeout: Duration,
    },
    EngineRaw {
        payload: Vec<u8>,
        timeout: Duration,
    },
}

enum RuntimeReply {
    OrderSnapshot(Result<Vec<crate::state::Order>, mpsc::RecvTimeoutError>),
    BalanceSnapshot(Result<crate::state::BalancesState, mpsc::RecvTimeoutError>),
    Balance(Result<f64, EngineRequestError>),
    HedgeMode(Result<bool, EngineRequestError>),
    ApiExpirationTime(Result<crate::commands::engine_api::ApiExpirationTime, EngineRequestError>),
    TransferAssets(Result<Vec<crate::commands::engine_api::TransferAsset>, EngineRequestError>),
    CandlesData(Result<MergedCandles, mpsc::RecvTimeoutError>),
    CoinCardCandles(Result<Vec<crate::commands::candles::DeepPrice>, EngineRequestError>),
    ClientSettings(Result<crate::commands::ui::ClientSettingsCommand, mpsc::RecvTimeoutError>),
    EngineRaw(Result<EngineResponse, mpsc::RecvTimeoutError>),
}

enum UiRuntimeCommand {
    SettingsRequest,
    MmSubscribe(bool),
    SendSettings(crate::commands::ui::ClientSettingsCommand),
    UpdateVersion {
        version_name: String,
        is_release: bool,
    },
    SwitchDex(String),
    SwitchSpot(u8),
}

enum StratRuntimeCommand {
    SellPriceUpdate {
        strategy_id: u64,
        sell_price: f64,
    },
    Delete {
        strategy_id: u64,
        folder_path: String,
    },
}

enum RuntimeCommandKind {
    MoveOrder {
        uid: u64,
        new_price: f64,
    },
    CancelOrder {
        uid: u64,
    },
    UpdateStops {
        uid: u64,
        stops: crate::commands::trade::StopSettings,
    },
    UpdateVStop {
        uid: u64,
        on: bool,
        fixed: bool,
        level: f64,
        vol: f64,
    },
    SetImmune {
        items: Vec<crate::commands::trade::ImmuneItem>,
    },
    TurnOrderPanicSell {
        uid: u64,
        turn_on: bool,
    },
    RequestOrderStatus {
        uid: u64,
    },
    SwitchPanicSellByMarket {
        market_name: String,
        turn_on: bool,
    },
}

enum RuntimeTradeCommandKind {
    NewOrder(NewOrderParams),
    JoinOrders {
        market_name: String,
        side: OrderSide,
    },
    SplitOrder(SplitOrderParams),
    MoveAllSells {
        market_name: String,
        params: crate::commands::trade::MoveAllSellsParams,
    },
    MoveAllBuys {
        market_name: String,
        params: crate::commands::trade::MoveAllBuysParams,
    },
    ClosePosition(ClosePositionParams),
    LimitClosePosition {
        market_name: String,
        side: OrderSide,
    },
    SplitPosition {
        market_name: String,
        side: OrderSide,
    },
    SellOrder(SellOrderParams),
    MarketSplitPosition {
        market_name: String,
        side: OrderSide,
    },
    Penalty {
        market_name: String,
    },
}

fn runtime_loop(
    mut client: Client,
    mut dispatcher: crate::events::EventDispatcher,
    rx: mpsc::Receiver<RuntimeCommand>,
    events_tx: mpsc::Sender<crate::events::Event>,
    snapshot: Arc<RwLock<Option<Arc<crate::events::EventDispatcherSnapshot>>>>,
) {
    loop {
        let (stop, changed) = drain_commands(&mut client, &mut dispatcher, &rx);
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }

        client.run_with_dispatcher_worker_queued(ACTIVE_RUNTIME_TICK, &mut dispatcher);

        if publish_queued_events(&mut dispatcher, &events_tx) {
            publish_snapshot(&dispatcher, &snapshot);
        }

        let (stop, changed) = drain_commands(&mut client, &mut dispatcher, &rx);
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }
    }
}

fn drain_commands(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    rx: &mpsc::Receiver<RuntimeCommand>,
) -> (bool, bool) {
    let mut changed = false;
    loop {
        match rx.try_recv() {
            Ok(RuntimeCommand::Stop) | Err(mpsc::TryRecvError::Disconnected) => {
                return (true, changed)
            }
            Ok(cmd) => {
                changed |= handle_command(client, dispatcher, cmd);
            }
            Err(mpsc::TryRecvError::Empty) => return (false, changed),
        }
    }
}

fn handle_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: RuntimeCommand,
) -> bool {
    match cmd {
        RuntimeCommand::Stop => false,
        RuntimeCommand::SubscribeOrderBook(name) => {
            client.subscribe_orderbook(&name);
            false
        }
        RuntimeCommand::SubscribeOrderBooks(names) => {
            client.subscribe_orderbooks(names);
            false
        }
        RuntimeCommand::UnsubscribeOrderBook(name) => {
            client.unsubscribe_orderbook(&name);
            false
        }
        RuntimeCommand::UnsubscribeOrderBooks(names) => {
            client.unsubscribe_orderbooks(names);
            false
        }
        RuntimeCommand::UnsubscribeAllOrderBooks => {
            client.unsubscribe_all_orderbooks();
            false
        }
        RuntimeCommand::SubscribeAllTrades(want_mm) => {
            client.subscribe_all_trades(want_mm);
            false
        }
        RuntimeCommand::SubscribeTradesFor { want_mm, markets } => {
            client.subscribe_trades_for(want_mm, markets);
            false
        }
        RuntimeCommand::UnsubscribeAllTrades => {
            client.unsubscribe_all_trades();
            false
        }
        RuntimeCommand::BalanceRefresh => {
            client.balance_request_refresh();
            false
        }
        RuntimeCommand::Ui(cmd) => {
            handle_ui_command(client, cmd);
            false
        }
        RuntimeCommand::Strat(cmd) => {
            handle_strat_command(client, cmd);
            false
        }
        RuntimeCommand::StrategySetChecked {
            strategy_id,
            checked,
            reply,
        } => {
            let changed = dispatcher.set_strategy_checked(strategy_id, checked);
            let _ = reply.send(changed);
            changed
        }
        RuntimeCommand::StrategySendCheckedDelta => {
            dispatcher.send_strategy_checked_delta(client);
            false
        }
        RuntimeCommand::StrategyStartStop { is_start } => {
            dispatcher.ui_strat_start_stop_v2(client, is_start);
            false
        }
        RuntimeCommand::WithUsizeReply { cmd, reply } => {
            let result = handle_usize_command(client, dispatcher, *cmd);
            let _ = reply.send(result);
            false
        }
        RuntimeCommand::Request { request, reply } => {
            let (response, changed) = handle_request_command(client, dispatcher, request);
            let _ = reply.send(response);
            changed
        }
        RuntimeCommand::OrderAction { kind, reply } => {
            let result = handle_order_action(client, dispatcher, kind);
            let _ = reply.send(result);
            result
        }
        RuntimeCommand::TradeAction { kind, reply } => {
            let result = handle_trade_action(client, dispatcher, kind);
            let _ = reply.send(result);
            false
        }
    }
}

fn handle_request_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    request: RuntimeCommandRequest,
) -> (RuntimeReply, bool) {
    match request {
        RuntimeCommandRequest::OrderSnapshot { timeout } => (
            RuntimeReply::OrderSnapshot(client.request_order_snapshot(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::BalanceSnapshot { timeout } => (
            RuntimeReply::BalanceSnapshot(client.request_balance_snapshot(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::Balance { asset, timeout } => (
            RuntimeReply::Balance(client.request_balance(dispatcher, &asset, timeout)),
            false,
        ),
        RuntimeCommandRequest::HedgeMode { timeout } => (
            RuntimeReply::HedgeMode(client.request_hedge_mode(dispatcher, timeout)),
            false,
        ),
        RuntimeCommandRequest::ApiExpirationTime { timeout } => (
            RuntimeReply::ApiExpirationTime(
                client.request_api_expiration_time(dispatcher, timeout),
            ),
            false,
        ),
        RuntimeCommandRequest::TransferAssets {
            balance_type,
            timeout,
        } => (
            RuntimeReply::TransferAssets(client.request_transfer_assets(
                dispatcher,
                balance_type,
                timeout,
            )),
            false,
        ),
        RuntimeCommandRequest::CandlesData { timeout } => (
            RuntimeReply::CandlesData(client.request_candles_data(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::CoinCardCandles {
            market,
            ticks,
            timeout,
        } => (
            RuntimeReply::CoinCardCandles(
                client.request_coin_card_candles(dispatcher, &market, ticks, timeout),
            ),
            false,
        ),
        RuntimeCommandRequest::ClientSettings { timeout } => (
            RuntimeReply::ClientSettings(client.request_client_settings(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::EngineRaw { payload, timeout } => (
            RuntimeReply::EngineRaw(client.request_engine_response(dispatcher, &payload, timeout)),
            false,
        ),
    }
}

fn handle_usize_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: RuntimeCommand,
) -> usize {
    match cmd {
        RuntimeCommand::StrategySendCheckedDelta => dispatcher.send_strategy_checked_delta(client),
        RuntimeCommand::StrategyStartStop { is_start } => {
            dispatcher.ui_strat_start_stop_v2(client, is_start)
        }
        _ => {
            handle_command(client, dispatcher, cmd);
            0
        }
    }
}

fn handle_ui_command(client: &mut Client, cmd: UiRuntimeCommand) {
    match cmd {
        UiRuntimeCommand::SettingsRequest => client.ui_settings_request(),
        UiRuntimeCommand::MmSubscribe(subscribe) => client.ui_mm_subscribe(subscribe),
        UiRuntimeCommand::SendSettings(settings) => client.ui_send_settings(&settings),
        UiRuntimeCommand::UpdateVersion {
            version_name,
            is_release,
        } => client.ui_update_version(&version_name, is_release),
        UiRuntimeCommand::SwitchDex(dex_name) => client.ui_switch_dex(&dex_name),
        UiRuntimeCommand::SwitchSpot(spot_index) => client.ui_switch_spot(spot_index),
    }
}

fn handle_strat_command(client: &mut Client, cmd: StratRuntimeCommand) {
    match cmd {
        StratRuntimeCommand::SellPriceUpdate {
            strategy_id,
            sell_price,
        } => client.strat_sell_price_update(strategy_id, sell_price),
        StratRuntimeCommand::Delete {
            strategy_id,
            folder_path,
        } => client.strat_delete(strategy_id, &folder_path),
    }
}

fn handle_order_action(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    kind: RuntimeCommandKind,
) -> bool {
    match kind {
        RuntimeCommandKind::MoveOrder { uid, new_price } => {
            client.replace_tracked_order(dispatcher.orders_mut(), uid, new_price)
        }
        RuntimeCommandKind::CancelOrder { uid } => {
            client.cancel_tracked_order(dispatcher.orders_mut(), uid)
        }
        RuntimeCommandKind::UpdateStops { uid, stops } => {
            client.update_tracked_order_stops(dispatcher.orders_mut(), uid, &stops)
        }
        RuntimeCommandKind::UpdateVStop {
            uid,
            on,
            fixed,
            level,
            vol,
        } => client.update_tracked_order_vstop(dispatcher.orders_mut(), uid, on, fixed, level, vol),
        RuntimeCommandKind::SetImmune { items } => {
            client.set_immune(dispatcher.orders_mut(), &items)
        }
        RuntimeCommandKind::TurnOrderPanicSell { uid, turn_on } => {
            client.turn_tracked_order_panic_sell(dispatcher.orders_mut(), uid, turn_on)
        }
        RuntimeCommandKind::RequestOrderStatus { uid } => {
            let Some(order) = dispatcher.orders().get(uid).cloned() else {
                return false;
            };
            client.request_tracked_order_status(&order)
        }
        RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name,
            turn_on,
        } => client.switch_panic_sell_by_market(dispatcher.orders_mut(), &market_name, turn_on),
    }
}

fn handle_trade_action(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    kind: RuntimeTradeCommandKind,
) -> Result<bool, TradeContextError> {
    match kind {
        RuntimeTradeCommandKind::NewOrder(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.new_order(
                ctx,
                &params.market,
                params.side.is_short(),
                params.price,
                params.strategy_id.unwrap_or(0),
                params.size,
            ))
        }
        RuntimeTradeCommandKind::JoinOrders { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.join_orders(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::SplitOrder(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.split_order(
                ctx,
                &params.market,
                params.parts,
                params.split_small,
                params.split_small_sell,
            ))
        }
        RuntimeTradeCommandKind::MoveAllSells {
            market_name,
            params,
        } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.move_all_sells(dispatcher.orders(), ctx, &market_name, params))
        }
        RuntimeTradeCommandKind::MoveAllBuys {
            market_name,
            params,
        } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.move_all_buys(dispatcher.orders(), ctx, &market_name, params))
        }
        RuntimeTradeCommandKind::ClosePosition(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_close_position(ctx, &params.market, params.market_sell))
        }
        RuntimeTradeCommandKind::LimitClosePosition { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_limit_close_position(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::SplitPosition { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_split_position(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::SellOrder(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_sell_order(ctx, &params.market, params.price, params.size))
        }
        RuntimeTradeCommandKind::MarketSplitPosition { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_market_split_position(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::Penalty { market_name } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.penalty(ctx, &market_name))
        }
    }
}

fn publish_queued_events(
    dispatcher: &mut crate::events::EventDispatcher,
    events_tx: &mpsc::Sender<crate::events::Event>,
) -> bool {
    let events = dispatcher.take_queued_events();
    let changed = !events.is_empty();
    for event in events {
        let _ = events_tx.send(event);
    }
    changed
}

fn publish_snapshot(
    dispatcher: &crate::events::EventDispatcher,
    snapshot: &RwLock<Option<Arc<crate::events::EventDispatcherSnapshot>>>,
) {
    *snapshot.write().unwrap() = Some(Arc::new(dispatcher.snapshot()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::engine_api::ServerInfo;
    use crate::commands::trade::TradeCommand;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn ready_client() -> Client {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        client.set_server_info(ServerInfo {
            exchange_code: Some(9),
            base_currency_code: Some(17),
            ..Default::default()
        });
        client
    }

    #[test]
    fn moon_trade_new_order_derives_route_and_builds_delphi_wire() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();

        let queued = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::NewOrder(
                NewOrderParams::new("DOGEUSDT", OrderSide::Short, 12.5, 0.25).with_strategy_id(42),
            ),
        )
        .expect("BaseCheck route is present");

        assert!(queued);
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid new order") {
            TradeCommand::NewOrder(cmd) => {
                assert_eq!(cmd.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.market.currency, 17);
                assert_eq!(cmd.market.platform, 9);
                assert!(cmd.is_short);
                assert_eq!(cmd.price, 12.5);
                assert_eq!(cmd.strat_id, 42);
                assert_eq!(cmd.order_size, 0.25);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn moon_trade_returns_route_error_before_base_check_fields() {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        let mut dispatcher = crate::events::EventDispatcher::new();

        let err = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::Penalty {
                market_name: "DOGEUSDT".to_string(),
            },
        )
        .expect_err("new Client has no BaseCheck route");

        assert!(err.missing_exchange_code);
        assert!(err.missing_base_currency_code);
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
    }
}
