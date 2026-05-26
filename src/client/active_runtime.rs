//! High-level Active Lib runtime handle.
//!
//! This is the public happy-path layer: applications start one MoonProto runtime
//! and stop/drop it explicitly. The finite-duration pump remains an internal
//! implementation detail for tests and protocol tools.

use super::*;
use std::sync::RwLock;

const ACTIVE_RUNTIME_TICK: Duration = Duration::from_millis(20);

/// Error returned by the high-level [`MoonClient`] runtime API.
#[derive(Debug)]
pub enum MoonClientError {
    /// Connect/init failed before the runtime became usable.
    Connect(ConnectError),
    /// The runtime thread stopped, panicked, or its command channel is closed.
    RuntimeStopped,
}

impl std::fmt::Display for MoonClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(err) => write!(f, "{err}"),
            Self::RuntimeStopped => write!(f, "MoonProto runtime is stopped"),
        }
    }
}

impl std::error::Error for MoonClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(err) => Some(err),
            Self::RuntimeStopped => None,
        }
    }
}

impl From<ConnectError> for MoonClientError {
    fn from(err: ConnectError) -> Self {
        Self::Connect(err)
    }
}

/// High-level Active Lib client for regular applications.
///
/// `MoonClient::connect` owns the protocol/runtime thread. It runs until
/// [`Self::stop`] or drop, keeps reconnect/subscriptions/gap recovery alive, and
/// exposes read snapshots plus user-intent commands. Applications do not choose
/// a protocol-loop duration.
pub struct MoonClient {
    tx: mpsc::Sender<RuntimeCommand>,
    events_rx: Mutex<mpsc::Receiver<crate::events::Event>>,
    snapshot: Arc<RwLock<Option<Arc<crate::events::EventDispatcherSnapshot>>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl MoonClient {
    /// Connect, run the one-time Init sequence, then start the Active Lib
    /// runtime thread.
    pub fn connect(cfg: ClientConfig, connect: ConnectConfig) -> Result<Self, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        let (events_tx, events_rx) = mpsc::channel();
        let (init_tx, init_rx) = mpsc::channel();
        let snapshot = Arc::new(RwLock::new(None));
        let runtime_snapshot = Arc::clone(&snapshot);

        let join = thread::spawn(move || {
            let mut client = Client::new(cfg);
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

    /// Order intent API. The live `Orders` state remains owned by the runtime.
    pub fn orders(&self) -> MoonOrders {
        MoonOrders {
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

    /// Request a fresh balance snapshot through the active runtime.
    pub fn refresh_balances(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::BalanceRefresh)
    }

    /// Request a fresh UI/settings snapshot through the active runtime.
    pub fn refresh_settings(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SettingsRequest))
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
}

impl Drop for MoonClient {
    fn drop(&mut self) {
        let _ = self.tx.send(RuntimeCommand::Stop);
        if let Some(join) = self.join.get_mut().unwrap().take() {
            let _ = join.join();
        }
    }
}

/// Order intent handle.
///
/// UI code can keep immutable order snapshots for rendering, but all stateful
/// order actions go through this handle so the runtime applies them to the live
/// `Orders` model before queueing protocol commands.
#[derive(Clone)]
pub struct MoonOrders {
    tx: mpsc::Sender<RuntimeCommand>,
}

impl MoonOrders {
    /// Move/replace one tracked order by UID.
    pub fn move_order(&self, uid: u64, new_price: f64) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::MoveOrder { uid, new_price })
    }

    /// Cancel one tracked order by UID.
    pub fn cancel(&self, uid: u64) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::CancelOrder { uid })
    }

    /// Update Stops for one tracked order by UID.
    pub fn update_stops(
        &self,
        uid: u64,
        stops: crate::commands::trade::StopSettings,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::UpdateStops { uid, stops })
    }

    /// Update VStop for one tracked order by UID.
    pub fn update_vstop(
        &self,
        uid: u64,
        on: bool,
        fixed: bool,
        level: f64,
        vol: f64,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::UpdateVStop {
            uid,
            on,
            fixed,
            level,
            vol,
        })
    }

    /// Apply click-immune intent for found active orders.
    pub fn set_immune(
        &self,
        items: Vec<crate::commands::trade::ImmuneItem>,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::SetImmune { items })
    }

    /// Toggle panic sell for one tracked order by UID.
    pub fn turn_panic_sell(&self, uid: u64, turn_on: bool) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::TurnOrderPanicSell { uid, turn_on })
    }

    /// Apply market-level panic sell button semantics.
    pub fn switch_panic_sell_by_market(
        &self,
        market_name: impl Into<String>,
        turn_on: bool,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name: market_name.into(),
            turn_on,
        })
    }

    fn send_bool(&self, kind: RuntimeCommandKind) -> Result<bool, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::OrderAction { kind, reply: tx })
            .map_err(|_| MoonClientError::RuntimeStopped)?;
        rx.recv().map_err(|_| MoonClientError::RuntimeStopped)
    }
}

enum RuntimeCommand {
    Stop,
    SubscribeOrderBook(String),
    SubscribeOrderBooks(Vec<String>),
    SubscribeAllTrades(bool),
    SubscribeTradesFor {
        want_mm: bool,
        markets: Vec<String>,
    },
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
    OrderAction {
        kind: RuntimeCommandKind,
        reply: mpsc::Sender<bool>,
    },
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
    SwitchPanicSellByMarket {
        market_name: String,
        turn_on: bool,
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
        RuntimeCommand::SubscribeAllTrades(want_mm) => {
            client.subscribe_all_trades(want_mm);
            false
        }
        RuntimeCommand::SubscribeTradesFor { want_mm, markets } => {
            client.subscribe_trades_for(want_mm, markets);
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
        RuntimeCommand::OrderAction { kind, reply } => {
            let result = handle_order_action(client, dispatcher, kind);
            let _ = reply.send(result);
            result
        }
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
        RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name,
            turn_on,
        } => client.switch_panic_sell_by_market(dispatcher.orders_mut(), &market_name, turn_on),
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
