//! High-level Active Lib runtime handle.
//!
//! This is the public happy-path layer: applications start one MoonProto runtime
//! and stop/drop it explicitly. The finite-duration pump remains an internal
//! implementation detail for tests and protocol tools.

use super::*;
use std::sync::RwLock;

mod commands;
mod handles;
mod runtime_loop;
mod types;

use commands::{
    RuntimeCommand, RuntimeCommandRequest, RuntimeReply, StratRuntimeCommand, UiRuntimeCommand,
};
pub use handles::{MoonOrders, MoonTrade, OrderTarget};
use runtime_loop::{publish_queued_events, publish_snapshot, runtime_loop};
pub use types::{
    ClosePositionParams, CoinCardCandlesTicket, EngineActionTicket, MoonClientError,
    NewOrderParams, OrderSide, SellOrderParams, SplitOrderParams, TradesStreamMode,
};

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
    pub fn subscribe_all_trades(&self, mode: TradesStreamMode) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::SubscribeAllTrades(
            mode.want_market_makers(),
        ))
    }

    /// Subscribe to all trades on the wire while retaining Active Lib data for
    /// all markets when `market_names` is empty, or for the given markets.
    pub fn subscribe_trades_for<I, S>(
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
    pub fn unsubscribe_all_trades(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::UnsubscribeAllTrades)
    }

    /// Request a fresh balance snapshot through the active runtime.
    pub fn refresh_balances(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::BalanceRefresh)
    }

    /// Request transferable asset refresh for Spot, Futures, and Quarterly.
    ///
    /// This returns as soon as the requests are queued. The runtime applies each
    /// response to `snapshot().transfer_assets()` and emits
    /// `Event::TransferAssets` when that wallet kind finishes.
    pub fn refresh_transfer_assets(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::TransferAssetsRefresh)
    }

    /// Request transferable asset refresh for one wallet kind.
    pub fn refresh_transfer_assets_kind(
        &self,
        kind: crate::state::ExchangeKind,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::TransferAssetsRefreshKind(kind))
    }

    /// Request a fresh full balance snapshot and return immediately.
    ///
    /// Completion arrives through `Event::Balance`; read the current read model
    /// through `snapshot().balances()`.
    pub fn request_balance_snapshot(&self) -> Result<(), MoonClientError> {
        self.refresh_balances()
    }

    /// Blocking diagnostic counterpart of [`Self::request_balance_snapshot`].
    pub fn blocking_request_balance_snapshot(
        &self,
        timeout: Duration,
    ) -> Result<crate::state::BalancesState, MoonClientError> {
        self.send_request(RuntimeCommandRequest::BalanceSnapshot { timeout })
            .and_then(|reply| match reply {
                RuntimeReply::BalanceSnapshot(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request a fresh order snapshot and return immediately.
    ///
    /// Completion arrives through order events, including `OrderEvent::Snapshot`;
    /// read the current read model through `snapshot().orders()`.
    pub fn request_order_snapshot(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::OrderSnapshotRefresh)
    }

    /// Blocking diagnostic counterpart of [`Self::request_order_snapshot`].
    pub fn blocking_request_order_snapshot(
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
    ///
    /// This is a direct blocking request/response helper. Regular UI code
    /// should prefer `refresh_transfer_assets()` plus
    /// `snapshot().transfer_assets()` so the runtime remains the owner of
    /// Active Lib state.
    pub fn request_transfer_assets(
        &self,
        kind: crate::state::ExchangeKind,
        timeout: Duration,
    ) -> Result<Vec<crate::commands::engine_api::TransferAsset>, MoonClientError> {
        self.send_request(RuntimeCommandRequest::TransferAssets { kind, timeout })
            .and_then(|reply| match reply {
                RuntimeReply::TransferAssets(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })
    }

    /// Request server-side full balance refresh and return immediately.
    ///
    /// The balance state arrives through the normal balance channel.
    pub fn refresh_markets_balance_full(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::MarketsBalanceFullRefresh,
            crate::commands::engine_request::get_markets_balance_full(),
        )
    }

    /// Cancel all exchange orders through Engine API and return immediately.
    pub fn cancel_all_orders(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::CancelAllOrders,
            crate::commands::engine_request::cancel_all_orders(),
        )
    }

    /// Set leverage for a market through Engine API and return immediately.
    pub fn set_leverage(
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
            crate::commands::engine_request::set_leverage(market.as_ref(), new_leverage),
        )
    }

    /// Set account hedge mode through Engine API and return immediately.
    pub fn set_hedge_mode(&self, hedge_mode: bool) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::SetHedgeMode { hedge_mode },
            crate::commands::engine_request::set_hedge_mode(hedge_mode),
        )
    }

    /// Change position type for a market through Engine API and return immediately.
    pub fn change_position_type(
        &self,
        market: impl AsRef<str>,
        position_type: u8,
        new_market: bool,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let market = market.as_ref();
        self.queue_engine_action(
            crate::events::EngineActionKind::ChangePositionType {
                market: market.to_string(),
                position_type,
                new_market,
            },
            crate::commands::engine_request::change_position_type(
                market.as_ref(),
                position_type,
                new_market,
            ),
        )
    }

    /// Convert dust to BNB through Engine API and return immediately.
    pub fn convert_dust_bnb(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::ConvertDustBnb,
            crate::commands::engine_request::convert_dust_bnb(),
        )
    }

    /// Confirm risk limit for a market through Engine API and return immediately.
    pub fn confirm_risk_limit(
        &self,
        market: impl AsRef<str>,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let market = market.as_ref();
        self.queue_engine_action(
            crate::events::EngineActionKind::ConfirmRiskLimit {
                market: market.to_string(),
            },
            crate::commands::engine_request::confirm_risk_limit(market.as_ref()),
        )
    }

    /// Set MA mode through Engine API and return immediately.
    pub fn set_ma_mode(&self, ma_mode: bool) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::SetMaMode { ma_mode },
            crate::commands::engine_request::set_ma_mode(ma_mode),
        )
    }

    /// Transfer an asset between exchange wallets through Engine API and return immediately.
    pub fn transfer_asset(
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

    /// Delphi-name alias for [`Self::transfer_asset`].
    pub fn do_transfer_asset(
        &self,
        asset: impl AsRef<str>,
        qty: f64,
        from: crate::state::ExchangeKind,
        to: crate::state::ExchangeKind,
    ) -> Result<EngineActionTicket, MoonClientError> {
        self.transfer_asset(asset, qty, from, to)
    }

    /// Reload orderbook data through Engine API and return immediately.
    pub fn reload_order_book(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.queue_engine_action(
            crate::events::EngineActionKind::ReloadOrderBook,
            crate::commands::engine_request::reload_order_book(),
        )
    }

    /// Request chunked candles and return the merged response.
    #[doc(hidden)]
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

    /// Request a full 5m candles refresh and apply it to retained market history.
    ///
    /// This is the normal Active Lib API for chart state. It hides the
    /// chunked/zipped protocol payload and returns the number of parsed market
    /// candle entries in the received snapshot. Read retained candles through
    /// `snapshot().market_history_readers(market_name)`.
    pub fn refresh_candles(&self, timeout: Duration) -> Result<usize, MoonClientError> {
        self.request_candles_data(timeout)
            .map(|merged| merged.markets.len())
    }

    /// Request CoinCard deep-history candles and return immediately.
    ///
    /// These are demand-driven candles such as Delphi `hk_4h` for CoinCard UI.
    /// They are separate from the retained 5m candles that Active Lib loads and
    /// maintains from trades. Completion arrives as `Event::CoinCardCandles`;
    /// read the latest rows through `snapshot().coin_card_candles()`.
    pub fn request_coin_card_candles(
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

    /// Blocking diagnostic counterpart of [`Self::request_coin_card_candles`].
    pub fn blocking_request_coin_card_candles(
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

    fn queue_engine_action(
        &self,
        kind: crate::events::EngineActionKind,
        payload: Vec<u8>,
    ) -> Result<EngineActionTicket, MoonClientError> {
        let ticket = EngineActionTicket {
            kind: kind.clone(),
            request_uid: engine_request_uid(&payload),
            method: engine_request_method(&payload).unwrap_or(crate::commands::EngineMethod::None),
        };
        self.send_no_reply(RuntimeCommand::EngineAction {
            kind,
            ticket: ticket.clone(),
            payload,
        })?;
        Ok(ticket)
    }

    fn request_engine_success(
        &self,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        let response = self
            .send_request(RuntimeCommandRequest::EngineRaw { payload, timeout })
            .and_then(|reply| match reply {
                RuntimeReply::EngineRaw(result) => result.map_err(MoonClientError::from),
                _ => Err(MoonClientError::RuntimeStopped),
            })?;
        if response.success {
            Ok(())
        } else {
            Err(MoonClientError::EngineRequest(EngineRequestError::Server {
                method: response.method,
                code: response.error_code,
                message: response.error_msg,
            }))
        }
    }

    /// Blocking diagnostic counterpart of [`Self::refresh_markets_balance_full`].
    pub fn blocking_request_markets_balance_full(
        &self,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::get_markets_balance_full(),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::cancel_all_orders`].
    pub fn blocking_cancel_all_orders(&self, timeout: Duration) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::cancel_all_orders(),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::set_leverage`].
    pub fn blocking_set_leverage(
        &self,
        market: impl AsRef<str>,
        new_leverage: i32,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::set_leverage(market.as_ref(), new_leverage),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::set_hedge_mode`].
    pub fn blocking_set_hedge_mode(
        &self,
        hedge_mode: bool,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::set_hedge_mode(hedge_mode),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::change_position_type`].
    pub fn blocking_change_position_type(
        &self,
        market: impl AsRef<str>,
        position_type: u8,
        new_market: bool,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::change_position_type(
                market.as_ref(),
                position_type,
                new_market,
            ),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::convert_dust_bnb`].
    pub fn blocking_convert_dust_bnb(&self, timeout: Duration) -> Result<(), MoonClientError> {
        self.request_engine_success(crate::commands::engine_request::convert_dust_bnb(), timeout)
    }

    /// Blocking diagnostic counterpart of [`Self::confirm_risk_limit`].
    pub fn blocking_confirm_risk_limit(
        &self,
        market: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::confirm_risk_limit(market.as_ref()),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::set_ma_mode`].
    pub fn blocking_set_ma_mode(
        &self,
        ma_mode: bool,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::set_ma_mode(ma_mode),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::transfer_asset`].
    pub fn blocking_do_transfer_asset(
        &self,
        asset: impl AsRef<str>,
        qty: f64,
        from: crate::state::ExchangeKind,
        to: crate::state::ExchangeKind,
        timeout: Duration,
    ) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::do_transfer_asset(
                asset.as_ref(),
                qty,
                from.to_byte(),
                to.to_byte(),
            ),
            timeout,
        )
    }

    /// Blocking diagnostic counterpart of [`Self::reload_order_book`].
    pub fn blocking_reload_order_book(&self, timeout: Duration) -> Result<(), MoonClientError> {
        self.request_engine_success(
            crate::commands::engine_request::reload_order_book(),
            timeout,
        )
    }

    /// Request a fresh UI/settings snapshot through the active runtime.
    ///
    /// The command returns after being queued. Completion arrives as
    /// `Event::Settings(SettingsEvent::ClientSettingsUpdated)`, and the latest
    /// value is readable through `snapshot().settings().client_settings`.
    pub fn request_client_settings(&self) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SettingsRequest))
    }

    /// Alias for [`Self::request_client_settings`].
    pub fn refresh_settings(&self) -> Result<(), MoonClientError> {
        self.request_client_settings()
    }

    /// Blocking diagnostic counterpart of [`Self::request_client_settings`].
    pub fn blocking_request_client_settings(
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
    pub fn set_mm_orders_subscription(&self, subscribe: bool) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::MmSubscribe(subscribe)))
    }

    /// Send a full client-settings snapshot.
    pub fn send_settings(
        &self,
        settings: crate::commands::ui::ClientSettingsCommand,
    ) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SendSettings(settings)))
    }

    /// Request a MoonBot version update.
    pub fn request_version_update(
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
    pub fn switch_dex(&self, dex_name: impl Into<String>) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SwitchDex(
            dex_name.into(),
        )))
    }

    /// Switch spot mode.
    pub fn switch_spot(&self, spot_index: u8) -> Result<(), MoonClientError> {
        self.send_no_reply(RuntimeCommand::Ui(UiRuntimeCommand::SwitchSpot(spot_index)))
    }

    #[doc(hidden)]
    pub fn ui_mm_subscribe(&self, subscribe: bool) -> Result<(), MoonClientError> {
        self.set_mm_orders_subscription(subscribe)
    }

    #[doc(hidden)]
    pub fn ui_send_settings(
        &self,
        settings: crate::commands::ui::ClientSettingsCommand,
    ) -> Result<(), MoonClientError> {
        self.send_settings(settings)
    }

    #[doc(hidden)]
    pub fn ui_update_version(
        &self,
        version_name: impl Into<String>,
        is_release: bool,
    ) -> Result<(), MoonClientError> {
        self.request_version_update(version_name, is_release)
    }

    #[doc(hidden)]
    pub fn ui_switch_dex(&self, dex_name: impl Into<String>) -> Result<(), MoonClientError> {
        self.switch_dex(dex_name)
    }

    #[doc(hidden)]
    pub fn ui_switch_spot(&self, spot_index: u8) -> Result<(), MoonClientError> {
        self.switch_spot(spot_index)
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
