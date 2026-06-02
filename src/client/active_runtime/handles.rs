//! High-level Active Lib intent handles.

use super::{
    commands::{RuntimeCommand, RuntimeCommandKind, RuntimeTradeCommandKind},
    CoinCardCandlesTicket, EngineActionTicket, MoonClient, MoonClientError, NewOrderParams,
    NewOrderTicket, OrderSide, SellOrderParams, SplitOrderParams, TradesStreamMode, VStopParams,
};
use std::sync::mpsc;

/// Existing order selected by UI code for a stateful action.
///
/// Application code can pass either an order UID or a borrowed
/// [`crate::state::Order`] from a snapshot. The runtime still resolves and
/// mutates the live order state before sending, so this is only a user-facing
/// selector, not a copied worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderTarget {
    uid: u64,
}

impl OrderTarget {
    /// Server order UID.
    pub fn uid(self) -> u64 {
        self.uid
    }
}

impl From<u64> for OrderTarget {
    fn from(uid: u64) -> Self {
        Self { uid }
    }
}

impl From<&crate::state::Order> for OrderTarget {
    fn from(order: &crate::state::Order) -> Self {
        Self { uid: order.uid }
    }
}

/// Order intent handle.
///
/// UI code can keep immutable order snapshots for rendering, but all stateful
/// order actions go through this handle so the runtime applies them to the live
/// `Orders` model before queueing protocol commands.
#[derive(Clone)]
pub struct MoonOrders {
    pub(super) tx: mpsc::Sender<RuntimeCommand>,
}

impl MoonOrders {
    /// Request a fresh order snapshot and return immediately.
    pub fn request_snapshot(&self) -> Result<(), MoonClientError> {
        self.tx
            .send(RuntimeCommand::OrderSnapshotRefresh)
            .map_err(|_| MoonClientError::RuntimeStopped)
    }

    /// Move/replace one tracked order.
    pub fn move_order(
        &self,
        order: impl Into<OrderTarget>,
        new_price: f64,
    ) -> Result<(), MoonClientError> {
        let uid = order.into().uid();
        self.send_intent(RuntimeCommandKind::MoveOrder { uid, new_price })
    }

    /// Cancel one tracked order.
    pub fn cancel(&self, order: impl Into<OrderTarget>) -> Result<(), MoonClientError> {
        let uid = order.into().uid();
        self.send_intent(RuntimeCommandKind::CancelOrder { uid })
    }

    /// Update Stops for one tracked order.
    ///
    /// Set the stop-loss / trailing / take-profit values you want; the runtime
    /// only sends a command when they differ from the order's current stops
    /// (Delphi `SendStopsIfChanged`). Build settings with
    /// `StopSettings::disabled().with_stop_loss_percent(...).with_take_profit_price(...)`;
    /// the internal take-profit latch is computed by the runtime on send, so
    /// application code does not maintain it.
    pub fn update_stops(
        &self,
        order: impl Into<OrderTarget>,
        stops: crate::commands::trade::StopSettings,
    ) -> Result<(), MoonClientError> {
        let uid = order.into().uid();
        self.send_intent(RuntimeCommandKind::UpdateStops { uid, stops })
    }

    /// Update VStop for one tracked order.
    pub fn update_vstop(
        &self,
        order: impl Into<OrderTarget>,
        params: VStopParams,
    ) -> Result<(), MoonClientError> {
        let uid = order.into().uid();
        self.send_intent(RuntimeCommandKind::UpdateVStop { uid, params })
    }

    /// Apply click-immune intent to selected orders.
    ///
    /// The UI passes visible order rows (or their UIDs) plus the desired flag.
    /// Active Lib resolves the live order state and sends only orders that are
    /// still active, matching Delphi's click-immunity behavior.
    pub fn set_immune_for_orders<I, T>(&self, orders: I, value: bool) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OrderTarget>,
    {
        let items = orders
            .into_iter()
            .map(|order| crate::commands::trade::ImmuneItem {
                uid: order.into().uid(),
                value,
            })
            .collect::<Vec<_>>();
        if items.is_empty() {
            return Ok(());
        }
        self.set_immune(items)
    }

    fn set_immune(
        &self,
        items: Vec<crate::commands::trade::ImmuneItem>,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeCommandKind::SetImmune { items })
    }

    /// Toggle panic sell for one tracked order.
    pub fn turn_panic_sell(
        &self,
        order: impl Into<OrderTarget>,
        turn_on: bool,
    ) -> Result<(), MoonClientError> {
        let uid = order.into().uid();
        self.send_intent(RuntimeCommandKind::TurnOrderPanicSell { uid, turn_on })
    }

    /// Request a fresh status for one tracked order.
    pub fn request_status(&self, order: impl Into<OrderTarget>) -> Result<(), MoonClientError> {
        let uid = order.into().uid();
        self.send_intent(RuntimeCommandKind::RequestOrderStatus { uid })
    }

    /// Apply market-level panic sell button semantics.
    pub fn switch_panic_sell_by_market(
        &self,
        market_name: impl Into<String>,
        turn_on: bool,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name: market_name.into(),
            turn_on,
        })
    }

    fn send_intent(&self, kind: RuntimeCommandKind) -> Result<(), MoonClientError> {
        self.tx
            .send(RuntimeCommand::OrderAction(kind))
            .map_err(|_| MoonClientError::RuntimeStopped)
    }
}

/// Market-level trade intent handle.
///
/// These actions create or manage orders by market name. The caller does not
/// pass `TradeCtx`; the runtime owner derives Delphi route bytes from the
/// active session and queues the same wire commands as the low-level `Client`.
#[derive(Clone)]
pub struct MoonTrade {
    pub(super) tx: mpsc::Sender<RuntimeCommand>,
}

impl MoonTrade {
    /// Send `TNewOrderCommand`.
    pub fn new_order(&self, params: NewOrderParams) -> Result<NewOrderTicket, MoonClientError> {
        let request_uid = random_nonzero_u64();
        self.send_intent(RuntimeTradeCommandKind::NewOrder {
            params,
            request_uid,
        })?;
        Ok(NewOrderTicket { request_uid })
    }

    /// Send `TJoinOrdersCommand`.
    pub fn join_orders(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::JoinOrders {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TSplitOrderCommand`.
    pub fn split_order(&self, params: SplitOrderParams) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::SplitOrder(params))
    }

    /// Move all matching sell orders.
    ///
    /// Build `params` with `MoveAllSellsParams` named constructors; the runtime
    /// still performs the Delphi live-order pre-send gate before queuing.
    pub fn move_all_sells(
        &self,
        market_name: impl Into<String>,
        params: crate::commands::trade::MoveAllSellsParams,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::MoveAllSells {
            market_name: market_name.into(),
            params,
        })
    }

    /// Move all matching buy orders.
    ///
    /// Build `params` with `MoveAllBuysParams` named constructors; the runtime
    /// still performs the Delphi live-order pre-send gate before queuing.
    pub fn move_all_buys(
        &self,
        market_name: impl Into<String>,
        params: crate::commands::trade::MoveAllBuysParams,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::MoveAllBuys {
            market_name: market_name.into(),
            params,
        })
    }

    /// Send `TDoClosePositionCommand`.
    pub fn close_position(
        &self,
        params: super::ClosePositionParams,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::ClosePosition(params))
    }

    /// Send `TDoLimitClosePositionCommand`.
    pub fn limit_close_position(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::LimitClosePosition {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TDoSplitPositionCommand`.
    pub fn split_position(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::SplitPosition {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TDoSellOrderCommand`.
    pub fn sell_order(&self, params: SellOrderParams) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::SellOrder(params))
    }

    /// Send `TDoMarketSplitPositionCommand`.
    pub fn market_split_position(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::MarketSplitPosition {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TPenaltyCommand`.
    pub fn penalty(&self, market_name: impl Into<String>) -> Result<(), MoonClientError> {
        self.send_intent(RuntimeTradeCommandKind::Penalty {
            market_name: market_name.into(),
        })
    }

    fn send_intent(&self, kind: RuntimeTradeCommandKind) -> Result<(), MoonClientError> {
        self.tx
            .send(RuntimeCommand::TradeAction(kind))
            .map_err(|_| MoonClientError::RuntimeStopped)
    }
}

fn random_nonzero_u64() -> u64 {
    loop {
        let value = rand::random::<u64>();
        if value != 0 {
            return value;
        }
    }
}

/// Stream subscription handle for orderbooks and trades.
///
/// This is the user-facing Active Lib shape for market streams: the runtime
/// remembers these intents and restores them after reconnects.
pub struct MoonStreams<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonStreams<'_> {
    /// Subscribe to one orderbook by market name.
    pub fn subscribe_orderbook(
        &self,
        market_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.client.subscribe_orderbook(market_name)
    }

    /// Subscribe to several orderbooks by market name.
    pub fn subscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.client.subscribe_orderbooks(market_names)
    }

    /// Unsubscribe from one orderbook by market name.
    pub fn unsubscribe_orderbook(
        &self,
        market_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.client.unsubscribe_orderbook(market_name)
    }

    /// Unsubscribe from several orderbooks by market name.
    pub fn unsubscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.client.unsubscribe_orderbooks(market_names)
    }

    /// Unsubscribe from all remembered orderbooks.
    pub fn unsubscribe_all_orderbooks(&self) -> Result<(), MoonClientError> {
        self.client.unsubscribe_all_orderbooks()
    }

    /// Subscribe to all trades and retain Active Lib data for all markets.
    pub fn subscribe_all_trades(&self, mode: TradesStreamMode) -> Result<(), MoonClientError> {
        self.client.subscribe_all_trades(mode)
    }

    /// Subscribe to all trades on the wire and retain Active Lib data only for
    /// the listed markets. An empty list means all markets.
    pub fn subscribe_trades_for<I, S>(
        &self,
        mode: TradesStreamMode,
        market_names: I,
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.client.subscribe_trades_for(mode, market_names)
    }

    /// Unsubscribe from all trades and clear the reconnect registry intent.
    pub fn unsubscribe_all_trades(&self) -> Result<(), MoonClientError> {
        self.client.unsubscribe_all_trades()
    }

    /// Reload orderbook data through Engine API.
    pub fn reload_order_book(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.client.reload_order_book()
    }
}

/// Balance, position, and transferable-assets refresh handle.
pub struct MoonBalances<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonBalances<'_> {
    /// Request a fresh balance/position snapshot and return immediately.
    pub fn refresh(&self) -> Result<(), MoonClientError> {
        self.client.refresh_balances()
    }

    /// Request transferable asset refresh for Spot, Futures, and Quarterly.
    pub fn refresh_transfer_assets(&self) -> Result<(), MoonClientError> {
        self.client.refresh_transfer_assets()
    }

    /// Request transferable asset refresh for one wallet kind.
    pub fn refresh_transfer_assets_kind(
        &self,
        kind: crate::state::ExchangeKind,
    ) -> Result<(), MoonClientError> {
        self.client.refresh_transfer_assets_kind(kind)
    }

    /// Transfer an asset between exchange wallets through Engine API.
    pub fn transfer_asset(
        &self,
        asset: impl AsRef<str>,
        qty: f64,
        from: crate::state::ExchangeKind,
        to: crate::state::ExchangeKind,
    ) -> Result<EngineActionTicket, MoonClientError> {
        self.client.transfer_asset(asset, qty, from, to)
    }

    /// Convert dust to BNB through Engine API.
    pub fn convert_dust_bnb(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.client.convert_dust_bnb()
    }
}

/// Account metadata and account-level Engine API handle.
pub struct MoonAccount<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonAccount<'_> {
    /// Request a fresh hedge-mode value and return immediately.
    pub fn refresh_hedge_mode(&self) -> Result<(), MoonClientError> {
        self.client.refresh_hedge_mode()
    }

    /// Request fresh API-key expiration metadata and return immediately.
    pub fn refresh_api_expiration_time(&self) -> Result<(), MoonClientError> {
        self.client.refresh_api_expiration_time()
    }

    /// Set leverage for a market through Engine API.
    pub fn set_leverage(
        &self,
        market: impl AsRef<str>,
        new_leverage: i32,
    ) -> Result<EngineActionTicket, MoonClientError> {
        self.client.set_leverage(market, new_leverage)
    }

    /// Set account hedge mode through Engine API.
    pub fn set_hedge_mode(&self, hedge_mode: bool) -> Result<EngineActionTicket, MoonClientError> {
        self.client.set_hedge_mode(hedge_mode)
    }

    /// Cancel all exchange orders through Engine API.
    pub fn cancel_all_orders(&self) -> Result<EngineActionTicket, MoonClientError> {
        self.client.cancel_all_orders()
    }

    /// Change position type for a market through Engine API.
    pub fn change_position_type(
        &self,
        market: impl AsRef<str>,
        position_type: crate::commands::market::PositionType,
    ) -> Result<EngineActionTicket, MoonClientError> {
        self.client.change_position_type(market, position_type)
    }

    /// Confirm risk limit for a market through Engine API.
    pub fn confirm_risk_limit(
        &self,
        market: impl AsRef<str>,
    ) -> Result<EngineActionTicket, MoonClientError> {
        self.client.confirm_risk_limit(market)
    }

    /// Set MA mode through Engine API.
    pub fn set_ma_mode(&self, ma_mode: bool) -> Result<EngineActionTicket, MoonClientError> {
        self.client.set_ma_mode(ma_mode)
    }
}

/// UI/settings command handle.
pub struct MoonSettings<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonSettings<'_> {
    /// Request a fresh UI/settings snapshot and return immediately.
    pub fn refresh(&self) -> Result<(), MoonClientError> {
        self.client.request_client_settings()
    }

    /// Set the market-maker orders subscription flag.
    pub fn set_mm_orders_subscription(&self, subscribe: bool) -> Result<(), MoonClientError> {
        self.client.set_mm_orders_subscription(subscribe)
    }

    /// Send a full client-settings snapshot.
    pub fn send(
        &self,
        settings: crate::commands::ui::ClientSettingsCommand,
    ) -> Result<(), MoonClientError> {
        self.client.send_settings(settings)
    }

    /// Request the normal release update flow.
    pub fn request_release_update(&self) -> Result<(), MoonClientError> {
        self.client.request_version_update("", true)
    }

    /// Request a named beta/test version update.
    pub fn request_version_update(
        &self,
        version_name: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.client.request_version_update(version_name, false)
    }

    /// Switch DEX mode.
    pub fn switch_dex(&self, dex_name: impl Into<String>) -> Result<(), MoonClientError> {
        self.client.switch_dex(dex_name)
    }

    /// Switch spot mode.
    pub fn switch_spot(
        &self,
        spot: crate::commands::ui::SpotMarketKind,
    ) -> Result<(), MoonClientError> {
        self.client.switch_spot(spot)
    }

    /// Send a leverage-management command (`TLevManageCommand`, CmdId 9).
    ///
    /// Set the behavioural fields on `cmd` (auto max-order, auto lev-up,
    /// isolated/cross, fix-lev, telegram report, lev-control text). Its `uid`
    /// and `cmd_ver` fields are ignored on send: the runtime assigns a fresh UID
    /// and always writes Delphi's `LevCmdVer = 1`.
    pub fn manage_leverage(
        &self,
        cmd: &crate::commands::ui::LevManage,
    ) -> Result<(), MoonClientError> {
        self.client.manage_leverage(cmd.clone())
    }

    /// Send a trigger-management command (`TTriggerManageCommand`, CmdId 10).
    ///
    /// `market_names` are regular terminal market names. The runtime resolves
    /// them to the current server indexes when the command is queued, matching
    /// Delphi UI code: users select `TMarket` objects, and `mIndex` is only the
    /// final wire detail.
    pub fn manage_triggers_for_markets<I, S>(
        &self,
        action: crate::commands::ui::TriggerAction,
        market_names: I,
        keys: &[u16],
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let indexes = resolve_market_indexes(self.client, market_names)?;
        self.client
            .manage_triggers(action.to_byte(), false, indexes, keys.to_vec())
    }

    /// Arm trigger keys for selected markets.
    pub fn set_triggers_for_markets<I, S>(
        &self,
        market_names: I,
        keys: &[u16],
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.manage_triggers_for_markets(
            crate::commands::ui::TriggerAction::Set,
            market_names,
            keys,
        )
    }

    /// Clear trigger keys for selected markets.
    pub fn clear_triggers_for_markets<I, S>(
        &self,
        market_names: I,
        keys: &[u16],
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.manage_triggers_for_markets(
            crate::commands::ui::TriggerAction::Clear,
            market_names,
            keys,
        )
    }

    /// Arm trigger keys for all current markets.
    pub fn set_triggers_for_all(&self, keys: &[u16]) -> Result<(), MoonClientError> {
        self.client.manage_triggers(
            crate::commands::ui::TriggerAction::Set.to_byte(),
            true,
            Vec::new(),
            keys.to_vec(),
        )
    }

    /// Clear trigger keys for all current markets.
    pub fn clear_triggers_for_all(&self, keys: &[u16]) -> Result<(), MoonClientError> {
        self.client.manage_triggers(
            crate::commands::ui::TriggerAction::Clear.to_byte(),
            true,
            Vec::new(),
            keys.to_vec(),
        )
    }

    /// Low-level diagnostic helper for callers that already have server market
    /// indexes. Regular terminal code should use the market-name helpers above.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn manage_triggers(
        &self,
        action: crate::commands::ui::TriggerAction,
        all_markets: bool,
        markets: &[u16],
        keys: &[u16],
    ) -> Result<(), MoonClientError> {
        self.client.manage_triggers(
            action.to_byte(),
            all_markets,
            markets.to_vec(),
            keys.to_vec(),
        )
    }

    /// Send a reset-profit command (`TResetProfitCommand`, CmdId 11): reset the
    /// current-session or all-time profit counter on the server.
    pub fn reset_profit(
        &self,
        kind: crate::commands::ui::ResetProfitKind,
    ) -> Result<(), MoonClientError> {
        self.client.reset_profit(kind.to_byte())
    }

    /// Send an arb-activation notify (`TArbActivateNotify`, CmdId 12): tell the
    /// server arbitrage is valid until `valid_until`.
    pub fn notify_arb_activation(
        &self,
        valid_until: crate::MoonTime,
    ) -> Result<(), MoonClientError> {
        self.client
            .notify_arb_activation(valid_until.to_delphi_days())
    }
}

/// Chart-trade emulator command handle.
///
/// This is the high-level path matching Delphi's draw-tool emulator: terminal
/// code selects a market, builds `EmuTradePoint` values from chart points, and
/// Active Lib resolves the current server index before sending
/// `TEmuTradesCommand`.
pub struct MoonEmulator<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonEmulator<'_> {
    /// Send chart-pencil points for a retained market handle.
    ///
    /// This is the Delphi `TChartFrame.TryEmulatePrices` shape: the UI passes
    /// absolute chart points, Active Lib starts from the market's current
    /// `LastAsk`, converts falling points to sell ticks, skips points outside
    /// Delphi's `Word` millisecond window, and queues one `TEmuTradesCommand`.
    pub fn send_pencil_prices_for_market<I>(
        &self,
        market: &crate::state::MarketHandle,
        base_time: crate::MoonTime,
        points: I,
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = crate::EmuPencilPoint>,
    {
        let initial_price = market.price().last_ask as f32;
        let emu_points = emu_trade_points_from_pencil(base_time, initial_price, points)?;
        self.send_trades_for_market(market, base_time, &emu_points)
    }

    /// Send chart-pencil points by terminal market name.
    ///
    /// Prefer [`Self::send_pencil_prices_for_market`] when UI code already
    /// keeps a stable `MarketHandle` for the selected chart.
    pub fn send_pencil_prices<I>(
        &self,
        market_name: impl AsRef<str>,
        base_time: crate::MoonTime,
        points: I,
    ) -> Result<(), MoonClientError>
    where
        I: IntoIterator<Item = crate::EmuPencilPoint>,
    {
        let market_name = market_name.as_ref();
        let snapshot = self
            .client
            .snapshot()
            .ok_or(MoonClientError::StateUnavailable(
                "market map is not published yet",
            ))?;
        let market = snapshot
            .markets()
            .get(market_name)
            .ok_or_else(|| MoonClientError::UnknownMarket(market_name.to_string()))?;
        self.send_pencil_prices_for_market(&market, base_time, points)
    }

    /// Send emulated trades for a retained market handle.
    pub fn send_trades_for_market(
        &self,
        market: &crate::state::MarketHandle,
        base_time: crate::MoonTime,
        points: &[crate::EmuTradePoint],
    ) -> Result<(), MoonClientError> {
        self.send_trades(market.name(), base_time, points)
    }

    /// Send emulated trades by terminal market name.
    ///
    /// Empty `points` is a no-op, matching Delphi UI code which only sends the
    /// command after a drawn pencil produced at least one valid point. Sell side
    /// is encoded by `EmuTradePoint::sell`.
    pub fn send_trades(
        &self,
        market_name: impl AsRef<str>,
        base_time: crate::MoonTime,
        points: &[crate::EmuTradePoint],
    ) -> Result<(), MoonClientError> {
        if points.is_empty() {
            return Ok(());
        }
        if points.len() > usize::from(u16::MAX) {
            return Err(MoonClientError::TooManyEmuTradePoints(points.len()));
        }
        let market_index = resolve_market_index(self.client, market_name.as_ref())?;
        self.client
            .send_emulated_trades(market_index, base_time.to_delphi_days(), points.to_vec())
    }
}

fn emu_trade_points_from_pencil<I>(
    base_time: crate::MoonTime,
    initial_price: f32,
    points: I,
) -> Result<Vec<crate::EmuTradePoint>, MoonClientError>
where
    I: IntoIterator<Item = crate::EmuPencilPoint>,
{
    let mut out = Vec::new();
    let mut prev_price = initial_price;
    let base_time_ms = base_time.unix_millis();
    for point in points {
        let delta_ms = point.time.unix_millis().saturating_sub(base_time_ms);
        if !(0..=i64::from(u16::MAX)).contains(&delta_ms) {
            continue;
        }
        if out.len() >= usize::from(u16::MAX) {
            return Err(MoonClientError::TooManyEmuTradePoints(
                usize::from(u16::MAX) + 1,
            ));
        }
        let mut price = point.price;
        if price < prev_price {
            price = -price;
        }
        prev_price = price.abs();
        out.push(crate::EmuTradePoint {
            time_delta_ms: delta_ms as u16,
            price,
        });
    }
    Ok(out)
}

fn resolve_market_index(client: &MoonClient, market_name: &str) -> Result<u16, MoonClientError> {
    let snapshot = client.snapshot().ok_or(MoonClientError::StateUnavailable(
        "market map is not published yet",
    ))?;
    let markets = snapshot.markets();
    if !markets.indexes_synchronized() {
        return Err(MoonClientError::StateUnavailable(
            "market indexes are not synchronized",
        ));
    }
    markets
        .market_index_by_name(market_name)
        .ok_or_else(|| MoonClientError::UnknownMarket(market_name.to_string()))
}

fn resolve_market_indexes<I, S>(
    client: &MoonClient,
    market_names: I,
) -> Result<Vec<u16>, MoonClientError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let snapshot = client.snapshot().ok_or(MoonClientError::StateUnavailable(
        "market map is not published yet",
    ))?;
    let markets = snapshot.markets();
    if !markets.indexes_synchronized() {
        return Err(MoonClientError::StateUnavailable(
            "market indexes are not synchronized",
        ));
    }

    let mut indexes = Vec::new();
    for market in market_names {
        let market = market.as_ref();
        let index = markets
            .market_index_by_name(market)
            .ok_or_else(|| MoonClientError::UnknownMarket(market.to_string()))?;
        indexes.push(index);
    }
    Ok(indexes)
}

/// Demand-driven candle request handle.
pub struct MoonCandles<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonCandles<'_> {
    /// Request CoinCard deep-history candles for a retained market handle.
    ///
    /// Prefer this in terminal UI code that already keeps a selected
    /// `MarketHandle`, matching Delphi chart code acting on its current
    /// `TMarket` reference.
    pub fn request_coin_card_for(
        &self,
        market: &crate::state::MarketHandle,
        ticks: crate::commands::candles::DeepHistoryKind,
    ) -> Result<CoinCardCandlesTicket, MoonClientError> {
        self.request_coin_card(market.name(), ticks)
    }

    /// Request CoinCard deep-history candles and return immediately.
    ///
    /// This string-keyed path is convenient for scripts and one-shot tools.
    /// Terminal UI code that already keeps a selected `MarketHandle` should use
    /// [`Self::request_coin_card_for`].
    pub fn request_coin_card(
        &self,
        market: impl Into<String>,
        ticks: crate::commands::candles::DeepHistoryKind,
    ) -> Result<CoinCardCandlesTicket, MoonClientError> {
        self.client.request_coin_card_candles(market, ticks)
    }
}

/// Strategy-state command handle.
pub struct MoonStrategies<'a> {
    pub(super) client: &'a MoonClient,
}

impl MoonStrategies<'_> {
    /// Send a strategy sell-price update.
    pub fn sell_price_update(
        &self,
        strategy_id: u64,
        sell_price: f64,
    ) -> Result<(), MoonClientError> {
        self.client.strat_sell_price_update(strategy_id, sell_price)
    }

    /// Delete one strategy or folder.
    pub fn delete(
        &self,
        strategy_id: u64,
        folder_path: impl Into<String>,
    ) -> Result<(), MoonClientError> {
        self.client.strat_delete(strategy_id, folder_path)
    }

    /// Synchronize the application's current local strategy list.
    pub fn sync_local_strategies(
        &self,
        strategies: Vec<crate::commands::strategy_serializer::StrategySnapshot>,
    ) -> Result<(), MoonClientError> {
        self.client.send_strategy_snapshot_batch(strategies)
    }

    /// Change a local strategy checked flag in the active runtime state.
    pub fn set_checked(&self, strategy_id: u64, checked: bool) -> Result<(), MoonClientError> {
        self.client.set_strategy_checked(strategy_id, checked)
    }

    /// Send Delphi checked-state delta if any local strategy changed.
    pub fn send_checked_delta(&self) -> Result<(), MoonClientError> {
        self.client.send_strategy_checked_delta()
    }

    /// Start checked strategies.
    pub fn start(&self) -> Result<(), MoonClientError> {
        self.client.strategy_start_stop(true)
    }

    /// Stop checked strategies.
    pub fn stop(&self) -> Result<(), MoonClientError> {
        self.client.strategy_start_stop(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at_ms(base: crate::MoonTime, delta_ms: i64) -> crate::MoonTime {
        crate::MoonTime::from_unix_millis(base.unix_millis() + delta_ms)
    }

    #[test]
    fn pencil_points_follow_delphi_prev_price_signing_and_delta_filter() {
        let base = crate::MoonTime::from_unix_millis(1_678_780_800_000);
        let points = [
            crate::EmuPencilPoint::new(at_ms(base, -1), 111.0),
            crate::EmuPencilPoint::new(at_ms(base, 0), 101.0),
            crate::EmuPencilPoint::new(at_ms(base, 500), 99.0),
            crate::EmuPencilPoint::new(at_ms(base, 1_000), 100.0),
            crate::EmuPencilPoint::new(at_ms(base, 70_000), 120.0),
        ];

        let out = emu_trade_points_from_pencil(base, 100.0, points).unwrap();

        assert_eq!(
            out,
            vec![
                crate::EmuTradePoint {
                    time_delta_ms: 0,
                    price: 101.0,
                },
                crate::EmuTradePoint {
                    time_delta_ms: 500,
                    price: -99.0,
                },
                crate::EmuTradePoint {
                    time_delta_ms: 1000,
                    price: 100.0,
                },
            ]
        );
    }
}
