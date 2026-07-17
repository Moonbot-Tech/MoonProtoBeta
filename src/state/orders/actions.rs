//! Local outgoing order-worker actions.
//!
//! These methods keep the local Active Lib pre-send gates next to the retained
//! order state: stop/VStop dedup, replace intents, pending-cancel repeats,
//! panic-sell toggles, and bulk-move candidate checks.

use super::*;

impl Orders {
    fn is_proper(order: &Order, market_name: &str, side: FixedPosition) -> bool {
        if order.market_name != market_name {
            return false;
        }
        match side {
            FixedPosition::Both => true,
            FixedPosition::Long => !order.is_short,
            FixedPosition::Short => order.is_short,
            _ => true,
        }
    }

    /// Local pre-send gate for bulk moving active sell orders.
    ///
    /// `MoveKind` mode checks side + non-immune and rejects `RM_None`;
    /// `PriceZone` mode checks only market/status/non-immune before sending;
    /// `%` (`Pers`) mode ignores immunity because it targets the whole sell
    /// set by percentage.
    pub fn has_move_all_sells_candidate(
        &self,
        market_name: &str,
        params: MoveAllSellsParams,
    ) -> bool {
        match params.cmd_type {
            MoveAllCmdType::MoveKind => {
                params.move_kind != ReplaceMultiKind::None
                    && self.map.values().any(|order| {
                        Self::is_proper(order, market_name, params.side)
                            && order.status == OrderWorkerStatus::SellSet
                            && !order.immune_for_clicks
                    })
            }
            MoveAllCmdType::PriceZone => self.map.values().any(|order| {
                order.market_name == market_name
                    && order.status == OrderWorkerStatus::SellSet
                    && !order.immune_for_clicks
            }),
            MoveAllCmdType::Pers => self.map.values().any(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::SellSet
            }),
            _ => false,
        }
    }

    /// Local pre-send gate for bulk moving active buy orders.
    ///
    /// Buy bulk move supports only `MoveKind` and `%` modes; there is no
    /// buy-side `PriceZone` command on the wire.
    pub fn has_move_all_buys_candidate(
        &self,
        market_name: &str,
        cmd_type: MoveAllBuysCmdType,
        move_kind: ReplaceMultiKind,
        side: FixedPosition,
    ) -> bool {
        match cmd_type {
            MoveAllBuysCmdType::MoveKind => {
                move_kind != ReplaceMultiKind::None
                    && self.map.values().any(|order| {
                        Self::is_proper(order, market_name, side)
                            && order.status == OrderWorkerStatus::BuySet
                            && !order.immune_for_clicks
                    })
            }
            MoveAllBuysCmdType::Pers => self.map.values().any(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::BuySet
            }),
            _ => false,
        }
    }

    /// Apply the local "immune for clicks" side effect before sending it.
    ///
    /// Returns only items whose local active order was found and mutated. The
    /// caller should send exactly these items in the immune-click update
    /// command; an empty list means there is no valid wire command to send.
    pub fn set_immune_clicks(&mut self, items: &[ImmuneItem]) -> Vec<ImmuneItem> {
        let mut applied = Vec::new();
        for item in items {
            let Some(order) = self.order_mut(item.uid) else {
                continue;
            };
            if order.status.is_terminal() {
                continue;
            }
            order.immune_for_clicks = item.value;
            applied.push(*item);
        }
        applied
    }

    /// Deduplicate and prepare an outgoing stop-settings update.
    ///
    /// Returns the wire context only when a tracked order exists and the stop
    /// record differs from the last applied/sent value. The comparison uses
    /// `StopSettings::eq`, which is bit-exact over every packed field.
    pub(crate) fn send_stops_if_changed(
        &mut self,
        uid: u64,
        stops: &StopSettings,
    ) -> Option<(TradeCtx, String, OrderWorkerStatus, StopSettings)> {
        let order = self.order_mut(uid)?;
        // U2 (sverka #14): derive the `take_profit_changed` wire flag here instead
        // of trusting the caller. It is the "trader explicitly set TP" signal that
        // stops the server auto-defaulting take-profit on the SELL transition
        // (Unit1.pas:18760 `not v.TakeProfitChanged -> v.TakeProfit := DefTakeProfit`).
        // Forgetting to set it silently clobbers the trader's TP, so the runtime
        // computes it from the current retained order: true when the take-profit
        // value or its enable flag differs from the order's current stops, and
        // latched true once it has ever been set.
        let mut stops = *stops;
        let tp_changed = bool::from(order.stops.take_profit_changed)
            || stops.use_take_profit != order.stops.use_take_profit
            || stops.take_profit.to_bits() != order.stops.take_profit.to_bits();
        stops.take_profit_changed = crate::commands::trade::DelphiBool::from_bool(tp_changed);
        if order.stops == stops {
            return None;
        }
        order.stops = stops;
        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            order.status,
            stops,
        ))
    }

    /// Deduplicate and prepare an outgoing VStop update.
    ///
    /// The outgoing packet uses the current worker status, not a caller-provided
    /// status. The local VStop state is updated before queueing, so repeated
    /// calls do not send unchanged VStop records.
    pub(crate) fn send_vstop_if_changed(
        &mut self,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> Option<(TradeCtx, String, VStopUpdateParams)> {
        let order = self.order_mut(uid)?;
        if order.vstop_on == vstop_on
            && order.vstop_fixed == vstop_fixed
            && order.vstop_level == vstop_level
            && order.vstop_vol == vstop_vol
        {
            return None;
        }

        order.vstop_on = vstop_on;
        order.vstop_fixed = vstop_fixed;
        order.vstop_level = vstop_level;
        order.vstop_vol = vstop_vol;

        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            VStopUpdateParams {
                status: order.status,
                vstop_on,
                vstop_fixed,
                vstop_level,
                vstop_vol,
            },
        ))
    }

    /// Apply and prepare one outgoing replace intent.
    ///
    /// The active runtime command queue is the Rust equivalent of Delphi's
    /// per-side `FClientReplacePending`: once an intent reaches the runtime
    /// owner it must be sent even when an older replace is still in flight.
    /// The `UK_OrderMove` send-queue key coalesces an older unsent packet, while
    /// a packet already copied by the writer is followed by the newer target.
    pub(crate) fn send_replace_if_requested(
        &mut self,
        uid: u64,
        new_price: f64,
        now_ms: i64,
    ) -> Option<(TradeCtx, String, OrderType, f64)> {
        let eps_m = self.eps_profile.eps_m;
        let order = self.order_mut(uid)?;
        let order_type = match order.status {
            OrderWorkerStatus::None => {
                let prev = order.pending_buy_cond_price?;
                if (prev - new_price).abs() <= eps_m {
                    return None;
                }
                order.pending_buy_cond_price = Some(new_price);
                OrderType::Buy
            }
            OrderWorkerStatus::BuySet => {
                let order_type = order.buy_order.order_type;
                order.buy_price = new_price;
                order.bulk_replace_buy = true;
                order.replace_sent_time_ms = now_ms.max(1);
                order_type
            }
            OrderWorkerStatus::SellSet => {
                let order_type = order.sell_order.order_type;
                order.sell_price = new_price;
                order.bulk_replace_sell = true;
                order.replace_sent_time_ms = now_ms.max(1);
                order_type
            }
            _ => return None,
        };

        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            order_type,
            new_price,
        ))
    }

    /// Prepare one outgoing cancel request from retained local order state.
    ///
    /// Active buy/sell orders use local `FOrder.CancelRequest` and clear it
    /// after queueing. Pending `OS_None` orders keep `pending_cancel` set so the
    /// runtime can repeat the replace-then-cancel pair until the server moves
    /// the order out of pending state.
    pub(crate) fn send_cancel_if_requested(
        &mut self,
        uid: u64,
        now_ms: i64,
    ) -> Option<OrderCancelSend> {
        let order = self.order_mut(uid)?;
        match order.status {
            OrderWorkerStatus::None => {
                let price = order.pending_buy_cond_price?;
                order.pending_cancel = true;
                order.pending_cancel_sent_ms = now_ms.max(1);
                let out = OrderCancelSend::PendingReplaceThenCancel {
                    ctx: order.trade_ctx(),
                    market: order.market_name.clone(),
                    price,
                };
                return Some(out);
            }
            OrderWorkerStatus::BuySet | OrderWorkerStatus::SellSet => {}
            _ => return None,
        }

        order.cancel_request = true;
        let out = OrderCancelSend::Cancel {
            ctx: order.trade_ctx(),
            market: order.market_name.clone(),
            status: order.status,
        };
        order.cancel_request = false;
        Some(out)
    }

    /// Repeat pending cancel commands while an `OS_None` order is still pending.
    ///
    /// Once `pending_cancel` is true, the runtime keeps sending the
    /// replace-then-cancel pair on the 32 ms worker cadence until status leaves
    /// `OS_None`.
    pub(crate) fn tick_pending_cancel_resends(&mut self, now_ms: i64) -> Vec<OrderCancelSend> {
        let mut sends = Vec::new();
        for order in self.map.values_mut() {
            // O1 (sverka #14): read the guard conditions through the shared Arc
            // first and only escalate to `make_mut` for the order that actually
            // resends. The old order made every Order mutable (deep-cloning its
            // String/Vec fields) on every tick before these `continue` guards.
            if order.status != OrderWorkerStatus::None || !order.pending_cancel {
                continue;
            }
            let Some(price) = order.pending_buy_cond_price else {
                continue;
            };
            if order.pending_cancel_sent_ms > 0
                && (now_ms - order.pending_cancel_sent_ms).abs() < PENDING_CANCEL_REPEAT_MS
            {
                continue;
            }
            let order = std::sync::Arc::make_mut(order);
            order.pending_cancel_sent_ms = now_ms.max(1);
            sends.push(OrderCancelSend::PendingReplaceThenCancel {
                ctx: order.trade_ctx(),
                market: order.market_name.clone(),
                price,
            });
        }
        sends
    }

    /// Deduplicate and prepare one panic-sell toggle.
    ///
    /// Sends only for `OS_SellSet` and only when `FPanicSell` differs from
    /// `PrevPanicSell`; then updates the previous value before queueing.
    pub(crate) fn send_panic_sell_if_changed(
        &mut self,
        uid: u64,
        turn_on: bool,
    ) -> Option<PanicSellSend> {
        let order = self.order_mut(uid)?;
        if order.status != OrderWorkerStatus::SellSet {
            return None;
        }
        order.panic_sell = turn_on;
        if order.prev_panic_sell == order.panic_sell {
            return None;
        }
        order.prev_panic_sell = order.panic_sell;
        Some(PanicSellSend {
            ctx: order.trade_ctx(),
            market: order.market_name.clone(),
            turn_on: order.panic_sell,
        })
    }

    /// Set panic-sell state for all active sell workers in one market.
    ///
    /// Sets `FPanicSell := AValue` on all active `OS_SellSet` workers in the
    /// market. The returned sends are exactly the workers whose
    /// `PrevPanicSell` differs and whose worker tick must enqueue a
    /// `TurnPanicSell` command.
    pub(crate) fn turn_panic_sell_by_market(
        &mut self,
        market_name: &str,
        turn_on: bool,
    ) -> Vec<PanicSellSend> {
        let uids: Vec<u64> = self
            .map
            .values()
            .filter(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::SellSet
            })
            .map(|order| order.uid)
            .collect();

        let mut sends = Vec::new();
        for uid in uids {
            if let Some(send) = self.send_panic_sell_if_changed(uid, turn_on) {
                sends.push(send);
            }
        }
        sends
    }

    /// Toggle panic-sell state for one market.
    ///
    /// If any active sell worker in the market already has `FPanicSell`, the
    /// function turns panic sell off for all active sell workers and returns
    /// `false`. Otherwise, when `TurnON` is true, it turns panic sell on for all
    /// active sell workers and returns `true`. When `TurnON` is false and
    /// nothing was active, it does nothing and returns `false`.
    pub(crate) fn switch_panic_sell_by_market(
        &mut self,
        market_name: &str,
        turn_on: bool,
    ) -> (bool, Vec<PanicSellSend>) {
        let mut was_turned_off = false;
        let mut was_turned_on = false;

        for order in self.map.values() {
            if order.market_name == market_name && order.status == OrderWorkerStatus::SellSet {
                if order.panic_sell {
                    was_turned_off = true;
                    break;
                } else if turn_on {
                    was_turned_on = true;
                    break;
                }
            }
        }

        if was_turned_off {
            (false, self.turn_panic_sell_by_market(market_name, false))
        } else if was_turned_on {
            (true, self.turn_panic_sell_by_market(market_name, true))
        } else {
            (false, Vec::new())
        }
    }
}
