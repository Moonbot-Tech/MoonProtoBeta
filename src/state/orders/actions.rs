//! Local outgoing order-worker actions.
//!
//! These methods mirror Delphi worker-side pre-send gates such as
//! `SendStopsIfChanged`, `SendVStopIfChanged`, and
//! `DoTheJobVirtual.CheckReplaceFlag`.

use super::*;

impl Orders {
    /// Mark an order UID as having a Delphi local `vOrder`.
    ///
    /// This is the public counterpart of UI/local order paths that assign
    /// `NewOrder.vOrder := vo` before worker-side stop/VStop actions can send.
    /// If the server order has not arrived yet, the marker is stored and applied
    /// to the first `TOrderStatus` with the same UID.
    pub fn mark_local_visual_order(&mut self, uid: u64) -> bool {
        if let Some(order) = self.order_mut(uid) {
            order.has_local_visual_order = true;
            true
        } else {
            self.pending_local_visual_orders.insert(uid);
            false
        }
    }

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

    /// Delphi active-client pre-send gate for
    /// `TOrdersWorkers.MoveAllSells`.
    ///
    /// `MoveKind` mode checks side + non-immune and rejects `RM_None`;
    /// `PriceZone` mode checks only market/status/non-immune before sending;
    /// `%` (`Pers`) mode ignores immunity, matching the separate Delphi overload.
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

    /// Delphi active-client pre-send gate for
    /// `TOrdersWorkers.MoveAllBuys`.
    ///
    /// Buy bulk move has only `MoveKind` and `%` modes in Delphi; there is no
    /// buy-side `PriceZone` command.
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

    /// Delphi `TOrdersWorkers.SetImmuneClicks` local side effect.
    ///
    /// Returns only items whose local active order was found and mutated. The
    /// caller should send exactly these items in `TSetImmuneCommand`; an empty
    /// list means Delphi would not put anything on the wire.
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

    /// Delphi `BOrderWorker.SendStopsIfChanged` local machine effect.
    ///
    /// Returns the wire context only when a local worker exists and the stop
    /// record differs from the last applied/sent value. The comparison uses
    /// `StopSettings::eq`, which is bit-exact like Delphi `CompareMem`.
    pub(crate) fn send_stops_if_changed(
        &mut self,
        uid: u64,
        stops: &StopSettings,
    ) -> Option<(TradeCtx, String, OrderWorkerStatus, StopSettings)> {
        let order = self.order_mut(uid)?;
        if !order.has_local_visual_order {
            return None;
        }
        if order.stops == *stops {
            return None;
        }
        order.stops = *stops;
        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            order.status,
            *stops,
        ))
    }

    /// Delphi `BOrderWorker.SendVStopIfChanged` local machine effect.
    ///
    /// The outgoing packet uses the current worker status, not a caller-provided
    /// status. The local VStop state is updated before queueing, just as Delphi
    /// updates `FPrevVStop*` before `MClient.SendOrderCmd`.
    pub(crate) fn send_vstop_if_changed(
        &mut self,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> Option<(TradeCtx, String, VStopUpdateParams)> {
        let order = self.order_mut(uid)?;
        if !order.has_local_visual_order {
            return None;
        }
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

    /// Delphi `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` replace part.
    ///
    /// This combines the local UI intent (`p*Order.Price` +
    /// `p*Order.OrderReplace := true`) with the worker tick that sends only when
    /// `ReplaceSentTime = 0`. If a replace is already in flight, the local
    /// desired price is updated but no new packet is queued.
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
                if order.replace_sent_time_ms > 0 && !order.bulk_replace_buy {
                    order.replace_sent_time_ms = 0;
                }
                order.buy_price = new_price;
                order.bulk_replace_buy = true;
                if order.replace_sent_time_ms > 0 {
                    return None;
                }
                order.replace_sent_time_ms = now_ms.max(1);
                order_type
            }
            OrderWorkerStatus::SellSet => {
                let order_type = order.sell_order.order_type;
                if order.replace_sent_time_ms > 0 && !order.bulk_replace_sell {
                    order.replace_sent_time_ms = 0;
                }
                order.sell_price = new_price;
                order.bulk_replace_sell = true;
                if order.replace_sent_time_ms > 0 {
                    return None;
                }
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

    /// Delphi `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` cancel part.
    ///
    /// Active buy/sell orders use local `FOrder.CancelRequest` and clear it
    /// after queueing. Pending `OS_None` orders use `vOrder.PendingCancel` and
    /// keep that flag set like Delphi.
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

    /// Delphi `DoTheJobVirtual.CheckReplaceFlag` pending cancel branch.
    ///
    /// Once `vOrder.PendingCancel` is true, Delphi keeps sending the
    /// replace-then-cancel pair from the 32 ms worker loop until status leaves
    /// `OS_None`.
    pub(crate) fn tick_pending_cancel_resends(&mut self, now_ms: i64) -> Vec<OrderCancelSend> {
        let mut sends = Vec::new();
        for order in self.map.values_mut() {
            let order = std::sync::Arc::make_mut(order);
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
            order.pending_cancel_sent_ms = now_ms.max(1);
            sends.push(OrderCancelSend::PendingReplaceThenCancel {
                ctx: order.trade_ctx(),
                market: order.market_name.clone(),
                price,
            });
        }
        sends
    }

    /// Delphi `CheckReplaceFlag` panic-sell part.
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

    /// Delphi `TOrdersWorkers.TurnPanicSell(m, AValue)`.
    ///
    /// Sets `FPanicSell := AValue` on all active `OS_SellSet` workers in the
    /// market. The returned sends are exactly the workers whose
    /// `PrevPanicSell` differs and whose `CheckReplaceFlag` would enqueue
    /// `TTurnPanicSellCommand` on its next tick.
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

    /// Delphi `TOrdersWorkers.SwitchPanicSellByMarket(m, TurnON)`.
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
