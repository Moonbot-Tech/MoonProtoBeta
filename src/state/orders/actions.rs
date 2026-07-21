//! Local order intents. Addressed setters become grouped `TOrderCommand`s;
//! the send queue performs last-writer-wins coalescing by
//! `{command group, order id}`. Bulk and one-shot gestures keep their protocol
//! shapes instead of being expanded into an unrelated API model.

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
}

impl OrderState {
    pub(crate) fn set_immune_clicks(&mut self, items: &[ImmuneItem]) -> Vec<OrderCommandPayload> {
        let mut commands = Vec::with_capacity(items.len());
        for item in items {
            let Some(order) = self.order_mut(item.uid) else {
                continue;
            };
            if order.status.is_terminal() || order.immune_for_clicks == item.value {
                continue;
            }
            order.immune_for_clicks = item.value;
            commands.push(OrderCommandPayload::Immune {
                order_id: item.uid,
                enabled: item.value,
            });
        }
        commands
    }

    pub(crate) fn send_stops_if_changed(
        &mut self,
        uid: u64,
        stops: &StopSettings,
    ) -> Option<OrderCommandPayload> {
        if !self.section_seen(uid, OSEC_STOPS) {
            return None;
        }
        let order = self.order_mut(uid)?;
        let mut stops = *stops;
        let tp_changed = bool::from(order.stops.take_profit_changed)
            || stops.use_take_profit != order.stops.use_take_profit
            || stops.take_profit.to_bits() != order.stops.take_profit.to_bits();
        stops.take_profit_changed = DelphiBool::from_bool(tp_changed);
        if order.stops == stops {
            return None;
        }
        order.stops = stops;
        Some(OrderCommandPayload::Stops {
            order_id: uid,
            stops,
        })
    }

    pub(crate) fn send_vstop_if_changed(
        &mut self,
        uid: u64,
        enabled: bool,
        fixed: bool,
        level: f64,
        volume: f64,
    ) -> Option<OrderCommandPayload> {
        if !self.section_seen(uid, OSEC_VSTOP) {
            return None;
        }
        let order = self.order_mut(uid)?;
        if order.vstop_on == enabled
            && order.vstop_fixed == fixed
            && order.vstop_level == level
            && order.vstop_vol == volume
        {
            return None;
        }
        order.vstop_on = enabled;
        order.vstop_fixed = fixed;
        order.vstop_level = level;
        order.vstop_vol = volume;
        Some(OrderCommandPayload::VStop {
            order_id: uid,
            enabled,
            fixed,
            level,
            volume,
        })
    }

    pub(crate) fn send_replace_if_requested(
        &mut self,
        uid: u64,
        new_price: f64,
        now_ms: i64,
    ) -> Option<OrderCommandPayload> {
        let eps_m = self.read.eps_profile.eps_m;
        let order = self.order_mut(uid)?;
        let payload = match order.status {
            OrderWorkerStatus::None => {
                let old = order.pending_buy_cond_price?;
                if (old - new_price).abs() <= eps_m {
                    return None;
                }
                order.pending_buy_cond_price = Some(new_price);
                OrderCommandPayload::TargetBuy {
                    order_id: uid,
                    price: new_price,
                    size: order.buy_size,
                }
            }
            OrderWorkerStatus::BuySet => {
                order.buy_price = new_price;
                order.bulk_replace_buy = true;
                OrderCommandPayload::TargetBuy {
                    order_id: uid,
                    price: new_price,
                    size: if order.buy_size > 0.0 {
                        order.buy_size
                    } else {
                        order.buy_order.quantity
                    },
                }
            }
            OrderWorkerStatus::SellSet => {
                order.sell_price = new_price;
                order.bulk_replace_sell = true;
                OrderCommandPayload::TargetSell {
                    order_id: uid,
                    price: new_price,
                }
            }
            _ => return None,
        };
        order.last_sent_target_is_buy = !matches!(payload, OrderCommandPayload::TargetSell { .. });
        order.last_sent_target_price = new_price;
        order.last_sent_target_size = match payload {
            OrderCommandPayload::TargetBuy { size, .. } => size,
            _ => 0.0,
        };
        order.replace_sent_time_ms = now_ms.max(1);
        let due_ms = order
            .replace_sent_time_ms
            .saturating_add(TARGET_CONFIRM_TIMEOUT_MS)
            .saturating_add(1);
        self.next_replace_timeout_ms = Some(
            self.next_replace_timeout_ms
                .map_or(due_ms, |current| current.min(due_ms)),
        );
        Some(payload)
    }

    pub(crate) fn send_cancel_if_requested(&mut self, uid: u64) -> Option<OrderCommandPayload> {
        let order = self.order_mut(uid)?;
        let payload = match order.status {
            OrderWorkerStatus::None => OrderCommandPayload::PendingCancel { order_id: uid },
            OrderWorkerStatus::BuySet => OrderCommandPayload::CancelBuy { order_id: uid },
            OrderWorkerStatus::SellSet => OrderCommandPayload::CancelSell { order_id: uid },
            _ => return None,
        };
        order.pending_cancel = order.status == OrderWorkerStatus::None;
        Some(payload)
    }

    pub(crate) fn send_panic_sell_if_changed(
        &mut self,
        uid: u64,
        enabled: bool,
    ) -> Option<OrderCommandPayload> {
        let order = self.order_mut(uid)?;
        if order.status != OrderWorkerStatus::SellSet || order.panic_sell == enabled {
            return None;
        }
        order.panic_sell = enabled;
        Some(OrderCommandPayload::Panic {
            order_id: uid,
            enabled,
        })
    }

    pub(crate) fn switch_panic_sell_by_market(
        &mut self,
        market_name: &str,
        turn_on: bool,
    ) -> Vec<OrderCommandPayload> {
        let uids: Vec<_> = self
            .read
            .map
            .values()
            .filter(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::SellSet
            })
            .map(|order| order.uid)
            .collect();
        let any_on = uids.iter().any(|uid| self.read.map[uid].panic_sell);
        if uids.is_empty() || (!any_on && !turn_on) {
            return Vec::new();
        }

        let enabled = !any_on;
        let mut commands = Vec::with_capacity(uids.len());
        let mut stop_commands = Vec::new();
        for uid in uids {
            let (clear_auto_stops, mut stops, vstop) = {
                let order = &self.read.map[&uid];
                (
                    !enabled && order.panic_sell_auto,
                    order.stops,
                    (order.vstop_fixed, order.vstop_level, order.vstop_vol),
                )
            };

            let order = self.order_mut(uid).expect("retained order disappeared");
            order.panic_sell = enabled;
            if clear_auto_stops {
                order.panic_sell_auto = false;
            }
            commands.push(OrderCommandPayload::Panic {
                order_id: uid,
                enabled,
            });

            if clear_auto_stops {
                // The production client changes these visual fields and its
                // normal stop detector emits the grouped setters. Active Lib
                // has no polling detector, so the same accepted gesture emits
                // those setters directly while preserving every numeric field.
                stops.stop_loss_on = DelphiBool::FALSE;
                stops.trailing_on = DelphiBool::FALSE;
                if let Some(payload) = self.send_stops_if_changed(uid, &stops) {
                    stop_commands.push(payload);
                }
                if let Some(payload) =
                    self.send_vstop_if_changed(uid, false, vstop.0, vstop.1, vstop.2)
                {
                    stop_commands.push(payload);
                }
            }
        }
        // The production client sends the accepted Panic decision to every
        // matching worker first; its regular stop detector publishes the
        // resulting Stops/VStop changes afterwards.
        commands.extend(stop_commands);
        commands
    }

    pub(crate) fn mark_panic_sell_all(&mut self) -> bool {
        let uids: Vec<_> = self
            .read
            .map
            .values()
            .filter(|order| order.status == OrderWorkerStatus::SellSet && !order.panic_sell)
            .map(|order| order.uid)
            .collect();
        for uid in &uids {
            self.order_mut(*uid)
                .expect("retained order disappeared")
                .panic_sell = true;
        }
        !uids.is_empty()
    }
}
