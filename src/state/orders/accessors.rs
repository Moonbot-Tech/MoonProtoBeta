//! Read-only accessors for the orders read-model.

use super::{MarketPositionProtection, Order, Orders, PositionProtectionSide};
use crate::commands::trade::{FixedPosition, OrderWorkerStatus};

impl Orders {
    /// Get one order by UID.
    pub fn get(&self, uid: u64) -> Option<&Order> {
        self.map.get(&uid).map(AsRef::as_ref)
    }

    /// Iterate all retained orders.
    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.map.values().map(AsRef::as_ref)
    }

    /// Number of retained orders.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Current snapshot flag value.
    pub fn current_snapshot_flag(&self) -> u8 {
        self.current_snapshot_flag
    }

    /// Delphi `TOrdersWorkers.TotalSellQuantity(m, PositionMode)`.
    ///
    /// Counts active non-emulator `OS_SellSet` workers for the requested market
    /// and side, summing `pSellOrder.QuantityRemaining`.
    pub fn total_sell_quantity(&self, market_name: &str, side: FixedPosition) -> f64 {
        self.map
            .values()
            .map(AsRef::as_ref)
            .filter(|order| Self::is_proper_sell_for_position(order, market_name, side))
            .map(|order| order.sell_order.quantity_remaining)
            .sum()
    }

    /// Delphi chart warning helper built from `TotalSellQuantity`.
    ///
    /// The UI should use this instead of reading a position snapshot and then
    /// manually scanning all orders on every render tick.
    pub fn position_protection(
        &self,
        market_name: &str,
        both_pos_size: f64,
        long_pos_size: f64,
        short_pos_size: f64,
    ) -> MarketPositionProtection {
        MarketPositionProtection {
            both: self.position_protection_side(market_name, FixedPosition::Both, both_pos_size),
            long: self.position_protection_side(market_name, FixedPosition::Long, long_pos_size),
            short: self.position_protection_side(market_name, FixedPosition::Short, short_pos_size),
        }
    }

    fn position_protection_side(
        &self,
        market_name: &str,
        side: FixedPosition,
        position_size: f64,
    ) -> PositionProtectionSide {
        let closing_sell_quantity = self.total_sell_quantity(market_name, side);
        let difference = position_size - closing_sell_quantity;
        PositionProtectionSide {
            side,
            position_size,
            closing_sell_quantity,
            difference,
            missing_quantity: difference.max(0.0),
            has_warning: position_size > self.eps_profile.eps_m
                && difference.abs() > self.eps_profile.eps_m,
        }
    }

    fn is_proper_sell_for_position(order: &Order, market_name: &str, side: FixedPosition) -> bool {
        if order.market_name != market_name
            || order.emulator_mode
            || order.status != OrderWorkerStatus::SellSet
        {
            return false;
        }
        match side {
            FixedPosition::Long => !order.is_short,
            FixedPosition::Short => order.is_short,
            _ => true,
        }
    }
}
