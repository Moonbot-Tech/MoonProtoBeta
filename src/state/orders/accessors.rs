//! Read-only accessors for the orders read-model.

use super::{MarketPositionProtection, Order, Orders, PositionProtectionSide};
use crate::commands::trade::{OrderWorkerStatus, PositionFilter};
use std::sync::Arc;

impl Orders {
    pub fn new() -> Self {
        Self::default()
    }

    /// Clear an application-owned read model.
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Remove one order from an application-owned read model.
    pub fn remove(&mut self, uid: u64) -> Option<Order> {
        self.map
            .remove(&uid)
            .map(|order| Arc::try_unwrap(order).unwrap_or_else(|order| (*order).clone()))
    }

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

    /// Total active closing sell quantity for one market and position side.
    ///
    /// Counts active non-emulator `OS_SellSet` workers for the requested market
    /// and side, summing `pSellOrder.QuantityRemaining`.
    pub fn total_sell_quantity(&self, market_name: &str, side: PositionFilter) -> f64 {
        self.map
            .values()
            .map(AsRef::as_ref)
            .filter(|order| Self::is_proper_sell_for_position(order, market_name, side))
            .map(|order| order.sell_order.quantity_remaining)
            .sum()
    }

    /// Chart position-protection helper built from retained orders.
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
            both: self.position_protection_side(market_name, PositionFilter::Both, both_pos_size),
            long: self.position_protection_side(market_name, PositionFilter::Long, long_pos_size),
            short: self.position_protection_side(
                market_name,
                PositionFilter::Short,
                short_pos_size,
            ),
        }
    }

    fn position_protection_side(
        &self,
        market_name: &str,
        side: PositionFilter,
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

    fn is_proper_sell_for_position(order: &Order, market_name: &str, side: PositionFilter) -> bool {
        if order.market_name != market_name
            || order.emulator_mode
            || order.status != OrderWorkerStatus::SellSet
        {
            return false;
        }
        match side {
            PositionFilter::Long => !order.is_short,
            PositionFilter::Short => order.is_short,
            PositionFilter::Both => true,
        }
    }
}
