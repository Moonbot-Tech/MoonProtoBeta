//! Canonical protocol-v4 order replica and terminal-facing order projection.

use crate::commands::market::{BaseCurrency, ExchangeCode};
use crate::commands::trade::*;
use crate::state::eps::EpsProfile;
use std::collections::HashMap;
use std::sync::Arc;

mod accessors;
mod actions;
mod apply_helpers;
mod maintenance;
mod model;
mod types;

pub use self::model::Order;
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
pub use self::types::ApplyResult;
pub use self::types::{
    MarketPositionProtection, OrderEvent, OrderTraceChartPoint, OrderTraceLine,
    PositionProtectionSide, SellReason,
};

const TARGET_CONFIRM_TIMEOUT_MS: i64 = 5_000;
const SELL_DONE_REMOVAL_GRACE_MS: i64 = 400;
const ORDER_TRACE_LINE_SHRINK_TO: usize = 800;
const ORDER_TRACE_LINE_SHRINK_INTERVAL_MS: i64 = 30_000;
const ORDER_TOMBSTONE_COUNT: usize = 128;

fn order_type_uses_buy_side(order_type: OrderType) -> bool {
    order_type == OrderType::Buy
}

#[derive(Debug, Clone)]
struct OrderMirror {
    desc: OrderDescription,
    state: CanonicalOrderState,
    seen_rev: [u64; ORDER_SECTION_COUNT],
    expected_hash: u32,
}

impl OrderMirror {
    fn new(desc: OrderDescription) -> Self {
        Self {
            desc,
            state: CanonicalOrderState::default(),
            seen_rev: [0; ORDER_SECTION_COUNT],
            expected_hash: 0,
        }
    }

    fn replica_rev(&self) -> u64 {
        self.seen_rev.iter().copied().max().unwrap_or(0)
    }

    fn is_exact(&self) -> bool {
        let rev = self.seen_rev[0];
        rev != 0 && self.seen_rev.iter().all(|seen| *seen == rev)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderRepair {
    pub order_id: u64,
    pub exact_rev: u64,
}

#[derive(Debug, Clone, Copy)]
struct PendingRemoval {
    uid: u64,
    due_ms: i64,
    instance_id: u64,
}

/// Persistent read model published to application snapshots.
///
/// `im::HashMap` keeps snapshot clones O(1) and copies only the changed trie
/// path when one order changes. Protocol mirrors and maintenance state are not
/// part of this value and therefore are never copied for UI publication.
#[derive(Debug, Clone)]
pub struct Orders {
    map: im::HashMap<u64, Arc<Order>>,
    eps_profile: EpsProfile,
}

impl Default for Orders {
    fn default() -> Self {
        Self {
            map: im::HashMap::new(),
            eps_profile: EpsProfile::default(),
        }
    }
}

/// Mutable protocol/order-action state owned by the Active Lib runtime.
#[derive(Debug)]
pub(crate) struct OrderState {
    read: Orders,
    mirrors: HashMap<u64, OrderMirror>,
    pending_removals: Vec<PendingRemoval>,
    tombstones: [u64; ORDER_TOMBSTONE_COUNT],
    tombstone_index: usize,
    world_app_token: u64,
    next_instance_id: u64,
    route_currency: BaseCurrency,
    route_platform: ExchangeCode,
    next_pending_removal_ms: Option<i64>,
    next_replace_timeout_ms: Option<i64>,
    next_order_line_shrink_ms: i64,
}

impl std::ops::Deref for OrderState {
    type Target = Orders;

    fn deref(&self) -> &Self::Target {
        &self.read
    }
}

impl Default for OrderState {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderState {
    pub(crate) fn new() -> Self {
        Self {
            read: Orders::default(),
            mirrors: HashMap::new(),
            pending_removals: Vec::new(),
            tombstones: [0; ORDER_TOMBSTONE_COUNT],
            tombstone_index: 0,
            world_app_token: 0,
            next_instance_id: 0,
            route_currency: BaseCurrency::UNKNOWN,
            route_platform: ExchangeCode::None,
            next_pending_removal_ms: None,
            next_replace_timeout_ms: None,
            next_order_line_shrink_ms: ORDER_TRACE_LINE_SHRINK_INTERVAL_MS,
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.read.eps_profile = eps_profile;
    }

    pub(crate) fn set_route(&mut self, currency: BaseCurrency, platform: ExchangeCode) {
        self.route_currency = currency;
        self.route_platform = platform;
    }

    fn order_mut(&mut self, uid: u64) -> Option<&mut Order> {
        self.read.map.get_mut(&uid).map(Arc::make_mut)
    }

    fn order_arc(&self, uid: u64) -> Option<Arc<Order>> {
        self.read.map.get(&uid).cloned()
    }

    fn remove_order_arc(&mut self, uid: u64) -> Option<Arc<Order>> {
        self.read.map.remove(&uid)
    }

    pub(crate) fn read_model(&self) -> &Orders {
        &self.read
    }

    pub(crate) fn snapshot(&self) -> Orders {
        self.read.clone()
    }

    fn allocate_instance_id(&mut self) -> u64 {
        self.next_instance_id = self.next_instance_id.wrapping_add(1);
        if self.next_instance_id == 0 {
            self.next_instance_id = 1;
        }
        self.next_instance_id
    }

    fn section_seen(&self, uid: u64, section: usize) -> bool {
        self.mirrors
            .get(&uid)
            .is_some_and(|mirror| mirror.seen_rev[section] != 0)
    }

    fn is_tombstoned(&self, uid: u64) -> bool {
        uid != 0 && self.tombstones.contains(&uid)
    }

    fn record_tombstone(&mut self, uid: u64) {
        if uid == 0 || self.is_tombstoned(uid) {
            return;
        }
        self.tombstones[self.tombstone_index] = uid;
        self.tombstone_index = (self.tombstone_index + 1) & (ORDER_TOMBSTONE_COUNT - 1);
    }

    /// Apply one parsed order-channel command with the current transport/world
    /// identity. Follow-up repairs are returned separately from UI events.
    pub(crate) fn apply_protocol(
        &mut self,
        command: TradeCommand,
        now_ms: i64,
        server_token: u64,
        peer_app_token: u64,
        server_time_delta: f64,
        market_exists: &dyn Fn(&str) -> bool,
        events: &mut Vec<OrderEvent>,
        repairs: &mut Vec<OrderRepair>,
    ) {
        if server_token == 0 {
            return;
        }
        self.ensure_world(peer_app_token, events);

        match command {
            TradeCommand::OrderImage(image) => {
                let hash = state_hash(image.state_rev, &image.desc, &image.state);
                self.merge_state(
                    image.header.uid,
                    image.state_rev,
                    hash,
                    image.section_mask,
                    image.state,
                    Some(image.desc),
                    market_exists,
                    now_ms,
                    events,
                    repairs,
                );
            }
            TradeCommand::OrderPatch(patch) => self.merge_state(
                patch.header.uid,
                patch.state_rev,
                patch.state_hash,
                patch.section_mask,
                patch.state,
                None,
                market_exists,
                now_ms,
                events,
                repairs,
            ),
            TradeCommand::OrdersSnapshot(snapshot) => {
                let mut catalog = Vec::with_capacity(snapshot.records.len());
                for record in snapshot.records {
                    let hash = state_hash(record.state_rev, &record.desc, &record.state);
                    catalog.push(OrderCatalogRecord {
                        order_id: record.order_id,
                        state_rev: record.state_rev,
                    });
                    self.merge_state(
                        record.order_id,
                        record.state_rev,
                        hash,
                        record.section_mask,
                        record.state,
                        Some(record.desc),
                        market_exists,
                        now_ms,
                        events,
                        repairs,
                    );
                }
                self.process_cold_page(
                    snapshot.from_uid,
                    snapshot.range_end_uid,
                    &catalog,
                    events,
                    repairs,
                );
                events.push(OrderEvent::Snapshot);
            }
            TradeCommand::OrdersCatalog(catalog) => {
                self.process_cold_page(
                    catalog.from_uid,
                    catalog.range_end_uid,
                    &catalog.records,
                    events,
                    repairs,
                );
                events.push(OrderEvent::Snapshot);
            }
            TradeCommand::OrderNotFound(header) => self.apply_gone(header.uid, events),
            TradeCommand::OrderTracePoint(mut point) => {
                point.adjust_time(server_time_delta);
                if self.mirrors.contains_key(&point.market.base.uid) {
                    if let Some(order) = self
                        .order_mut(point.market.base.uid)
                        .filter(|order| !order.job_is_done)
                    {
                        Self::apply_trace_line(order, &point);
                        events.push(OrderEvent::TracePoint { uid: order.uid });
                    }
                }
            }
            TradeCommand::CorridorUpdate(corridor) => {
                let uid = corridor.market.base.uid;
                if self.mirrors.contains_key(&uid) {
                    if let Some(order) = self.order_mut(uid).filter(|order| !order.job_is_done) {
                        order.is_moon_shot = true;
                        order.corridor_price_down = corridor.price_down;
                        order.corridor_price_up = corridor.price_up;
                        events.push(OrderEvent::CorridorChanged(uid));
                    }
                }
            }
            _ => {}
        }
    }

    fn ensure_world(&mut self, peer_app_token: u64, events: &mut Vec<OrderEvent>) {
        if peer_app_token == 0 || peer_app_token == self.world_app_token {
            return;
        }
        for order in self.read.map.values() {
            events.push(OrderEvent::Removed(Arc::clone(order)));
        }
        self.read.map.clear();
        self.mirrors.clear();
        self.pending_removals.clear();
        self.next_pending_removal_ms = None;
        self.next_replace_timeout_ms = None;
        self.tombstones.fill(0);
        self.tombstone_index = 0;
        self.world_app_token = peer_app_token;
    }

    /// Attach mirrors that arrived before their market was present locally.
    /// The protocol replica remains complete while parked; market-list growth
    /// only materializes its current canonical state for terminal consumers.
    pub(crate) fn rescan_parked(
        &mut self,
        now_ms: i64,
        market_exists: &dyn Fn(&str) -> bool,
        events: &mut Vec<OrderEvent>,
    ) {
        let parked: Vec<u64> = self
            .mirrors
            .iter()
            .filter(|(uid, mirror)| {
                !self.read.map.contains_key(uid) && market_exists(&mirror.desc.market_name())
            })
            .map(|(uid, _)| *uid)
            .collect();
        for uid in parked {
            let instance_id = self.allocate_instance_id();
            let Some(mirror) = self.mirrors.get(&uid) else {
                continue;
            };
            let exact_terminal = mirror.is_exact() && mirror.state.is_terminal();
            let order = Order::from_canonical(
                uid,
                &mirror.desc,
                instance_id,
                self.route_currency,
                self.route_platform,
                &mirror.state,
            );
            let order = Arc::new(order);
            self.read.map.insert(uid, Arc::clone(&order));
            events.push(OrderEvent::Created(order));
            if exact_terminal {
                let delay = if self.read.map[&uid].status == OrderWorkerStatus::SellDone {
                    SELL_DONE_REMOVAL_GRACE_MS
                } else {
                    0
                };
                self.mark_pending_removal(uid, now_ms, delay);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_state(
        &mut self,
        order_id: u64,
        revision: u64,
        expected_hash: u32,
        mask: u16,
        incoming: CanonicalOrderState,
        desc: Option<OrderDescription>,
        market_exists: &dyn Fn(&str) -> bool,
        now_ms: i64,
        events: &mut Vec<OrderEvent>,
        repairs: &mut Vec<OrderRepair>,
    ) {
        let is_image = desc.is_some();
        let is_new = !self.mirrors.contains_key(&order_id);
        if is_new {
            if self.is_tombstoned(order_id) {
                return;
            }
            let Some(desc) = desc else {
                push_repair(repairs, order_id, 0);
                return;
            };
            self.mirrors.insert(order_id, OrderMirror::new(desc));
        } else if let Some(desc) = desc.as_ref() {
            if self
                .mirrors
                .get(&order_id)
                .is_some_and(|mirror| mirror.desc != *desc)
            {
                log::warn!(target: "moonproto::orders", "drop order image {}: description mismatch", order_id);
                return;
            }
        }

        let (applied_mask, exact) = {
            let mirror = self.mirrors.get_mut(&order_id).expect("mirror exists");
            let previous_rev = mirror.replica_rev();
            if revision > previous_rev {
                mirror.expected_hash = expected_hash;
            }
            let mut applied_mask = 0u16;
            for section in 0..ORDER_SECTION_COUNT {
                let carries = is_image || mask & (1 << section) != 0;
                if carries && revision > mirror.seen_rev[section] {
                    mirror.state.copy_section_from(&incoming, section);
                    mirror.seen_rev[section] = revision;
                    applied_mask |= 1 << section;
                }
            }
            let replica_rev = if applied_mask != 0 {
                previous_rev.max(revision)
            } else {
                previous_rev
            };
            let proof_matches =
                state_hash(replica_rev, &mirror.desc, &mirror.state) == mirror.expected_hash;
            if proof_matches {
                mirror.seen_rev.fill(replica_rev);
            } else {
                push_repair(repairs, order_id, 0);
            }
            let exact = if proof_matches {
                replica_rev != 0
            } else {
                mirror.is_exact()
            };
            (applied_mask, exact)
        };

        let should_attach = !self.read.map.contains_key(&order_id)
            && self
                .mirrors
                .get(&order_id)
                .is_some_and(|mirror| market_exists(&mirror.desc.market_name()));
        if applied_mask == 0 && !should_attach {
            return;
        }
        if should_attach {
            let instance_id = self.allocate_instance_id();
            let mirror = self.mirrors.get(&order_id).expect("mirror exists");
            let order = Order::from_canonical(
                order_id,
                &mirror.desc,
                instance_id,
                self.route_currency,
                self.route_platform,
                &mirror.state,
            );
            let order = Arc::new(order);
            self.read.map.insert(order_id, Arc::clone(&order));
            events.push(OrderEvent::Created(order));
        } else if applied_mask != 0 {
            let (mirrors, read) = (&self.mirrors, &mut self.read);
            let Some(mirror) = mirrors.get(&order_id) else {
                return;
            };
            let Some(entry) = read.map.get_mut(&order_id) else {
                return;
            };
            Arc::make_mut(entry).apply_canonical(&mirror.state, applied_mask);
            events.push(OrderEvent::Updated(Arc::clone(entry)));
        }

        if exact
            && self
                .read
                .map
                .get(&order_id)
                .is_some_and(|order| order.status.is_terminal())
        {
            let delay = if self.read.map[&order_id].status == OrderWorkerStatus::SellDone {
                SELL_DONE_REMOVAL_GRACE_MS
            } else {
                0
            };
            self.mark_pending_removal(order_id, now_ms, delay);
        }
    }

    fn process_cold_page(
        &mut self,
        from_uid: u64,
        range_end_uid: u64,
        catalog: &[OrderCatalogRecord],
        events: &mut Vec<OrderEvent>,
        repairs: &mut Vec<OrderRepair>,
    ) {
        for item in catalog {
            let Some(mirror) = self.mirrors.get(&item.order_id) else {
                if !self.is_tombstoned(item.order_id) {
                    push_repair(repairs, item.order_id, 0);
                }
                continue;
            };
            if mirror.state.is_terminal() && mirror.is_exact() {
                continue;
            }
            if !mirror.is_exact() {
                push_repair(repairs, item.order_id, 0);
            } else if mirror.replica_rev() != item.state_rev {
                push_repair(repairs, item.order_id, mirror.replica_rev());
            } else {
                let (mirrors, read) = (&self.mirrors, &mut self.read);
                let Some(mirror) = mirrors.get(&item.order_id) else {
                    continue;
                };
                if let Some(entry) = read.map.get_mut(&item.order_id) {
                    Arc::make_mut(entry).apply_canonical(&mirror.state, ORDER_RECONCILE_MASK);
                    events.push(OrderEvent::Updated(Arc::clone(entry)));
                }
            }
        }

        for (&uid, mirror) in &self.mirrors {
            if uid < from_uid || (range_end_uid != 0 && uid > range_end_uid) {
                continue;
            }
            let exact = mirror.is_exact();
            if mirror.state.is_terminal() && exact {
                continue;
            }
            if catalog
                .binary_search_by_key(&uid, |record| record.order_id)
                .is_err()
            {
                push_repair(repairs, uid, if exact { mirror.replica_rev() } else { 0 });
            }
        }
    }

    fn apply_gone(&mut self, order_id: u64, events: &mut Vec<OrderEvent>) {
        let Some(mirror) = self.mirrors.get(&order_id) else {
            self.record_tombstone(order_id);
            return;
        };
        if mirror.state.is_terminal() {
            return;
        }
        self.mirrors.remove(&order_id);
        self.pending_removals
            .retain(|pending| pending.uid != order_id);
        self.record_tombstone(order_id);
        if let Some(order) = self.remove_order_arc(order_id) {
            events.push(OrderEvent::Removed(order));
        }
    }
}

fn push_repair(repairs: &mut Vec<OrderRepair>, order_id: u64, exact_rev: u64) {
    // Delphi appends repair intents linearly and sends them after releasing the
    // mirror lock. Duplicate snapshot/proof requests are harmless and cheaper
    // than an O(R^2) uniqueness scan on a large cold page.
    repairs.push(OrderRepair {
        order_id,
        exact_rev,
    });
}

#[cfg(test)]
mod tests;
