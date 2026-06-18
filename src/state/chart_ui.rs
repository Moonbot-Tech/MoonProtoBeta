use std::collections::HashMap;

use crate::commands::ui::{AlertObjectCommand, ChartTextSnapshotCommand, ChartTextStateCommand};

/// Accepted chart-alert object owned by the MoonProto core.
///
/// The blob is the authoritative saved chart-object payload from the core.
/// The SDK deliberately does not invent a partial chart-object schema here:
/// UI code that understands chart objects can decode the blob; other UIs can
/// keep it as an opaque snapshot keyed by `(market_name, obj_uid)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChartAlertObject {
    pub market_name: String,
    pub obj_uid: u64,
    pub blob: Vec<u8>,
}

/// Change in the authoritative chart-alert set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChartAlertEvent {
    Upserted(ChartAlertObject),
    Deleted { market_name: String, obj_uid: u64 },
}

impl ChartAlertEvent {
    pub fn market_name(&self) -> &str {
        match self {
            Self::Upserted(obj) => &obj.market_name,
            Self::Deleted { market_name, .. } => market_name,
        }
    }

    pub fn obj_uid(&self) -> u64 {
        match self {
            Self::Upserted(obj) => obj.obj_uid,
            Self::Deleted { obj_uid, .. } => *obj_uid,
        }
    }
}

/// Retained authoritative chart-alert state.
#[derive(Debug, Clone, Default)]
pub struct ChartAlertsState {
    by_market: HashMap<String, HashMap<u64, ChartAlertObject>>,
}

impl ChartAlertsState {
    pub fn get(&self, market_name: &str, obj_uid: u64) -> Option<&ChartAlertObject> {
        self.by_market.get(market_name)?.get(&obj_uid)
    }

    pub fn for_market(&self, market_name: &str) -> impl Iterator<Item = &ChartAlertObject> {
        self.by_market
            .get(market_name)
            .into_iter()
            .flat_map(|items| items.values())
    }

    pub(crate) fn apply(&mut self, cmd: AlertObjectCommand) -> Option<ChartAlertEvent> {
        if cmd.skipped() || cmd.obj_uid == 0 {
            return None;
        }
        if !cmd.upsert {
            if let Some(items) = self.by_market.get_mut(&cmd.market_name) {
                items.remove(&cmd.obj_uid);
                if items.is_empty() {
                    self.by_market.remove(&cmd.market_name);
                }
            }
            return Some(ChartAlertEvent::Deleted {
                market_name: cmd.market_name,
                obj_uid: cmd.obj_uid,
            });
        }
        if cmd.blob.len() < 4 {
            return None;
        }
        let snapshot = ChartAlertObject {
            market_name: cmd.market_name,
            obj_uid: cmd.obj_uid,
            blob: cmd.blob,
        };
        self.by_market
            .entry(snapshot.market_name.clone())
            .or_default()
            .insert(snapshot.obj_uid, snapshot.clone());
        Some(ChartAlertEvent::Upserted(snapshot))
    }
}

/// Last full chart-text replacement sent by the core for one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChartTextSnapshot {
    pub market_name: String,
    pub filter_lines: Vec<String>,
    pub debug_lines: Vec<String>,
}

/// Retained chart-text rows for markets requested by this client.
#[derive(Debug, Clone, Default)]
pub struct ChartTextState {
    by_market: HashMap<String, ChartTextSnapshot>,
    request: Option<ChartTextRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChartTextRequest {
    market_name: String,
    need_filters: bool,
    need_debug_lines: bool,
}

impl ChartTextState {
    pub fn get(&self, market_name: &str) -> Option<&ChartTextSnapshot> {
        self.by_market.get(market_name)
    }

    pub fn for_market(&self, market_name: &str) -> Option<&ChartTextSnapshot> {
        self.get(market_name)
    }

    pub(crate) fn set_visible_market(&mut self, cmd: &ChartTextStateCommand) -> bool {
        let next = if cmd.market_name.is_empty() || !(cmd.need_filters || cmd.need_debug_lines) {
            None
        } else {
            Some(ChartTextRequest {
                market_name: cmd.market_name.clone(),
                need_filters: cmd.need_filters,
                need_debug_lines: cmd.need_debug_lines,
            })
        };
        if self.request == next {
            return false;
        }
        if let Some(request) = &next {
            self.by_market.remove(&request.market_name);
        } else {
            self.by_market.clear();
        }
        self.request = next;
        true
    }

    pub(crate) fn apply_snapshot(
        &mut self,
        cmd: ChartTextSnapshotCommand,
    ) -> Option<ChartTextSnapshot> {
        let Some(request) = &self.request else {
            return None;
        };
        if !request.market_name.eq_ignore_ascii_case(&cmd.market_name) {
            return None;
        }
        let snapshot = ChartTextSnapshot {
            market_name: cmd.market_name,
            filter_lines: cmd.filter_lines,
            debug_lines: cmd.debug_lines,
        };
        self.by_market
            .insert(snapshot.market_name.clone(), snapshot.clone());
        Some(snapshot)
    }
}
