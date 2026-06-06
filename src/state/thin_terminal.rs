use std::collections::HashMap;

use crate::commands::ui::{AlertObjectCommand, ChartTextSnapshotCommand, ChartTextStateCommand};

/// Accepted chart alert object owned by the MoonProto core.
///
/// The blob is exactly Delphi `TChartObject.Save`. Rust does not invent a
/// partial chart-object schema here: terminal code that understands chart
/// objects can decode the blob; other UIs can keep it as an authoritative
/// opaque snapshot and key it by `(market_name, obj_uid)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertObjectSnapshot {
    pub market_name: String,
    pub obj_uid: u64,
    pub blob: Vec<u8>,
}

/// Change in the authoritative chart-alert set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertObjectEvent {
    Upserted(AlertObjectSnapshot),
    Deleted { market_name: String, obj_uid: u64 },
}

impl AlertObjectEvent {
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

/// Last full chart-text replacement sent by the core for one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChartTextSnapshot {
    pub market_name: String,
    pub filter_lines: Vec<String>,
    pub debug_lines: Vec<String>,
}

/// Retained thin-terminal UI state.
#[derive(Debug, Clone, Default)]
pub struct ThinTerminalState {
    alerts_by_market: HashMap<String, HashMap<u64, AlertObjectSnapshot>>,
    chart_text_by_market: HashMap<String, ChartTextSnapshot>,
    chart_text_request: Option<ChartTextRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChartTextRequest {
    market_name: String,
    need_filters: bool,
    need_debug_lines: bool,
}

impl ThinTerminalState {
    pub fn alert_object(&self, market_name: &str, obj_uid: u64) -> Option<&AlertObjectSnapshot> {
        self.alerts_by_market.get(market_name)?.get(&obj_uid)
    }

    pub fn alert_objects_for_market(
        &self,
        market_name: &str,
    ) -> impl Iterator<Item = &AlertObjectSnapshot> {
        self.alerts_by_market
            .get(market_name)
            .into_iter()
            .flat_map(|items| items.values())
    }

    pub fn chart_text(&self, market_name: &str) -> Option<&ChartTextSnapshot> {
        self.chart_text_by_market.get(market_name)
    }

    pub(crate) fn set_chart_text_state(&mut self, cmd: &ChartTextStateCommand) -> bool {
        let next = if cmd.market_name.is_empty() || !(cmd.need_filters || cmd.need_debug_lines) {
            None
        } else {
            Some(ChartTextRequest {
                market_name: cmd.market_name.clone(),
                need_filters: cmd.need_filters,
                need_debug_lines: cmd.need_debug_lines,
            })
        };
        if self.chart_text_request == next {
            return false;
        }
        if let Some(request) = &next {
            self.chart_text_by_market.remove(&request.market_name);
        } else {
            self.chart_text_by_market.clear();
        }
        self.chart_text_request = next;
        true
    }

    pub(crate) fn apply_alert_object(
        &mut self,
        cmd: AlertObjectCommand,
    ) -> Option<AlertObjectEvent> {
        if cmd.skipped() || cmd.obj_uid == 0 {
            return None;
        }
        if !cmd.upsert {
            if let Some(items) = self.alerts_by_market.get_mut(&cmd.market_name) {
                items.remove(&cmd.obj_uid);
                if items.is_empty() {
                    self.alerts_by_market.remove(&cmd.market_name);
                }
            }
            return Some(AlertObjectEvent::Deleted {
                market_name: cmd.market_name,
                obj_uid: cmd.obj_uid,
            });
        }
        if cmd.blob.len() < 4 {
            return None;
        }
        let snapshot = AlertObjectSnapshot {
            market_name: cmd.market_name,
            obj_uid: cmd.obj_uid,
            blob: cmd.blob,
        };
        self.alerts_by_market
            .entry(snapshot.market_name.clone())
            .or_default()
            .insert(snapshot.obj_uid, snapshot.clone());
        Some(AlertObjectEvent::Upserted(snapshot))
    }

    pub(crate) fn apply_chart_text_snapshot(
        &mut self,
        cmd: ChartTextSnapshotCommand,
    ) -> Option<ChartTextSnapshot> {
        let Some(request) = &self.chart_text_request else {
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
        self.chart_text_by_market
            .insert(snapshot.market_name.clone(), snapshot.clone());
        Some(snapshot)
    }
}
