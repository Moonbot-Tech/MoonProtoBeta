//! Strategy schema apply and public accessors.

use super::{StratEvent, StratsState};
use crate::commands::strategy_schema::StrategySchema;
use std::collections::HashMap;
use std::sync::Arc;

impl StratsState {
    pub(super) fn apply_schema_raw(&mut self, data: Vec<u8>) -> StratEvent {
        let raw_len = data.len();
        match StrategySchema::parse_compressed(&data) {
            Some(schema) => {
                let format_version = schema.format_version;
                let kind_count = schema.kinds.len();
                let field_count = schema.fields.len();
                let field_types = schema
                    .fields
                    .iter()
                    .map(|field| (field.name.clone(), field.raw_type_id))
                    .collect::<HashMap<_, _>>();
                self.schema_raw = Some(Arc::new(data));
                self.schema = Some(Arc::new(schema));
                self.schema_field_types = Some(Arc::new(field_types));
                self.invalidate_snapshot_payload_cache();
                self.schema_revision = self.schema_revision.saturating_add(1);
                self.schema_last_error = None;
                StratEvent::SchemaApplied {
                    raw_len,
                    format_version,
                    kind_count,
                    field_count,
                }
            }
            None => {
                self.schema_failures = self.schema_failures.saturating_add(1);
                self.schema_last_error = Some(format!(
                    "failed to parse TStratSchema raw blob ({raw_len} bytes)"
                ));
                StratEvent::SchemaParseFailed { raw_len }
            }
        }
    }

    /// Последняя schema стратегий, полученная через `TStratSchemaRequest` в Init.
    pub fn strategy_schema(&self) -> Option<&StrategySchema> {
        self.schema.as_deref()
    }

    /// Raw-deflate blob последней schema, как пришёл в `TStratSchema.Data`.
    pub fn strategy_schema_raw(&self) -> Option<&[u8]> {
        self.schema_raw.as_deref().map(Vec::as_slice)
    }

    pub fn strategy_schema_revision(&self) -> u64 {
        self.schema_revision
    }

    pub fn strategy_schema_failures(&self) -> u64 {
        self.schema_failures
    }

    pub fn strategy_schema_last_error(&self) -> Option<&str> {
        self.schema_last_error.as_deref()
    }
}
