//! Typed report-DB replication state.

use crate::commands::registry::decode_utf8_delphi;
use crate::commands::report::{RepSchema as WireSchema, RepSyncBatch, RepSyncDone};
use crate::compression::synlz_decompress;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

const REPORT_SCHEMA_FORMAT_VERSION: u8 = 1;
const REPORT_TEXT_MAX_BYTES: usize = 8192;
const REPORT_REC_ID_FIELD_NAME: &str = "newRecID";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReportFieldKind {
    Integer,
    Float,
    Text,
}

impl ReportFieldKind {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Integer),
            2 => Some(Self::Float),
            3 => Some(Self::Text),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSchemaField {
    pub index: u16,
    pub name: String,
    pub kind: ReportFieldKind,
    pub sql_spec: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSchema {
    format_version: u8,
    fields: Arc<[ReportSchemaField]>,
    rec_id_field_index: u16,
}

impl ReportSchema {
    pub fn format_version(&self) -> u8 {
        self.format_version
    }

    /// Append-only field count. It also serves as the schema revision.
    pub fn revision(&self) -> usize {
        self.fields.len()
    }

    pub fn fields(&self) -> &[ReportSchemaField] {
        &self.fields
    }

    pub fn field(&self, index: u16) -> Option<&ReportSchemaField> {
        self.fields.get(usize::from(index))
    }

    pub fn field_by_name(&self, name: &str) -> Option<&ReportSchemaField> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn rec_id_field_index(&self) -> u16 {
        self.rec_id_field_index
    }

    /// Build a SQLite table with `newRecID` as its stable primary key.
    ///
    /// `sql_spec` comes from the authenticated MoonBot core schema. Identifiers
    /// are quoted locally; the helper does not open or own an application DB.
    pub fn sqlite_create_table_sql(&self, table: &str) -> String {
        let table = quote_sqlite_identifier(table);
        let mut columns = self
            .fields
            .iter()
            .map(|field| {
                format!(
                    "{} {}",
                    quote_sqlite_identifier(&field.name),
                    field.sql_spec
                )
            })
            .collect::<Vec<_>>();
        columns.push(format!(
            "PRIMARY KEY ({})",
            quote_sqlite_identifier(REPORT_REC_ID_FIELD_NAME)
        ));
        format!("CREATE TABLE {table} ({})", columns.join(", "))
    }

    pub fn sqlite_add_column_sql(&self, table: &str, field: &ReportSchemaField) -> String {
        format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            quote_sqlite_identifier(table),
            quote_sqlite_identifier(&field.name),
            field.sql_spec
        )
    }

    pub fn sqlite_unique_index_sql(&self, table: &str) -> String {
        let index_name = format!("I{}_newRecID", table);
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})",
            quote_sqlite_identifier(&index_name),
            quote_sqlite_identifier(table),
            quote_sqlite_identifier(REPORT_REC_ID_FIELD_NAME)
        )
    }

    fn parse_compressed(data: &[u8]) -> Option<Self> {
        let decoded = synlz_decompress(data)?;
        let mut pos = 0usize;
        let format_version = read_u8(&decoded, &mut pos)?;
        if format_version != REPORT_SCHEMA_FORMAT_VERSION {
            return None;
        }
        let field_count = usize::from(read_u16(&decoded, &mut pos)?);
        let mut fields = Vec::new();
        fields.try_reserve_exact(field_count).ok()?;
        let mut names = HashSet::new();
        names.try_reserve(field_count).ok()?;
        let mut rec_id_field_index = None;
        for index in 0..field_count {
            let name = read_str8(&decoded, &mut pos)?;
            if !names.insert(name.clone()) {
                return None;
            }
            let kind = ReportFieldKind::from_wire(read_u8(&decoded, &mut pos)?)?;
            let sql_spec = read_str8(&decoded, &mut pos)?;
            let index = u16::try_from(index).ok()?;
            if name == REPORT_REC_ID_FIELD_NAME {
                if kind != ReportFieldKind::Integer {
                    return None;
                }
                rec_id_field_index = Some(index);
            }
            fields.push(ReportSchemaField {
                index,
                name,
                kind,
                sql_spec,
            });
        }
        if pos != decoded.len() {
            return None;
        }
        Some(Self {
            format_version,
            fields: fields.into(),
            rec_id_field_index: rec_id_field_index?,
        })
    }

    fn is_append_only_successor_of(&self, previous: &Self) -> bool {
        self.fields.len() >= previous.fields.len()
            && self
                .fields
                .iter()
                .zip(previous.fields.iter())
                .all(|(new, old)| {
                    new.index == old.index
                        && new.name == old.name
                        && new.kind == old.kind
                        && new.sql_spec == old.sql_spec
                })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReportValue {
    Integer(i64),
    Float(f64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReportFieldValue {
    pub field_index: u16,
    pub value: ReportValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReportRow {
    pub rec_id: i64,
    pub fields: Vec<ReportFieldValue>,
}

impl ReportRow {
    pub fn value(&self, field_index: u16) -> Option<&ReportValue> {
        self.fields
            .binary_search_by_key(&field_index, |field| field.field_index)
            .ok()
            .map(|index| &self.fields[index].value)
    }

    pub fn value_by_name<'a>(
        &'a self,
        schema: &ReportSchema,
        name: &str,
    ) -> Option<&'a ReportValue> {
        self.value(schema.field_by_name(name)?.index)
    }

    pub fn integer_by_name(&self, schema: &ReportSchema, name: &str) -> Option<i64> {
        match self.value_by_name(schema, name)? {
            ReportValue::Integer(value) => Some(*value),
            _ => None,
        }
    }

    pub fn float_by_name(&self, schema: &ReportSchema, name: &str) -> Option<f64> {
        match self.value_by_name(schema, name)? {
            ReportValue::Float(value) => Some(*value),
            _ => None,
        }
    }

    pub fn text_by_name<'a>(&'a self, schema: &ReportSchema, name: &str) -> Option<&'a str> {
        match self.value_by_name(schema, name)? {
            ReportValue::Text(value) => Some(value),
            _ => None,
        }
    }

    fn parse(data: &[u8], rec_id_field_index: u16, outer_rec_id: Option<i64>) -> Option<Self> {
        let mut pos = 0usize;
        let row = Self::parse_from(data, &mut pos, rec_id_field_index, outer_rec_id)?;
        (pos == data.len()).then_some(row)
    }

    fn parse_from(
        data: &[u8],
        pos: &mut usize,
        rec_id_field_index: u16,
        outer_rec_id: Option<i64>,
    ) -> Option<Self> {
        let field_count = usize::from(read_u16(data, pos)?);
        let mut fields = Vec::new();
        fields.try_reserve_exact(field_count).ok()?;
        let mut row_rec_id = None;
        for _ in 0..field_count {
            let field_index = read_u16(data, pos)?;
            let kind = ReportFieldKind::from_wire(read_u8(data, pos)?)?;
            let value = match kind {
                ReportFieldKind::Integer => ReportValue::Integer(read_i64(data, pos)?),
                ReportFieldKind::Float => ReportValue::Float(read_f64(data, pos)?),
                ReportFieldKind::Text => {
                    let len = usize::from(read_u16(data, pos)?);
                    if len > REPORT_TEXT_MAX_BYTES {
                        return None;
                    }
                    let end = pos.checked_add(len)?;
                    if end > data.len() {
                        return None;
                    }
                    let value = decode_utf8_delphi(&data[*pos..end]);
                    *pos = end;
                    ReportValue::Text(value)
                }
            };
            if field_index == rec_id_field_index {
                let ReportValue::Integer(value) = value else {
                    return None;
                };
                row_rec_id = Some(value);
                fields.push(ReportFieldValue {
                    field_index,
                    value: ReportValue::Integer(value),
                });
            } else {
                fields.push(ReportFieldValue { field_index, value });
            }
        }
        fields.sort_unstable_by_key(|field| field.field_index);
        if fields
            .windows(2)
            .any(|pair| pair[0].field_index == pair[1].field_index)
        {
            return None;
        }
        let rec_id = outer_rec_id.or(row_rec_id)?;
        if rec_id <= 0 || row_rec_id.is_some_and(|row| row != rec_id) {
            return None;
        }
        Some(Self { rec_id, fields })
    }

    fn parse_many(data: &[u8], count: u16, rec_id_field_index: u16) -> Option<Vec<Self>> {
        let mut pos = 0usize;
        let mut rows = Vec::new();
        rows.try_reserve_exact(usize::from(count)).ok()?;
        for _ in 0..count {
            rows.push(Self::parse_from(data, &mut pos, rec_id_field_index, None)?);
        }
        if pos != data.len() {
            return None;
        }
        Some(rows)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportHistoryDepth {
    ServerDefault,
    Days(u16),
    All,
}

impl ReportHistoryDepth {
    pub(crate) fn to_wire(self) -> u16 {
        match self {
            Self::ServerDefault => 0,
            Self::Days(days) => days,
            Self::All => u16::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportSyncRequest {
    pub from_rec_id: i64,
    pub history_depth: ReportHistoryDepth,
}

impl ReportSyncRequest {
    pub fn fresh(history_depth: ReportHistoryDepth) -> Self {
        Self {
            from_rec_id: 0,
            history_depth,
        }
    }

    pub fn resume(from_rec_id: i64) -> Self {
        Self {
            from_rec_id,
            history_depth: ReportHistoryDepth::ServerDefault,
        }
    }

    pub(crate) fn is_valid(self) -> bool {
        self.from_rec_id >= 0
            && !matches!(self.history_depth, ReportHistoryDepth::Days(0 | u16::MAX))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReportSyncTicket {
    pub request_uid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSyncComplete {
    pub request_uid: u64,
    pub from_rec_id: i64,
    pub batch_count: u16,
    pub total_rows: u32,
    pub max_rec_id: i64,
    pub keep_rec_ids: Arc<[i64]>,
    pub database_recreated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReportEvent {
    Schema(Arc<ReportSchema>),
    RowUpsert(ReportRow),
    RowDelete {
        rec_id: i64,
    },
    SyncStarted {
        ticket: ReportSyncTicket,
        request: ReportSyncRequest,
    },
    SyncComplete(ReportSyncComplete),
    SchemaRejected {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy)]
struct SyncDoneState {
    batch_count: u16,
    total_rows: u32,
    max_rec_id: i64,
}

#[derive(Debug)]
struct ActiveSync {
    ticket: ReportSyncTicket,
    request: ReportSyncRequest,
    received_batches: HashSet<u16>,
    parsed_rows: u32,
    keep_rec_ids: HashSet<i64>,
    live_touched: HashSet<i64>,
    done: Option<SyncDoneState>,
}

impl ActiveSync {
    fn new(ticket: ReportSyncTicket, request: ReportSyncRequest) -> Self {
        Self {
            ticket,
            request,
            received_batches: HashSet::new(),
            parsed_rows: 0,
            keep_rec_ids: HashSet::new(),
            live_touched: HashSet::new(),
            done: None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReportControl {
    SendSync {
        ticket: ReportSyncTicket,
        request: ReportSyncRequest,
    },
    SyncCompleted {
        request_uid: u64,
    },
    SyncProgress {
        request_uid: u64,
    },
}

#[derive(Default)]
pub(crate) struct ReportReplicationState {
    schema: Option<Arc<ReportSchema>>,
    pending_after_schema: Option<(ReportSyncTicket, ReportSyncRequest)>,
    active: Option<ActiveSync>,
}

impl ReportReplicationState {
    pub(crate) fn schema(&self) -> Option<&Arc<ReportSchema>> {
        self.schema.as_ref()
    }

    pub(crate) fn defer_sync_until_schema(
        &mut self,
        ticket: ReportSyncTicket,
        request: ReportSyncRequest,
    ) {
        self.pending_after_schema = Some((ticket, request));
        self.active = None;
    }

    pub(crate) fn begin_sync(
        &mut self,
        ticket: ReportSyncTicket,
        request: ReportSyncRequest,
        out: &mut Vec<ReportEvent>,
    ) {
        self.pending_after_schema = None;
        self.active = Some(ActiveSync::new(ticket, request));
        out.push(ReportEvent::SyncStarted { ticket, request });
    }

    pub(crate) fn apply_schema(
        &mut self,
        wire: WireSchema,
        out: &mut Vec<ReportEvent>,
        controls: &mut Vec<ReportControl>,
    ) -> bool {
        let Some(schema) = ReportSchema::parse_compressed(&wire.data) else {
            return false;
        };
        if let Some(previous) = self.schema.as_deref() {
            if !schema.is_append_only_successor_of(previous) {
                out.push(ReportEvent::SchemaRejected {
                    reason: "schema changed an existing field; append-only invariant violated"
                        .to_string(),
                });
                return true;
            }
        }
        let schema = Arc::new(schema);
        self.schema = Some(Arc::clone(&schema));
        out.push(ReportEvent::Schema(schema));
        if let Some((ticket, request)) = self.pending_after_schema.take() {
            self.begin_sync(ticket, request, out);
            controls.push(ReportControl::SendSync { ticket, request });
        }
        true
    }

    pub(crate) fn apply_live_upsert(
        &mut self,
        rec_id: i64,
        raw: &[u8],
        out: &mut Vec<ReportEvent>,
    ) -> bool {
        let Some(schema) = self.schema.as_deref() else {
            // Asking for the schema also enables live delivery on the server.
            // A live row may overtake that sliced schema on UDP. The deferred
            // catch-up starts after schema validation and carries the current
            // row, so publishing an unmappable row or buffering it is unnecessary.
            return true;
        };
        let Some(row) = ReportRow::parse(raw, schema.rec_id_field_index, Some(rec_id)) else {
            return false;
        };
        if let Some(active) = self.active.as_mut() {
            active.live_touched.insert(rec_id);
            active.keep_rec_ids.insert(rec_id);
        }
        out.push(ReportEvent::RowUpsert(row));
        true
    }

    pub(crate) fn apply_live_delete(&mut self, rec_id: i64, out: &mut Vec<ReportEvent>) -> bool {
        if rec_id <= 0 {
            return false;
        }
        if self.schema.is_none() {
            return true;
        }
        if let Some(active) = self.active.as_mut() {
            active.live_touched.insert(rec_id);
            active.keep_rec_ids.remove(&rec_id);
        }
        out.push(ReportEvent::RowDelete { rec_id });
        true
    }

    pub(crate) fn apply_sync_batch(
        &mut self,
        wire: RepSyncBatch,
        out: &mut Vec<ReportEvent>,
        controls: &mut Vec<ReportControl>,
    ) -> bool {
        let Some(schema) = self.schema.as_deref() else {
            return false;
        };
        let Some(active) = self.active.as_mut() else {
            return true;
        };
        if wire.request_uid != active.ticket.request_uid {
            return true;
        }
        if active.received_batches.contains(&wire.batch_num) {
            return true;
        }
        let Some(decoded) = synlz_decompress(&wire.blob) else {
            return false;
        };
        let Some(rows) = ReportRow::parse_many(&decoded, wire.row_count, schema.rec_id_field_index)
        else {
            return false;
        };
        active.received_batches.insert(wire.batch_num);
        active.parsed_rows = active
            .parsed_rows
            .checked_add(u32::from(wire.row_count))
            .unwrap_or(u32::MAX);
        for row in rows {
            if active.live_touched.contains(&row.rec_id) {
                continue;
            }
            active.keep_rec_ids.insert(row.rec_id);
            out.push(ReportEvent::RowUpsert(row));
        }
        controls.push(ReportControl::SyncProgress {
            request_uid: wire.request_uid,
        });
        self.try_complete(out, controls);
        true
    }

    pub(crate) fn apply_sync_done(
        &mut self,
        wire: RepSyncDone,
        out: &mut Vec<ReportEvent>,
        controls: &mut Vec<ReportControl>,
    ) -> bool {
        let Some(active) = self.active.as_mut() else {
            return true;
        };
        if wire.request_uid != active.ticket.request_uid {
            return true;
        }
        let Ok(total_rows) = u32::try_from(wire.total_rows) else {
            return false;
        };
        active.done = Some(SyncDoneState {
            batch_count: wire.batch_count,
            total_rows,
            max_rec_id: wire.max_rec_id,
        });
        controls.push(ReportControl::SyncProgress {
            request_uid: wire.request_uid,
        });
        self.try_complete(out, controls);
        true
    }

    fn try_complete(&mut self, out: &mut Vec<ReportEvent>, controls: &mut Vec<ReportControl>) {
        let Some(active) = self.active.as_ref() else {
            return;
        };
        let Some(done) = active.done else {
            return;
        };
        if active.parsed_rows != done.total_rows
            || active.received_batches.len() != usize::from(done.batch_count)
            || !(0..done.batch_count).all(|batch| active.received_batches.contains(&batch))
        {
            return;
        }

        let active = self.active.take().expect("active sync just checked");
        let database_recreated = active.request.from_rec_id > 0
            && done.max_rec_id < active.request.from_rec_id.saturating_sub(1);
        let request_uid = active.ticket.request_uid;
        out.push(ReportEvent::SyncComplete(ReportSyncComplete {
            request_uid,
            from_rec_id: active.request.from_rec_id,
            batch_count: done.batch_count,
            total_rows: done.total_rows,
            max_rec_id: done.max_rec_id,
            keep_rec_ids: active.keep_rec_ids.into_iter().collect::<Vec<_>>().into(),
            database_recreated,
        }));
        controls.push(ReportControl::SyncCompleted { request_uid });
    }
}

fn quote_sqlite_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn read_u8(data: &[u8], pos: &mut usize) -> Option<u8> {
    let value = *data.get(*pos)?;
    *pos += 1;
    Some(value)
}

fn read_u16(data: &[u8], pos: &mut usize) -> Option<u16> {
    let end = pos.checked_add(2)?;
    let value = u16::from_le_bytes(data.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(value)
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    let end = pos.checked_add(8)?;
    let value = i64::from_le_bytes(data.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(value)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    let end = pos.checked_add(8)?;
    let value = f64::from_le_bytes(data.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(value)
}

fn read_str8(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = usize::from(read_u8(data, pos)?);
    let end = pos.checked_add(len)?;
    let value = decode_utf8_delphi(data.get(*pos..end)?);
    *pos = end;
    Some(value)
}

impl fmt::Display for ReportHistoryDepth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerDefault => f.write_str("server-default"),
            Self::Days(days) => write!(f, "{days} days"),
            Self::All => f.write_str("all history"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::report::{RepSyncBatch, RepSyncDone};
    use crate::commands::trade::BaseCommandHeader;
    use crate::compression::synlz_compress;

    fn header(cmd_id: u8) -> BaseCommandHeader {
        BaseCommandHeader {
            cmd_id,
            ver: 3,
            uid: 1,
        }
    }

    fn schema_blob_with_status_spec(status_sql_spec: &str) -> Vec<u8> {
        let mut raw = vec![1];
        raw.extend_from_slice(&2u16.to_le_bytes());
        raw.push(6);
        raw.extend_from_slice(b"Status");
        raw.push(1);
        raw.push(u8::try_from(status_sql_spec.len()).unwrap());
        raw.extend_from_slice(status_sql_spec.as_bytes());
        raw.push(8);
        raw.extend_from_slice(b"newRecID");
        raw.push(1);
        raw.push(23);
        raw.extend_from_slice(b"sqlite3_int64 default 0");
        synlz_compress(&raw)
    }

    fn schema_blob() -> Vec<u8> {
        schema_blob_with_status_spec("INT")
    }

    fn row(rec_id: i64, status: i64) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&2u16.to_le_bytes());
        raw.extend_from_slice(&0u16.to_le_bytes());
        raw.push(1);
        raw.extend_from_slice(&status.to_le_bytes());
        raw.extend_from_slice(&1u16.to_le_bytes());
        raw.push(1);
        raw.extend_from_slice(&rec_id.to_le_bytes());
        raw
    }

    fn ready_state() -> ReportReplicationState {
        let mut state = ReportReplicationState::default();
        let mut out = Vec::new();
        let mut controls = Vec::new();
        assert!(state.apply_schema(
            WireSchema {
                header: header(38),
                data: schema_blob(),
            },
            &mut out,
            &mut controls,
        ));
        state
    }

    #[test]
    fn schema_ddl_quotes_names_and_keys_new_rec_id() {
        let state = ready_state();
        let schema = state.schema().unwrap();
        let ddl = schema.sqlite_create_table_sql("Orders");
        assert!(ddl.contains("\"Status\" INT"));
        assert!(ddl.contains("PRIMARY KEY (\"newRecID\")"));
    }

    #[test]
    fn sync_request_reserves_zero_and_max_depth_sentinels() {
        assert!(ReportSyncRequest::fresh(ReportHistoryDepth::ServerDefault).is_valid());
        assert!(ReportSyncRequest::fresh(ReportHistoryDepth::Days(1)).is_valid());
        assert!(ReportSyncRequest::fresh(ReportHistoryDepth::All).is_valid());
        assert!(!ReportSyncRequest::fresh(ReportHistoryDepth::Days(0)).is_valid());
        assert!(!ReportSyncRequest::fresh(ReportHistoryDepth::Days(u16::MAX)).is_valid());
        assert!(!ReportSyncRequest::resume(-1).is_valid());
    }

    #[test]
    fn schema_rejects_changes_to_existing_sql_declaration() {
        let mut state = ready_state();
        let mut out = Vec::new();
        let mut controls = Vec::new();
        assert!(state.apply_schema(
            WireSchema {
                header: header(38),
                data: schema_blob_with_status_spec("INT DEFAULT 0"),
            },
            &mut out,
            &mut controls,
        ));
        assert!(matches!(
            out.as_slice(),
            [ReportEvent::SchemaRejected { .. }]
        ));
        assert_eq!(state.schema().unwrap().revision(), 2);
    }

    #[test]
    fn live_upsert_before_schema_waits_for_the_deferred_catch_up() {
        let mut state = ReportReplicationState::default();
        let mut out = Vec::new();
        assert!(state.apply_live_upsert(7, &row(99, 1), &mut out));
        assert!(out.is_empty());
        assert!(state.apply_live_delete(7, &mut out));
        assert!(out.is_empty());
    }

    #[test]
    fn done_before_batch_completes_only_after_batch_arrives() {
        let mut state = ready_state();
        let ticket = ReportSyncTicket { request_uid: 55 };
        let request = ReportSyncRequest::fresh(ReportHistoryDepth::ServerDefault);
        let mut out = Vec::new();
        let mut controls = Vec::new();
        state.begin_sync(ticket, request, &mut out);
        out.clear();

        assert!(state.apply_sync_done(
            RepSyncDone {
                header: header(36),
                request_uid: 55,
                batch_count: 1,
                total_rows: 1,
                max_rec_id: 7,
            },
            &mut out,
            &mut controls,
        ));
        assert!(out.is_empty());

        assert!(state.apply_sync_batch(
            RepSyncBatch {
                header: header(35),
                request_uid: 55,
                batch_num: 0,
                row_count: 1,
                blob: synlz_compress(&row(7, 1)),
            },
            &mut out,
            &mut controls,
        ));
        assert!(matches!(out[0], ReportEvent::RowUpsert(_)));
        assert!(matches!(out[1], ReportEvent::SyncComplete(_)));
    }

    #[test]
    fn live_delete_wins_over_late_batch_and_reconcile_set() {
        let mut state = ready_state();
        let ticket = ReportSyncTicket { request_uid: 99 };
        let request = ReportSyncRequest::resume(5);
        let mut out = Vec::new();
        let mut controls = Vec::new();
        state.begin_sync(ticket, request, &mut out);
        out.clear();

        assert!(state.apply_live_delete(7, &mut out));
        out.clear();
        assert!(state.apply_sync_batch(
            RepSyncBatch {
                header: header(35),
                request_uid: 99,
                batch_num: 0,
                row_count: 1,
                blob: synlz_compress(&row(7, 0)),
            },
            &mut out,
            &mut controls,
        ));
        assert!(out.is_empty(), "stale batch row must not be emitted");
        assert!(state.apply_sync_done(
            RepSyncDone {
                header: header(36),
                request_uid: 99,
                batch_count: 1,
                total_rows: 1,
                max_rec_id: 7,
            },
            &mut out,
            &mut controls,
        ));
        let ReportEvent::SyncComplete(done) = &out[0] else {
            panic!("expected completion")
        };
        assert!(done.keep_rec_ids.is_empty());
    }

    #[test]
    fn duplicate_batch_and_wrong_uid_do_not_advance_sync() {
        let mut state = ready_state();
        let ticket = ReportSyncTicket { request_uid: 41 };
        let request = ReportSyncRequest::fresh(ReportHistoryDepth::ServerDefault);
        let mut out = Vec::new();
        let mut controls = Vec::new();
        state.begin_sync(ticket, request, &mut out);
        out.clear();

        let batch = RepSyncBatch {
            header: header(35),
            request_uid: 41,
            batch_num: 0,
            row_count: 1,
            blob: synlz_compress(&row(7, 0)),
        };
        assert!(state.apply_sync_batch(batch.clone(), &mut out, &mut controls));
        assert!(matches!(out.as_slice(), [ReportEvent::RowUpsert(_)]));
        out.clear();
        assert!(state.apply_sync_batch(batch, &mut out, &mut controls));
        assert!(out.is_empty(), "duplicate batch must be idempotent");

        assert!(state.apply_sync_done(
            RepSyncDone {
                header: header(36),
                request_uid: 42,
                batch_count: 1,
                total_rows: 1,
                max_rec_id: 7,
            },
            &mut out,
            &mut controls,
        ));
        assert!(out.is_empty(), "wrong request UID must be ignored");
    }

    #[test]
    fn missing_declared_batch_never_emits_completion() {
        let mut state = ready_state();
        let ticket = ReportSyncTicket { request_uid: 51 };
        let request = ReportSyncRequest::fresh(ReportHistoryDepth::ServerDefault);
        let mut out = Vec::new();
        let mut controls = Vec::new();
        state.begin_sync(ticket, request, &mut out);
        out.clear();

        assert!(state.apply_sync_batch(
            RepSyncBatch {
                header: header(35),
                request_uid: 51,
                batch_num: 0,
                row_count: 1,
                blob: synlz_compress(&row(7, 0)),
            },
            &mut out,
            &mut controls,
        ));
        out.clear();
        assert!(state.apply_sync_done(
            RepSyncDone {
                header: header(36),
                request_uid: 51,
                batch_count: 2,
                total_rows: 2,
                max_rec_id: 8,
            },
            &mut out,
            &mut controls,
        ));
        assert!(out.is_empty());
    }

    #[test]
    fn live_upsert_wins_over_late_batch_copy() {
        let mut state = ready_state();
        let ticket = ReportSyncTicket { request_uid: 61 };
        let request = ReportSyncRequest::resume(5);
        let mut out = Vec::new();
        let mut controls = Vec::new();
        state.begin_sync(ticket, request, &mut out);
        out.clear();

        assert!(state.apply_live_upsert(7, &row(7, 1), &mut out));
        assert!(matches!(out.as_slice(), [ReportEvent::RowUpsert(_)]));
        out.clear();
        assert!(state.apply_sync_batch(
            RepSyncBatch {
                header: header(35),
                request_uid: 61,
                batch_num: 0,
                row_count: 1,
                blob: synlz_compress(&row(7, 0)),
            },
            &mut out,
            &mut controls,
        ));
        assert!(
            out.is_empty(),
            "late batch copy must not overwrite live row"
        );
        assert!(state.apply_sync_done(
            RepSyncDone {
                header: header(36),
                request_uid: 61,
                batch_count: 1,
                total_rows: 1,
                max_rec_id: 7,
            },
            &mut out,
            &mut controls,
        ));
        let ReportEvent::SyncComplete(done) = &out[0] else {
            panic!("expected completion")
        };
        assert_eq!(done.keep_rec_ids.as_ref(), &[7]);
    }

    #[test]
    fn cursor_ahead_of_global_max_reports_database_recreation() {
        let mut state = ready_state();
        let ticket = ReportSyncTicket { request_uid: 71 };
        let request = ReportSyncRequest::resume(100);
        let mut out = Vec::new();
        let mut controls = Vec::new();
        state.begin_sync(ticket, request, &mut out);
        out.clear();

        assert!(state.apply_sync_done(
            RepSyncDone {
                header: header(36),
                request_uid: 71,
                batch_count: 0,
                total_rows: 0,
                max_rec_id: 50,
            },
            &mut out,
            &mut controls,
        ));
        let ReportEvent::SyncComplete(done) = &out[0] else {
            panic!("expected completion")
        };
        assert!(done.database_recreated);
    }

    #[test]
    fn row_rejects_unknown_value_kind() {
        let mut malformed = Vec::new();
        malformed.extend_from_slice(&1u16.to_le_bytes());
        malformed.extend_from_slice(&0u16.to_le_bytes());
        malformed.push(99);
        assert!(ReportRow::parse(&malformed, 1, Some(7)).is_none());
    }
}
