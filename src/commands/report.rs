//! Wire payloads for the `MPC_Order` report-replication commands.

use super::strict_read::{read_i32, read_i64, read_u16, read_u32, read_u64};
use super::trade::builders::write_base_command_header;
use super::trade::BaseCommandHeader;

pub(crate) const CMD_ROW_UPSERT: u8 = 32;
pub(crate) const CMD_ROW_DELETE: u8 = 33;
pub(crate) const CMD_SYNC_REQUEST: u8 = 34;
pub(crate) const CMD_SYNC_BATCH: u8 = 35;
pub(crate) const CMD_SYNC_DONE: u8 = 36;
pub(crate) const CMD_SCHEMA_REQUEST: u8 = 37;
pub(crate) const CMD_SCHEMA: u8 = 38;

#[derive(Debug, Clone)]
pub struct RepRowUpsert {
    pub(crate) header: BaseCommandHeader,
    pub(crate) rec_id: i64,
    pub(crate) row: Vec<u8>,
}

impl RepRowUpsert {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let rec_id = read_i64(r, &mut pos)?;
        let row = read_len_bytes(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self {
            header,
            rec_id,
            row,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RepRowDelete {
    pub(crate) header: BaseCommandHeader,
    pub(crate) rec_id: i64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Parsed for command-registry parity; clients only send this command.
pub struct RepSyncRequest {
    pub(crate) header: BaseCommandHeader,
    pub(crate) from_rec_id: i64,
    pub(crate) depth_days: u16,
}

impl RepSyncRequest {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let from_rec_id = read_i64(r, &mut pos)?;
        let depth_days = read_u16(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self {
            header,
            from_rec_id,
            depth_days,
        })
    }
}

impl RepRowDelete {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let rec_id = read_i64(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self { header, rec_id })
    }
}

#[derive(Debug, Clone)]
pub struct RepSyncBatch {
    pub(crate) header: BaseCommandHeader,
    pub(crate) request_uid: u64,
    pub(crate) batch_num: u16,
    pub(crate) row_count: u16,
    pub(crate) blob: Vec<u8>,
}

impl RepSyncBatch {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let request_uid = read_u64(r, &mut pos)?;
        let batch_num = read_u16(r, &mut pos)?;
        let row_count = read_u16(r, &mut pos)?;
        let blob = read_len_bytes(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self {
            header,
            request_uid,
            batch_num,
            row_count,
            blob,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RepSyncDone {
    pub(crate) header: BaseCommandHeader,
    pub(crate) request_uid: u64,
    pub(crate) batch_count: u16,
    pub(crate) total_rows: i32,
    pub(crate) max_rec_id: i64,
}

impl RepSyncDone {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let request_uid = read_u64(r, &mut pos)?;
        let batch_count = read_u16(r, &mut pos)?;
        let total_rows = read_i32(r, &mut pos)?;
        let max_rec_id = read_i64(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self {
            header,
            request_uid,
            batch_count,
            total_rows,
            max_rec_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RepSchema {
    pub(crate) header: BaseCommandHeader,
    pub(crate) data: Vec<u8>,
}

impl RepSchema {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let data = read_len_bytes(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self { header, data })
    }
}

pub(crate) fn build_sync_request(uid: u64, from_rec_id: i64, depth_days: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(21);
    write_base_command_header(&mut out, CMD_SYNC_REQUEST, uid);
    out.extend_from_slice(&from_rec_id.to_le_bytes());
    out.extend_from_slice(&depth_days.to_le_bytes());
    out
}

pub(crate) fn build_schema_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_base_command_header(&mut out, CMD_SCHEMA_REQUEST, uid);
    out
}

fn read_len_bytes(data: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = usize::try_from(read_u32(data, pos)?).ok()?;
    let end = pos.checked_add(len)?;
    if end > data.len() {
        return None;
    }
    let mut out = Vec::new();
    out.try_reserve_exact(len).ok()?;
    out.extend_from_slice(&data[*pos..end]);
    *pos = end;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::registry::CURRENT_PROTO_CMD_VER;

    fn header(cmd: u8, uid: u64) -> Vec<u8> {
        let mut out = vec![cmd];
        out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
        out.extend_from_slice(&uid.to_le_bytes());
        out
    }

    fn len_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + value.len());
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
        out
    }

    #[test]
    fn request_builders_match_delphi_field_order() {
        let sync = build_sync_request(0x0102_0304_0506_0708, 123, 30);
        let mut expected = header(CMD_SYNC_REQUEST, 0x0102_0304_0506_0708);
        expected.extend_from_slice(&123i64.to_le_bytes());
        expected.extend_from_slice(&30u16.to_le_bytes());
        assert_eq!(sync, expected);

        assert_eq!(
            build_schema_request(0x1112_1314_1516_1718),
            header(CMD_SCHEMA_REQUEST, 0x1112_1314_1516_1718)
        );
    }

    #[test]
    fn inbound_commands_match_delphi_field_order() {
        let uid = 0x1112_1314_1516_1718;

        let mut upsert_raw = header(CMD_ROW_UPSERT, uid);
        upsert_raw.extend_from_slice(&123i64.to_le_bytes());
        upsert_raw.extend_from_slice(&len_bytes(&[1, 2, 3, 4]));
        let mut input = upsert_raw.as_slice();
        let upsert = RepRowUpsert::read(&mut input).unwrap();
        assert_eq!(upsert.header.uid, uid);
        assert_eq!(upsert.rec_id, 123);
        assert_eq!(upsert.row, [1, 2, 3, 4]);
        assert!(input.is_empty());

        let mut delete_raw = header(CMD_ROW_DELETE, uid);
        delete_raw.extend_from_slice(&456i64.to_le_bytes());
        let mut input = delete_raw.as_slice();
        let delete = RepRowDelete::read(&mut input).unwrap();
        assert_eq!(delete.header.uid, uid);
        assert_eq!(delete.rec_id, 456);
        assert!(input.is_empty());

        let sync_raw = build_sync_request(uid, 789, 30);
        let mut input = sync_raw.as_slice();
        let sync = RepSyncRequest::read(&mut input).unwrap();
        assert_eq!(sync.header.uid, uid);
        assert_eq!(sync.from_rec_id, 789);
        assert_eq!(sync.depth_days, 30);
        assert!(input.is_empty());

        let mut batch_raw = header(CMD_SYNC_BATCH, uid);
        batch_raw.extend_from_slice(&0x2122_2324_2526_2728u64.to_le_bytes());
        batch_raw.extend_from_slice(&3u16.to_le_bytes());
        batch_raw.extend_from_slice(&17u16.to_le_bytes());
        batch_raw.extend_from_slice(&len_bytes(&[5, 6, 7]));
        let mut input = batch_raw.as_slice();
        let batch = RepSyncBatch::read(&mut input).unwrap();
        assert_eq!(batch.header.uid, uid);
        assert_eq!(batch.request_uid, 0x2122_2324_2526_2728);
        assert_eq!(batch.batch_num, 3);
        assert_eq!(batch.row_count, 17);
        assert_eq!(batch.blob, [5, 6, 7]);
        assert!(input.is_empty());

        let schema_request_raw = build_schema_request(uid);
        let mut input = schema_request_raw.as_slice();
        let schema_request = BaseCommandHeader::read(&mut input).unwrap();
        assert_eq!(schema_request.cmd_id, CMD_SCHEMA_REQUEST);
        assert_eq!(schema_request.uid, uid);
        assert!(input.is_empty());

        let mut schema_raw = header(CMD_SCHEMA, uid);
        schema_raw.extend_from_slice(&len_bytes(&[8, 9, 10]));
        let mut input = schema_raw.as_slice();
        let schema = RepSchema::read(&mut input).unwrap();
        assert_eq!(schema.header.uid, uid);
        assert_eq!(schema.data, [8, 9, 10]);
        assert!(input.is_empty());
    }

    #[test]
    fn upsert_rejects_declared_row_past_payload() {
        let mut raw = header(CMD_ROW_UPSERT, 1);
        raw.extend_from_slice(&7i64.to_le_bytes());
        raw.extend_from_slice(&100u32.to_le_bytes());
        raw.extend_from_slice(&[1, 2, 3]);
        let mut slice = raw.as_slice();
        assert!(RepRowUpsert::read(&mut slice).is_none());
    }

    #[test]
    fn sync_done_reads_global_cursor_fields() {
        let mut raw = header(CMD_SYNC_DONE, 9);
        raw.extend_from_slice(&55u64.to_le_bytes());
        raw.extend_from_slice(&3u16.to_le_bytes());
        raw.extend_from_slice(&120i32.to_le_bytes());
        raw.extend_from_slice(&999i64.to_le_bytes());
        let mut slice = raw.as_slice();
        let done = RepSyncDone::read(&mut slice).unwrap();
        assert_eq!(done.header.uid, 9);
        assert_eq!(done.request_uid, 55);
        assert_eq!(done.batch_count, 3);
        assert_eq!(done.total_rows, 120);
        assert_eq!(done.max_rec_id, 999);
    }
}
