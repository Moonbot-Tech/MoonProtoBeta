//! Wire payloads for the `MPC_Order` report-replication commands.

use super::strict_read::{read_i64, read_u16, read_u32, read_u64};
use super::trade::builders::write_base_command_header;
use super::trade::BaseCommandHeader;

pub(crate) const CMD_ROW_UPSERT: u8 = 32;
pub(crate) const CMD_ROW_DELETE: u8 = 33;
pub(crate) const CMD_SYNC_REQUEST: u8 = 34;
pub(crate) const CMD_SCHEMA_REQUEST: u8 = 37;
pub(crate) const CMD_SCHEMA: u8 = 38;
pub(crate) const CMD_SYNC_PAGE: u8 = 39;
pub(crate) const CMD_CHECK_ROWS_REQUEST: u8 = 40;
pub(crate) const MAX_CHECK_ROW_IDS: usize = 100;

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
pub struct RepSyncPage {
    pub(crate) header: BaseCommandHeader,
    pub(crate) request_uid: u64,
    pub(crate) last_rec_id: i64,
    pub(crate) max_rec_id: i64,
    pub(crate) row_count: u16,
    pub(crate) blob: Vec<u8>,
}

impl RepSyncPage {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let request_uid = read_u64(r, &mut pos)?;
        let last_rec_id = read_i64(r, &mut pos)?;
        let max_rec_id = read_i64(r, &mut pos)?;
        let row_count = read_u16(r, &mut pos)?;
        let blob = read_len_bytes(r, &mut pos)?;
        *r = &r[pos..];
        Some(Self {
            header,
            request_uid,
            last_rec_id,
            max_rec_id,
            row_count,
            blob,
        })
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Parsed for command-registry parity; clients only send this command.
pub struct RepCheckRowsRequest {
    pub(crate) header: BaseCommandHeader,
    pub(crate) rec_ids: Vec<i64>,
}

impl RepCheckRowsRequest {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let count = usize::from(read_u16(r, &mut pos)?);
        let bytes = count.checked_mul(std::mem::size_of::<i64>())?;
        if bytes > r.len().saturating_sub(pos) {
            return None;
        }
        let mut rec_ids = Vec::new();
        rec_ids.try_reserve_exact(count).ok()?;
        for _ in 0..count {
            rec_ids.push(read_i64(r, &mut pos)?);
        }
        *r = &r[pos..];
        Some(Self { header, rec_ids })
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

pub(crate) fn build_check_rows_request(uid: u64, rec_ids: &[i64]) -> Vec<u8> {
    let count = rec_ids.len().min(MAX_CHECK_ROW_IDS);
    let start = rec_ids.len() - count;
    let mut out = Vec::with_capacity(13 + count * std::mem::size_of::<i64>());
    write_base_command_header(&mut out, CMD_CHECK_ROWS_REQUEST, uid);
    out.extend_from_slice(&(count as u16).to_le_bytes());
    for rec_id in &rec_ids[start..] {
        out.extend_from_slice(&rec_id.to_le_bytes());
    }
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

        let rec_ids = (1..=105).map(i64::from).collect::<Vec<_>>();
        let check = build_check_rows_request(0x3132_3334_3536_3738, &rec_ids);
        let mut expected = header(CMD_CHECK_ROWS_REQUEST, 0x3132_3334_3536_3738);
        expected.extend_from_slice(&100u16.to_le_bytes());
        for rec_id in 6i64..=105 {
            expected.extend_from_slice(&rec_id.to_le_bytes());
        }
        assert_eq!(check, expected);
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

        let mut page_raw = header(CMD_SYNC_PAGE, uid);
        page_raw.extend_from_slice(&0x2122_2324_2526_2728u64.to_le_bytes());
        page_raw.extend_from_slice(&77i64.to_le_bytes());
        page_raw.extend_from_slice(&99i64.to_le_bytes());
        page_raw.extend_from_slice(&17u16.to_le_bytes());
        page_raw.extend_from_slice(&len_bytes(&[5, 6, 7]));
        let mut input = page_raw.as_slice();
        let page = RepSyncPage::read(&mut input).unwrap();
        assert_eq!(page.header.uid, uid);
        assert_eq!(page.request_uid, 0x2122_2324_2526_2728);
        assert_eq!(page.last_rec_id, 77);
        assert_eq!(page.max_rec_id, 99);
        assert_eq!(page.row_count, 17);
        assert_eq!(page.blob, [5, 6, 7]);
        assert!(input.is_empty());

        let check_raw = build_check_rows_request(uid, &[7, 8, 9]);
        let mut input = check_raw.as_slice();
        let check = RepCheckRowsRequest::read(&mut input).unwrap();
        assert_eq!(check.header.uid, uid);
        assert_eq!(check.rec_ids, [7, 8, 9]);
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
    fn sync_page_reads_global_cursor_fields() {
        let mut raw = header(CMD_SYNC_PAGE, 9);
        raw.extend_from_slice(&55u64.to_le_bytes());
        raw.extend_from_slice(&998i64.to_le_bytes());
        raw.extend_from_slice(&999i64.to_le_bytes());
        raw.extend_from_slice(&0u16.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
        let mut slice = raw.as_slice();
        let page = RepSyncPage::read(&mut slice).unwrap();
        assert_eq!(page.header.uid, 9);
        assert_eq!(page.request_uid, 55);
        assert_eq!(page.last_rec_id, 998);
        assert_eq!(page.max_rec_id, 999);
        assert_eq!(page.row_count, 0);
    }
}
