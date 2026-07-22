//! Wire payloads for the `MPC_Order` report-replication commands.

use super::strict_read::{read_i64, read_u16, read_u32, read_u64, read_u8};
use super::trade::builders::write_base_command_header;
use super::trade::BaseCommandHeader;

pub(crate) const CMD_ROW_UPSERT: u8 = 32;
pub(crate) const CMD_ROW_DELETE: u8 = 33;
pub(crate) const CMD_SYNC_REQUEST: u8 = 34;
pub(crate) const CMD_SCHEMA_REQUEST: u8 = 37;
pub(crate) const CMD_SCHEMA: u8 = 38;
pub(crate) const CMD_SYNC_PAGE: u8 = 39;
pub(crate) const CMD_CHECK_ROWS_REQUEST: u8 = 40;
pub(crate) const CMD_SET_ROWS_DELETED: u8 = 48;
pub(crate) const MAX_CHECK_ROW_IDS: usize = 100;
pub(crate) const MAX_SET_ROWS_DELETED_WIRE_BYTES: usize = 1000;

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
pub struct RepSetRowsDeleted {
    pub(crate) header: BaseCommandHeader,
    pub(crate) deleted: bool,
    pub(crate) ranges: Vec<(i64, i64)>,
    pub(crate) singles: Vec<i64>,
}

impl RepSetRowsDeleted {
    pub(crate) fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        let mut pos = 0usize;
        let deleted = read_u8(r, &mut pos)? != 0;
        let range_count = usize::from(read_u16(r, &mut pos)?);
        let range_bytes = range_count.checked_mul(2 * std::mem::size_of::<i64>())?;
        if range_bytes > r.len().saturating_sub(pos) {
            return None;
        }
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(range_count).ok()?;
        for _ in 0..range_count {
            ranges.push((read_i64(r, &mut pos)?, read_i64(r, &mut pos)?));
        }
        let single_count = usize::from(read_u16(r, &mut pos)?);
        let single_bytes = single_count.checked_mul(std::mem::size_of::<i64>())?;
        if single_bytes > r.len().saturating_sub(pos) {
            return None;
        }
        let mut singles = Vec::new();
        singles.try_reserve_exact(single_count).ok()?;
        for _ in 0..single_count {
            singles.push(read_i64(r, &mut pos)?);
        }
        *r = &r[pos..];
        Some(Self {
            header,
            deleted,
            ranges,
            singles,
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

pub(crate) fn build_set_rows_deleted(
    uid: u64,
    change: &crate::state::ReportRowsDeleted,
) -> Vec<u8> {
    debug_assert!(change.ranges.len() <= u16::MAX as usize);
    debug_assert!(change.singles.len() <= u16::MAX as usize);
    let mut out = Vec::with_capacity(
        16 + change.ranges.len() * 2 * std::mem::size_of::<i64>()
            + change.singles.len() * std::mem::size_of::<i64>(),
    );
    write_base_command_header(&mut out, CMD_SET_ROWS_DELETED, uid);
    out.push(u8::from(change.deleted));
    out.extend_from_slice(&(change.ranges.len() as u16).to_le_bytes());
    for range in change.ranges.iter() {
        out.extend_from_slice(&range.from_rec_id.to_le_bytes());
        out.extend_from_slice(&range.to_rec_id.to_le_bytes());
    }
    out.extend_from_slice(&(change.singles.len() as u16).to_le_bytes());
    for rec_id in change.singles.iter() {
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

        let change = crate::state::ReportRowsDeleted::new(
            true,
            [crate::state::ReportRecIdRange::new(10, 20)],
            [30, 40],
        );
        let set_deleted = build_set_rows_deleted(0x4142_4344_4546_4748, &change);
        let mut expected = header(CMD_SET_ROWS_DELETED, 0x4142_4344_4546_4748);
        expected.push(1);
        expected.extend_from_slice(&1u16.to_le_bytes());
        expected.extend_from_slice(&10i64.to_le_bytes());
        expected.extend_from_slice(&20i64.to_le_bytes());
        expected.extend_from_slice(&2u16.to_le_bytes());
        expected.extend_from_slice(&30i64.to_le_bytes());
        expected.extend_from_slice(&40i64.to_le_bytes());
        assert_eq!(set_deleted, expected);
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

        let change = crate::state::ReportRowsDeleted::new(
            false,
            [crate::state::ReportRecIdRange::new(90, 80)],
            [70],
        );
        let set_deleted_raw = build_set_rows_deleted(uid, &change);
        let mut input = set_deleted_raw.as_slice();
        let set_deleted = RepSetRowsDeleted::read(&mut input).unwrap();
        assert_eq!(set_deleted.header.uid, uid);
        assert!(!set_deleted.deleted);
        assert_eq!(set_deleted.ranges, [(90, 80)]);
        assert_eq!(set_deleted.singles, [70]);
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

    #[test]
    fn set_rows_deleted_rejects_truncated_ranges_or_singles() {
        let uid = 7;
        let mut truncated_range = header(CMD_SET_ROWS_DELETED, uid);
        truncated_range.push(1);
        truncated_range.extend_from_slice(&1u16.to_le_bytes());
        truncated_range.extend_from_slice(&10i64.to_le_bytes());
        let mut input = truncated_range.as_slice();
        assert!(RepSetRowsDeleted::read(&mut input).is_none());

        let mut truncated_single = header(CMD_SET_ROWS_DELETED, uid);
        truncated_single.push(0);
        truncated_single.extend_from_slice(&0u16.to_le_bytes());
        truncated_single.extend_from_slice(&1u16.to_le_bytes());
        truncated_single.extend_from_slice(&[1, 2, 3]);
        let mut input = truncated_single.as_slice();
        assert!(RepSetRowsDeleted::read(&mut input).is_none());
    }
}
