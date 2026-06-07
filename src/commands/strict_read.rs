//! Strict byte readers for schema/serializer formats where Delphi uses
//! fail-fast length checks or where the current parser policy is intentionally
//! strict.
//!
//! Do not use these helpers for Delphi `TStream.Read` soft-tail fields. Those
//! paths need explicit zero-tail/preserve-tail helpers next to their parser.

use super::registry::decode_utf8_delphi;

pub(crate) fn read_u8(d: &[u8], p: &mut usize) -> Option<u8> {
    if *p + 1 > d.len() {
        return None;
    }
    let v = d[*p];
    *p += 1;
    Some(v)
}

pub(crate) fn read_u16(d: &[u8], p: &mut usize) -> Option<u16> {
    if *p + 2 > d.len() {
        return None;
    }
    let v = u16::from_le_bytes(d[*p..*p + 2].try_into().unwrap());
    *p += 2;
    Some(v)
}

pub(crate) fn read_i32(d: &[u8], p: &mut usize) -> Option<i32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = i32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}

pub(crate) fn read_u32(d: &[u8], p: &mut usize) -> Option<u32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = u32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}

pub(crate) fn read_i64(d: &[u8], p: &mut usize) -> Option<i64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = i64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}

pub(crate) fn read_u64(d: &[u8], p: &mut usize) -> Option<u64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = u64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}

pub(crate) fn read_f32(d: &[u8], p: &mut usize) -> Option<f32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = f32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}

pub(crate) fn read_f64(d: &[u8], p: &mut usize) -> Option<f64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = f64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}

pub(crate) fn read_str8(d: &[u8], p: &mut usize) -> Option<String> {
    let len = read_u8(d, p)? as usize;
    if *p + len > d.len() {
        return None;
    }
    let s = decode_utf8_delphi(&d[*p..*p + len]);
    *p += len;
    Some(s)
}

pub(crate) fn read_str16(d: &[u8], p: &mut usize) -> Option<String> {
    let len = read_u16(d, p)? as usize;
    if *p + len > d.len() {
        return None;
    }
    let s = decode_utf8_delphi(&d[*p..*p + len]);
    *p += len;
    Some(s)
}
