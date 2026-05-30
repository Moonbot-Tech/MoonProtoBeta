//! Command registry — matches MoonProtoBaseStruct.pas:314-348.
//! Channel dispatch by command class is handled in `protocol::Command`; this
//! module holds the shared wire-string codec and the proto command version gate.
//!
//! Wire format of every command:
//!   CmdId (1 byte) + ver (2 bytes u16 LE) + UID (8 bytes u64 LE) + payload
//!
//! Version gate: if ver > CURRENT_VER (3), skip the command.

pub(crate) const CURRENT_PROTO_CMD_VER: u16 = 3;

/// Read a UTF-8 string with 2-byte LE length prefix.
/// Matches Delphi WriteStringToStreamUtf8/ReadStringFromStreamUtf8.
pub(crate) fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() {
        return None;
    }
    let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    if *pos + len > data.len() {
        return None;
    }
    let s = decode_utf8_delphi(&data[*pos..*pos + len]);
    *pos += len;
    Some(s)
}

/// Decode UTF-8 with Delphi `TEncoding.UTF8.GetString` replacement semantics.
///
/// Rust `from_utf8_lossy` inserts U+FFFD for invalid input, but Delphi's default
/// replacement fallback inserts ASCII `?`. Protocol parsers use this for every
/// wire string so damaged bytes produce the same machine effect as Delphi.
pub(crate) fn decode_utf8_delphi(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            let mut out = String::with_capacity(bytes.len());
            let mut rest = bytes;
            while !rest.is_empty() {
                match std::str::from_utf8(rest) {
                    Ok(s) => {
                        out.push_str(s);
                        break;
                    }
                    Err(err) => {
                        let valid_up_to = err.valid_up_to();
                        if valid_up_to > 0 {
                            out.push_str(std::str::from_utf8(&rest[..valid_up_to]).unwrap());
                        }
                        out.push('?');
                        let invalid_len = err.error_len().unwrap_or(rest.len() - valid_up_to);
                        rest = &rest[valid_up_to + invalid_len..];
                    }
                }
            }
            out
        }
    }
}

/// Write a UTF-8 string with 2-byte LE length prefix.
pub(crate) fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len() as u16;
    let len_usize = usize::from(len);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes[..len_usize]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // parity: MoonBot Vars.pas:WriteStringToStreamUtf8
    fn write_string_writes_only_declared_wrapped_len() {
        let s = "a".repeat(65_537);
        let mut buf = Vec::new();
        write_string(&mut buf, &s);

        assert_eq!(&buf[..2], &1u16.to_le_bytes());
        assert_eq!(buf.len(), 2 + 1);

        let mut pos = 0;
        assert_eq!(read_string(&buf, &mut pos).unwrap(), "a");
        assert_eq!(pos, buf.len());
    }

    #[test]
    // parity: MoonBot Vars.pas:ReadStringFromStreamUtf8
    fn read_string_replaces_invalid_utf8_with_question_mark() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(&[b'a', 0xFF, b'b', 0x80]);

        let mut pos = 0;
        assert_eq!(read_string(&buf, &mut pos).unwrap(), "a?b?");
        assert_eq!(pos, buf.len());
    }

    #[test]
    // parity: MoonBot Vars.pas:ReadStringFromStreamUtf8
    fn read_string_rejects_truncated_declared_body() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(b"ab");

        let mut pos = 0;
        assert!(read_string(&buf, &mut pos).is_none());
        assert_eq!(
            pos, 2,
            "Delphi ReadBuffer has consumed the length before failing on body bytes"
        );
    }

    #[test]
    fn decode_utf8_delphi_replaces_incomplete_sequence_with_single_question_mark() {
        assert_eq!(decode_utf8_delphi(&[b'a', 0xE2, 0x82]), "a?");
    }
}
