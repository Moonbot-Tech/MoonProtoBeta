//! Command registry — matches MoonProtoBaseStruct.pas:314-348.
//! Dispatches by (CmdClass << 8) | CmdId to the correct deserializer.
//!
//! Wire format of every command:
//!   CmdId (1 byte) + ver (2 bytes u16 LE) + UID (8 bytes u64 LE) + payload
//!
//! Version gate: if ver > CURRENT_VER (3), skip the command.

pub const CURRENT_PROTO_CMD_VER: u16 = 3;

/// Common header for all sub-commands within a channel.
#[derive(Debug, Clone)]
pub struct CommandHeader {
    pub cmd_id: u8,
    pub ver: u16,
    pub uid: u64,
}

impl CommandHeader {
    /// Read command header from bytes. Returns (header, bytes_consumed).
    pub fn from_bytes(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 11 {
            // 1 + 2 + 8
            return None;
        }
        let cmd_id = data[0];
        let ver = u16::from_le_bytes([data[1], data[2]]);
        let uid = u64::from_le_bytes(data[3..11].try_into().unwrap());
        Some((Self { cmd_id, ver, uid }, 11))
    }

    /// Check version gate. Returns true if command should be processed.
    pub fn is_valid_version(&self) -> bool {
        self.ver <= CURRENT_PROTO_CMD_VER
    }
}

/// Read a UTF-8 string with 2-byte LE length prefix.
/// Matches Delphi WriteStringToStreamUtf8/ReadStringFromStreamUtf8.
pub fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() {
        return None;
    }
    let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    if *pos + len > data.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
    *pos += len;
    Some(s)
}

/// Write a UTF-8 string with 2-byte LE length prefix.
pub fn write_string(buf: &mut Vec<u8>, s: &str) {
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
    fn write_string_writes_only_declared_wrapped_len_like_delphi() {
        let s = "a".repeat(65_537);
        let mut buf = Vec::new();
        write_string(&mut buf, &s);

        assert_eq!(&buf[..2], &1u16.to_le_bytes());
        assert_eq!(buf.len(), 2 + 1);

        let mut pos = 0;
        assert_eq!(read_string(&buf, &mut pos).unwrap(), "a");
        assert_eq!(pos, buf.len());
    }
}
