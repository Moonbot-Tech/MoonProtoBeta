//! `TEngineResponse` parser.

use super::{read_i32_zero_tail, read_u64_zero_tail, read_u8_zero_tail, EngineMethod};
use crate::commands::registry::read_string;
use flate2::read::DeflateDecoder;

/// Parsed Engine API response (`TEngineResponse`, server to client).
#[derive(Debug, Clone)]
pub struct EngineResponse {
    /// Delphi `TBaseCommand.ver` from the response header.
    pub ver: u16,
    /// UID of the original `TEngineRequest`.
    pub request_uid: u64,
    /// Engine method echoed by the server.
    pub method: EngineMethod,
    /// Server success flag.
    pub success: bool,
    /// Server error code when `success == false`.
    pub error_code: i32,
    /// Server error message when `success == false`.
    pub error_msg: String,
    /// Method-specific response payload, already DEFLATE-decompressed when the
    /// wire response was compressed.
    pub data: Vec<u8>,
}

/// Parse TEngineResponse from command payload.
///
/// **Wire-format** (after Crypted decrypt + CryptoHeader strip, payload **with** the Engine
/// TBaseCommand header):
/// ```text
/// [CmdId(1)=1][ver(2)][own_UID(8)][RequestUID(8)][Method(1)][Success(1)][ErrorCode(4)][ErrorMsg(string)][IsCompressed(1)][DataSize(4)][Data]
/// ```
///
/// The Engine TBaseCommand header (11 bytes: `CmdId + ver + own_UID`) is **skipped**
/// before reading `RequestUID` — matching Delphi `TEngineResponse.CreateFromStream`,
/// which via `inherited CreateFromStream` (TBaseCommand) first reads ver+UID,
/// then its own fields.
///
/// **Historical bug** (fixed): the parser used to start at `pos=0`, reading
/// `[ver][own_UID first 5 bytes]` as `request_uid` — it never matched the
/// registered uid -> all Engine API responses were lost (BaseCheck/AuthCheck/
/// GetMarketsList timeouts).
///
/// Matches `MoonProtoEngineStruct.pas:364-403`.
pub fn parse_engine_response(data: &[u8]) -> Option<EngineResponse> {
    if data.len() < 11 {
        return None;
    }
    let ver = u16::from_le_bytes(data[1..3].try_into().unwrap());
    // Skip Engine TBaseCommand header: CmdId(1) + ver(2) + own_UID(8) = 11 bytes.
    let mut pos = 11usize;

    let request_uid = read_u64_zero_tail(data, &mut pos);
    let method = EngineMethod::from_byte(read_u8_zero_tail(data, &mut pos));
    let success = read_u8_zero_tail(data, &mut pos) != 0;
    let error_code = read_i32_zero_tail(data, &mut pos);

    let error_msg = read_string(data, &mut pos)?;

    // Delphi uses TMemoryStream.Read for these scalar fields. Missing tail bytes
    // stay zero after the strict ErrorMsg string has already been read.
    let is_compressed = read_u8_zero_tail(data, &mut pos) != 0;
    let sz = read_i32_zero_tail(data, &mut pos);

    let response_data = if sz > 0 {
        let sz = sz as usize;
        let available = data.len().saturating_sub(pos);
        let end = pos + available.min(sz);
        let raw = &data[pos..end];
        if is_compressed {
            use std::io::Read;
            let mut decoder = DeflateDecoder::new(raw);
            let mut decompressed = Vec::new();
            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => decompressed,
                Err(e) => {
                    log::warn!(target: "moonproto::engine_api",
                        "DEFLATE decompress failed for EngineResponse uid={}: {}",
                        request_uid, e);
                    return None;
                }
            }
        } else {
            raw.to_vec()
        }
    } else {
        Vec::new()
    };

    Some(EngineResponse {
        ver,
        request_uid,
        method,
        success,
        error_code,
        error_msg,
        data: response_data,
    })
}
