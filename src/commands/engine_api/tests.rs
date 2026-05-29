use super::*;
use crate::commands::registry::write_string;

#[cfg(test)]
mod parse_engine_response_tests {
    use super::*;

    /// Helper: builds a fake response wire-payload as the server would emit
    /// (after Crypted decrypt + CryptoHeader strip). Layout:
    /// [CmdId(1)=1][ver(2)=3][own_UID(8)][RequestUID(8)][Method(1)][Success(1)]
    /// [ErrorCode(4)][ErrorMsg_len(2)][ErrorMsg][IsCompressed(1)][DataSize(4)][Data]
    fn build_wire_response(
        own_uid: u64,
        request_uid: u64,
        method: EngineMethod,
        success: bool,
        error_code: i32,
        error_msg: &str,
        data: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8); // CmdId = 1
        buf.extend_from_slice(&3u16.to_le_bytes()); // ver = 3
        buf.extend_from_slice(&own_uid.to_le_bytes()); // own_UID
        buf.extend_from_slice(&request_uid.to_le_bytes()); // RequestUID (echo)
        buf.push(method.to_byte()); // Method
        buf.push(success as u8); // Success
        buf.extend_from_slice(&error_code.to_le_bytes()); // ErrorCode
        write_string(&mut buf, error_msg);
        buf.push(0u8); // IsCompressed = false
        buf.extend_from_slice(&(data.len() as i32).to_le_bytes()); // DataSize
        buf.extend_from_slice(data);
        buf
    }

    #[test]
    fn parse_skips_basecmd_header_and_reads_request_uid_correctly() {
        // Regression for a critical bug: before the fix the parser started at
        // offset 0 and read request_uid from `[CmdId][ver][own_UID 5 bytes]` — garbage.
        let request_uid = 0x12_34_56_78_9A_BC_DE_F0u64;
        let payload = build_wire_response(
            0xAAAA_BBBB_CCCC_DDDD, // own_UID (random)
            request_uid,           // RequestUID (echo)
            EngineMethod::BaseCheck,
            true,
            0,
            "",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.ver, 3);
        assert_eq!(resp.request_uid, request_uid);
        assert_eq!(resp.method, EngineMethod::BaseCheck);
        assert!(resp.success);
        assert_eq!(resp.error_code, 0);
        assert!(resp.error_msg.is_empty());
        assert!(resp.data.is_empty());
    }

    #[test]
    fn parse_carries_method_byte_after_request_uid() {
        // Each method byte is read correctly at offset 19 (after header + request_uid).
        for method in [
            EngineMethod::AuthCheck,
            EngineMethod::GetMarketsList,
            EngineMethod::GetMarketsIndexes,
            EngineMethod::SubscribeAllTrades,
            EngineMethod::GetOpenOrders,
        ] {
            let payload = build_wire_response(0xDEAD, 0xBEEF, method, true, 0, "", &[]);
            let resp = parse_engine_response(&payload).expect("parse ok");
            assert_eq!(resp.method, method, "method mismatch for {:?}", method);
            assert_eq!(resp.request_uid, 0xBEEF);
        }
    }

    #[test]
    fn parse_preserves_unknown_method_ordinal_like_delphi() {
        let payload = build_wire_response(
            0xDEAD,
            0xBEEF,
            EngineMethod::from_byte(99),
            false,
            400,
            "Unknown method",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.method.to_byte(), 99);
        assert_eq!(resp.method.name(), "Unknown");
        assert_eq!(resp.request_uid, 0xBEEF);
        assert_eq!(resp.error_code, 400);
        assert_eq!(resp.error_msg, "Unknown method");
    }

    #[test]
    fn parse_carries_error_payload() {
        let payload = build_wire_response(
            1,
            42,
            EngineMethod::AuthCheck,
            false, // success = false
            -123,  // error_code
            "Invalid API key",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert!(!resp.success);
        assert_eq!(resp.error_code, -123);
        assert_eq!(resp.error_msg, "Invalid API key");
        assert_eq!(resp.request_uid, 42);
    }

    #[test]
    fn response_helper_writes_error_msg_like_delphi_string() {
        let payload = build_wire_response(
            1,
            42,
            EngineMethod::AuthCheck,
            false,
            -123,
            &"E".repeat(65_537),
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.error_msg, "E");
        assert_eq!(resp.request_uid, 42);
    }

    #[test]
    fn parse_carries_uncompressed_data() {
        let blob = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78];
        let payload = build_wire_response(0, 100, EngineMethod::GetMarketsList, true, 0, "", &blob);
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.data, blob);
        assert_eq!(resp.method, EngineMethod::GetMarketsList);
    }

    #[test]
    fn parse_handles_negative_data_size_without_panic() {
        let blob = [0xAA, 0xBB, 0xCC];
        let mut payload =
            build_wire_response(0, 100, EngineMethod::GetMarketsList, true, 0, "", &blob);
        let size_pos = payload.len() - blob.len() - 4;
        payload[size_pos..size_pos + 4].copy_from_slice(&(-1i32).to_le_bytes());
        payload.truncate(size_pos + 4);

        let resp = parse_engine_response(&payload).expect("parse ok");
        assert!(resp.data.is_empty());
    }

    #[test]
    fn parse_inflates_compressed_response_data() {
        use flate2::{write::DeflateEncoder, Compression};
        use std::io::Write;

        let plain = b"compressed engine response payload".repeat(4);
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut payload = build_wire_response(
            0,
            101,
            EngineMethod::UpdateMarketsList,
            true,
            0,
            "",
            &compressed,
        );
        let size_pos = payload.len() - compressed.len() - 4;
        payload[size_pos - 1] = 1; // IsCompressed = true

        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.data, plain);
    }

    #[test]
    fn parse_get_balance_response_reads_delphi_double() {
        let mut data = 1234.5f64.to_le_bytes().to_vec();
        data.extend_from_slice(&[0xAA, 0xBB]);

        assert_eq!(parse_get_balance_response(&data), Some(1234.5));
        let mut short = [0u8; 8];
        short[..7].copy_from_slice(&data[..7]);
        assert_eq!(
            parse_get_balance_response(&data[..7]),
            Some(f64::from_le_bytes(short))
        );
        assert_eq!(parse_get_balance_response(&[]), Some(0.0));
    }

    #[test]
    fn parse_query_hedge_mode_response_reads_delphi_bool() {
        assert_eq!(parse_query_hedge_mode_response(&[0]), Some(false));
        assert_eq!(parse_query_hedge_mode_response(&[1]), Some(true));
        assert_eq!(parse_query_hedge_mode_response(&[2, 0xAA]), Some(true));
        assert_eq!(parse_query_hedge_mode_response(&[]), Some(false));
    }

    #[test]
    fn parse_api_expiration_time_response_reads_delphi_datetime() {
        let mut data = 45_000.25f64.to_le_bytes().to_vec();
        data.extend_from_slice(&[0xAA, 0xBB]);

        let parsed = parse_api_expiration_time_response(&data).unwrap();
        assert_eq!(parsed.delphi_time(), 45_000.25);
        let mut short = [0u8; 8];
        short[..7].copy_from_slice(&data[..7]);
        assert_eq!(
            parse_api_expiration_time_response(&data[..7])
                .unwrap()
                .delphi_time(),
            f64::from_le_bytes(short)
        );
        assert_eq!(
            parse_api_expiration_time_response(&[])
                .unwrap()
                .delphi_time(),
            0.0
        );
    }

    #[test]
    fn api_expiration_time_converts_unix_epoch() {
        let parsed = ApiExpirationTime::from_delphi_time(DELPHI_UNIX_EPOCH_DAYS);
        assert!(parsed.is_known());
        assert_eq!(parsed.unix_seconds(), Some(0));
        assert_eq!(parsed.system_time(), Some(SystemTime::UNIX_EPOCH));
        assert_eq!(
            parsed.days_until(SystemTime::UNIX_EPOCH + Duration::from_secs(2 * 86_400)),
            Some(-2)
        );

        let unknown = ApiExpirationTime::from_delphi_time(0.0);
        assert!(!unknown.is_known());
        assert_eq!(unknown.system_time(), None);
    }

    #[test]
    fn parse_update_transfer_assets_response_reads_delphi_rows() {
        let mut data = Vec::new();
        data.extend_from_slice(&(2i32).to_le_bytes());
        data.extend_from_slice(&(4u16).to_le_bytes());
        data.extend_from_slice(b"USDT");
        data.extend_from_slice(&(12.5f64).to_le_bytes());
        data.extend_from_slice(&(100.0f64).to_le_bytes());
        data.extend_from_slice(&(3u16).to_le_bytes());
        data.extend_from_slice(b"BTC");
        data.extend_from_slice(&(0.25f64).to_le_bytes());
        data.extend_from_slice(&(1.0f64).to_le_bytes());

        let parsed = parse_update_transfer_assets_response(&data).unwrap();
        assert_eq!(
            parsed,
            vec![
                TransferAsset {
                    currency: "USDT".to_string(),
                    amount: 12.5,
                    total: 100.0,
                },
                TransferAsset {
                    currency: "BTC".to_string(),
                    amount: 0.25,
                    total: 1.0,
                },
            ]
        );
    }

    #[test]
    fn parse_update_transfer_assets_response_matches_read_vs_readbuffer_tails() {
        assert_eq!(parse_update_transfer_assets_response(&[]), Some(Vec::new()));
        assert_eq!(
            parse_update_transfer_assets_response(&(-1i32).to_le_bytes()),
            Some(Vec::new())
        );

        let mut truncated = Vec::new();
        truncated.extend_from_slice(&(1i32).to_le_bytes());
        truncated.extend_from_slice(&(4u16).to_le_bytes());
        truncated.extend_from_slice(b"USDT");
        truncated.extend_from_slice(&(12.5f64).to_le_bytes());
        assert_eq!(
            parse_update_transfer_assets_response(&truncated),
            Some(vec![TransferAsset {
                currency: "USDT".to_string(),
                amount: 12.5,
                total: 0.0,
            }])
        );

        let mut bad_string = Vec::new();
        bad_string.extend_from_slice(&(1i32).to_le_bytes());
        bad_string.extend_from_slice(&(4u16).to_le_bytes());
        bad_string.extend_from_slice(b"USD");
        assert_eq!(parse_update_transfer_assets_response(&bad_string), None);
    }

    #[test]
    fn parse_returns_none_on_short_payload() {
        // < 11 bytes: header does not parse.
        let too_short = vec![0u8; 10];
        assert!(parse_engine_response(&too_short).is_none());
    }

    #[test]
    fn parse_returns_none_when_truncated_at_request_uid() {
        // header (11) + 4 bytes (instead of 8 for request_uid) → None.
        let mut buf = vec![0u8; 11];
        buf.extend_from_slice(&[1, 2, 3, 4]);
        assert!(parse_engine_response(&buf).is_none());
    }

    #[test]
    fn parse_returns_none_when_error_msg_body_is_truncated_like_delphi_readbuffer() {
        let mut payload = build_wire_response(0, 100, EngineMethod::AuthCheck, false, 401, "", &[]);
        let error_msg_len_pos = 11 + 8 + 1 + 1 + 4;
        payload.truncate(error_msg_len_pos);
        payload.extend_from_slice(&(4u16).to_le_bytes());
        payload.extend_from_slice(b"NO");

        assert!(parse_engine_response(&payload).is_none());
    }

    #[test]
    fn parse_zero_tails_missing_compression_flag_like_delphi_stream_read() {
        let mut payload = build_wire_response(0, 100, EngineMethod::BaseCheck, true, 0, "", &[]);
        payload.truncate(11 + 8 + 1 + 1 + 4 + 2);

        let resp = parse_engine_response(&payload).expect("missing IsCompressed zero-tails");
        assert_eq!(resp.request_uid, 100);
        assert!(resp.success);
        assert!(resp.data.is_empty());
    }

    #[test]
    fn parse_zero_tails_missing_data_size_like_delphi_stream_read() {
        let mut payload = build_wire_response(0, 100, EngineMethod::BaseCheck, true, 0, "", &[]);
        payload.truncate(11 + 8 + 1 + 1 + 4 + 2 + 1);

        let resp = parse_engine_response(&payload).expect("missing DataSize zero-tails");
        assert_eq!(resp.request_uid, 100);
        assert!(resp.data.is_empty());
    }

    #[test]
    fn parse_keeps_available_uncompressed_data_when_declared_body_is_short_like_delphi_copyfrom() {
        let mut payload = build_wire_response(
            0,
            100,
            EngineMethod::GetMarketsList,
            true,
            0,
            "",
            &[0xAA, 0xBB],
        );
        let size_pos = payload.len() - 2 - 4;
        payload[size_pos..size_pos + 4].copy_from_slice(&(8i32).to_le_bytes());

        let resp = parse_engine_response(&payload).expect("short Data body copies available bytes");
        assert_eq!(resp.data, vec![0xAA, 0xBB]);
    }
}

#[cfg(test)]
mod base_check_tests {
    use super::*;
    use crate::commands::market::{BaseCurrency, ExchangeCode};

    /// Helper: build wire-payload for BaseCheck response from a fully-populated `ServerInfo`.
    /// Reverse of `parse_base_check_response` for round-trip testing.
    ///
    /// Fields are written in the same order as the server (`MoonProtoEngineServer.pas:262-271`).
    /// Each field is written only when `Some(...)`; the first `None` stops writing
    /// (this matches truncation semantics — the following fields become
    /// "unavailable" to the parser).
    fn encode_full(info: &ServerInfo) -> Vec<u8> {
        let mut buf = Vec::new();
        let Some(id) = info.bot_id else { return buf };
        buf.extend_from_slice(&id.to_le_bytes());
        let Some(name) = &info.server_name else {
            return buf;
        };
        write_string(&mut buf, name);
        let Some(ex_code) = info.exchange_code else {
            return buf;
        };
        buf.push(ex_code.to_byte());
        let Some(ex_name) = &info.exchange_name else {
            return buf;
        };
        write_string(&mut buf, ex_name);
        let Some(mask) = info.exchange_type_mask else {
            return buf;
        };
        buf.push(mask.to_byte());
        let Some(dex) = &info.dex_name else {
            return buf;
        };
        write_string(&mut buf, dex);
        let Some(bc_name) = &info.base_currency_name else {
            return buf;
        };
        write_string(&mut buf, bc_name);
        let Some(bc_code) = info.base_currency_code else {
            return buf;
        };
        buf.push(bc_code.to_byte());
        let Some(sv) = info.server_version else {
            return buf;
        };
        buf.extend_from_slice(&(sv as i32).to_le_bytes());
        let Some(mp) = info.moonproto_version else {
            return buf;
        };
        buf.extend_from_slice(&(mp as i32).to_le_bytes());
        buf
    }

    #[test]
    fn parse_empty_payload_returns_all_none() {
        // An old server (before the multi-server extension) sends an empty response.
        // The parser must not crash — it returns the default with all None.
        let info = parse_base_check_response(&[]);
        assert_eq!(info, ServerInfo::default());
        assert!(!info.has_identity());
        assert!(info.bot_id.is_none());
        assert!(info.moonproto_version.is_none());
    }

    #[test]
    fn parse_full_payload_returns_all_fields() {
        let original = ServerInfo {
            bot_id: Some(0x12_34_56_78_9A_BC_DE_F0_i64),
            server_name: Some("Binance Main".to_string()),
            exchange_code: Some(ExchangeCode::from_byte(1)),
            exchange_name: Some("Binance Futures".to_string()),
            exchange_type_mask: Some(exchange_type_flags::FUTURES),
            dex_name: Some(String::new()), // not HL futures → empty
            base_currency_name: Some("USDT".to_string()),
            base_currency_code: Some(BaseCurrency::USDT),
            server_version: Some(763), // v7.63
            moonproto_version: Some(3),
        };
        let payload = encode_full(&original);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed, original);
        assert!(parsed.has_identity());
        assert!(parsed.supports(exchange_type_flags::FUTURES));
        assert!(!parsed.supports(exchange_type_flags::SPOT));
    }

    #[test]
    fn base_check_helper_writes_strings_like_delphi() {
        let original = ServerInfo {
            bot_id: Some(123456789),
            server_name: Some("S".repeat(65_537)),
            exchange_code: Some(ExchangeCode::Binance),
            exchange_name: Some("Exchange".to_string()),
            exchange_type_mask: Some(exchange_type_flags::SPOT | exchange_type_flags::FUTURES),
            dex_name: Some("Dex".to_string()),
            base_currency_name: Some("USDT".to_string()),
            base_currency_code: Some(BaseCurrency::USDT),
            server_version: Some(763),
            moonproto_version: Some(3),
        };

        let payload = encode_full(&original);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed.server_name, Some("S".to_string()));
        assert_eq!(parsed.exchange_code, Some(ExchangeCode::Binance));
        assert_eq!(parsed.moonproto_version, Some(3));
    }

    #[test]
    fn parse_hl_futures_with_hip3_dex_name() {
        // Hyperliquid futures with a HIP-3 dex — all 4 types in mask + non-empty dex_name.
        let original = ServerInfo {
            bot_id: Some(42),
            server_name: Some("Hyper Test".to_string()),
            exchange_code: Some(ExchangeCode::ByBit),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(exchange_type_flags::FUTURES | exchange_type_flags::DEX),
            dex_name: Some("HIP3-PERPS".to_string()),
            base_currency_name: Some("USDC".to_string()),
            base_currency_code: Some(BaseCurrency::TUSD),
            server_version: Some(763),
            moonproto_version: Some(3),
        };
        let payload = encode_full(&original);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed, original);
        assert!(parsed.supports(exchange_type_flags::FUTURES));
        assert!(parsed.supports(exchange_type_flags::DEX));
        assert!(!parsed.supports(exchange_type_flags::SPOT));
        assert!(!parsed.supports(exchange_type_flags::PREDICT));
    }

    #[test]
    fn parse_truncated_at_server_name_returns_only_bot_id() {
        // bot_id is present, server_name is truncated in the middle of the string header.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(42_i64).to_le_bytes());
        buf.push(0x05); // partial u16 length for server_name (only 1 byte)
        let info = parse_base_check_response(&buf);
        assert_eq!(info.bot_id, Some(42));
        assert!(info.server_name.is_none());
        assert!(info.exchange_code.is_none());
    }

    #[test]
    fn parse_truncated_at_exchange_code_returns_three_fields() {
        // bot_id + server_name are present, exchange_code (1 byte) is truncated.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(7_i64).to_le_bytes());
        buf.extend_from_slice(&(4u16.to_le_bytes()));
        buf.extend_from_slice(b"name");
        // exchange_code (1 byte) is absent.
        let info = parse_base_check_response(&buf);
        assert_eq!(info.bot_id, Some(7));
        assert_eq!(info.server_name.as_deref(), Some("name"));
        assert!(info.exchange_code.is_none());
        assert!(info.exchange_name.is_none());
    }

    #[test]
    fn parse_truncated_at_server_version_keeps_eight_fields() {
        // Eight fields are populated; there is not enough data for server_version (i32).
        let info_partial = ServerInfo {
            bot_id: Some(1),
            server_name: Some("y".to_string()),
            exchange_code: Some(ExchangeCode::FBybit),
            exchange_name: Some("Bybit".to_string()),
            exchange_type_mask: Some(exchange_type_flags::FUTURES),
            dex_name: Some(String::new()),
            base_currency_name: Some("USD".to_string()),
            base_currency_code: Some(BaseCurrency::BNB),
            server_version: None,
            moonproto_version: None,
        };
        let mut payload = encode_full(&info_partial);
        // Append a truncated 2 bytes instead of the full 4 for server_version.
        payload.extend_from_slice(&[0xAA, 0xBB]);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed.bot_id, Some(1));
        assert_eq!(parsed.base_currency_code, Some(BaseCurrency::BNB));
        assert!(parsed.server_version.is_none());
        assert!(parsed.moonproto_version.is_none());
    }

    #[test]
    fn parse_only_moonproto_version_missing() {
        // All 9 fields except the last one.
        let info_partial = ServerInfo {
            bot_id: Some(0xABC_i64),
            server_name: Some("Test".to_string()),
            exchange_code: Some(ExchangeCode::FBinance),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(exchange_type_flags::DEX | exchange_type_flags::FUTURES),
            dex_name: Some("DEX-NAME".to_string()),
            base_currency_name: Some("USDC".to_string()),
            base_currency_code: Some(BaseCurrency::TUSD),
            server_version: Some(763),
            moonproto_version: None,
        };
        let payload = encode_full(&info_partial);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed, info_partial);
        assert!(parsed.has_identity());
        assert!(parsed.moonproto_version.is_none());
    }

    #[test]
    fn parse_predict_market_bit() {
        let info = ServerInfo {
            bot_id: Some(99),
            server_name: Some("HL Predict".to_string()),
            exchange_code: Some(ExchangeCode::ByBit),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(exchange_type_flags::DEX | exchange_type_flags::PREDICT),
            dex_name: Some(String::new()),
            base_currency_name: Some("USDC".to_string()),
            base_currency_code: Some(BaseCurrency::TUSD),
            server_version: Some(763),
            moonproto_version: Some(3),
        };
        let parsed = parse_base_check_response(&encode_full(&info));
        assert!(parsed.supports(exchange_type_flags::PREDICT));
        assert!(parsed.supports(exchange_type_flags::DEX));
        assert!(!parsed.supports(exchange_type_flags::FUTURES));
        assert!(!parsed.supports(exchange_type_flags::SPOT));
    }

    #[test]
    fn server_info_default_has_no_identity_and_no_flags() {
        let info = ServerInfo::default();
        assert!(!info.has_identity());
        assert!(!info.supports(exchange_type_flags::SPOT));
        assert!(!info.supports(exchange_type_flags::FUTURES));
    }

    #[test]
    fn parse_zero_length_strings_are_some_empty() {
        // The server may explicitly send an empty string (for example `dex_name` for a
        // non-HL exchange). `Some("")` is distinct from `None`.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1_i64).to_le_bytes());
        buf.extend_from_slice(&(0u16.to_le_bytes())); // server_name = ""
        let info = parse_base_check_response(&buf);
        assert_eq!(info.bot_id, Some(1));
        assert_eq!(info.server_name.as_deref(), Some(""));
        assert!(info.exchange_code.is_none());
    }

    #[test]
    fn parse_does_not_panic_on_random_garbage() {
        // Stress: random bytes must not cause a panic.
        // The Delphi-style decoder replaces invalid bytes with '?'.
        let garbage: Vec<u8> = (0..200).map(|i| ((i * 7) ^ 0xA5) as u8).collect();
        let _info = parse_base_check_response(&garbage);
        // The parser survives; the concrete values depend on the random pattern.
    }
}

#[cfg(test)]
mod auth_check_tests {
    use super::*;

    #[test]
    fn parse_minimal_auth_check() {
        // BinanceAccountID(8) + BTCAddress("")(2) + spot_ref(4) + is_sub_account(1) + AccountID("acc")(2+3)
        let mut data = Vec::new();
        data.extend_from_slice(&(123i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes()); // empty BTCAddress
        data.extend_from_slice(&(7i32).to_le_bytes()); // spot_ref
        data.push(1); // is_sub_account=true
        data.extend_from_slice(&(3u16).to_le_bytes()); // AccountID length
        data.extend_from_slice(b"acc");
        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.binance_account_id, 123);
        assert_eq!(resp.btc_address, "");
        assert_eq!(resp.spot_ref, 7);
        assert!(resp.is_sub_account);
        assert_eq!(resp.account_id, "acc");
        assert!(resp.recvd_max_payload.is_none());
        assert!(resp.known_dexes.is_empty());
    }

    #[test]
    fn parse_with_dexes() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        // Phase 2:
        data.extend_from_slice(&(1024i32).to_le_bytes()); // recvd_max_payload
        data.push(2); // cnt=2 dexes
                      // Dex 0: name="" + collateral=0 (USDC)
        let mut dex0 = vec![0u8; 18];
        dex0[0] = 0; // name length=0
                     // collateral_token (offset 16..18) = 0 LE → already zero
        data.extend_from_slice(&dex0);
        // Dex 1: name="usdh", collateral=360 (USDH)
        let mut dex1 = vec![0u8; 18];
        dex1[0] = 4; // name length=4
        dex1[1..5].copy_from_slice(b"usdh");
        dex1[16..18].copy_from_slice(&(360u16).to_le_bytes());
        data.extend_from_slice(&dex1);
        data.push(7); // hl_dex_market=7
        data.push(3); // hl_spot_market=3

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.recvd_max_payload, Some(1024));
        assert_eq!(resp.known_dexes.len(), 2);
        assert_eq!(resp.known_dexes[0].collateral_token, 0);
        assert_eq!(resp.known_dexes[1].name, "usdh");
        assert_eq!(resp.known_dexes[1].collateral_token, 360);
        assert_eq!(resp.hl_dex_market, Some(HyperDexIndex::from_byte(7)));
        assert_eq!(resp.hl_spot_market, Some(HyperDexIndex::from_byte(3)));
    }

    #[test]
    fn auth_check_dex_count_keeps_declared_zero_tail_records_like_delphi_loop() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(1024i32).to_le_bytes());
        data.push(2);

        let mut dex0 = vec![0u8; 18];
        dex0[0] = 4;
        dex0[1..5].copy_from_slice(b"usdc");
        dex0[16..18].copy_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&dex0);

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.recvd_max_payload, Some(1024));
        assert_eq!(resp.known_dexes.len(), 2);
        assert_eq!(resp.known_dexes[0].name, "usdc");
        assert_eq!(resp.known_dexes[1].name, "");
        assert_eq!(resp.known_dexes[1].collateral_token, 0);
        assert_eq!(resp.hl_dex_market, None);
        assert_eq!(resp.hl_spot_market, None);
    }

    #[test]
    fn auth_check_partial_recvd_max_payload_uses_delphi_read_tail() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&[0x34, 0x12]);

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.recvd_max_payload, Some(0x1234));
        assert!(resp.known_dexes.is_empty());
        assert_eq!(resp.hl_dex_market, None);
        assert_eq!(resp.hl_spot_market, None);
    }

    #[test]
    fn auth_check_partial_dex_record_is_not_reused_as_hl_dex_market() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(1024i32).to_le_bytes());
        data.push(1);
        data.push(4); // partial ShortString length byte inside THLDexInfo, not HLDexMarket

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.known_dexes.len(), 1);
        assert_eq!(resp.known_dexes[0].name, "\0\0\0\0");
        assert_eq!(resp.known_dexes[0].collateral_token, 0);
        assert_eq!(
            resp.hl_dex_market, None,
            "Delphi consumed the byte inside DataStream.Read(THLDexInfo); Rust must not treat it as HLDexMarket"
        );
        assert_eq!(resp.hl_spot_market, None);
    }

    #[test]
    fn parse_dex_invalid_utf8_uses_delphi_question_mark_fallback() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(1024i32).to_le_bytes());
        data.push(1);

        let mut dex = vec![0u8; 18];
        dex[0] = 3;
        dex[1..4].copy_from_slice(&[b'd', 0xFF, b'x']);
        dex[16..18].copy_from_slice(&(7u16).to_le_bytes());
        data.extend_from_slice(&dex);

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.known_dexes[0].name, "d?x");
        assert_eq!(resp.known_dexes[0].collateral_token, 7);
    }
}
