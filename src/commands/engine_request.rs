use super::engine_api::EngineMethod;
/// Engine API request builders for server RPC calls.
///
/// Byte-exact port of Delphi `TEngineRequest.StoreToStream`
/// (`MoonProtoEngineStruct.pas:326-349`).
///
/// Wire format:
///   [BaseCommand header: CmdId(1) + ver(2) + UID(8)]
///   Method      (1 byte, TEngineMethodKind ordinal)
///   MarketName  (u16 length prefix + UTF-8 bytes)
///   MarketNames count (i32 little-endian)
///   MarketNames[] (count x UTF-8 strings)
///   ParamsSize  (i32 little-endian)
///   Params      (ParamsSize bytes; Delphi FStream contents)
///
/// Build `Params` with the `params::write_*` helpers below. Common Delphi
/// calls are:
///   `req.WriteInt(NewLev)`, `req.WriteByte(...)`, `req.WriteBool(...)`, `req.WriteWord(...)`.
use super::registry::{write_string, CURRENT_PROTO_CMD_VER};

const ENGINE_REQUEST_CMD_ID: u8 = 2; // TEngineRequest CmdId

/// Helpers for building the `params` payload.
///
/// They match Delphi `TEngineStreamCommand.Write*`; all multi-byte values are
/// little-endian.
pub mod params {
    /// Delphi `WriteDouble(v: double)`: 8-byte little-endian `f64`.
    pub fn write_double(buf: &mut Vec<u8>, v: f64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// Delphi `WriteInt(v: integer)`: 4-byte little-endian `i32`.
    pub fn write_int(buf: &mut Vec<u8>, v: i32) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// Delphi `WriteWord(v: word)`: 2-byte little-endian `u16`.
    pub fn write_word(buf: &mut Vec<u8>, v: u16) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// Delphi `WriteByte(v: byte)`: one byte.
    pub fn write_byte(buf: &mut Vec<u8>, v: u8) {
        buf.push(v);
    }
    /// Delphi `WriteInt64(v: int64)`: 8-byte little-endian `i64`.
    pub fn write_int64(buf: &mut Vec<u8>, v: i64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// Delphi `WriteBool(v: boolean)`: one byte (`0` or `1`).
    pub fn write_bool(buf: &mut Vec<u8>, v: bool) {
        buf.push(v as u8);
    }
    /// Delphi `WriteStr(s: string)`: UTF-8 with a 2-byte little-endian length prefix.
    pub fn write_str(buf: &mut Vec<u8>, s: &str) {
        super::write_string(buf, s);
    }
}

/// Build a complete low-level `TEngineRequest` wire payload.
///
/// `params` must already contain the serialized Delphi FStream payload, usually
/// assembled with [`params::write_int`], [`params::write_bool`], and related
/// helpers.
pub fn build_engine_request_full(
    method: EngineMethod,
    market_name: &str,
    market_names: &[&str],
    params: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + params.len());

    // BaseCommand header: CmdId + ver + UID
    buf.push(ENGINE_REQUEST_CMD_ID);
    buf.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    let uid: u64 = rand::random();
    buf.extend_from_slice(&uid.to_le_bytes());

    // Method (1 byte)
    buf.push(method as u8);

    // MarketName (UTF-8 string with u16 length prefix)
    write_string(&mut buf, market_name);

    // MarketNames array: count:i32 LE + N strings
    let count = market_names.len() as i32;
    buf.extend_from_slice(&count.to_le_bytes());
    for name in market_names {
        write_string(&mut buf, name);
    }

    // ParamsSize (i32 LE) + params
    let params_size = params.len() as i32;
    buf.extend_from_slice(&params_size.to_le_bytes());
    buf.extend_from_slice(params);

    buf
}

/// Backward-compatible wrapper for requests without `params`.
pub fn build_engine_request(
    method: EngineMethod,
    market_name: &str,
    market_names: &[&str],
) -> Vec<u8> {
    build_engine_request_full(method, market_name, market_names, &[])
}

// ============================================================================
//  Simple builders without params
// ============================================================================

/// `emk_SubscribeAllTrades`: subscribe to the TradesStream.
///
/// `want_mm_orders` requests market-maker order sections in the stream. This is
/// application policy (active MM strategies, heatmap UI, etc.); the library does
/// not infer it.
///
/// Wire-format: params = 1 byte bool (Delphi `MoonProtoEngine.pas:274
/// req.WriteBool(MMOrdersSubscribed)`).
pub fn subscribe_all_trades(want_mm_orders: bool) -> Vec<u8> {
    let params = [if want_mm_orders { 1u8 } else { 0u8 }];
    build_engine_request_full(EngineMethod::SubscribeAllTrades, "", &[], &params)
}

/// `emk_UnsubscribeAllTrades`: unsubscribe from the TradesStream.
pub fn unsubscribe_all_trades() -> Vec<u8> {
    build_engine_request(EngineMethod::UnsubscribeAllTrades, "", &[])
}

/// `emk_SubscribeOrderBook`: subscribe to orderbooks for a batch of market names.
pub fn subscribe_order_book(markets: &[&str]) -> Vec<u8> {
    build_engine_request(EngineMethod::SubscribeOrderBook, "", markets)
}

/// `emk_UnsubscribeOrderBook`: unsubscribe from orderbooks.
pub fn unsubscribe_order_book(markets: &[&str]) -> Vec<u8> {
    build_engine_request(EngineMethod::UnsubscribeOrderBook, "", markets)
}

/// `emk_BaseCheck`: server health and identity check.
pub fn base_check() -> Vec<u8> {
    build_engine_request(EngineMethod::BaseCheck, "", &[])
}

/// `emk_AuthCheck`: check exchange API authorization.
pub fn auth_check() -> Vec<u8> {
    build_engine_request(EngineMethod::AuthCheck, "", &[])
}

/// `emk_GetMarketsList`: fetch the full market list.
pub fn get_markets_list() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsList, "", &[])
}

/// `emk_GetMarketsIndexes`: fetch the server `mIndex -> market name` mapping.
pub fn get_markets_indexes() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsIndexes, "", &[])
}

/// `emk_UpdateMarketsList`: refresh market prices, funding, and correlations.
pub fn update_markets_list() -> Vec<u8> {
    build_engine_request(EngineMethod::UpdateMarketsList, "", &[])
}

/// `emk_GetMarketsBalanceFull` — asks the server to refresh full market balances.
///
/// Current Delphi server code calls `Engine.GetMarketsBalanceFull`, but does not
/// serialize balances into the response yet (`WriteBalancesToStream` is TODO), so
/// successful responses have an empty payload.
pub fn get_markets_balance_full() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsBalanceFull, "", &[])
}

/// `emk_CancelAllOrders`: request cancellation of all orders.
pub fn cancel_all_orders() -> Vec<u8> {
    build_engine_request(EngineMethod::CancelAllOrders, "", &[])
}

/// `emk_CheckAPIExpirationTime`: fetch the exchange API-key expiration time.
pub fn check_api_expiration_time() -> Vec<u8> {
    build_engine_request(EngineMethod::CheckAPIExpirationTime, "", &[])
}

/// `emk_CheckBinanceTags`: refresh Binance token permission tags.
pub fn check_binance_tags() -> Vec<u8> {
    build_engine_request(EngineMethod::CheckBinanceTags, "", &[])
}

/// `emk_ReloadOrderBook`: trigger a full orderbook reload, like the Delphi hotkey.
pub fn reload_order_book() -> Vec<u8> {
    build_engine_request(EngineMethod::ReloadOrderBook, "", &[])
}

/// `emk_ConvertDustBNB`: convert dust balances to BNB.
pub fn convert_dust_bnb() -> Vec<u8> {
    build_engine_request(EngineMethod::ConvertDustBNB, "", &[])
}

// ============================================================================
//  Parametrized builders
// ============================================================================

/// `emk_SetLeverage(m, NewLev)`: set leverage for one market.
/// Wire: market_name + WriteInt(NewLev).
/// Delphi MoonProtoEngine.pas:934-946.
pub fn set_leverage(market_name: &str, new_lev: i32) -> Vec<u8> {
    let mut params = Vec::with_capacity(4);
    params::write_int(&mut params, new_lev);
    build_engine_request_full(EngineMethod::SetLeverage, market_name, &[], &params)
}

/// `emk_SetHedgeMode(HedgeMode)`: enable or disable hedge mode.
/// Wire: WriteBool(HedgeMode).
/// Delphi MoonProtoEngine.pas:948-960.
pub fn set_hedge_mode(hedge_mode: bool) -> Vec<u8> {
    let mut params = Vec::with_capacity(1);
    params::write_bool(&mut params, hedge_mode);
    build_engine_request_full(EngineMethod::SetHedgeMode, "", &[], &params)
}

/// `emk_QueryHedgeMode()`: query current hedge mode.
pub fn query_hedge_mode() -> Vec<u8> {
    build_engine_request(EngineMethod::QueryHedgeMode, "", &[])
}

/// `emk_ChangePositionType(Market, NewType, NewMarket)`.
/// Wire: market_name + WriteByte(Ord(NewType)) + WriteBool(NewMarket).
/// Delphi MoonProtoEngine.pas:1067-1080.
pub fn change_position_type(market_name: &str, new_type: u8, new_market: bool) -> Vec<u8> {
    let mut params = Vec::with_capacity(2);
    params::write_byte(&mut params, new_type);
    params::write_bool(&mut params, new_market);
    build_engine_request_full(EngineMethod::ChangePositionType, market_name, &[], &params)
}

/// `emk_RequestOrderBookFull(marketIdx, bookKind)`: request a full orderbook snapshot.
/// Wire: WriteWord(marketIdx) + WriteByte(Ord(bookKind)).
/// Delphi MoonProtoEngine.pas:1940-1948.
/// `bookKind`: 0=Futures, 1=Spot.
pub fn request_order_book_full(market_idx: u16, book_kind: u8) -> Vec<u8> {
    let mut params = Vec::with_capacity(3);
    params::write_word(&mut params, market_idx);
    params::write_byte(&mut params, book_kind);
    build_engine_request_full(EngineMethod::RequestOrderBookFull, "", &[], &params)
}

/// `emk_TradesResend(packet_nums)`: request resend of missing TradesStream packets.
///
/// Wire: `WriteByte(count) + count x WriteWord(packet_num)`. The count is one
/// byte and Delphi clamps each request to at most 200 packet numbers, so this
/// helper returns one or more ready-to-send request payloads.
pub fn trades_resend_batches(packet_nums: &[u16]) -> Vec<Vec<u8>> {
    if packet_nums.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < packet_nums.len() {
        let cnt = (packet_nums.len() - i).min(200);
        let mut params = Vec::with_capacity(1 + cnt * 2);
        params::write_byte(&mut params, cnt as u8);
        for &n in &packet_nums[i..i + cnt] {
            params::write_word(&mut params, n);
        }
        out.push(build_engine_request_full(
            EngineMethod::TradesResend,
            "",
            &[],
            &params,
        ));
        i += cnt;
    }
    out
}

/// `emk_ConfirmRiskLimit(m)`: fire-and-forget risk-limit confirmation.
///
/// Only `market_name` is set; params are empty.
pub fn confirm_risk_limit(market_name: &str) -> Vec<u8> {
    build_engine_request(EngineMethod::ConfirmRiskLimit, market_name, &[])
}

/// `emk_SetMAMode(MAMode)`: fire-and-forget MA mode update.
/// Wire: WriteBool(MAMode).
pub fn set_ma_mode(ma_mode: bool) -> Vec<u8> {
    let mut params = Vec::with_capacity(1);
    params::write_bool(&mut params, ma_mode);
    build_engine_request_full(EngineMethod::SetMAMode, "", &[], &params)
}

/// `emk_DoTransferAsset(Asset, q, EFrom, ETo)`.
/// Wire: WriteStr(Asset) + WriteDouble(q) + WriteByte(EFrom) + WriteByte(ETo).
pub fn do_transfer_asset(asset: &str, q: f64, e_from: u8, e_to: u8) -> Vec<u8> {
    let mut params = Vec::with_capacity(2 + asset.len() + 8 + 2);
    params::write_str(&mut params, asset);
    params::write_double(&mut params, q);
    params::write_byte(&mut params, e_from);
    params::write_byte(&mut params, e_to);
    build_engine_request_full(EngineMethod::DoTransferAsset, "", &[], &params)
}

/// `emk_UpdateTransferAssets(EKind)`: request the transferable asset list for an
/// exchange kind.
///
/// The Delphi server handles this through the Engine API worker and sends a
/// normal `EngineResponse`; use `Client::api_update_transfer_assets` when the
/// caller needs that response. The response payload is
/// `count:i32 + count * (currency:string, amount:f64, total:f64)`.
/// Wire: WriteByte(EKind).
pub fn update_transfer_assets(e_kind: u8) -> Vec<u8> {
    let mut params = Vec::with_capacity(1);
    params::write_byte(&mut params, e_kind);
    build_engine_request_full(EngineMethod::UpdateTransferAssets, "", &[], &params)
}

/// `emk_GetOrder(AOrder)` — enum/request wire exists, but the current Delphi
/// reference server does not implement this request branch and returns
/// `Unknown method`.
/// Wire: WriteInt64(uid).
pub fn get_order(uid: u64) -> Vec<u8> {
    let mut params = Vec::with_capacity(8);
    params::write_int64(&mut params, uid as i64);
    build_engine_request_full(EngineMethod::GetOrder, "", &[], &params)
}

/// `emk_GetBalance(Currency)`: request balance for one currency.
/// Wire: WriteStr(Currency).
pub fn get_balance(currency: &str) -> Vec<u8> {
    let mut params = Vec::with_capacity(2 + currency.len());
    params::write_str(&mut params, currency);
    build_engine_request_full(EngineMethod::GetBalance, "", &[], &params)
}

/// `emk_GetOpenOrders` / `emk_GetActiveOrders` — enum/request wire exists, but
/// the current Delphi reference server does not implement these request branches
/// and returns `Unknown method`.
pub fn get_open_orders() -> Vec<u8> {
    build_engine_request(EngineMethod::GetOpenOrders, "", &[])
}
pub fn get_active_orders() -> Vec<u8> {
    build_engine_request(EngineMethod::GetActiveOrders, "", &[])
}

/// `emk_RequestCandlesData`: request chunked candles and wall data.
///
/// The request has no params. The response is chunked and should normally be
/// consumed through `Client::request_candles_data` or
/// `commands::candles::CandlesAggregator`.
pub fn request_candles_data() -> Vec<u8> {
    build_engine_request(EngineMethod::RequestCandlesData, "", &[])
}

// `emk_GetCoinCardCandles` lives in `commands::candles::get_coin_card_candles`
// together with DeepPrice and CandlesAggregator.
