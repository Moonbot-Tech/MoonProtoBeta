/// Engine API Request builder — для отправки RPC-вызовов на сервер.
/// Byte-exact порт `TEngineRequest.StoreToStream` (MoonProtoEngineStruct.pas:326-349).
///
/// Wire format:
///   [BaseCommand header: CmdId(1) + ver(2) + UID(8)]
///   Method      (1 байт, TEngineMethodKind ordinal)
///   MarketName  (2 + N байт, UTF-8 с u16 префиксом длины)
///   MarketNames count (4 байта, i32 LE)
///   MarketNames[] (count × UTF-8 строк)
///   ParamsSize  (4 байта, i32 LE)
///   Params      (ParamsSize байт — содержимое FStream)
///
/// Params собирается через хелперы `params::write_*` (см. ниже) и обычно содержит:
///   `req.WriteInt(NewLev)`, `req.WriteByte(...)`, `req.WriteBool(...)`, `req.WriteWord(...)`.

use super::registry::{CURRENT_PROTO_CMD_VER, write_string};
use super::engine_api::EngineMethod;

const ENGINE_REQUEST_CMD_ID: u8 = 2; // TEngineRequest CmdId

/// Хелперы для построения `params` payload — соответствуют Delphi `TEngineStreamCommand.Write*`.
/// Все поля LE.
pub mod params {
    /// `WriteDouble(v: double)` — 8 байт LE f64.
    pub fn write_double(buf: &mut Vec<u8>, v: f64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// `WriteInt(v: integer)` — 4 байта LE i32.
    pub fn write_int(buf: &mut Vec<u8>, v: i32) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// `WriteWord(v: word)` — 2 байта LE u16.
    pub fn write_word(buf: &mut Vec<u8>, v: u16) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// `WriteByte(v: byte)` — 1 байт.
    pub fn write_byte(buf: &mut Vec<u8>, v: u8) {
        buf.push(v);
    }
    /// `WriteInt64(v: int64)` — 8 байт LE i64.
    pub fn write_int64(buf: &mut Vec<u8>, v: i64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    /// `WriteBool(v: boolean)` — 1 байт (0 / 1).
    pub fn write_bool(buf: &mut Vec<u8>, v: bool) {
        buf.push(v as u8);
    }
    /// `WriteStr(s: string)` — UTF-8 с 2-байтовым LE префиксом длины.
    pub fn write_str(buf: &mut Vec<u8>, s: &str) {
        super::write_string(buf, s);
    }
}

/// Общий low-level билдер — собирает полный wire-пакет `TEngineRequest`.
/// `params` — уже сериализованное содержимое FStream (через `params::write_*`).
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

    // MarketName (UTF-8 string с u16 префиксом)
    write_string(&mut buf, market_name);

    // MarketNames array: count:i32 LE + N × string
    let count = market_names.len() as i32;
    buf.extend_from_slice(&count.to_le_bytes());
    for name in market_names {
        write_string(&mut buf, name);
    }

    // ParamsSize (4 байта LE i32) + params
    let params_size = params.len() as i32;
    buf.extend_from_slice(&params_size.to_le_bytes());
    buf.extend_from_slice(params);

    buf
}

/// Backward-compat обёртка — для команд БЕЗ параметров (params_size = 0).
pub fn build_engine_request(method: EngineMethod, market_name: &str, market_names: &[&str]) -> Vec<u8> {
    build_engine_request_full(method, market_name, market_names, &[])
}

// ============================================================================
//  Простые builders (без параметров)
// ============================================================================

/// `emk_SubscribeAllTrades` — подписка на TradesStream.
///
/// `want_mm_orders` — нужны ли market-maker ордера (TradesStream section 01). Зависит от
/// прикладной логики app (есть ли активные MM-стратегии / включена ли heatmap). Liба
/// не решает за app — это публичный параметр.
///
/// Wire-format: params = 1 byte bool (Delphi `MoonProtoEngine.pas:274
/// req.WriteBool(MMOrdersSubscribed)`).
pub fn subscribe_all_trades(want_mm_orders: bool) -> Vec<u8> {
    let params = [if want_mm_orders { 1u8 } else { 0u8 }];
    build_engine_request_full(EngineMethod::SubscribeAllTrades, "", &[], &params)
}

/// `emk_UnsubscribeAllTrades` — отписка от TradesStream.
pub fn unsubscribe_all_trades() -> Vec<u8> {
    build_engine_request(EngineMethod::UnsubscribeAllTrades, "", &[])
}

/// `emk_SubscribeOrderBook` — подписка на orderbook для списка маркетов.
pub fn subscribe_order_book(markets: &[&str]) -> Vec<u8> {
    build_engine_request(EngineMethod::SubscribeOrderBook, "", markets)
}

/// `emk_UnsubscribeOrderBook` — отписка.
pub fn unsubscribe_order_book(markets: &[&str]) -> Vec<u8> {
    build_engine_request(EngineMethod::UnsubscribeOrderBook, "", markets)
}

/// `emk_BaseCheck` — health-check.
pub fn base_check() -> Vec<u8> {
    build_engine_request(EngineMethod::BaseCheck, "", &[])
}

/// `emk_AuthCheck` — auth-проверка.
pub fn auth_check() -> Vec<u8> {
    build_engine_request(EngineMethod::AuthCheck, "", &[])
}

/// `emk_GetMarketsList` — получить полный список маркетов.
pub fn get_markets_list() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsList, "", &[])
}

/// `emk_GetMarketsIndexes` — получить mIndex маппинг.
pub fn get_markets_indexes() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsIndexes, "", &[])
}

/// `emk_UpdateMarketsList` — апдейт списка маркетов.
pub fn update_markets_list() -> Vec<u8> {
    build_engine_request(EngineMethod::UpdateMarketsList, "", &[])
}

/// `emk_GetMarketsBalanceFull` — полный snapshot балансов.
pub fn get_markets_balance_full() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsBalanceFull, "", &[])
}

/// `emk_CancelAllOrders` — отменить все ордера.
pub fn cancel_all_orders() -> Vec<u8> {
    build_engine_request(EngineMethod::CancelAllOrders, "", &[])
}

/// `emk_CheckAPIExpirationTime` — проверка expiration API ключа.
pub fn check_api_expiration_time() -> Vec<u8> {
    build_engine_request(EngineMethod::CheckAPIExpirationTime, "", &[])
}

/// `emk_CheckBinanceTags` — проверка Binance tags.
pub fn check_binance_tags() -> Vec<u8> {
    build_engine_request(EngineMethod::CheckBinanceTags, "", &[])
}

/// `emk_ReloadOrderBook` — full reload orderbook (как хоткей).
pub fn reload_order_book() -> Vec<u8> {
    build_engine_request(EngineMethod::ReloadOrderBook, "", &[])
}

/// `emk_ConvertDustBNB` — конвертация dust в BNB.
pub fn convert_dust_bnb() -> Vec<u8> {
    build_engine_request(EngineMethod::ConvertDustBNB, "", &[])
}

// ============================================================================
//  Параметризованные builders (с params payload)
// ============================================================================

/// `emk_SetLeverage(m, NewLev)` — установить leverage.
/// Wire: market_name + WriteInt(NewLev).
/// Delphi MoonProtoEngine.pas:934-946.
pub fn set_leverage(market_name: &str, new_lev: i32) -> Vec<u8> {
    let mut params = Vec::with_capacity(4);
    params::write_int(&mut params, new_lev);
    build_engine_request_full(EngineMethod::SetLeverage, market_name, &[], &params)
}

/// `emk_SetHedgeMode(HedgeMode)` — установить hedge mode.
/// Wire: WriteBool(HedgeMode).
/// Delphi MoonProtoEngine.pas:948-960.
pub fn set_hedge_mode(hedge_mode: bool) -> Vec<u8> {
    let mut params = Vec::with_capacity(1);
    params::write_bool(&mut params, hedge_mode);
    build_engine_request_full(EngineMethod::SetHedgeMode, "", &[], &params)
}

/// `emk_QueryHedgeMode()` — запрос текущего hedge mode (без параметров).
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

/// `emk_RequestOrderBookFull(marketIdx, bookKind)` — запросить full snapshot OB.
/// Wire: WriteWord(marketIdx) + WriteByte(Ord(bookKind)).
/// Delphi MoonProtoEngine.pas:1940-1948.
/// `bookKind`: 0=Futures, 1=Spot.
pub fn request_order_book_full(market_idx: u16, book_kind: u8) -> Vec<u8> {
    let mut params = Vec::with_capacity(3);
    params::write_word(&mut params, market_idx);
    params::write_byte(&mut params, book_kind);
    build_engine_request_full(EngineMethod::RequestOrderBookFull, "", &[], &params)
}

/// `emk_TradesResend(packet_nums)` — запросить resend пакетов трейдов.
/// Wire: WriteByte(count) + count × WriteWord(packet_num).
/// **NB:** count кодируется как `Byte` (1 байт), MAX 200 пакетов на батч (Delphi clamps).
/// Если у тебя > 200 — делай несколько вызовов (как в Delphi `SendTradesResendBatch` MoonProtoEngine.pas:1348-1362).
/// Возвращает Vec\<Vec\<u8\>\>: один или несколько готовых wire-payload'ов.
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
        out.push(build_engine_request_full(EngineMethod::TradesResend, "", &[], &params));
        i += cnt;
    }
    out
}

/// `emk_ConfirmRiskLimit(m)` — fire-and-forget подтверждение risk limit.
/// Только market_name, params пустые.
pub fn confirm_risk_limit(market_name: &str) -> Vec<u8> {
    build_engine_request(EngineMethod::ConfirmRiskLimit, market_name, &[])
}

/// `emk_SetMAMode(MAMode)` — fire-and-forget set MA mode.
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

/// `emk_UpdateTransferAssets(EKind)` — fire-and-forget.
/// Wire: WriteByte(EKind).
pub fn update_transfer_assets(e_kind: u8) -> Vec<u8> {
    let mut params = Vec::with_capacity(1);
    params::write_byte(&mut params, e_kind);
    build_engine_request_full(EngineMethod::UpdateTransferAssets, "", &[], &params)
}

/// `emk_GetOrder(AOrder)` — запросить статус конкретного ордера по UID.
/// Wire: WriteInt64(uid).
pub fn get_order(uid: u64) -> Vec<u8> {
    let mut params = Vec::with_capacity(8);
    params::write_int64(&mut params, uid as i64);
    build_engine_request_full(EngineMethod::GetOrder, "", &[], &params)
}

/// `emk_GetBalance(Currency)` — запросить балансы для конкретной валюты.
/// Wire: WriteStr(Currency).
pub fn get_balance(currency: &str) -> Vec<u8> {
    let mut params = Vec::with_capacity(2 + currency.len());
    params::write_str(&mut params, currency);
    build_engine_request_full(EngineMethod::GetBalance, "", &[], &params)
}

/// `emk_GetOpenOrders` / `emk_GetActiveOrders` — без параметров.
pub fn get_open_orders() -> Vec<u8> {
    build_engine_request(EngineMethod::GetOpenOrders, "", &[])
}
pub fn get_active_orders() -> Vec<u8> {
    build_engine_request(EngineMethod::GetActiveOrders, "", &[])
}

/// `emk_RequestCandlesData` — запрос chunked candles + wall data.
/// Request: empty (no params). Response: chunked, см. `commands::candles::CandlesAggregator`.
/// MoonProtoServer.pas:992 `SendCandlesDataChunked` — сервер сам решает что слать
/// (использует `Markets.GetCandlesStream`).
pub fn request_candles_data() -> Vec<u8> {
    build_engine_request(EngineMethod::RequestCandlesData, "", &[])
}

// `emk_GetCoinCardCandles` — реализован в `commands::candles::get_coin_card_candles`
// (отдельный модуль для compactness — там же DeepPrice struct и CandlesAggregator).
