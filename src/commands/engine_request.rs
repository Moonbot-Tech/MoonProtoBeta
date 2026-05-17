/// Engine API Request builder — for sending RPC calls to server.
/// Byte-exact port of TEngineRequest.StoreToStream (MoonProtoEngineStruct.pas:326-349).
///
/// Wire format:
///   [BaseCommand header: CmdId(1) + ver(2) + UID(8)]
///   Method      (1 byte, TEngineMethodKind ordinal)
///   MarketName  (2+N bytes, UTF-8 string with u16 length prefix)
///   MarketNames count (4 bytes, i32 LE)
///   MarketNames[] (count × UTF-8 strings)
///   ParamsSize  (4 bytes, i32 LE)
///   Params      (ParamsSize bytes)

use super::registry::{CURRENT_PROTO_CMD_VER, write_string};
use super::engine_api::EngineMethod;

const ENGINE_REQUEST_CMD_ID: u8 = 2; // TEngineRequest CmdId

/// Build an Engine API request packet (ready to be sent as MPC_API command).
pub fn build_engine_request(method: EngineMethod, market_name: &str, market_names: &[&str]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);

    // BaseCommand header: CmdId + ver + UID
    buf.push(ENGINE_REQUEST_CMD_ID);
    buf.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    let uid: u64 = rand::random();
    buf.extend_from_slice(&uid.to_le_bytes());

    // Method (1 byte)
    buf.push(method as u8);

    // MarketName (string)
    write_string(&mut buf, market_name);

    // MarketNames array
    let count = market_names.len() as i32;
    buf.extend_from_slice(&count.to_le_bytes());
    for name in market_names {
        write_string(&mut buf, name);
    }

    // Params (empty for subscribe/unsubscribe)
    let params_size: i32 = 0;
    buf.extend_from_slice(&params_size.to_le_bytes());

    buf
}

/// Build a simple subscribe-all-trades request
pub fn subscribe_all_trades() -> Vec<u8> {
    build_engine_request(EngineMethod::SubscribeAllTrades, "", &[])
}

/// Build unsubscribe-all-trades request
pub fn unsubscribe_all_trades() -> Vec<u8> {
    build_engine_request(EngineMethod::UnsubscribeAllTrades, "", &[])
}

/// Build subscribe-orderbook request for specific markets
pub fn subscribe_order_book(markets: &[&str]) -> Vec<u8> {
    build_engine_request(EngineMethod::SubscribeOrderBook, "", markets)
}

/// Build base check request (health check)
pub fn base_check() -> Vec<u8> {
    build_engine_request(EngineMethod::BaseCheck, "", &[])
}

/// Build auth check request
pub fn auth_check() -> Vec<u8> {
    build_engine_request(EngineMethod::AuthCheck, "", &[])
}

/// Build get markets list request
pub fn get_markets_list() -> Vec<u8> {
    build_engine_request(EngineMethod::GetMarketsList, "", &[])
}
