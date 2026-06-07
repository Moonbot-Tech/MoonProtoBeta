#[cfg(test)]
use super::{
    read_corr_market, read_market_with_local_shift, write_corr_market,
    write_market_with_local_shift, EngineStreamReader,
};
use super::{CorrMarket, Market};
#[cfg(test)]
use crate::commands::candles::current_local_time_shift_minutes;

pub(crate) const CORR_MARKET_MIN_WIRE_SIZE: usize = 6;
pub(crate) const MARKET_MIN_WIRE_SIZE: usize = 20;
pub(crate) const MAX_MARKETS_LIST_ROWS: usize = u16::MAX as usize + 1;

/// `emk_GetMarketsList` response: full list of markets + CorrMarkets.
/// Wire-form (MoonProtoEngineServer.pas:60-82 `WriteMarketsToStream`):
///   `count:i32 + markets[count] + corr_count:i32 + corr_markets[corr_count]`.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct MarketsListResponse {
    pub markets: Vec<Market>,
    pub corr_markets: Vec<CorrMarket>,
}

/// Parse `EngineResponse.data` for `emk_GetMarketsList`.
#[cfg(test)]
pub(crate) fn parse_markets_list_response(data: &[u8], ver: u16) -> Option<MarketsListResponse> {
    parse_markets_list_response_with_local_shift(data, ver, current_local_time_shift_minutes())
}

#[cfg(test)]
pub(super) fn parse_markets_list_response_with_local_shift(
    data: &[u8],
    ver: u16,
    local_shift_minutes: f64,
) -> Option<MarketsListResponse> {
    let mut r = EngineStreamReader::new(data);
    let count = r.read_count_bounded(
        MARKET_MIN_WIRE_SIZE,
        MAX_MARKETS_LIST_ROWS,
        "GetMarketsList.markets",
    )?;
    let mut markets = Vec::new();
    markets.try_reserve_exact(count).ok()?;
    for _ in 0..count {
        markets.push(read_market_with_local_shift(
            &mut r,
            ver,
            local_shift_minutes,
        )?);
    }
    let corr_count = r.read_count_bounded(
        CORR_MARKET_MIN_WIRE_SIZE,
        MAX_MARKETS_LIST_ROWS,
        "GetMarketsList.corr_markets",
    )?;
    let mut corr_markets = Vec::new();
    corr_markets.try_reserve_exact(corr_count).ok()?;
    for _ in 0..corr_count {
        corr_markets.push(read_corr_market(&mut r)?);
    }
    Some(MarketsListResponse {
        markets,
        corr_markets,
    })
}

/// Optional builder for tests.
#[cfg(test)]
pub(crate) fn build_markets_list_response(resp: &MarketsListResponse, ver: u16) -> Vec<u8> {
    build_markets_list_response_with_local_shift(resp, ver, current_local_time_shift_minutes())
}

#[cfg(test)]
pub(super) fn build_markets_list_response_with_local_shift(
    resp: &MarketsListResponse,
    ver: u16,
    local_shift_minutes: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);
    out.extend_from_slice(&(resp.markets.len() as i32).to_le_bytes());
    for m in &resp.markets {
        write_market_with_local_shift(&mut out, m, ver, local_shift_minutes);
    }
    out.extend_from_slice(&(resp.corr_markets.len() as i32).to_le_bytes());
    for c in &resp.corr_markets {
        write_corr_market(&mut out, c);
    }
    out
}
