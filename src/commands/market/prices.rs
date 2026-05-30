#[cfg(test)]
use super::{
    apply_delphi_local_funding_shift, remove_delphi_local_funding_shift, write_str,
    EngineStreamReader,
};
#[cfg(test)]
use crate::commands::candles::current_local_time_shift_minutes;
use crate::time::DelphiTime;

/// Price update for a single market (byte-exact with `WriteMarketPricesToStream`
/// MoonProtoSerialization.pas:195-209).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketPriceUpdate {
    pub m_index: u16,
    pub bid: f64,
    pub ask: f64,
    /// If `MarketsPricesResponse.send_funding == false`, this is 0.0.
    pub funding_rate: f64,
    /// Delphi client-local `TDateTime` after adding local TZShift. If source
    /// `funding_time` was 0 → 0.
    pub funding_time: f64,
    pub mark_price: f64,
    pub mark_price_found: bool,
}

impl MarketPriceUpdate {
    pub fn funding_time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.funding_time)
    }
}

/// `CorrMarket` price update.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct CorrMarketPriceUpdate {
    pub bn_market_name: String,
    pub last_price: f64,
}

/// Full `emk_UpdateMarketsList` response.
/// Wire-form (MoonProtoEngineServer.pas:84-111):
///   `send_funding:bool + count:i32 + prices[count] + send_corr_markets:bool +
///    (if send_corr_markets) corr_count:i32 + corr_prices[corr_count]`.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct MarketsPricesResponse {
    pub send_funding: bool,
    pub prices: Vec<MarketPriceUpdate>,
    pub send_corr_markets: bool,
    pub corr_prices: Vec<CorrMarketPriceUpdate>,
}

#[cfg(test)]
pub(super) fn parse_markets_prices_response_with_local_shift(
    data: &[u8],
    local_shift_minutes: f64,
) -> Option<MarketsPricesResponse> {
    let mut r = EngineStreamReader::new(data);
    let send_funding = r.read_bool()?;
    // MarketPriceUpdate minimum: m_index(2) + bid(8) + ask(8) + mark_price(8) + mark_found(1) = 27 bytes.
    // If send_funding=true, +16 more. 27 is used only for bounded prealloc.
    let count = r.read_count()?;
    let mut prices = Vec::with_capacity(r.bounded_count_capacity(count, 27));
    for _ in 0..count {
        let m_index = r.read_word()?;
        let bid = r.read_double()?;
        let ask = r.read_double()?;
        let (funding_rate, funding_time) = if send_funding {
            (
                r.read_double()?,
                apply_delphi_local_funding_shift(r.read_double()?, local_shift_minutes),
            )
        } else {
            (0.0, 0.0)
        };
        let mark_price = r.read_double()?;
        let mark_price_found = r.read_bool()?;
        prices.push(MarketPriceUpdate {
            m_index,
            bid,
            ask,
            funding_rate,
            funding_time,
            mark_price,
            mark_price_found,
        });
    }
    let send_corr_markets = r.read_bool()?;
    let mut corr_prices = Vec::new();
    if send_corr_markets {
        // CorrMarketPriceUpdate: bn_market_name (string u16+chars) + last_price (8) = at least 10 bytes.
        let corr_count = r.read_count()?;
        corr_prices.reserve(r.bounded_count_capacity(corr_count, 10));
        for _ in 0..corr_count {
            let bn_market_name = r.read_str()?;
            let last_price = r.read_double()?;
            corr_prices.push(CorrMarketPriceUpdate {
                bn_market_name,
                last_price,
            });
        }
    }
    Some(MarketsPricesResponse {
        send_funding,
        prices,
        send_corr_markets,
        corr_prices,
    })
}

#[cfg(test)]
pub(crate) fn build_markets_prices_response(resp: &MarketsPricesResponse) -> Vec<u8> {
    build_markets_prices_response_with_local_shift(resp, current_local_time_shift_minutes())
}

#[cfg(test)]
pub(super) fn build_markets_prices_response_with_local_shift(
    resp: &MarketsPricesResponse,
    local_shift_minutes: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + resp.prices.len() * 50);
    out.push(resp.send_funding as u8);
    out.extend_from_slice(&(resp.prices.len() as i32).to_le_bytes());
    for p in &resp.prices {
        out.extend_from_slice(&p.m_index.to_le_bytes());
        out.extend_from_slice(&p.bid.to_le_bytes());
        out.extend_from_slice(&p.ask.to_le_bytes());
        if resp.send_funding {
            out.extend_from_slice(&p.funding_rate.to_le_bytes());
            let wire_funding_time =
                remove_delphi_local_funding_shift(p.funding_time, local_shift_minutes);
            out.extend_from_slice(&wire_funding_time.to_le_bytes());
        }
        out.extend_from_slice(&p.mark_price.to_le_bytes());
        out.push(p.mark_price_found as u8);
    }
    out.push(resp.send_corr_markets as u8);
    if resp.send_corr_markets {
        out.extend_from_slice(&(resp.corr_prices.len() as i32).to_le_bytes());
        for c in &resp.corr_prices {
            write_str(&mut out, &c.bn_market_name);
            out.extend_from_slice(&c.last_price.to_le_bytes());
        }
    }
    out
}
