use crate::commands::candles::{CandlesAggregator, RequestCandlesMarket};
use crate::commands::engine_api::EngineMethod;
use std::sync::mpsc;
// =============================================================================
//  CandlesAggregator async API
// =============================================================================

/// Merged result returned by `api_request_candles_data_async`.
///
/// The server answers `RequestCandlesData` with several `EngineResponse` chunks
/// sharing one `request_uid`. The library aggregates those chunks through
/// [`CandlesAggregator`] and returns both the merged zlib stream and parsed
/// market entries.
#[derive(Debug, Clone)]
pub struct MergedCandles {
    /// Request UID used to correlate the chunked response.
    pub uid: u64,
    /// Merged zlib stream from Delphi `TMarkets.StoreCandlesToZip`.
    pub zipped_data: Vec<u8>,
    /// Parsed market entries from the zipped stream.
    pub markets: Vec<RequestCandlesMarket>,
}

/// Внутреннее состояние частично собранного набора свечей.
pub(crate) struct PartialCandles {
    pub(crate) aggregator: CandlesAggregator,
    /// Sender который будет уведомлён когда aggregator вернёт merged.
    pub(crate) sender: mpsc::Sender<MergedCandles>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EngineResponseMeta {
    pub(crate) request_uid: u64,
    pub(crate) method: EngineMethod,
    pub(crate) success: bool,
}
