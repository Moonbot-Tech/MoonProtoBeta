use crate::commands::candles::{CandlesAggregator, RequestCandlesMarket};
use crate::commands::engine_api::EngineMethod;
use std::sync::mpsc;
// =============================================================================
//  Full candles snapshot collector
// =============================================================================

/// Parsed result returned by the internal full-candles snapshot collector.
///
/// The server answers `RequestCandlesData` with several `EngineResponse` chunks
/// sharing one `request_uid`. The library aggregates those chunks through
/// [`CandlesAggregator`] and hands parsed market entries to Active Lib.
#[derive(Debug, Clone)]
pub(crate) struct MergedCandles {
    /// Request UID used to correlate the chunked response.
    pub uid: u64,
    /// Parsed market entries from the zipped stream.
    pub markets: Vec<RequestCandlesMarket>,
}

/// Internal state for a partially assembled full-candles snapshot.
pub(crate) struct PartialCandles {
    pub(crate) aggregator: CandlesAggregator,
    /// Completion sender used after the aggregator returns parsed data.
    pub(crate) sender: mpsc::Sender<MergedCandles>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EngineResponseMeta {
    pub(crate) request_uid: u64,
    pub(crate) method: EngineMethod,
    pub(crate) success: bool,
}
