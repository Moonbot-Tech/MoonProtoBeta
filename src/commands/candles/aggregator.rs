/// Aggregator for the chunked candles response. Each chunk has the header
/// `ChunkIndex:u16 + ChunkTotal:u16`, followed by the data payload. After all
/// chunks are assembled, `merged_data()` returns the concatenated stream for parsing.
///
/// **Caller requirements:**
/// 1. `response_data` is `EngineResponse.data` **already after DEFLATE-decompression**
///    (if `is_compressed=true`, `parse_engine_response` decompressed it automatically).
/// 2. Filter chunks by `request_uid`: if several parallel `RequestCandlesData` are
///    in flight, keep a separate `CandlesAggregator` per `request_uid` or call
///    `reset()` when switching requests. In Delphi this filtering is done via
///    `resp.RequestUID == CandlesRequestUID`.
/// 3. The aggregator does not parse the payload, but it still enforces the
///    candles-domain aggregate size cap before copying chunks.
///
/// Used like this:
/// ```ignore
/// let mut agg = CandlesAggregator::new();
/// // For each response with emk_RequestCandlesData:
/// if let Some(merged) = agg.on_chunk(&response.data) {
///     // All chunks received — merged holds the zlib stream from StoreCandlesToZip.
///     let markets = parse_request_candles_data_response(&merged)?;
/// }
/// ```
#[derive(Debug)]
pub struct CandlesAggregator {
    chunks: Vec<Option<Vec<u8>>>,
    received: usize,
    total: usize,
    payload_bytes: usize,
    max_payload_bytes: usize,
}

impl Default for CandlesAggregator {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            received: 0,
            total: 0,
            payload_bytes: 0,
            max_payload_bytes: super::MAX_REQUEST_CANDLES_CHUNKED_PAYLOAD_BYTES,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CandlesChunkResult {
    Ignored,
    Stored,
    Complete(Vec<u8>),
}

impl CandlesAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn with_max_payload_bytes(max_payload_bytes: usize) -> Self {
        Self {
            max_payload_bytes,
            ..Self::default()
        }
    }

    /// Add a chunk. If all chunks are assembled, return the concatenated buffer and reset state.
    /// Wire: `ChunkIndex:u16 + ChunkTotal:u16 + chunk_payload`.
    #[cfg(any(test, feature = "diagnostics"))]
    pub fn on_chunk(&mut self, response_data: &[u8]) -> Option<Vec<u8>> {
        match self.on_chunk_result(response_data) {
            CandlesChunkResult::Complete(merged) => Some(merged),
            CandlesChunkResult::Ignored | CandlesChunkResult::Stored => None,
        }
    }

    /// Add a chunk and return the exact processing status.
    ///
    /// Delphi updates `Markets.LastChunkTime` only after storing a new chunk in
    /// an empty slot. The caller uses `Stored`/`Complete` so as not to extend the
    /// timeout on duplicates or invalid chunk headers.
    pub(crate) fn on_chunk_result(&mut self, response_data: &[u8]) -> CandlesChunkResult {
        if response_data.len() < 4 {
            return CandlesChunkResult::Ignored;
        }
        let chunk_index = u16::from_le_bytes([response_data[0], response_data[1]]) as usize;
        let chunk_total = u16::from_le_bytes([response_data[2], response_data[3]]) as usize;
        let payload = &response_data[4..];

        if chunk_total == 0 {
            return CandlesChunkResult::Ignored;
        }

        // Resize if first time or total changed
        if self.total != chunk_total {
            let mut chunks = Vec::new();
            if chunks.try_reserve_exact(chunk_total).is_err() {
                log::warn!(target: "moonproto::candles",
                    "RequestCandlesData chunk table for {chunk_total} chunks cannot be allocated");
                self.reset_state();
                return CandlesChunkResult::Ignored;
            }
            chunks.resize_with(chunk_total, || None);
            self.chunks = chunks;
            self.received = 0;
            self.total = chunk_total;
            self.payload_bytes = 0;
        }

        // Store the chunk (deduplicate on repeat)
        if chunk_index < chunk_total && self.chunks[chunk_index].is_none() {
            let Some(new_payload_bytes) = self.payload_bytes.checked_add(payload.len()) else {
                log::warn!(target: "moonproto::candles",
                    "RequestCandlesData chunk payload size overflow");
                self.reset_state();
                return CandlesChunkResult::Ignored;
            };
            if new_payload_bytes > self.max_payload_bytes {
                log::warn!(target: "moonproto::candles",
                    "RequestCandlesData chunk payload {new_payload_bytes} exceeds cap {}",
                    self.max_payload_bytes);
                self.reset_state();
                return CandlesChunkResult::Ignored;
            }

            let mut owned = Vec::new();
            if owned.try_reserve_exact(payload.len()).is_err() {
                log::warn!(target: "moonproto::candles",
                    "RequestCandlesData chunk payload {} cannot be allocated", payload.len());
                self.reset_state();
                return CandlesChunkResult::Ignored;
            }
            owned.extend_from_slice(payload);
            self.chunks[chunk_index] = Some(owned);
            self.payload_bytes = new_payload_bytes;
            self.received += 1;
        } else {
            return CandlesChunkResult::Ignored;
        }

        // Are all of them assembled?
        if self.received == self.total && self.total > 0 {
            let mut merged = Vec::new();
            if merged.try_reserve_exact(self.payload_bytes).is_err() {
                log::warn!(target: "moonproto::candles",
                    "RequestCandlesData merged payload {} cannot be allocated", self.payload_bytes);
                self.reset_state();
                return CandlesChunkResult::Ignored;
            }
            for chunk in self.chunks.drain(..).flatten() {
                merged.extend_from_slice(&chunk);
            }
            self.reset_counters_after_complete();
            return CandlesChunkResult::Complete(merged);
        }
        CandlesChunkResult::Stored
    }

    /// Reset state (on a new candles request).
    #[cfg(any(test, feature = "diagnostics"))]
    pub fn reset(&mut self) {
        self.reset_state();
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub fn progress(&self) -> (usize, usize) {
        (self.received, self.total)
    }

    fn reset_state(&mut self) {
        self.chunks.clear();
        self.reset_counters_after_complete();
    }

    fn reset_counters_after_complete(&mut self) {
        self.received = 0;
        self.total = 0;
        self.payload_bytes = 0;
    }
}
