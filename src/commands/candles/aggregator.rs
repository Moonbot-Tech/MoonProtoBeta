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
/// 3. The aggregator does not validate the payload — it just concatenates in `ChunkIndex` order.
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
#[derive(Debug, Default)]
pub struct CandlesAggregator {
    chunks: Vec<Option<Vec<u8>>>,
    received: usize,
    total: usize,
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

    /// Add a chunk. If all chunks are assembled, return the concatenated buffer and reset state.
    /// Wire: `ChunkIndex:u16 + ChunkTotal:u16 + chunk_payload`.
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

        // Delphi stores ChunkTotal as Word and has no additional capacity cap.
        // `chunk_total` is already bounded by u16::MAX by wire format.
        if chunk_total == 0 {
            return CandlesChunkResult::Ignored;
        }

        // Resize if first time or total changed
        if self.total != chunk_total {
            self.chunks.clear();
            self.chunks.resize_with(chunk_total, || None);
            self.received = 0;
            self.total = chunk_total;
        }

        // Store the chunk (deduplicate on repeat)
        if chunk_index < chunk_total && self.chunks[chunk_index].is_none() {
            self.chunks[chunk_index] = Some(payload.to_vec());
            self.received += 1;
        } else {
            return CandlesChunkResult::Ignored;
        }

        // Are all of them assembled?
        if self.received == self.total && self.total > 0 {
            let mut merged = Vec::with_capacity(
                self.chunks
                    .iter()
                    .filter_map(|c| c.as_ref().map(|v| v.len()))
                    .sum(),
            );
            for chunk in self.chunks.drain(..).flatten() {
                merged.extend_from_slice(&chunk);
            }
            self.received = 0;
            self.total = 0;
            return CandlesChunkResult::Complete(merged);
        }
        CandlesChunkResult::Stored
    }

    /// Reset state (on a new candles request).
    pub fn reset(&mut self) {
        self.chunks.clear();
        self.received = 0;
        self.total = 0;
    }

    pub fn progress(&self) -> (usize, usize) {
        (self.received, self.total)
    }
}
