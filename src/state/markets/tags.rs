//! Delphi `emk_CheckBinanceTags` apply logic.

use std::collections::HashSet;
use std::sync::Arc;

use crate::commands::market::EngineStreamReader;

use super::{MarketTokenTags, MarketsEvent, MarketsState, TokenTags};

impl MarketsState {
    /// Apply the `emk_CheckBinanceTags` response.
    ///
    /// Delphi `TMoonProtoEngine.CheckBinanceTags` clears seen state for all
    /// markets, applies tags for markets present in the response, then clears
    /// tags for every market not seen in that response.
    pub fn apply_token_tags(&mut self, items: Vec<MarketTokenTags>) -> MarketsEvent {
        Arc::make_mut(&mut self.token_tags).clear();
        let mut count = 0usize;
        for it in items {
            if self.by_name.contains_key(&it.market_name) {
                Arc::make_mut(&mut self.token_tags).insert(it.market_name, it.tags);
                count += 1;
            }
        }
        MarketsEvent::TokenTagsUpdated { count }
    }

    /// Active-library direct counterpart of Delphi `CheckBinanceTags`.
    ///
    /// Delphi applies tags inside the read loop and clears unseen tags only
    /// after the loop completes. A late string read error therefore leaves
    /// already-read tag updates applied and does not clear old absent tags.
    pub(crate) fn apply_token_tags_payload_like_delphi(
        &mut self,
        data: &[u8],
    ) -> Option<MarketsEvent> {
        let mut r = EngineStreamReader::new(data);
        let count = r.read_count()?;
        let mut seen = HashSet::with_capacity(r.bounded_count_capacity(count, 6));

        for _ in 0..count {
            let market_name = r.read_str()?;
            let tags = TokenTags::from_bits(r.read_int()? as u32);
            if self.by_name.contains_key(&market_name) {
                Arc::make_mut(&mut self.token_tags).insert(market_name.clone(), tags);
                seen.insert(market_name);
            }
        }

        Arc::make_mut(&mut self.token_tags).retain(|name, _| seen.contains(name));
        Some(MarketsEvent::TokenTagsUpdated { count: seen.len() })
    }
}
