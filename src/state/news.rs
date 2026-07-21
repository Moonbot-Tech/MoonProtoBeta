use crate::commands::inflate::{read_inflate_to_vec, MAX_INFLATE_OUTPUT_SIZE};
use crate::commands::ui::{
    NewsHistoryCommand, NewsRelayCommand, NEWS_RELAY_KIND_NEWS, NEWS_RELAY_KIND_TAGS,
};
use flate2::read::GzDecoder;
use std::collections::VecDeque;
use std::sync::Arc;

pub const NEWS_HISTORY_CAPACITY: usize = 50;

/// Decoded news-service frames retained by Active Lib.
///
/// The protocol's GZip and binary string envelope are decoded once on receive.
/// Each retained row is the first UTF-8 string from that envelope: the JSON
/// document consumed by the terminal. Several frames may describe the same
/// logical news item (for example, an initial English item followed by a
/// translation), so UI code indexes logical rows by `meta.id`.
#[derive(Debug, Clone, Default)]
pub struct NewsState {
    items: VecDeque<Arc<str>>,
    tags_json: Option<Arc<str>>,
    live_tags_seen: bool,
}

impl NewsState {
    pub fn items(&self) -> impl DoubleEndedIterator<Item = &str> {
        self.items.iter().map(AsRef::as_ref)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn latest(&self) -> Option<&str> {
        self.items.back().map(AsRef::as_ref)
    }

    /// Latest JSON tags catalog received from the news service.
    pub fn tags_json(&self) -> Option<&str> {
        self.tags_json.as_deref()
    }

    pub(crate) fn clear_for_new_world(&mut self) {
        self.items.clear();
        self.tags_json = None;
        self.live_tags_seen = false;
    }

    pub(crate) fn clear_for_hard_session(&mut self) {
        self.clear_for_new_world();
    }

    pub(crate) fn apply_relay(
        &mut self,
        command: NewsRelayCommand,
    ) -> Result<Option<NewsEvent>, ()> {
        let json = decode_news_frame(&command.data).ok_or(())?;
        match command.kind {
            NEWS_RELAY_KIND_NEWS => {
                self.push_news(Arc::clone(&json));
                Ok(Some(NewsEvent::Received { json }))
            }
            NEWS_RELAY_KIND_TAGS => {
                self.tags_json = Some(Arc::clone(&json));
                self.live_tags_seen = true;
                Ok(Some(NewsEvent::TagsUpdated { json }))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn apply_history(&mut self, command: NewsHistoryCommand) -> Result<NewsEvent, ()> {
        let mut decoded = Vec::new();
        decoded.try_reserve(command.frames.len()).map_err(|_| ())?;
        for frame in command.frames {
            decoded.push(decode_news_frame(&frame).ok_or(())?);
        }
        let news_count = decoded.len();
        let tags = if command.tags.is_empty() {
            None
        } else {
            Some(decode_news_frame(&command.tags).ok_or(())?)
        };

        // A live relay may overtake the sliced startup history. Rebuild the
        // retained ring as history-then-live so iteration remains
        // chronological regardless of UDP delivery order.
        let mut merged = VecDeque::new();
        merged
            .try_reserve(decoded.len().saturating_add(self.items.len()))
            .map_err(|_| ())?;
        for json in decoded.into_iter().chain(self.items.iter().cloned()) {
            if merged.len() == NEWS_HISTORY_CAPACITY {
                merged.pop_front();
            }
            merged.push_back(json);
        }
        self.items = merged;
        let tags_included = tags.is_some();
        if !self.live_tags_seen {
            self.tags_json = tags;
        }
        Ok(NewsEvent::HistoryApplied {
            news_count,
            tags_included,
        })
    }

    fn push_news(&mut self, json: Arc<str>) {
        if self.items.len() == NEWS_HISTORY_CAPACITY {
            self.items.pop_front();
        }
        self.items.push_back(json);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewsEvent {
    /// One live news-service frame arrived.
    ///
    /// Several frames may carry the same `meta.id`; later frames can add a
    /// translation or clarification. Consumers should update their logical row
    /// by ID instead of treating every event as a new news item.
    Received { json: Arc<str> },
    /// The complete live tags catalog was replaced.
    TagsUpdated { json: Arc<str> },
    /// The startup frame history was decoded and retained.
    HistoryApplied {
        /// Number of news frames carried by this history command.
        news_count: usize,
        tags_included: bool,
    },
}

fn decode_news_frame(frame: &[u8]) -> Option<Arc<str>> {
    let mut decoder = GzDecoder::new(frame);
    let raw = read_inflate_to_vec(
        &mut decoder,
        frame.len().saturating_mul(4),
        MAX_INFLATE_OUTPUT_SIZE,
    )
    .ok()?;
    if raw.len() < 4 {
        return None;
    }
    let count = u16::from_le_bytes([raw[0], raw[1]]);
    if count == 0 {
        return None;
    }
    let len = u16::from_le_bytes([raw[2], raw[3]]) as usize;
    let available = raw.len().saturating_sub(4).min(len);
    let json = String::from_utf8_lossy(&raw[4..4 + available]);
    if json.is_empty() {
        return None;
    }
    Some(Arc::from(json.into_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    fn frame(json: &str) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1u16.to_le_bytes());
        raw.extend_from_slice(&(json.len() as u16).to_le_bytes());
        raw.extend_from_slice(json.as_bytes());
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&raw).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn history_keeps_oldest_to_newest_and_applies_tags() {
        let mut state = NewsState::default();
        let event = state
            .apply_history(NewsHistoryCommand {
                frames: vec![
                    frame(r#"{"meta":{"id":"old"}}"#),
                    frame(r#"{"meta":{"id":"new"}}"#),
                ],
                tags: frame(r#"{"tags":[{"name":"ETF"}]}"#),
            })
            .unwrap();

        assert_eq!(
            state.items().collect::<Vec<_>>(),
            vec![r#"{"meta":{"id":"old"}}"#, r#"{"meta":{"id":"new"}}"#]
        );
        assert_eq!(state.tags_json(), Some(r#"{"tags":[{"name":"ETF"}]}"#));
        assert_eq!(
            event,
            NewsEvent::HistoryApplied {
                news_count: 2,
                tags_included: true,
            }
        );
    }

    #[test]
    fn live_tags_win_if_sliced_history_finishes_later() {
        let mut state = NewsState::default();
        state
            .apply_relay(NewsRelayCommand {
                kind: NEWS_RELAY_KIND_TAGS,
                data: frame(r#"{"tags":[{"name":"live"}]}"#),
            })
            .unwrap();
        state
            .apply_history(NewsHistoryCommand {
                frames: Vec::new(),
                tags: frame(r#"{"tags":[{"name":"old"}]}"#),
            })
            .unwrap();

        assert_eq!(state.tags_json(), Some(r#"{"tags":[{"name":"live"}]}"#));
    }

    #[test]
    fn sliced_history_is_placed_before_live_news_that_overtook_it() {
        let mut state = NewsState::default();
        state
            .apply_relay(NewsRelayCommand {
                kind: NEWS_RELAY_KIND_NEWS,
                data: frame(r#"{"meta":{"id":"live"}}"#),
            })
            .unwrap();
        state
            .apply_history(NewsHistoryCommand {
                frames: vec![frame(r#"{"meta":{"id":"old"}}"#)],
                tags: Vec::new(),
            })
            .unwrap();

        assert_eq!(
            state.items().collect::<Vec<_>>(),
            vec![r#"{"meta":{"id":"old"}}"#, r#"{"meta":{"id":"live"}}"#]
        );
    }

    #[test]
    fn hard_session_replaces_old_ring_but_keeps_live_news_that_overtakes_history() {
        let mut state = NewsState::default();
        state
            .apply_relay(NewsRelayCommand {
                kind: NEWS_RELAY_KIND_NEWS,
                data: frame(r#"{"meta":{"id":"previous-session"}}"#),
            })
            .unwrap();

        state.clear_for_hard_session();
        state
            .apply_relay(NewsRelayCommand {
                kind: NEWS_RELAY_KIND_NEWS,
                data: frame(r#"{"meta":{"id":"live"}}"#),
            })
            .unwrap();
        state
            .apply_history(NewsHistoryCommand {
                frames: vec![frame(r#"{"meta":{"id":"history"}}"#)],
                tags: Vec::new(),
            })
            .unwrap();

        assert_eq!(
            state.items().collect::<Vec<_>>(),
            vec![r#"{"meta":{"id":"history"}}"#, r#"{"meta":{"id":"live"}}"#]
        );
    }

    #[test]
    fn exact_duplicate_frames_keep_server_ring_multiplicity() {
        let mut state = NewsState::default();
        let json = r#"{"meta":{"id":"same"}}"#;
        for _ in 0..2 {
            state
                .apply_relay(NewsRelayCommand {
                    kind: NEWS_RELAY_KIND_NEWS,
                    data: frame(json),
                })
                .unwrap();
        }

        assert_eq!(state.items().collect::<Vec<_>>(), vec![json, json]);
    }

    #[test]
    fn malformed_history_is_rejected_without_partial_state() {
        let mut state = NewsState::default();
        state
            .apply_relay(NewsRelayCommand {
                kind: NEWS_RELAY_KIND_NEWS,
                data: frame(r#"{"meta":{"id":"kept"}}"#),
            })
            .unwrap();
        let before = state.items().map(str::to_owned).collect::<Vec<_>>();

        assert!(state
            .apply_history(NewsHistoryCommand {
                frames: vec![frame(r#"{"meta":{"id":"valid"}}"#), vec![1, 2, 3]],
                tags: Vec::new(),
            })
            .is_err());
        assert_eq!(state.items().collect::<Vec<_>>(), before);
    }

    #[test]
    fn retained_news_uses_server_ring_capacity() {
        let mut state = NewsState::default();
        for id in 0..=NEWS_HISTORY_CAPACITY {
            state
                .apply_relay(NewsRelayCommand {
                    kind: NEWS_RELAY_KIND_NEWS,
                    data: frame(&format!(r#"{{"meta":{{"id":{id}}}}}"#)),
                })
                .unwrap();
        }

        assert_eq!(state.len(), NEWS_HISTORY_CAPACITY);
        assert!(!state.items().any(|json| json.contains(r#""id":0"#)));
        assert!(state
            .latest()
            .unwrap()
            .contains(&NEWS_HISTORY_CAPACITY.to_string()));
    }
}
