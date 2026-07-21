# News And Tags

MoonProto receives the news feed available to the connected MoonBot core. No
subscription call is required. On a fresh connection, a running core news
client sends its retained news ring and latest tags catalog when either contains
data; later news and tag changes arrive as live events. A core with no active
news client or no retained news/tags sends no startup history, so applications
must not wait for `HistoryApplied` before becoming ready.

The library removes the protocol framing and GZip layer on receive. Application
code works with UTF-8 JSON strings, not compressed packets.

## Reading News

```rust
use moonproto::{Event, NewsEvent};

for event in client.drain_events() {
    match event {
        Event::News(NewsEvent::HistoryApplied {
            news_count,
            tags_included,
        }) => {
            let snapshot = client.snapshot().expect("snapshot");
            for json in snapshot.news().items() {
                index_news_json(json);
            }
            if tags_included {
                update_tags(snapshot.news().tags_json());
            }
            println!("loaded {news_count} retained news frames");
        }
        Event::News(NewsEvent::Received { json }) => index_news_json(&json),
        Event::News(NewsEvent::TagsUpdated { json }) => update_tags(Some(&json)),
        _ => {}
    }
}
```

`MoonStateSnapshot::news()` retains at most 50 decoded news frames, oldest to
newest. `latest()` returns the newest retained frame. A frame is not necessarily
a new logical news item: several frames can carry the same `meta.id`. Each
received frame occupies one ring position, including an exact duplicate,
matching the core's wire history.

A new hard session receives an authoritative history again. MoonProto clears
the previous session's local frame ring before applying it. If a live frame
overtakes the sliced history, the library keeps the final order as
`history -> overtaking live frames`.

The startup history is delivered as one reliable sliced command. It may arrive
before `LifecycleEvent::Ready`; the active runtime retains it immediately. A
live tags update that arrives while the startup history is still assembling
wins over the older tags copy in that history.

## News JSON

Each retained string is the first JSON document in one news-service frame. The
schema can grow, so deserialize only the fields your application uses and
ignore unknown fields. MoonBot currently consumes these fields:

| JSON path | Meaning |
|---|---|
| `meta.id` | News identity. Treat it as an opaque stable value for UI updates/deduplication. |
| `meta.timeMs` | Preferred publication time, Unix milliseconds. |
| `meta.time` | Publication time fallback, Unix seconds. |
| `meta.recvTime` | News-service receive time, Unix milliseconds. |
| `meta.sendTime` | Service send time, Unix milliseconds. |
| `meta.isOriginal` | Whether the item is original source material. |
| `meta.source` | Source code; current values include `toa` and `nm`. |
| `meta.author` | Optional author text prepended by the MoonBot UI. |
| `meta.coinsAuto` | Automatically detected ticker array. |
| `meta.coinsSelf` | Manually/provider supplied ticker array. |
| `meta.isLike` | Current user's optional vote. |
| `meta.cntLikes` / `meta.cntDislikes` | Vote counters. |
| `news.en` | English text fragments. |
| `news.ru` | Optional Russian text fragments. |
| `news.es` | Optional Spanish text fragments. |
| `tags.entity[*].text` | Tags attached to this news item. |

The language values are arrays of text fragments, not one guaranteed string.
Normalize escaped `\r\n` / `\n` inside each fragment to spaces, then join the
fragments with spaces. Fall back to `news.en` when the selected translation is
missing or empty.

## Frame Updates And Translations

Translations are asynchronous. The first frame for an ID commonly contains
English text only; a later frame with the same `meta.id` can add `news.ru` or
`news.es`, or otherwise clarify the item. Absence of a translation in the first
frame is therefore not a final state.

For example, these are two revisions of one logical news item, not two rows:

```json
{"meta":{"id":"n-42","isOriginal":true},"news":{"en":["BTC moves higher"]}}
{"meta":{"id":"n-42","isOriginal":false},"news":{"en":["BTC moves higher"],"ru":["BTC растет"]}}
```

Build the terminal's logical news list by ID, not by frame position:

1. Parse retained history oldest to newest using the same function as live
   `NewsEvent::Received` frames.
2. On the first valid `meta.id`, insert one logical UI row.
3. If the ID already exists and the retained row has `meta.isOriginal = true`,
   update that row from the later frame. This is the translation/clarification
   path used by the core UI.
4. If the retained row already has `meta.isOriginal = false`, ignore a later
   frame with the same ID. This preserves the already accepted copy instead of
   replacing it with a late original.
5. Replace only the row's translated texts, combined ticker list, and tags; then
   refresh its displayed body and chart-marker text. Keep the identity,
   publication/receive/send times, source, author, vote metadata, and other
   fields from the first accepted frame. Do not append a second UI row.

`NewsEvent::Received` means "one frame arrived", not "one new logical news item
was created". Likewise, `HistoryApplied.news_count` is a frame count and may be
larger than the number of unique `meta.id` values shown by the terminal.

Keep the logical index outside `NewsState`: the retained state intentionally
preserves the core ring's frame order and multiplicity after decoding each
frame's first JSON string, including same-ID revisions and exact duplicate
frames. A terminal rebuilds its logical rows from `snapshot.news().items()`
after `HistoryApplied`, then applies each live `Received` frame through the same
ID-based reducer.

Combine `meta.coinsAuto` and `meta.coinsSelf` as a de-duplicated ticker list.
Treat a missing/empty `meta.id` or missing `meta`/`news` object as an invalid
news document. Ignore unknown JSON fields so service-side schema additions stay
forward-compatible.

## Tags Catalog JSON

`NewsState::tags_json()` contains the latest complete catalog. Its current
shape is:

```json
{
  "count": 2,
  "tags": [
    { "context": "...", "name": "ETF", "id": 1 },
    { "context": "...", "name": "DeFi", "id": 2 }
  ]
}
```

Every tags event/history tail is a complete replacement catalog, not a patch.
MoonBot uses `tags[*].name` as the selectable/displayed tag list. Skip entries
without `name`. `id` and `context` may be retained by applications that need
them, but tag selection must not depend on array position because the catalog
can be reordered.

MoonProto deliberately exposes the decoded JSON instead of freezing these
service-owned documents into a rigid Rust schema. This keeps new optional news
fields forward-compatible while the protocol delivery, ordering, retention,
and reconnect behavior remain typed and maintained by the library.
