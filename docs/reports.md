# Report Database Replication

MoonProto can maintain an application-owned replica of the core's `Orders`
report database. The library owns transport, schema decoding, hard-reconnect
resubscription, catch-up ordering, and typed row parsing. The application owns
the SQLite connection, transactions, retention policy, and durable cursor.

This domain is separate from `snapshot().orders()`. Retained orders are the
live trading model; report rows are the historical database model.

## Recommended API

Use `MoonClient::reports()` and `Event::Report` for every new report database
integration. This is the maintained path for creating a local report database,
catching up rows written while the application was offline, and keeping that
database current during the live session.

The deprecated `Event::ClosedSellOrderReport` stream remains available for
existing consumers that execute the core's expanded SQL. It reports only the
legacy closed-sell SQL flow and does not provide schema negotiation, initial
history, offline catch-up, delete reconciliation, or reconnect recovery. Do not
build a new replica on that event.

This deprecation does not apply to `snapshot().orders()` or `Event::Order`.
Those remain the normal live order model for tables, charts, and order actions;
report replication is the durable historical database model.

## Start Replication

```rust
use moonproto::{ReportHistoryDepth, ReportSyncRequest};

let ticket = client.reports().sync(ReportSyncRequest::fresh(
    ReportHistoryDepth::ServerDefault,
))?;
```

The call returns immediately. If the schema is not known yet, Active Lib asks
for it first and starts catch-up only after the schema has been validated.
Results arrive through `Event::Report`:

```rust
match event {
    moonproto::Event::Report(moonproto::ReportEvent::Schema(schema)) => {
        migrate_local_table(&schema)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::RowUpsert(row)) => {
        upsert_local_row(row)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::RowDelete { rec_id }) => {
        delete_local_row(rec_id)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::SyncComplete(done)) => {
        commit_sync(done)?;
    }
    _ => {}
}
```

`ReportSyncTicket` identifies the first network attempt. A retry after timeout
or hard reconnect can use a new request id and emits a new `SyncStarted` event;
the durable operation is identified by its `ReportSyncRequest` and completed by
`SyncComplete`, not by waiting synchronously for the original ticket.

## Schema And SQLite

`ReportSchema` is append-only: existing field indices, names, kinds, and SQLite
declarations are stable; new fields are appended. Applications should create
missing columns, never rebuild row decoding around column order guessed locally.

The schema exposes SQLite helpers:

```rust
let create = schema.sqlite_create_table_sql("Orders");
let add = schema.sqlite_add_column_sql("Orders", field);
let index = schema.sqlite_unique_index_sql("Orders");
```

`newRecID` is the stable replication key. It is different from an active order
UID, an exchange order id, and the legacy report `db_id`.
The report column named `TaskID` is also a core-local worker number; it is not
the public MoonProto order UID.

The current validated schema is also available from
`snapshot().report_schema()`.

## Migrating From The Legacy SQL Event

Do not apply `Event::ClosedSellOrderReport` and `Event::Report` to the same
replica. During migration:

1. Create a separate typed replica from `ReportEvent::Schema`.
2. Start it with `ReportSyncRequest::fresh(...)`. Use
   `ReportHistoryDepth::All` when the replacement must preserve all history, or
   choose an explicit retained depth for a deliberately bounded database.
3. Apply typed row events idempotently by `newRecID` and commit the cursor only
   after `SyncComplete`.
4. After the first complete sync and reconciliation, atomically switch readers
   to the typed replica and stop executing the deprecated SQL stream.
5. Remove the legacy replica only after the replacement has been verified.

Do not resume from `max(newRecID) + 1` taken from a legacy SQL replica. The old
stream had no offline catch-up, so missing rows may exist below that maximum.
Even when its SQL happens to contain `newRecID`, completeness is not proven.
`db_id` and `newRecID` are also different identities and must not be converted
by assumption.

The core may emit both compatibility and typed events during the transition.
Receiving both does not mean both should be written to the database.

## Cursor Selection

Choose the next request from the local durable database:

- if open rows exist, use the smallest `newRecID` among them;
- otherwise use `max(newRecID) + 1`;
- for an empty database use `from_rec_id = 0` and select a history depth;
- `ReportHistoryDepth::All` requests all retained history;
- when `from_rec_id > 0`, history depth is intentionally ignored.

```rust
client.reports().sync(ReportSyncRequest::resume(next_rec_id))?;
```

Do not advance the durable cursor merely because row events arrived. Commit it
only after the matching `SyncComplete` has been applied successfully.

## Catch-Up And Live Ordering

The catch-up request also enables live report delivery for the current hard
session. Active Lib merges live changes with in-flight batches:

- a live upsert wins over an older copy of the same row in a late batch;
- a live delete prevents a late batch from recreating that row;
- duplicate batches are ignored;
- completion is emitted only after every declared batch and row is present,
  even when the completion packet arrived first.

`SyncComplete::keep_rec_ids` is the authoritative keep-set for the requested
range after batch/live merging. After applying all preceding row events, remove
local rows with `newRecID >= from_rec_id` that are absent from this set. Perform
that reconciliation only on `SyncComplete`; a partial response must never
delete local history.

If `database_recreated` is true, the core's global maximum is behind the local
cursor. Discard the stale replica and start a fresh sync instead of reconciling
the two unrelated databases.

## Reconnect And Retry

Report subscription belongs to the core session. A soft network rebind keeps
it. A hard session change loses it. Active Lib compares the current server
session token with the token under which report sync completed and resends the
same committed request when they differ. That single request both restores the
subscription and catches up the offline gap.

Missing progress is retried automatically. The application should keep event
handling non-blocking and make row application idempotent by `newRecID`.

Retention cleanup is reconciled by the next complete sync rather than emitted
as a storm of live deletes. An old closed row changed below an incremental
cursor is outside that cursor's range; periodically widen the cursor if the
application needs to refresh such historical edits.
