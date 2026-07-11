# Report Database Replication

MoonProto can maintain an application-owned replica of the core's historical
`Orders` report database. Active Lib owns transport, schema decoding, retries,
hard-reconnect recovery, and typed row parsing. The application owns its
SQLite connection, migrations, transactions, retention policy, and durable
data.

This domain is separate from `snapshot().orders()`. The snapshot is the live
trading model used for tables, charts, and order actions. Report replication is
the durable historical database model.

## Recommended Flow

Start from the cursor already committed in the local replica:

```rust
use moonproto::{ReportHistoryDepth, ReportSyncRequest};

let ticket = if local_db_is_empty() {
    client.reports().sync(ReportSyncRequest::fresh(
        ReportHistoryDepth::ServerDefault,
    ))?
} else {
    client.reports().sync(ReportSyncRequest::resume(
        local_max_rec_id + 1,
    ))?
};
```

The call returns immediately. If the schema is not known yet, Active Lib asks
for it first. Catch-up then advances one page at a time:

```rust
match event {
    moonproto::Event::Report(moonproto::ReportEvent::Schema(schema)) => {
        migrate_local_table(&schema)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::SyncPage(page)) => {
        let tx = db.transaction()?;
        upsert_page_with_one_prepared_statement(&tx, &page.rows)?;
        tx.commit()?;

        // This is the flow-control boundary. No next page is requested before it.
        client.reports().page_applied(&page)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::RowUpsert(row)) => {
        upsert_live_row(row)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::RowDelete { rec_id }) => {
        delete_local_row(rec_id)?;
    }
    moonproto::Event::Report(moonproto::ReportEvent::SyncComplete(done)) => {
        persist_cursor(done.next_from_rec_id)?;
    }
    _ => {}
}
```

One request produces one page. Active Lib never requests the next page until
the application acknowledges the current one after its database transaction.
This keeps at most one catch-up page in flight per core and makes the database
writer the natural backpressure boundary.

`SyncComplete` is emitted only after the final page has been acknowledged. It
therefore describes durably applied catch-up, not merely parsed network data.

## Page Contract

`ReportSyncPage` contains:

- `rows`: the complete typed page;
- `from_rec_id`: the cursor used for this page;
- `last_rec_id`: the last row in this page, or zero for an empty page;
- `max_rec_id`: the core database's global maximum at response time;
- `database_recreated`: the core database is behind the local cursor;
- `is_complete()`: no further page is needed for this catch-up pass.

Pages are idempotent by `newRecID`. If the application cannot commit a page,
it must not call `page_applied`; the next page will not be requested.

A live upsert/delete can overtake a sliced page on UDP. Active Lib tracks live
IDs only for the current in-flight page and removes their older page copies, so
the application always applies the live value last without retaining a
whole-sync reconciliation set.

When `database_recreated` is true, discard the stale local replica and then
call `page_applied`. Active Lib restarts the same operation from a fresh cursor.

Missing page responses are retried automatically. A retry repeats only the
current page, not the complete history.

## Open Rows After Reconnect

Report rows are not fully append-only. An open deal can close or be deleted
while the client is offline, even though its `newRecID` is below the committed
cursor. Keep the current open-row IDs registered with Active Lib:

```rust
client.reports().check_open_rows(&open_rec_ids)?;
```

The library sorts and deduplicates the IDs, keeps the newest 100, sends an
addressed check, and retains that set for hard-reconnect recovery. Results use
the normal `RowUpsert` and `RowDelete` events. `OpenRowsCheckComplete` means one
authoritative result was received for every retained ID.

Call `check_open_rows` again when the local set changes. Passing an empty slice
clears the retained check intent. Closed rows are not rechecked: they are
stable apart from accepted cosmetic edits.

## Schema And SQLite

`ReportSchema` is append-only: existing field indices, names, kinds, and SQLite
declarations are stable; new fields extend the tail. Create missing columns,
never infer wire indices from a locally guessed column order.

```rust
let create = schema.sqlite_create_table_sql("Orders");
let add = schema.sqlite_add_column_sql("Orders", field);
let index = schema.sqlite_unique_index_sql("Orders");
```

`newRecID` is the immutable replication key. It is different from an active
order UID, exchange order id, and the legacy report `db_id`. The current schema
is also available from `snapshot().report_schema()`.

For each page, use one SQLite transaction and reuse one prepared upsert
statement. Preparing SQL for every row can turn the local writer into the
bottleneck that page-level flow control is designed to avoid.

## Reconnect And Cursor

Report subscription belongs to the hard server session. Active Lib tracks the
server session token. After a hard reconnect it resumes from the last page that
the application acknowledged and repeats the retained open-row check. A soft
network rebind keeps the server session and does not cause a false resync.
The append-only schema is revalidated once per new hard session before page or
check traffic resumes, so newly appended fields are migrated before their rows
are applied.

The durable cursor is always `max(newRecID) + 1`. Never move it merely because
a page event arrived; the page must first be committed and acknowledged.

For an empty replica, `ReportHistoryDepth::ServerDefault` uses the core's
default retained depth, `Days(n)` requests an explicit depth, and `All`
requests all retained history. History depth applies only to a fresh cursor.

## Legacy SQL Event

`Event::ClosedSellOrderReport` remains only for compatibility with existing
consumers of the expanded SQL stream. It has no schema negotiation, initial
history, offline catch-up, or reconnect recovery. New report databases should
use `Event::Report` only, and the two streams must not write into the same
replica.
