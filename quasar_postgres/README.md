# quasar_postgres

Optional cluster-coordinated PostgreSQL Store for Quasar.
Requires `quasar_jobs >= 0.1.0 and < 0.2.0`.

See [Store contract and ownership](../docs/stores.md) and
[publishing prerequisites](../docs/publishing.md).

Migrations in `priv/NNN_name.sql` are loaded and applied strictly in version
order. Regular migrations run in individual transactions protected by a
PostgreSQL advisory lock. Files whose names begin with `NNN_online_` run outside
a transaction, on one checked-out connection protected by the same advisory
lock; this supports PostgreSQL operations such as `CREATE INDEX CONCURRENTLY`.
Applied versions are recorded in
`quasar_jobs_migrations`, so concurrent application startup is safe and repeated
calls are idempotent. Migration files must be included in the published package.
Never edit an applied migration; add the next numbered file.

## Retention

Retention policy lives in `quasar_jobs/retention`; this adapter only owns the
PostgreSQL execution strategy:

```gleam
import quasar_jobs/retention
import quasar_postgres/retention as postgres_retention

let policy =
  retention.new()
  |> retention.completed_for(days: 7)
  |> retention.cancelled_for(days: 30)
  |> retention.discarded_for(days: 30)

let assert Ok(cleaner) =
  postgres_retention.new(connection, policy)
  |> postgres_retention.with_batch_size(rows: 1000)
  |> postgres_retention.with_pause(milliseconds: 100)
  |> postgres_retention.with_interval(milliseconds: 60_000)
  |> postgres_retention.start
```

The linked cleaner deletes one bounded batch at a time with
`FOR UPDATE SKIP LOCKED`, pauses cooperatively between batches, and uses a
non-blocking PostgreSQL advisory lock to avoid overlapping delete transactions.
Configure `with_reporter` to observe rows deleted, duration, failures, and cycle
completion. Call `postgres_retention.stop(cleaner)` before closing the Pog pool.

Retention removes the job row. Application tables that reference
`quasar_jobs(id)` must either use `ON DELETE CASCADE`, remove their data first,
or archive it outside the retention transaction.
