# quasar_postgres

Optional cluster-coordinated PostgreSQL Store for Quasar.
This is the 0.3.0 release candidate, requiring `quasar_jobs >= 0.3.0 and < 0.4.0`.
Do not mix it with the already-published 0.2.0 documentation or binaries.

## Startup change: explicit lease recovery

`claim` now executes one atomic SQL statement. It does **not** reap expired
jobs. Start one linked reaper per application before starting Quasar:

```gleam
import quasar_postgres/reaper

let assert Ok(_) = quasar_postgres.migrate(connection)
let assert Ok(recovery) = reaper.start(connection, 1000, fn(_) { Nil })
let store = quasar_postgres.new(connection)
// Start Quasar with store, then listener; polling remains required.
// Shutdown: stop Quasar, listener, recovery/retention, then application pools.
let assert Ok(_) = reaper.stop(recovery)
```

Recovery is capped at 500 rows per statement, with 100ms between full batches.
`SKIP LOCKED` and a nonblocking transaction advisory lock coordinate replicas.
Failure is retried on the next interval; `Reaped`/`ReapFailed` expose outcomes.
Recovery latency is lease expiry plus the maintenance/polling delay, not an
immediate side effect of claim. Missing this startup step leaves expired jobs
in `executing`; this is a migration requirement for the next release.

`new_with_pools(producer, execution, control)` optionally isolates traffic;
all three pools must connect to the same database/schema and remain app-owned.
The listener still uses one additional dedicated connection. Budget total
connections for HPA maximum **plus rollout surge**, maintenance and other apps.

PostgreSQL rejects renewals, completions and failures after lease expiry using
its own clock, even before a reaper has visited the row. Hosts must have
synchronized clocks because claim timestamps are supplied by the runtime.

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
