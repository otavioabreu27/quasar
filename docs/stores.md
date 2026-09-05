# Stores

The core `quasar_jobs/store` is an injected port, not a driver registry. SQL
packages depend inward on this port; core users do not install SQL drivers.

## Memory

Import `quasar_jobs/store/memory` and call `memory.new()`. It is process-local,
uses caller-supplied timestamps for state transitions and is suitable for tests.
It is not durable across a VM restart.

## SQLite

Install `quasar_sqlite` and call `quasar_sqlite.open(path)`. It opens one
serialized Store actor, enables WAL, and applies its bundled `priv/` migration.
Use one application node. Sharing a SQLite file is not supported as cluster
coordination. Missing migration assets return `MigrationUnavailable`.

## PostgreSQL

Install `quasar_postgres`. Start and supervise a Pog pool, run
`quasar_postgres.migrate(connection)`, start the linked
`quasar_postgres/reaper.start(connection, 1000, reporter)`, then pass
`quasar_postgres.new(connection)` to `quasar.with_store`.

Claims use one SQL statement's implicit transaction with `FOR UPDATE SKIP LOCKED`. Only the granted
number of eligible jobs moves to Executing. Each package's SQL migration is
loaded from `priv/`, not duplicated in Gleam string constants.

Unlike Memory/SQLite, PostgreSQL recovery no longer occurs during claim.
Stop the explicit reaper before closing its borrowed pool. PostgreSQL also
rejects expired execution mutations using the database clock, even before
recovery. `new_with_pools(producer, execution, control)` optionally isolates
API, completion and claim/heartbeat traffic within the same database/schema.

The optional PostgreSQL retention worker consumes a policy from
`quasar_jobs/retention` and removes terminal rows in bounded, observable
batches. It borrows the same Pog pool and must be stopped before that pool.
Business tables referencing jobs must define their own cascade, archival, or
prior-cleanup policy.

## Contract and ownership

`store.claim` appends a fresh random nonce to the supplied diagnostic owner
prefix. The returned job's `ExecutionToken` contains JobId, owner and attempt.
Complete, fail and renewal must atomically check all three plus Executing
status. A lost predicate returns `StaleExecution`; it never mutates a newer
claim. Unique owners protect manual retry even when attempt resets to zero.

Invalid inserts fail before reaching the adapter. Unknown persisted statuses
fail decoding; they are never silently treated as runnable. Expired leases at
the attempt limit become Discarded. Cancel/retry errors report NotFound or an
invalid transition using the observed state, not an execution-token error.

`quasar.stop` never closes a Store. After stopping every runtime and in-flight
application command sharing it, the application calls `store.close`. Close
releases only adapter-created resources: Memory/SQLite actors and the SQLite
connection. PostgreSQL borrows the Pog pool, so its Store close is a no-op; the
application's supervisor owns the pool's lifetime.

Store callbacks must bound their own I/O and return typed errors. A timed-out
operation can still commit, so clients must not infer rollback from timeout.
The Store's close callback is a resource-release contract, not pool ownership.
