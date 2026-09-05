# Architecture

Quasar has four packages: `quasar_jobs` (core), `quasar_mist` (HTTP),
`quasar_sqlite` (single-node persistence), and `quasar_postgres` (cluster
persistence). Applications install only the adapters they use. The core
depends on the Constellation 0.2 public API, never its private implementation.

## Responsibilities and dependency direction

- `quasar_jobs`: configuration builders and a thin public façade.
- `error`, `event`, `request_id`: canonical boundary values, with no duplicated
  constructors or façade translation layer.
- `job`, `worker`: job state machine, typed payload codec and execution capability.
- `store`: injected atomic-operation contract. `store/memory` implements it;
  `store/codec` defines the shared SQL row layout and rejects unknown statuses.
- `internal/config`: validates configuration before any processes start.
- `internal/runtime`: owns linked processes, resolves capabilities and coordinates
  shutdown. It performs no Store I/O and invokes no user handlers.
- `internal/local`: typed reply handling, local admission and request measurements.
  It knows nothing about durable queues or Stores.
- `internal/jobs`: durable application commands and explicit queue routing.
  Store calls execute in the requesting process, outside the lifecycle actor.
- `internal/durable`: demand grants, bounded claims and polling, one scheduler
  per queue. A slow Store can stall that queue, not local capability resolution.
- `internal/job_executor` and `internal/lease`: one execution attempt and its
  heartbeat. Fresh claims defer renewal until close to expiry; delayed prefetch
  renews ownership before invoking user code. ACK and renewals match a fenced
  token atomically.
- `internal/completion_buffer`: collects up to 100 execution results for at
  most 5 ms. PostgreSQL persists each completion/failure group atomically;
  lifecycle completion events are emitted only after that commit succeeds.
- `internal/reporter`: asynchronous callback delivery, with panic containment.

This applies single responsibility and dependency inversion to functional
modules and injected functions. It does not introduce a class hierarchy or
generic framework. The Store is the extension boundary; adapters implement its
semantics, not just its function signatures.

## Capacity and lifecycle

Local tasks capture a typed reply subject in an opaque closure and submit one
event to a Constellation pool. The public execution API exposes no grants,
PIDs, subscriptions or Constellation pool values.

Each durable `DemandGranted` limits its atomic Store claim. Partial grants
remain open while the Store is empty. Failed claims mark the source unavailable;
the fallback poll restores availability. Unaccepted claims recover through
their leases. The database is the backlog; no second backlog is held in memory.

The supervised runtime name is allocated once in the child specification.
After a restart, existing handles resolve the new owner's pools and reporter.
In-flight local calls are not replayed. Drain completion is delivered through
an incarnation-local mailbox, so an old drain cannot stop the replacement.

Shutdown changes admission state before draining. The owner stays responsive
and rejects new resolutions with `ShuttingDown`. Store resources remain
application-owned. A command that resolved its Store before shutdown may still
finish afterwards; stop all such callers before closing the Store.

## Pre-release API changes

- Import constructors from `quasar_jobs/error` or `quasar_jobs/event`.
- Use `quasar_jobs/store/memory.new()` instead of `store.memory()`.
- Install/import `quasar_sqlite` or `quasar_postgres` for SQL.
- Supply `on: "queue"` to `enqueue` and `schedule`; wrong workers return
  `WorkerQueueMismatch`, unknown queues return `QueueNotFound`.
- Store implementations receive `ExecutionToken`, not just JobId, for complete,
  fail and renewal. Match owner and attempt in the same atomic mutation.
- `job.status_from_string` returns a Result; unknown persisted values are errors.
- HTTP adapters use `call_with_id` so request headers and telemetry share an ID.
- Migration files are the single source of truth in each SQL package's `priv/`.
- The benchmark is development-only: `quasar_mist/test/quasar_benchmark.gleam`.

## Regression evidence

The suite exercises a blocked Store while local execution and shutdown succeed,
an actual supervised restart using the original handle, admission rejection
during drain, routing one worker into two queues, request-ID propagation, stale
prefetched claims without invoking user code, and fresh claims without an eager
renewal write.

The same Store contract runs against Memory, SQLite and PostgreSQL: stale
complete/fail/renew, manual retry with reset attempts, invalid jobs, terminal
administrative operations and exhausted lease recovery. PostgreSQL additionally
runs 50 jobs through two runtimes. Its tests use unique queue names, never
truncate an existing database.

## Explicit limits

Lease fencing protects persisted state, not external side effects. Durable
handlers must remain idempotent. Retry backoff (1–60 seconds) and lease duration
(30 seconds) remain fixed MVP policies; polling is configurable and remains the
fallback for external wake signals. The reporter is asynchronous but its mailbox
is not bounded. Store errors preserve the portable contract, not driver diagnostics.

Constellation pool value constructors are imported from its public
`worker_pool/types` module only inside Quasar's local adapter. Quasar's own
public error/event constructors and call signatures do not change.
