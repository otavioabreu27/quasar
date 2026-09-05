# Quasar

Quasar is a Gleam runtime for bounded local execution and durable background
jobs on the BEAM. The Hex package and module namespace are `quasar_jobs`:

```gleam
import quasar_jobs as quasar
```

> Mist manages the connection. Quasar manages execution. Constellation
> distributes work according to capacity. The Store provides durability when
> it is needed.

## Status

This repository contains the initial `0.1.0` implementation. It requires the
prepared, not-yet-published Constellation `0.2.0` release for bounded public
worker pools and linked startup. Nothing is published automatically.

## Local execution

```gleam
import quasar_jobs as quasar

let assert Ok(runtime) =
  quasar.new()
  |> quasar.local_pool(
    name: "http",
    workers: 100,
    prefetch: 1,
    buffer_capacity: 500,
  )
  |> quasar.local_pool(
    name: "external-api",
    workers: 20,
    prefetch: 1,
    buffer_capacity: 100,
  )
  |> quasar.start

let result =
  quasar.call(runtime, on: "http", timeout: 5_000, run: fn() {
    handle_request(request)
  })
```

Applications with an OTP tree can add `quasar.supervised(config)` as a child
specification. The runtime owner links its pools, durable schedulers, and
reporter so the supervisor can restart the execution unit after a fatal exit.
Existing runtime handles resolve the replacement through a stable OTP name.

`call` preserves the handler's result type. `cast` is fire-and-forget. Both are
ephemeral, local to one BEAM node, at-most-once, and never retried. A
`DeadlineExceededMayComplete` result explicitly means the handler may still
finish after the caller's deadline.

The local buffer belongs exclusively to Constellation. Quasar does not track a
second capacity counter or queue.

## Durable jobs

```gleam
import gleam/int
import gleam/result
import quasar_jobs as quasar
import quasar_jobs/store/memory
import quasar_jobs/worker

let report_worker = worker.new(
  name: "report",
  encode: int.to_string,
  decode: fn(value) {
    int.parse(value) |> result.map_error(fn(_) { "invalid report id" })
  },
  perform: fn(report_id, context) {
    // Use context.job_id as an idempotency key.
    generate_report(report_id, context.job_id)
  },
)

let assert Ok(memory_store) = memory.new()
let assert Ok(runtime) =
  quasar.new()
  |> quasar.with_store(memory_store)
  |> quasar.queue(
    name: "reports",
    worker: report_worker,
    concurrency: 8,
    prefetch: 2,
  )
  |> quasar.start

let assert Ok(job_id) =
  worker.job(report_worker, 42)
  |> quasar.enqueue(runtime, on: "reports")
```

Durable queues use `worker_pool.start_with_source_link`. Each Constellation
demand grant is the exact upper bound for an atomic Store claim. Partial grants
stay open while the Store is empty, and a one-second fallback poll wakes
scheduled jobs and lease recovery without loading the backlog into memory.

Queue selection is explicit, even when two queues share the same worker.
The selected queue must be registered for that worker.

The Store is the source of truth for ACK, retry, and lease state. Every execution
mutation atomically checks a claim token (job ID, owner nonce and attempt);
a stale execution cannot ACK or renew a newer claim.
`source.supply` and `pool_types.BatchCompleted` (`constellation/worker_pool/types`) are not durable ACKs.

## Stores

- `memory.new()` is deterministic and intended for tests.
- `quasar_sqlite.open(path)` runs migrations and supports one application node.
- `quasar_postgres.new(connection)` uses an application-owned Pog pool and
  transactionally claims with `FOR UPDATE SKIP LOCKED` for multiple instances.

SQL drivers are optional packages: `quasar_sqlite` (module `quasar_sqlite`)
and `quasar_postgres` (module `quasar_postgres`). The core has no SQL driver
or HTTP dependency. Each adapter keeps its authoritative versioned schema in
its own `priv/` directory. SQLite does not
create a cluster. PostgreSQL workers remain at-least-once: a process may finish
an external effect and die before its completion transaction commits.

## Mist and Wisp

The separate `quasar_mist` package wraps the current Mist handler signature and
maps overload/failure/deadline results to 503/500/504. Wisp composes through
`wisp_mist.handler`; there is no duplicate Wisp executor.

WebSockets, SSE, streaming/chunked responses, and routes that transfer
connection ownership bypass the managed wrapper in the MVP. Ordinary HTTP
requests are never persisted or retried.

See [`quasar_mist/README.md`](quasar_mist/README.md).

## Guarantees

- Local execution: ephemeral, at-most-once, bounded per pool and per node.
- Durable execution: persisted and at-least-once.
- Worker demand is renewed only after its handler returns.
- Handler crashes are contained at the Quasar boundary where the BEAM permits,
  and the Constellation worker remains healthy.
- Slow or crashing reporters do not block the Stage or worker pool.
- Local concurrency is not a global cluster limit.
- No automatic retry is performed for local calls or HTTP requests.

Error and event constructors live in `quasar_jobs/error` and
`quasar_jobs/event`; the façade exposes type aliases, not a second copy of
these values. See [migration notes](docs/architecture.md#pre-release-api-changes).

See [architecture](docs/architecture.md), [execution guarantees](docs/guarantees.md),
[durable jobs](docs/durable-jobs.md), [stores](docs/stores.md), and
[operations](docs/operations.md). The reproducible HTTP load fixture and
measurement protocol are in [benchmarks](docs/benchmarks.md).

## Development

```sh
mise x rebar@3.27.0 -- gleam test
gleam format --check src test
gleam docs build
gleam export package-interface --out build/quasar-interface.json
gleam export hex-tarball
```

The final Hex configuration uses only versioned Hex dependencies. During local
development, Constellation `0.2.0` must first be released or temporarily linked;
the temporary link must never be committed or included in a tarball.
