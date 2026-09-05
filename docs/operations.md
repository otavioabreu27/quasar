# Operations

## Overload

Each local pool has workers, prefetch, and a buffer capacity. For synchronous
request pools, keep prefetch at 1 unless reserving several tasks for one worker
is intentional. When the undelivered remainder exceeds the Constellation
buffer, `call` and `cast` return `Overloaded`; `quasar_mist` maps that to 503 and
adds `Retry-After: 1`.

## Instrumentation

`with_reporter` receives request queued/started/completed/rejected/timed-out or
failed events, worker lifecycle events, runtime shutdown events, job lifecycle
events, lease renewal events, and typed claim/persistence failures. Import
constructors from `quasar_jobs/event`. Durations use the monotonic clock. The
reporter runs in a separate actor and panics are contained. Its mailbox is not
bounded: reporters should remain inexpensive or hand off to a bounded sink.
Mist passes the same request ID into handler headers, response headers and
Quasar request telemetry.

Durable queues additionally emit `QueueClaimCompleted`, with requested and
returned batch sizes plus claim duration, and `JobCompletionPersisted`, with
the completion-batch duration. Results are flushed at 100 items or after 5 ms;
`JobCompleted`, retry, and discard events are emitted only after persistence
succeeds. A process crash before flush leaves the job executing so normal lease
recovery can retry it. `LeaseRenewalDeferred` means a fresh claim had
enough time remaining to avoid an immediate write. `LeaseRenewed` records a
fenced renewal for delayed prefetch or a later heartbeat. Current execution can
be derived from `JobStarted` minus completed, retried, discarded, or failed
jobs.

## Retention

Build database-independent terminal-state policies with
`quasar_jobs/retention`. An unset state is retained indefinitely; configured
durations must be positive. The PostgreSQL adapter provides a linked periodic
cleaner with bounded batches, pauses, partial retention indexes, `SKIP LOCKED`,
and a non-blocking advisory lock. Its reporter emits batch count/duration/error
and cycle completion events.

Retention is based on the time a job entered its terminal state. PostgreSQL
stores this in `finished_at`. Historical rows created before that column use
`completed_at` for completed jobs and `attempted_at` or `inserted_at` as the
best available fallback for cancelled/discarded jobs.

Deleting old tuples does not immediately shrink the table file. PostgreSQL
autovacuum reclaims that space for reuse; reserve table rewrites for explicit
maintenance windows.

## Shutdown

`stop` stops durable schedulers from claiming new rows, gracefully drains
Constellation pools, records completion/failure transactions already in
progress, emits stopping/stopped events, and then terminates the runtime owner.
The Store handle is application-owned so persistent data remains available for
inspection or a subsequent runtime.

The default shutdown timeout is 5 seconds. Change it with
`quasar.with_shutdown_timeout(config, milliseconds)`; the same bound is used
for each synchronous Constellation pool stop, durable scheduler stop and for
waiting on the owner.
Draining multiple pools can outlast the caller's wait. An error does not prove
that draining or an in-flight Store mutation finished; retain the Store until
all users have stopped. New work receives `ShuttingDown` while drain is active.
A failed pool drain terminates the owner abnormally so linked children are not
silently orphaned.

## Supervision

`quasar.supervised(config)` returns an OTP child specification. The runtime
owner is linked to its local pools, durable schedulers, source-backed durable
pools, and event reporter. A fatal child exit terminates the owner and lets the
application supervisor apply its configured restart policy. The stable named
handle resolves the replacement runtime without capturing obsolete pool or
reporter subjects. Calls during restart may return `Unavailable`; they are not
automatically retried.

## HTTP boundary

Mist owns socket lifetime and byte transmission. Quasar considers execution
complete when the handler produces a response. This does not imply that all
response bytes reached the client. Mist's synchronous handler contract does
not expose disconnect cancellation to the wrapper, so Quasar makes no claim of
cancelling work on disconnect.
