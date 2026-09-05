# Durable jobs

Jobs contain an ID, queue and worker names, encoded payload, status, priority,
attempt and maximum attempts, availability and lifecycle timestamps, lease
owner/expiry, and a structured error.

The state machine is:

```text
Available/Scheduled/Retryable
  └── claim ──> Executing
                  ├── complete ──> Completed
                  ├── fail ──────> Retryable ──> Executing
                  ├── exhausted ─> Discarded
                  └── lease expiry -> Retryable (or Discarded at attempt limit)

Non-terminal ── cancel ──> Cancelled
Retryable/Discarded/Cancelled ── manual retry ──> Available
```

Retry backoff starts at one second and doubles to a 60-second cap. Queue
schedulers poll at one-second intervals as a fallback for scheduling and Store
recovery. Database notifications can be added later without changing the Store
or demand contracts.

Enqueue and schedule require an explicit queue name. Multiple queues may share
one worker; a job cannot be routed to an incompatible worker definition.

Claims carry fenced execution tokens. ACK and heartbeat mutations compare the
job ID, fresh owner nonce and attempt atomically. A fresh claim defers its first
renewal until the lease approaches its final third, avoiding one PostgreSQL
write for short jobs. Buffered claims that reach that margin must renew
ownership before user code begins. Later heartbeats run near expiry and stop
after a fenced renewal fails. This prevents an old worker from overwriting a
reclaimed job, but does not provide exactly-once external side effects.
