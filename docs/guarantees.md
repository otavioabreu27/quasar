# Guarantees and non-guarantees

| Operation | Persistence | Delivery | Automatic retry | Timeout meaning |
| --- | --- | --- | --- | --- |
| `call` | none | at-most-once | never | handler may complete later |
| `cast` | none | at-most-once | never | submission only |
| `enqueue` | Store | at-least-once | configured attempts | Store is authoritative |

Handlers for durable workers must be idempotent. A worker can commit an
external side effect and die before the Store records completion. Use `JobId`
as an idempotency key whenever the external system permits it.

A crashed durable worker leaves its row executing until the lease expires.
Another instance may claim and execute the recovered row. Heartbeats renew
leases while a handler is running.

Pool concurrency is local to one runtime and one node. SQLite is single-node.
PostgreSQL provides atomic multi-instance claims, not exactly-once side effects.
