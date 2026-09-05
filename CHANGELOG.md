# Changelog

## 0.1.0 - 2026-09-04

### Added

- Named, bounded local pools with typed `call` and at-most-once `cast`.
- Explicit overload, availability, shutdown, handler, and deadline errors.
- Asynchronous unified local and durable instrumentation.
- Typed durable workers, scheduling, retries, exponential backoff, leases,
  heartbeats, recovery, cancellation, and manual retry.
- Deterministic MemoryStore, single-node SQLiteStore, and PostgreSQL Store with
  `FOR UPDATE SKIP LOCKED` claims.
- Separate Mist adapter with Wisp composition and conservative HTTP errors.

### Prerequisite

- Requires Constellation 0.2.0, which adds bounded public worker pools and
  linked source-backed pool startup. This package must not be published before
  that Constellation release exists on Hex.

### Architecture refinement (pre-release)

- SQL adapters extracted into optional `quasar_sqlite` and `quasar_postgres`
  packages; no SQL drivers in core, benchmark excluded from published source.
- Lifecycle, local execution, durable commands, scheduling, execution, leases,
  reporting and validation separated by responsibility.
- Canonical error/event types, explicit queue routing and shared request IDs.
- Restart-stable runtime handles; responsive admission during shutdown.
- Atomic execution fencing, including manual retries and stale prefetch checks.
- Shared strict SQL decoder and one authoritative migration per adapter.
- Store contract regression tests for all three implementations.

### Constellation integration update

- Use the canonical pool constructors from Constellation 0.2.0 without exposing
  them through Quasar's public errors/events.
- Shutdown completion uses an incarnation-local mailbox; an old drain cannot
  terminate a replacement runtime reached through the same public handle.
