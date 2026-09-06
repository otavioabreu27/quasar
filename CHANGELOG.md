# Changelog

## 0.3.0 - Unreleased

### Breaking changes

- PostgreSQL applications must explicitly start `quasar_postgres/reaper` before
  Quasar. Claims no longer recover expired leases as a side effect.
- PostgreSQL rejects expired execution mutations using the database clock,
  in addition to execution token fencing.
- The public event type adds `QueueWakeReceived(queue, coalesced)`; update
  exhaustive reporters when upgrading.
- All adapters require `quasar_jobs >= 0.3.0 and < 0.4.0`.

### Fixed and improved

- Aggregate fragmented demand grants into one claim and coalesce wake bursts.
- Independently enforce execution lease deadlines and cancel handlers after
  heartbeat failure. Keep heartbeats and worker capacity until acknowledgement.
- Retry transient completion-buffer failures and isolate stale batch tokens.
- Recover crashed PostgreSQL notification clients in addition to socket reconnects.
- Add bounded, periodic PostgreSQL recovery and optional separated connection pools.
- Isolate report-service database traffic, replace duplicate status reads with a
  JOIN, and add bounded runtime/pool diagnostics.
- Prepare a phased benchmark with explicit generator drops, batch scenarios,
  validated business results, temporal metrics and conservative cost accounting.

The PostgreSQL migrations already shipped with 0.2.0 are unchanged. Constellation
remains on the `>= 0.2.0 and < 0.3.0` range; no Constellation release is needed.

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
