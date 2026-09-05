//// Expired leases are recovered independently of demand/claim traffic.

import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/result
import pog

pub type Event {
  Reaped(rows: Int, duration_ms: Int)
  ReapFailed
}

pub opaque type Runtime {
  Runtime(subject: Subject(Message))
}

type Message {
  Tick
  Stop(Subject(Nil))
}

/// One bounded statement. SKIP LOCKED makes concurrent replicas safe; the
/// advisory lock avoids all replicas doing the same empty scan simultaneously.
/// `now` is milliseconds since epoch, exposed for deterministic recovery tests.
pub fn batch(connection: pog.Connection, now: Int, limit: Int) {
  let decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(count)
  }
  pog.query(
    "WITH guard AS MATERIALIZED (
      SELECT pg_try_advisory_xact_lock(1903527853) AS acquired
    ), expired AS (
      SELECT id FROM quasar_jobs
      WHERE status = 'executing' AND lease_expires_at <= $1
        AND (SELECT acquired FROM guard)
      ORDER BY lease_expires_at, id FOR UPDATE SKIP LOCKED LIMIT $2
    ), recovered AS (
      UPDATE quasar_jobs q SET
        status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END,
        available_at = $1, finished_at = CASE WHEN attempt >= max_attempts THEN $1 ELSE NULL END,
        lease_owner = NULL, lease_expires_at = NULL,
        error_kind = 'lease_expired', error_message = 'execution lease expired'
      FROM expired WHERE q.id = expired.id RETURNING q.id
    ) SELECT count(*)::int FROM recovered",
  )
  |> pog.parameter(pog.int(now))
  |> pog.parameter(
    pog.int(case limit > 0 {
      True -> limit
      False -> 0
    }),
  )
  |> pog.returning(decoder)
  |> pog.execute(on: connection)
  |> result.map(fn(rows) {
    let assert [count] = rows.rows
    count
  })
}

/// Start once per application, before Quasar, and stop before closing the pool.
/// Batches are capped at 500; a full batch waits 100ms, otherwise interval_ms.
/// Queries failing never stop subsequent recovery attempts. Reporters receive
/// no database error payloads (which can contain connection credentials).
pub fn start(
  connection: pog.Connection,
  interval_ms: Int,
  report: fn(Event) -> Nil,
) {
  actor.new_with_initialiser(1000, fn(subject) {
    process.send(subject, Tick)
    Ok(actor.initialised(subject) |> actor.returning(Runtime(subject)))
  })
  |> actor.on_message(fn(subject, message) {
    case message {
      Stop(reply) -> {
        process.send(reply, Nil)
        actor.stop()
      }
      Tick -> {
        let started = monotonic_milliseconds()
        let delay = case batch(connection, system_milliseconds(), 500) {
          Ok(count) -> {
            report(Reaped(count, monotonic_milliseconds() - started))
            case count == 500 {
              True -> 100
              False -> interval_ms
            }
          }
          Error(_) -> {
            report(ReapFailed)
            interval_ms
          }
        }
        process.send_after(
          subject,
          case delay > 0 {
            True -> delay
            False -> 1000
          },
          Tick,
        )
        actor.continue(subject)
      }
    }
  })
  |> actor.start
  |> result.map(fn(started) { started.data })
}

pub fn stop(runtime: Runtime) {
  let reply = process.new_subject()
  process.send(runtime.subject, Stop(reply))
  process.receive(reply, within: 5000)
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int
