//// Periodic, bounded PostgreSQL cleanup for terminal Quasar jobs.

import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/otp/actor
import gleam/result
import pog.{type Connection, type QueryError}
import quasar_jobs/retention as policy

const retention_lock = 1_903_527_852

pub type Event {
  BatchCompleted(status: String, requested: Int, deleted: Int, duration_ms: Int)
  BatchFailed(status: String, reason: QueryError)
  CycleCompleted(deleted: Int)
}

pub type StartError {
  InvalidConfig
  ActorStart(actor.StartError)
}

pub opaque type Config {
  Config(
    connection: Connection,
    policy: policy.Policy,
    batch_size: Int,
    pause_ms: Int,
    interval_ms: Int,
    report: fn(Event) -> Nil,
  )
}

pub opaque type Runtime {
  Runtime(subject: Subject(Message))
}

type State {
  State(
    subject: Subject(Message),
    connection: Connection,
    policy: policy.Policy,
    batch_size: Int,
    pause_ms: Int,
    interval_ms: Int,
    report: fn(Event) -> Nil,
    pending: List(policy.Rule),
    deleted_in_cycle: Int,
  )
}

type Message {
  StartCycle
  RunBatch
  Stop(Subject(Nil))
}

/// Builds a conservative cleanup configuration.
///
/// Defaults: 1,000 rows per batch, 100 ms between batches, and one cycle per
/// minute. Disabled policy states are never touched.
pub fn new(connection: Connection, policy: policy.Policy) -> Config {
  Config(
    connection:,
    policy:,
    batch_size: 1000,
    pause_ms: 100,
    interval_ms: 60_000,
    report: fn(_) { Nil },
  )
}

pub fn with_batch_size(config: Config, rows rows: Int) -> Config {
  Config(..config, batch_size: rows)
}

pub fn with_pause(config: Config, milliseconds milliseconds: Int) -> Config {
  Config(..config, pause_ms: milliseconds)
}

pub fn with_interval(config: Config, milliseconds milliseconds: Int) -> Config {
  Config(..config, interval_ms: milliseconds)
}

pub fn with_reporter(config: Config, report: fn(Event) -> Nil) -> Config {
  Config(..config, report:)
}

/// Starts a linked retention worker. Stop it before closing the Pog pool.
pub fn start(config: Config) -> Result(Runtime, StartError) {
  case
    policy.valid(config.policy)
    && config.batch_size > 0
    && config.pause_ms >= 0
    && config.interval_ms > 0
  {
    False -> Error(InvalidConfig)
    True -> {
      let builder =
        actor.new_with_initialiser(1000, fn(subject) {
          Ok(
            actor.initialised(State(
              subject:,
              connection: config.connection,
              policy: config.policy,
              batch_size: config.batch_size,
              pause_ms: config.pause_ms,
              interval_ms: config.interval_ms,
              report: config.report,
              pending: [],
              deleted_in_cycle: 0,
            ))
            |> actor.returning(Runtime(subject)),
          )
        })
        |> actor.on_message(handle_message)
      use started <- result.try(
        actor.start(builder) |> result.map_error(ActorStart),
      )
      process.send(started.data.subject, StartCycle)
      Ok(started.data)
    }
  }
}

pub fn stop(runtime: Runtime) -> Nil {
  let reply = process.new_subject()
  process.send(runtime.subject, Stop(reply))
  let _ = process.receive(reply, within: 5000)
  Nil
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    StartCycle -> {
      let pending = policy.rules(state.policy, system_milliseconds())
      case pending {
        [] -> finish_cycle(State(..state, pending:))
        _ -> {
          process.send(state.subject, RunBatch)
          actor.continue(State(..state, pending:, deleted_in_cycle: 0))
        }
      }
    }
    RunBatch -> run_batch(state)
    Stop(reply) -> {
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

fn run_batch(state: State) -> actor.Next(State, Message) {
  case state.pending {
    [] -> finish_cycle(state)
    [rule, ..remaining] -> {
      let started = monotonic_milliseconds()
      case delete_batch(state.connection, rule, state.batch_size) {
        Error(reason) -> {
          emit(state.report, BatchFailed(policy.status(rule), reason))
          continue_after_pause(State(..state, pending: remaining))
        }
        Ok(deleted) -> {
          emit(
            state.report,
            BatchCompleted(
              policy.status(rule),
              state.batch_size,
              deleted,
              monotonic_milliseconds() - started,
            ),
          )
          let next =
            State(
              ..state,
              pending: case deleted == state.batch_size {
                True -> state.pending
                False -> remaining
              },
              deleted_in_cycle: state.deleted_in_cycle + deleted,
            )
          case next.pending {
            [] -> finish_cycle(next)
            _ -> continue_after_pause(next)
          }
        }
      }
    }
  }
}

fn continue_after_pause(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, state.pause_ms, RunBatch)
  actor.continue(state)
}

fn finish_cycle(state: State) -> actor.Next(State, Message) {
  emit(state.report, CycleCompleted(state.deleted_in_cycle))
  process.send_after(state.subject, state.interval_ms, StartCycle)
  actor.continue(State(..state, pending: [], deleted_in_cycle: 0))
}

fn delete_batch(
  connection: Connection,
  rule: policy.Rule,
  batch_size: Int,
) -> Result(Int, QueryError) {
  let timestamp = case rule {
    policy.Completed(_) -> "COALESCE(finished_at, completed_at, inserted_at)"
    policy.Cancelled(_) | policy.Discarded(_) ->
      "COALESCE(finished_at, attempted_at, inserted_at)"
  }
  let sql = "WITH retention_lock AS MATERIALIZED (
       SELECT pg_try_advisory_xact_lock(" <> int.to_string(retention_lock) <> ") AS acquired
     ), doomed AS (
       SELECT id FROM quasar_jobs
       WHERE status = '" <> policy.status(rule) <> "'
         AND " <> timestamp <> " < $1
         AND (SELECT acquired FROM retention_lock)
       ORDER BY " <> timestamp <> ", id
       FOR UPDATE SKIP LOCKED
       LIMIT $2
     ), deleted AS (
       DELETE FROM quasar_jobs q USING doomed
       WHERE q.id = doomed.id
       RETURNING 1
     )
     SELECT count(*)::int FROM deleted"
  let decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(count)
  }
  let query =
    pog.query(sql)
    |> pog.parameter(pog.int(policy.cutoff(rule)))
    |> pog.parameter(pog.int(batch_size))
    |> pog.returning(decoder)
  use returned <- result.try(pog.execute(query, on: connection))
  case returned.rows {
    [count] -> Ok(count)
    _ -> Ok(0)
  }
}

fn emit(report: fn(Event) -> Nil, event: Event) -> Nil {
  let _ = run_safely(fn() { report(event) })
  Nil
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int

@external(erlang, "quasar_jobs_ffi", "run_safely")
fn run_safely(run: fn() -> output) -> Result(output, Nil)
