//// Local execution boundary. No Store or durable scheduler dependencies.

import constellation/domains/stage_error
import constellation/worker_pool
import constellation/worker_pool/types as pool_types
import gleam/erlang/process
import gleam/result
import quasar_jobs/error.{
  type ExecuteError, DeadlineExceededMayComplete, HandlerFailed, Overloaded,
  ShuttingDown, Unavailable,
}
import quasar_jobs/event
import quasar_jobs/internal/reporter.{type Reporter}
import quasar_jobs/request_id.{type RequestId}

pub type PoolConfig {
  PoolConfig(name: String, workers: Int, prefetch: Int, buffer_capacity: Int)
}

pub opaque type Task {
  Task(run: fn() -> Nil)
}

pub type Pool =
  worker_pool.Pool(Task, Nil)

pub type Executor {
  Executor(pool: Pool, reporter: Reporter)
}

pub fn start(config: PoolConfig, timeout: Int, report: Reporter) {
  worker_pool.each(
    size: config.workers,
    prefetch: config.prefetch,
    initial_state: fn(_) { Nil },
    handle_event: fn(_, task: Task) { task.run() },
  )
  |> worker_pool.with_buffer_capacity(config.buffer_capacity)
  |> worker_pool.with_timeout(timeout)
  |> worker_pool.with_reporter(fn(message) {
    case message {
      pool_types.WorkerStarted(id, _) ->
        reporter.emit(
          report,
          event.WorkerStarted(config.name, worker_pool.worker_id_to_int(id)),
        )
      pool_types.WorkerReplaced(old, fresh, _) ->
        reporter.emit(
          report,
          event.WorkerReplaced(
            config.name,
            worker_pool.worker_id_to_int(old),
            worker_pool.worker_id_to_int(fresh),
          ),
        )
      _ -> Nil
    }
  })
  |> worker_pool.start_link
}

pub fn stop(pool: Pool) -> Result(Nil, ExecuteError) {
  worker_pool.stop(pool) |> result.map_error(translate_pool_error)
}

pub fn call(
  executor: Executor,
  pool_name: String,
  id: RequestId,
  timeout: Int,
  run: fn() -> output,
) -> Result(output, ExecuteError) {
  let reply = process.new_subject()
  let task =
    measured(executor.reporter, pool_name, id, fn() {
      let outcome = run_safely(run) |> result.map_error(fn(_) { HandlerFailed })
      process.send(reply, outcome)
      outcome |> result.replace(Nil)
    })
  use _ <- result.try(submit(executor, pool_name, id, task))
  case process.receive(reply, within: timeout) {
    Ok(outcome) -> outcome
    Error(_) -> {
      reporter.emit(executor.reporter, event.RequestTimedOut(id, pool_name))
      Error(DeadlineExceededMayComplete)
    }
  }
}

pub fn cast(
  executor: Executor,
  pool_name: String,
  run: fn() -> Nil,
) -> Result(Nil, ExecuteError) {
  let id = request_id.new()
  let task =
    measured(executor.reporter, pool_name, id, fn() {
      run_safely(run) |> result.map_error(fn(_) { HandlerFailed })
    })
  submit(executor, pool_name, id, task)
}

fn measured(
  report: Reporter,
  pool: String,
  id: RequestId,
  run: fn() -> Result(Nil, ExecuteError),
) -> Task {
  let queued = monotonic_milliseconds()
  reporter.emit(report, event.RequestQueued(id, pool, queued))
  Task(fn() {
    let started = monotonic_milliseconds()
    reporter.emit(report, event.RequestStarted(id, pool, started - queued))
    case run() {
      Ok(Nil) -> {
        let ended = monotonic_milliseconds()
        reporter.emit(
          report,
          event.RequestCompleted(id, pool, ended - started, ended - queued),
        )
      }
      Error(_) -> reporter.emit(report, event.RequestFailed(id, pool))
    }
  })
}

fn submit(executor: Executor, pool: String, id: RequestId, task: Task) {
  case worker_pool.push(executor.pool, [task]) {
    Ok(Nil) -> Ok(Nil)
    Error(reason) -> {
      let reason = translate_pool_error(reason)
      reporter.emit(executor.reporter, event.RequestRejected(id, pool, reason))
      Error(reason)
    }
  }
}

fn translate_pool_error(error: worker_pool.PoolError) -> ExecuteError {
  case error {
    pool_types.StageProtocol(stage_error.BufferCapacityExceeded(..)) ->
      Overloaded
    pool_types.PoolUnavailable -> Unavailable
    pool_types.PoolTimeout -> DeadlineExceededMayComplete
    pool_types.AlreadyStopping -> ShuttingDown
    pool_types.SourceManaged -> Unavailable
    pool_types.StageProtocol(_) -> Unavailable
  }
}

@external(erlang, "quasar_jobs_ffi", "run_safely")
fn run_safely(run: fn() -> output) -> Result(output, Nil)

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int
