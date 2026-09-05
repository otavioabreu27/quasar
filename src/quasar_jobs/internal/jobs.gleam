//// Durable application commands execute outside the lifecycle actor.

import gleam/dict
import gleam/list
import gleam/result
import quasar_jobs/error.{
  type JobError, JobRuntimeStopping, QueueNotFound, RuntimeUnavailable,
  StoreFailure, WorkerQueueMismatch,
}
import quasar_jobs/event
import quasar_jobs/internal/durable
import quasar_jobs/internal/reporter
import quasar_jobs/internal/runtime
import quasar_jobs/job
import quasar_jobs/store

pub fn enqueue(
  owner: runtime.Runtime,
  new_job: job.NewJob,
  queue: String,
  at: Int,
) -> Result(job.JobId, JobError) {
  use access <- result.try(access(owner))
  use config <- result.try(
    list.find(access.configs, fn(config) { durable.name(config) == queue })
    |> result.map_error(fn(_) { QueueNotFound }),
  )
  use _ <- result.try(
    case durable.worker_name(config) == job.worker_name(new_job) {
      True -> Ok(Nil)
      False -> Error(WorkerQueueMismatch)
    },
  )
  use id <- result.try(
    store.insert(access.store, new_job, queue, at, now())
    |> result.map_error(StoreFailure),
  )
  reporter.emit(access.reporter, event.JobInserted(id, queue))
  wake(access, queue)
  Ok(id)
}

pub fn get(owner, id) {
  use access <- result.try(access(owner))
  store.get(access.store, id) |> result.map_error(StoreFailure)
}

pub fn cancel(owner, id) {
  use access <- result.try(access(owner))
  store.cancel(access.store, id) |> result.map_error(StoreFailure)
}

pub fn retry(owner, id) {
  use access <- result.try(access(owner))
  use item <- result.try(
    store.retry(access.store, id, now()) |> result.map_error(StoreFailure),
  )
  wake(access, job.queue(item))
  Ok(item)
}

fn wake(access: runtime.DurableAccess, queue: String) {
  case dict.get(access.queues, queue) {
    Ok(queue) -> durable.wake(queue)
    Error(_) -> Nil
  }
}

fn access(owner) {
  runtime.durable_access(owner)
  |> result.map_error(fn(reason) {
    case reason {
      error.ShuttingDown -> JobRuntimeStopping
      _ -> RuntimeUnavailable
    }
  })
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
pub fn now() -> Int
