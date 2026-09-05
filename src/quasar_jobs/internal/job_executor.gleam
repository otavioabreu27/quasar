//// Executes one durable claim. Scheduling and Store ownership live elsewhere.

import gleam/int
import quasar_jobs/event.{
  type Event, JobCompleted, JobCompletionPersisted, JobDiscarded,
  JobPersistenceFailed, JobRetryScheduled, JobStarted, LeaseRenewed,
}
import quasar_jobs/internal/lease
import quasar_jobs/job.{type Job}
import quasar_jobs/store.{type Store}
import quasar_jobs/worker.{type Definition}

pub fn execute(
  queue: String,
  definition: Definition,
  lease_ms: Int,
  store: Store,
  claimed_job: Job,
  report: fn(Event) -> Nil,
) -> Nil {
  let assert Ok(token) = job.execution_token(claimed_job)
  // Prefetched work may have waited beyond its lease. Check ownership before
  // invoking user code; fencing still cannot prevent duplicate external effects.
  let expires_at = system_milliseconds() + lease_ms
  case store.renew_lease(store, token, expires_at) {
    Error(reason) ->
      report(JobPersistenceFailed(job.id(claimed_job), queue, "begin", reason))
    Ok(_) -> {
      report(LeaseRenewed(job.id(claimed_job), queue, expires_at))
      execute_owned(queue, definition, lease_ms, store, claimed_job, report)
    }
  }
}

fn execute_owned(queue, definition, lease_ms, store, claimed_job, report) {
  let id = job.id(claimed_job)
  let attempt = job.attempt(claimed_job)
  report(JobStarted(id, queue, attempt))
  let assert Ok(token) = job.execution_token(claimed_job)
  let heartbeat = lease.start(store, token, queue, lease_ms, report)
  let context = worker.Context(job_id: id, attempt: attempt)
  let execution =
    run_safely(fn() {
      worker.run(definition, job.payload(claimed_job), context)
    })
  case heartbeat {
    Ok(subject) -> lease.stop(subject)
    Error(_) -> Nil
  }
  case execution {
    Ok(Ok(Nil)) -> {
      let completion_started = monotonic_milliseconds()
      case store.complete(store, token, system_milliseconds()) {
        Ok(_) -> {
          report(JobCompletionPersisted(
            id,
            queue,
            monotonic_milliseconds() - completion_started,
          ))
          report(JobCompleted(id, queue))
        }
        Error(reason) ->
          report(JobPersistenceFailed(id, queue, "complete", reason))
      }
    }
    Ok(Error(message)) -> fail_job(queue, store, claimed_job, message, report)
    Error(_) -> fail_job(queue, store, claimed_job, "handler crashed", report)
  }
}

fn fail_job(
  queue: String,
  store: Store,
  claimed_job: Job,
  message: String,
  report: fn(Event) -> Nil,
) -> Nil {
  let assert Ok(token) = job.execution_token(claimed_job)
  let available_at = system_milliseconds() + backoff(job.attempt(claimed_job))
  case
    store.fail(
      store,
      token,
      job.JobError("execution_failed", message),
      available_at,
    )
  {
    Ok(updated) ->
      case job.status(updated) {
        job.Discarded -> report(JobDiscarded(job.id(updated), queue))
        _ -> report(JobRetryScheduled(job.id(updated), queue, available_at))
      }
    Error(reason) ->
      report(JobPersistenceFailed(job.id(claimed_job), queue, "fail", reason))
  }
}

fn backoff(attempt: Int) -> Int {
  int.min(60_000, 1000 * power_of_two(int.min(6, int.max(0, attempt - 1))))
}

fn power_of_two(exponent: Int) -> Int {
  case exponent <= 0 {
    True -> 1
    False -> 2 * power_of_two(exponent - 1)
  }
}

@external(erlang, "quasar_jobs_ffi", "run_safely")
fn run_safely(run: fn() -> output) -> Result(output, Nil)

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int
