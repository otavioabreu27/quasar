//// Executes one durable claim. Scheduling and Store ownership live elsewhere.

import gleam/int
import gleam/option.{None, Some}
import quasar_jobs/event.{
  type Event, JobCompleted, JobCompletionPersisted, JobDiscarded,
  JobPersistenceFailed, JobRetryScheduled, JobStarted, LeaseRenewalDeferred,
  LeaseRenewed,
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
  let now = system_milliseconds()
  let assert Ok(margin) = int.divide(lease_ms, 3)
  let renewal_margin = int.max(1, margin)
  case job.lease_expires_at(claimed_job) {
    Some(expires_at) if expires_at - now > renewal_margin -> {
      // A fresh claim is still exclusively owned. Avoid a write and schedule
      // the heartbeat relative to its actual expiry instead.
      report(LeaseRenewalDeferred(job.id(claimed_job), queue, expires_at))
      execute_owned(
        queue,
        definition,
        lease_ms,
        expires_at,
        store,
        claimed_job,
        report,
      )
    }
    Some(_) | None -> {
      // Prefetched work may be near or beyond expiry. This fenced update must
      // succeed before user code is allowed to run.
      let expires_at = now + lease_ms
      case store.renew_lease(store, token, expires_at) {
        Error(reason) ->
          report(JobPersistenceFailed(
            job.id(claimed_job),
            queue,
            "begin",
            reason,
          ))
        Ok(updated) -> {
          report(LeaseRenewed(job.id(claimed_job), queue, expires_at))
          execute_owned(
            queue,
            definition,
            lease_ms,
            expires_at,
            store,
            updated,
            report,
          )
        }
      }
    }
  }
}

fn execute_owned(
  queue,
  definition,
  lease_ms,
  lease_expires_at,
  store,
  claimed_job,
  report,
) {
  let id = job.id(claimed_job)
  let attempt = job.attempt(claimed_job)
  report(JobStarted(id, queue, attempt))
  let assert Ok(token) = job.execution_token(claimed_job)
  let heartbeat =
    lease.start(store, token, queue, lease_ms, lease_expires_at, report)
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
