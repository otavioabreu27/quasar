import quasar_jobs/job
import quasar_jobs/store

// Identical contract exercised by every adapter; no sleeps or runtime scheduler.
pub fn fencing(database: store.Store, queue: String, reap: fn(Int) -> Nil) {
  let assert Ok(id) =
    store.insert(
      database,
      job.new_job("worker", "payload", 0, 3),
      queue,
      100,
      100,
    )
  let assert Ok([first]) = store.claim(database, queue, 1, "same-node", 100, 10)
  let assert Ok(old) = job.execution_token(first)
  reap(111)
  let assert Ok([second]) =
    store.claim(database, queue, 1, "same-node", 111, 10)
  let assert Ok(current) = job.execution_token(second)
  assert job.attempt(second) == 2
  assert store.complete(database, old, 112) == Error(store.StaleExecution)
  assert store.fail(database, old, job.JobError("old", "stale"), 112)
    == Error(store.StaleExecution)
  assert store.renew_lease(database, old, 999) == Error(store.StaleExecution)
  let assert Ok(unchanged) = store.get(database, id)
  assert job.owns(unchanged, current)
  let assert Ok(_) = store.cancel(database, id)
  let assert Ok(_) = store.retry(database, id, 112)
  let assert Ok([third]) =
    store.claim(database, queue, 1, "same-node", system_milliseconds(), 30_000)
  let assert Ok(latest) = job.execution_token(third)
  assert job.attempt(third) == 1
  assert job.token_owner(old) != job.token_owner(latest)
  assert store.complete(database, old, 113) == Error(store.StaleExecution)
  let assert Ok(done) = store.complete(database, latest, 113)
  assert job.status(done) == job.Completed
  assert store.cancel(database, id)
    == Error(
      store.InvalidTransition(job.InvalidTransition(job.Completed, "cancel")),
    )
  assert store.retry(database, id, 114)
    == Error(
      store.InvalidTransition(job.InvalidTransition(job.Completed, "retry")),
    )
  assert store.get(database, job.new_id(-1)) == Error(store.NotFound)
  assert store.cancel(database, job.new_id(-1)) == Error(store.NotFound)
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int

pub fn validation_and_exhaustion(
  database: store.Store,
  queue: String,
  reap: fn(Int) -> Nil,
) {
  assert store.insert(
      database,
      job.new_job("worker", "p", 0, 0),
      queue,
      100,
      100,
    )
    == Error(
      store.InvalidTransition(job.InvalidJob("max_attempts must be positive")),
    )
  assert store.insert(database, job.new_job("", "p", 0, 1), queue, 100, 100)
    == Error(
      store.InvalidTransition(job.InvalidJob("worker name must not be empty")),
    )
  let assert Ok(id) =
    store.insert(database, job.new_job("worker", "p", 0, 1), queue, 100, 100)
  let assert Ok([_]) = store.claim(database, queue, 1, "node", 100, 10)
  reap(111)
  assert store.claim(database, queue, 1, "node", 111, 10) == Ok([])
  let assert Ok(item) = store.get(database, id)
  assert job.status(item) == job.Discarded
  assert job.attempt(item) == 1
}
