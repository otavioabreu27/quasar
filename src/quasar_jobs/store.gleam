//// Store port for durable state. Implementations provide atomic transitions.
//// Resource ownership remains with the application that opened the Store.

import quasar_jobs/job.{
  type ExecutionToken, type Job, type JobError, type JobId, type NewJob,
}

pub type Error {
  Unavailable
  Timeout
  NotFound
  InvalidTransition(job.TransitionError)
  StaleExecution
}

pub opaque type Store {
  Store(
    insert: fn(NewJob, String, Int, Int) -> Result(JobId, Error),
    get: fn(JobId) -> Result(Job, Error),
    claim: fn(String, Int, String, Int, Int) -> Result(List(Job), Error),
    complete: fn(ExecutionToken, Int) -> Result(Job, Error),
    fail: fn(ExecutionToken, JobError, Int) -> Result(Job, Error),
    cancel: fn(JobId) -> Result(Job, Error),
    retry: fn(JobId, Int) -> Result(Job, Error),
    renew_lease: fn(ExecutionToken, Int) -> Result(Job, Error),
    close: fn() -> Result(Nil, Error),
  )
}

pub fn get(store: Store, id: JobId) -> Result(Job, Error) {
  store.get(id)
}

pub fn cancel(store: Store, id: JobId) -> Result(Job, Error) {
  store.cancel(id)
}

pub fn retry(store: Store, id: JobId, now: Int) -> Result(Job, Error) {
  store.retry(id, now)
}

/// Releases only resources created by this adapter. Borrowed resources (such
/// as a Pog pool passed to quasar_postgres.new) are never stopped. Quasar never
/// invokes close: the application calls it after all runtimes and commands
/// using this Store have finished.
pub fn close(store: Store) -> Result(Nil, Error) {
  store.close()
}

pub fn insert(
  store: Store,
  new_job: NewJob,
  queue: String,
  available_at: Int,
  now: Int,
) -> Result(JobId, Error) {
  case job.validate(new_job) {
    Error(error) -> Error(InvalidTransition(error))
    Ok(Nil) -> store.insert(new_job, queue, available_at, now)
  }
}

/// The owner argument is a diagnostic prefix. Each call adds a fresh nonce;
/// adapters must preserve it and atomically match owner AND attempt for every
/// execution mutation. This also fences attempts after a manual retry reset.
pub fn claim(
  store: Store,
  queue: String,
  limit: Int,
  owner: String,
  now: Int,
  lease_ms: Int,
) -> Result(List(Job), Error) {
  case limit > 0 && lease_ms > 0 {
    False ->
      Error(InvalidTransition(job.InvalidJob("claim limits must be positive")))
    True ->
      store.claim(queue, limit, owner <> ":" <> unique_id(), now, lease_ms)
  }
}

pub fn complete(
  store: Store,
  token: ExecutionToken,
  now: Int,
) -> Result(Job, Error) {
  store.complete(token, now)
}

pub fn fail(
  store: Store,
  token: ExecutionToken,
  error: JobError,
  available_at: Int,
) -> Result(Job, Error) {
  store.fail(token, error, available_at)
}

pub fn renew_lease(
  store: Store,
  token: ExecutionToken,
  expires_at: Int,
) -> Result(Job, Error) {
  store.renew_lease(token, expires_at)
}

pub fn from_operations(
  insert insert: fn(NewJob, String, Int, Int) -> Result(JobId, Error),
  get get: fn(JobId) -> Result(Job, Error),
  claim claim: fn(String, Int, String, Int, Int) -> Result(List(Job), Error),
  complete complete: fn(ExecutionToken, Int) -> Result(Job, Error),
  fail fail: fn(ExecutionToken, JobError, Int) -> Result(Job, Error),
  cancel cancel: fn(JobId) -> Result(Job, Error),
  retry retry: fn(JobId, Int) -> Result(Job, Error),
  renew_lease renew_lease: fn(ExecutionToken, Int) -> Result(Job, Error),
  close close: fn() -> Result(Nil, Error),
) -> Store {
  Store(
    insert:,
    get:,
    claim:,
    complete:,
    fail:,
    cancel:,
    retry:,
    renew_lease:,
    close:,
  )
}

@external(erlang, "quasar_jobs_ffi", "unique_id")
fn unique_id() -> String
