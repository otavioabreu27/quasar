//// Store port for durable state. Implementations provide atomic transitions.
//// Resource ownership remains with the application that opened the Store.

import gleam/list
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

pub type Completion {
  Completion(token: ExecutionToken, completed_at: Int)
}

pub type Failure {
  Failure(token: ExecutionToken, error: JobError, available_at: Int)
}

pub opaque type Store {
  Store(
    insert: fn(NewJob, String, Int, Int) -> Result(JobId, Error),
    get: fn(JobId) -> Result(Job, Error),
    claim: fn(String, Int, String, Int, Int) -> Result(List(Job), Error),
    complete: fn(ExecutionToken, Int) -> Result(Job, Error),
    complete_many: fn(List(Completion)) -> Result(List(Job), Error),
    fail: fn(ExecutionToken, JobError, Int) -> Result(Job, Error),
    fail_many: fn(List(Failure)) -> Result(List(Job), Error),
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

pub fn complete_many(
  store: Store,
  completions: List(Completion),
) -> Result(List(Job), Error) {
  store.complete_many(completions)
}

pub fn fail(
  store: Store,
  token: ExecutionToken,
  error: JobError,
  available_at: Int,
) -> Result(Job, Error) {
  store.fail(token, error, available_at)
}

pub fn fail_many(
  store: Store,
  failures: List(Failure),
) -> Result(List(Job), Error) {
  store.fail_many(failures)
}

pub fn completion_token(completion: Completion) -> ExecutionToken {
  completion.token
}

pub fn completion_time(completion: Completion) -> Int {
  completion.completed_at
}

pub fn failure_token(failure: Failure) -> ExecutionToken {
  failure.token
}

pub fn failure_error(failure: Failure) -> JobError {
  failure.error
}

pub fn failure_available_at(failure: Failure) -> Int {
  failure.available_at
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
  from_operations_with_batch(
    insert:,
    get:,
    claim:,
    complete:,
    complete_many: fn(items) {
      list.try_map(items, fn(item) { complete(item.token, item.completed_at) })
    },
    fail:,
    fail_many: fn(items) {
      list.try_map(items, fn(item) {
        fail(item.token, item.error, item.available_at)
      })
    },
    cancel:,
    retry:,
    renew_lease:,
    close:,
  )
}

pub fn from_operations_with_batch(
  insert insert: fn(NewJob, String, Int, Int) -> Result(JobId, Error),
  get get: fn(JobId) -> Result(Job, Error),
  claim claim: fn(String, Int, String, Int, Int) -> Result(List(Job), Error),
  complete complete: fn(ExecutionToken, Int) -> Result(Job, Error),
  complete_many complete_many: fn(List(Completion)) -> Result(List(Job), Error),
  fail fail: fn(ExecutionToken, JobError, Int) -> Result(Job, Error),
  fail_many fail_many: fn(List(Failure)) -> Result(List(Job), Error),
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
    complete_many:,
    fail:,
    fail_many:,
    cancel:,
    retry:,
    renew_lease:,
    close:,
  )
}

@external(erlang, "quasar_jobs_ffi", "unique_id")
fn unique_id() -> String
