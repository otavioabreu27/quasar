//// Durable job values and their state machine.

import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/result

pub opaque type JobId {
  JobId(Int)
}

pub type Status {
  Available
  Scheduled
  Executing
  Completed
  Retryable
  Discarded
  Cancelled
}

pub type JobError {
  JobError(kind: String, message: String)
}

pub opaque type NewJob {
  NewJob(worker: String, payload: String, priority: Int, max_attempts: Int)
}

pub opaque type Job {
  Job(
    id: JobId,
    queue: String,
    worker: String,
    payload: String,
    status: Status,
    priority: Int,
    attempt: Int,
    max_attempts: Int,
    available_at: Int,
    inserted_at: Int,
    attempted_at: Option(Int),
    completed_at: Option(Int),
    lease_owner: Option(String),
    lease_expires_at: Option(Int),
    error: Option(JobError),
  )
}

pub type TransitionError {
  InvalidTransition(from: Status, operation: String)
  InvalidJob(String)
  StaleExecution
}

/// Capability for one claim. The owner nonce changes even across manual retries.
/// Generation is the attempt number; both fields must match atomically.
pub opaque type ExecutionToken {
  ExecutionToken(id: JobId, owner: String, generation: Int)
}

pub fn execution_token(item: Job) -> Result(ExecutionToken, TransitionError) {
  case item.status, item.lease_owner {
    Executing, Some(owner) -> Ok(ExecutionToken(item.id, owner, item.attempt))
    _, _ -> Error(StaleExecution)
  }
}

pub fn token_id(token: ExecutionToken) -> JobId {
  token.id
}

pub fn token_owner(token: ExecutionToken) -> String {
  token.owner
}

pub fn token_generation(token: ExecutionToken) -> Int {
  token.generation
}

pub fn owns(item: Job, token: ExecutionToken) -> Bool {
  item.id == token.id
  && item.status == Executing
  && item.lease_owner == Some(token.owner)
  && item.attempt == token.generation
}

pub fn validate(new_job: NewJob) -> Result(Nil, TransitionError) {
  case new_job.worker == "", new_job.max_attempts <= 0 {
    True, _ -> Error(InvalidJob("worker name must not be empty"))
    _, True -> Error(InvalidJob("max_attempts must be positive"))
    _, _ -> Ok(Nil)
  }
}

/// Changes the priority used when a store claims ready jobs.
pub fn with_priority(new_job: NewJob, priority: Int) -> NewJob {
  NewJob(..new_job, priority:)
}

/// Changes the maximum number of execution attempts.
pub fn with_max_attempts(new_job: NewJob, max_attempts: Int) -> NewJob {
  NewJob(..new_job, max_attempts:)
}

pub fn id(job: Job) -> JobId {
  job.id
}

pub fn queue(job: Job) -> String {
  job.queue
}

pub fn worker(job: Job) -> String {
  job.worker
}

pub fn payload(job: Job) -> String {
  job.payload
}

pub fn status(job: Job) -> Status {
  job.status
}

pub fn priority(job: Job) -> Int {
  job.priority
}

pub fn attempt(job: Job) -> Int {
  job.attempt
}

pub fn max_attempts(job: Job) -> Int {
  job.max_attempts
}

pub fn available_at(job: Job) -> Int {
  job.available_at
}

pub fn inserted_at(job: Job) -> Int {
  job.inserted_at
}

pub fn attempted_at(job: Job) -> Option(Int) {
  job.attempted_at
}

pub fn completed_at(job: Job) -> Option(Int) {
  job.completed_at
}

pub fn lease_owner(job: Job) -> Option(String) {
  job.lease_owner
}

pub fn lease_expires_at(job: Job) -> Option(Int) {
  job.lease_expires_at
}

pub fn error(job: Job) -> Option(JobError) {
  job.error
}

pub fn error_kind(error: JobError) -> String {
  error.kind
}

pub fn error_message(error: JobError) -> String {
  error.message
}

pub fn set_lease_expiry(job: Job, expires_at: Int) -> Job {
  Job(..job, lease_expires_at: Some(expires_at))
}

pub fn id_to_string(id: JobId) -> String {
  let JobId(value) = id
  int.to_string(value)
}

pub fn new_job(
  worker: String,
  payload: String,
  priority: Int,
  max_attempts: Int,
) -> NewJob {
  NewJob(worker:, payload:, priority:, max_attempts:)
}

pub fn worker_name(new_job: NewJob) -> String {
  new_job.worker
}

pub fn encoded_payload(new_job: NewJob) -> String {
  new_job.payload
}

pub fn new_job_priority(new_job: NewJob) -> Int {
  new_job.priority
}

pub fn new_job_max_attempts(new_job: NewJob) -> Int {
  new_job.max_attempts
}

pub fn restore(
  id: JobId,
  queue: String,
  worker: String,
  payload: String,
  status: Status,
  priority: Int,
  attempt: Int,
  max_attempts: Int,
  available_at: Int,
  inserted_at: Int,
  attempted_at: Option(Int),
  completed_at: Option(Int),
  lease_owner: Option(String),
  lease_expires_at: Option(Int),
  error_kind: Option(String),
  error_message: Option(String),
) -> Job {
  Job(
    id:,
    queue:,
    worker:,
    payload:,
    status:,
    priority:,
    attempt:,
    max_attempts:,
    available_at:,
    inserted_at:,
    attempted_at:,
    completed_at:,
    lease_owner:,
    lease_expires_at:,
    error: case error_kind, error_message {
      Some(kind), Some(message) -> Some(JobError(kind, message))
      _, _ -> None
    },
  )
}

pub fn status_to_string(status: Status) -> String {
  case status {
    Available -> "available"
    Scheduled -> "scheduled"
    Executing -> "executing"
    Completed -> "completed"
    Retryable -> "retryable"
    Discarded -> "discarded"
    Cancelled -> "cancelled"
  }
}

pub fn status_from_string(status: String) -> Result(Status, String) {
  case status {
    "scheduled" -> Ok(Scheduled)
    "executing" -> Ok(Executing)
    "completed" -> Ok(Completed)
    "retryable" -> Ok(Retryable)
    "discarded" -> Ok(Discarded)
    "cancelled" -> Ok(Cancelled)
    "available" -> Ok(Available)
    _ -> Error("unknown job status: " <> status)
  }
}

pub fn materialise(
  new_job: NewJob,
  id: JobId,
  queue: String,
  available_at: Int,
  now: Int,
) -> Result(Job, TransitionError) {
  use _ <- result.try(validate(new_job))
  Ok(Job(
    id:,
    queue:,
    worker: new_job.worker,
    payload: new_job.payload,
    status: case available_at > now {
      True -> Scheduled
      False -> Available
    },
    priority: new_job.priority,
    attempt: 0,
    max_attempts: new_job.max_attempts,
    available_at:,
    inserted_at: now,
    attempted_at: None,
    completed_at: None,
    lease_owner: None,
    lease_expires_at: None,
    error: None,
  ))
}

pub fn new_id(value: Int) -> JobId {
  JobId(value)
}

pub fn id_value(id: JobId) -> Int {
  let JobId(value) = id
  value
}

pub fn ready(job: Job, queue: String, now: Int) -> Bool {
  job.queue == queue
  && job.available_at <= now
  && case job.status {
    Available | Scheduled | Retryable -> True
    _ -> False
  }
}

pub fn claim(
  job: Job,
  owner: String,
  now: Int,
  lease_expires_at: Int,
) -> Result(Job, TransitionError) {
  case job.status {
    Available | Scheduled | Retryable ->
      Ok(
        Job(
          ..job,
          status: Executing,
          attempt: job.attempt + 1,
          attempted_at: Some(now),
          lease_owner: Some(owner),
          lease_expires_at: Some(lease_expires_at),
        ),
      )
    status -> Error(InvalidTransition(status, "claim"))
  }
}

pub fn complete(job: Job, now: Int) -> Result(Job, TransitionError) {
  case job.status {
    Executing ->
      Ok(
        Job(
          ..job,
          status: Completed,
          completed_at: Some(now),
          lease_owner: None,
          lease_expires_at: None,
          error: None,
        ),
      )
    status -> Error(InvalidTransition(status, "complete"))
  }
}

pub fn fail(
  job: Job,
  error: JobError,
  available_at: Int,
) -> Result(Job, TransitionError) {
  case job.status {
    Executing ->
      Ok(
        Job(
          ..job,
          status: case job.attempt >= job.max_attempts {
            True -> Discarded
            False -> Retryable
          },
          available_at:,
          lease_owner: None,
          lease_expires_at: None,
          error: Some(error),
        ),
      )
    status -> Error(InvalidTransition(status, "fail"))
  }
}

pub fn cancel(job: Job) -> Result(Job, TransitionError) {
  case job.status {
    Completed | Discarded | Cancelled ->
      Error(InvalidTransition(job.status, "cancel"))
    _ ->
      Ok(
        Job(..job, status: Cancelled, lease_owner: None, lease_expires_at: None),
      )
  }
}

pub fn retry(job: Job, now: Int) -> Result(Job, TransitionError) {
  case job.status {
    Discarded | Cancelled | Retryable ->
      Ok(
        Job(
          ..job,
          status: Available,
          attempt: 0,
          available_at: now,
          attempted_at: None,
          completed_at: None,
          lease_owner: None,
          lease_expires_at: None,
          error: None,
        ),
      )
    status -> Error(InvalidTransition(status, "retry"))
  }
}

pub fn recover_if_expired(job: Job, now: Int) -> Job {
  case job.status, job.lease_expires_at {
    Executing, Some(expires_at) if expires_at <= now ->
      Job(
        ..job,
        status: case job.attempt >= job.max_attempts {
          True -> Discarded
          False -> Retryable
        },
        available_at: now,
        lease_owner: None,
        lease_expires_at: None,
        error: Some(JobError("lease_expired", "execution lease expired")),
      )
    _, _ -> job
  }
}
