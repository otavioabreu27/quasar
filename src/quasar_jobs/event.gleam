//// Canonical runtime telemetry. Reporters receive these events directly.

import quasar_jobs/error.{type ExecuteError}
import quasar_jobs/job.{type JobId}
import quasar_jobs/request_id.{type RequestId}
import quasar_jobs/store

pub type Event {
  RequestQueued(RequestId, pool: String, at: Int)
  RequestStarted(RequestId, pool: String, wait_duration: Int)
  RequestCompleted(
    RequestId,
    pool: String,
    handler_duration: Int,
    total_duration: Int,
  )
  RequestRejected(RequestId, pool: String, reason: ExecuteError)
  RequestTimedOut(RequestId, pool: String)
  RequestFailed(RequestId, pool: String)
  WorkerStarted(pool: String, worker_id: Int)
  WorkerReplaced(pool: String, previous: Int, replacement: Int)
  RuntimeStopping
  RuntimeStopped
  JobInserted(JobId, queue: String)
  QueueClaimCompleted(
    queue: String,
    requested: Int,
    returned: Int,
    duration_ms: Int,
  )
  JobClaimed(JobId, queue: String, attempt: Int)
  JobStarted(JobId, queue: String, attempt: Int)
  JobCompleted(JobId, queue: String)
  JobCompletionPersisted(JobId, queue: String, duration_ms: Int)
  JobRetryScheduled(JobId, queue: String, available_at: Int)
  JobDiscarded(JobId, queue: String)
  LeaseRenewed(JobId, queue: String, expires_at: Int)
  JobPersistenceFailed(
    JobId,
    queue: String,
    operation: String,
    reason: store.Error,
  )
  QueueClaimFailed(queue: String, reason: store.Error)
}
