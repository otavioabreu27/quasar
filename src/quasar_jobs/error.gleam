//// Canonical errors shared by the facade and implementations.

import gleam/otp/actor
import quasar_jobs/store

pub type ExecuteError {
  Overloaded
  PoolNotFound
  Unavailable
  DeadlineExceededMayComplete
  HandlerFailed
  ShuttingDown
}

pub type ConfigError {
  DuplicatePool(String)
  EmptyPoolName
  InvalidWorkers(pool: String, workers: Int)
  InvalidPrefetch(pool: String, prefetch: Int)
  InvalidBufferCapacity(pool: String, capacity: Int)
  DuplicateQueue(String)
  InvalidQueue(String)
  StoreRequired
  InvalidShutdownTimeout(Int)
}

pub type StartError {
  InvalidConfig(ConfigError)
  RuntimeStartFailed(actor.StartError)
}

pub type JobError {
  QueueNotFound
  WorkerQueueMismatch
  StoreFailure(store.Error)
  JobRuntimeStopping
  RuntimeUnavailable
}
