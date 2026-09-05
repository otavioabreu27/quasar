//// Quasar configuration and application facade.

import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/supervision.{type ChildSpecification}
import gleam/result
import quasar_jobs/error
import quasar_jobs/event
import quasar_jobs/internal/durable
import quasar_jobs/internal/jobs
import quasar_jobs/internal/local
import quasar_jobs/internal/runtime as runtime_internal
import quasar_jobs/job
import quasar_jobs/request_id
import quasar_jobs/store
import quasar_jobs/worker

pub type ExecuteError =
  error.ExecuteError

pub type ConfigError =
  error.ConfigError

pub type StartError =
  error.StartError

pub type JobOperationError =
  error.JobError

pub type Event =
  event.Event

pub type RequestId =
  request_id.RequestId

pub opaque type Runtime {
  Runtime(runtime_internal.Runtime)
}

pub opaque type Config {
  Config(
    pools: List(local.PoolConfig),
    queues: List(durable.QueueConfig),
    store: Option(store.Store),
    reporter: fn(Event) -> Nil,
    poll_interval_ms: Int,
    shutdown_timeout: Int,
  )
}

/// Creates an empty Quasar runtime configuration.
pub fn new() -> Config {
  Config(
    pools: [],
    queues: [],
    store: None,
    reporter: fn(_) { Nil },
    poll_interval_ms: 1000,
    shutdown_timeout: 5000,
  )
}

/// Sets the maximum time synchronous shutdown operations may take.
pub fn with_shutdown_timeout(config: Config, milliseconds: Int) -> Config {
  Config(..config, shutdown_timeout: milliseconds)
}

/// Enables durable jobs using a Store implementation.
pub fn with_store(config: Config, durable_store: store.Store) -> Config {
  Config(..config, store: Some(durable_store))
}

/// Sets the durable queue polling interval used as a recovery fallback.
///
/// PostgreSQL adapters can wake queues immediately through LISTEN/NOTIFY, while
/// this interval guarantees eventual progress if a notification is lost.
pub fn with_poll_interval(config: Config, milliseconds: Int) -> Config {
  Config(
    ..config,
    poll_interval_ms: milliseconds,
    queues: list.map(config.queues, fn(queue) {
      durable.with_poll_interval(queue, milliseconds)
    }),
  )
}

/// Adds a named durable queue backed by a typed worker definition.
pub fn queue(
  config: Config,
  name name: String,
  worker durable_worker: worker.Worker(input),
  concurrency concurrency: Int,
) -> Config {
  queue_with_prefetch(config, name, durable_worker, concurrency, 1)
}

/// Adds a durable queue with an explicit amount of prefetched work.
///
/// Prefer `queue`, whose prefetch of one avoids leasing work before a worker is
/// ready. Values above one trade additional leased work for throughput.
pub fn queue_with_prefetch(
  config: Config,
  name name: String,
  worker durable_worker: worker.Worker(input),
  concurrency concurrency: Int,
  prefetch prefetch: Int,
) -> Config {
  Config(
    ..config,
    queues: list.append(config.queues, [
      durable.new_config(
        name,
        worker.erase(durable_worker),
        concurrency,
        prefetch,
      )
      |> durable.with_poll_interval(config.poll_interval_ms),
    ]),
  )
}

/// Adds a named ephemeral local pool.
///
/// `prefetch` defaults conceptually to 1 for synchronous calls; callers that
/// choose a larger value allow one worker to reserve multiple tasks at once.
pub fn local_pool(
  config: Config,
  name name: String,
  workers workers: Int,
  prefetch prefetch: Int,
  buffer_capacity buffer_capacity: Int,
) -> Config {
  Config(
    ..config,
    pools: list.append(config.pools, [
      local.PoolConfig(name, workers, prefetch, buffer_capacity),
    ]),
  )
}

/// Installs the runtime event reporter.
///
/// Events are delivered from a dedicated process, so a slow reporter never
/// blocks Constellation's Stage or worker-pool processes.
pub fn with_reporter(config: Config, reporter: fn(Event) -> Nil) -> Config {
  Config(..config, reporter:)
}

/// Starts the configured runtime and all local pools.
pub fn start(config: Config) -> Result(Runtime, StartError) {
  runtime_internal.start(
    config.pools,
    config.queues,
    config.store,
    config.shutdown_timeout,
    config.reporter,
  )
  |> result.map(Runtime)
}

/// Creates an OTP child specification for a Quasar runtime.
///
/// The runtime owns linked local pools, durable schedulers, source-backed
/// pools, and its asynchronous reporter. A fatal child exit therefore takes
/// down the runtime owner so the application supervisor can restart the unit.
pub fn supervised(
  config: Config,
) -> Result(ChildSpecification(Runtime), ConfigError) {
  runtime_internal.supervised(
    config.pools,
    config.queues,
    config.store,
    config.shutdown_timeout,
    config.reporter,
  )
  |> result.map(fn(child) { supervision.map_data(child, Runtime) })
}

/// Stops accepting work and drains pools. Ok confirms draining succeeded;
/// an error may return before draining finishes. Never closes the Store.
pub fn stop(runtime: Runtime) -> Result(Nil, ExecuteError) {
  let Runtime(runtime) = runtime
  runtime_internal.stop(runtime)
}

/// Executes a typed ephemeral call on a named pool.
///
/// A deadline error means the handler may still complete. Quasar never retries
/// local calls automatically.
pub fn call(
  runtime: Runtime,
  on pool: String,
  timeout timeout: Int,
  run run: fn() -> output,
) -> Result(output, ExecuteError) {
  let Runtime(runtime) = runtime
  use executor <- result.try(runtime_internal.get_pool(runtime, pool))
  local.call(executor, pool, request_id.new(), timeout, run)
}

/// Submits ephemeral fire-and-forget work with at-most-once semantics.
pub fn cast(
  runtime: Runtime,
  on pool: String,
  run run: fn() -> Nil,
) -> Result(Nil, ExecuteError) {
  let Runtime(runtime) = runtime
  use executor <- result.try(runtime_internal.get_pool(runtime, pool))
  local.cast(executor, pool, run)
}

/// Returns the printable representation of a request identity.
pub fn request_id_to_string(id: RequestId) -> String {
  request_id.to_string(id)
}

/// Inserts a durable job for immediate execution.
pub fn enqueue(
  new_job: job.NewJob,
  runtime: Runtime,
  on queue: String,
) -> Result(job.JobId, JobOperationError) {
  let Runtime(runtime) = runtime
  jobs.enqueue(runtime, new_job, queue, jobs.now())
}

/// Inserts a durable job that becomes available at an epoch millisecond.
pub fn schedule(
  new_job: job.NewJob,
  runtime: Runtime,
  on queue: String,
  at available_at: Int,
) -> Result(job.JobId, JobOperationError) {
  let Runtime(runtime) = runtime
  jobs.enqueue(runtime, new_job, queue, available_at)
}

/// Fetches the latest durable state for a job.
pub fn get_job(
  runtime: Runtime,
  id: job.JobId,
) -> Result(job.Job, JobOperationError) {
  let Runtime(runtime) = runtime
  jobs.get(runtime, id)
}

/// Cancels a non-terminal durable job.
pub fn cancel_job(
  runtime: Runtime,
  id: job.JobId,
) -> Result(job.Job, JobOperationError) {
  let Runtime(runtime) = runtime
  jobs.cancel(runtime, id)
}

/// Makes a discarded, cancelled, or retryable job immediately available.
pub fn retry_job(
  runtime: Runtime,
  id: job.JobId,
) -> Result(job.Job, JobOperationError) {
  let Runtime(runtime) = runtime
  jobs.retry(runtime, id)
}

/// Wakes one durable queue in this runtime without changing durable state.
///
/// This is intended for store adapters that receive an external availability
/// signal. The store remains the source of truth and the queue still polls.
pub fn wake(
  runtime: Runtime,
  on queue: String,
) -> Result(Nil, JobOperationError) {
  let Runtime(runtime) = runtime
  jobs.wake(runtime, queue)
}

/// Executes a call with an existing correlation identity (for HTTP adapters).
pub fn call_with_id(
  runtime: Runtime,
  on pool: String,
  id id: RequestId,
  timeout timeout: Int,
  run run: fn() -> output,
) -> Result(output, ExecuteError) {
  let Runtime(runtime) = runtime
  use executor <- result.try(runtime_internal.get_pool(runtime, pool))
  local.call(executor, pool, id, timeout, run)
}
