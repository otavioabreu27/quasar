//// Demand-driven durable queue integration.

import constellation/source.{type DemandGrant, type Source}
import constellation/worker_pool
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import quasar_jobs/error
import quasar_jobs/event.{
  type Event, JobClaimed, QueueClaimCompleted, QueueClaimFailed,
}
import quasar_jobs/internal/job_executor
import quasar_jobs/job.{type Job}
import quasar_jobs/store.{type Store}
import quasar_jobs/worker.{type Definition}

pub opaque type QueueConfig {
  QueueConfig(
    name: String,
    worker: Definition,
    concurrency: Int,
    prefetch: Int,
    lease_ms: Int,
    poll_interval_ms: Int,
  )
}

pub type StartError {
  SchedulerStart(actor.StartError)
  PoolStart(worker_pool.StartError)
}

pub opaque type QueueRuntime {
  QueueRuntime(
    scheduler: Subject(Message),
    pool: worker_pool.Pool(Job, Nil),
    timeout: Int,
  )
}

type PendingGrant {
  PendingGrant(grant: DemandGrant, offset: Int, remaining: Int)
}

type State {
  State(
    subject: Subject(Message),
    config: QueueConfig,
    store: Store,
    source: Option(Source(Job)),
    pending: List(PendingGrant),
    report: fn(Event) -> Nil,
    store_available: Bool,
    stopping: Bool,
  )
}

type Message {
  AttachSource(Source(Job))
  SourceEvent(source.Event)
  Wake
  Tick
  StopScheduler(Subject(Nil))
}

pub fn new_config(
  name: String,
  worker: Definition,
  concurrency: Int,
  prefetch: Int,
) -> QueueConfig {
  QueueConfig(
    name:,
    worker:,
    concurrency:,
    prefetch:,
    lease_ms: 30_000,
    poll_interval_ms: 1000,
  )
}

pub fn name(config: QueueConfig) -> String {
  config.name
}

pub fn worker_name(config: QueueConfig) -> String {
  worker.definition_name(config.worker)
}

pub fn with_poll_interval(
  config: QueueConfig,
  milliseconds: Int,
) -> QueueConfig {
  QueueConfig(..config, poll_interval_ms: milliseconds)
}

pub fn valid(config: QueueConfig) -> Bool {
  config.name != ""
  && config.concurrency > 0
  && config.prefetch > 0
  && config.lease_ms > 0
  && config.poll_interval_ms > 0
}

pub fn start(
  config: QueueConfig,
  store: Store,
  shutdown_timeout: Int,
  report: fn(Event) -> Nil,
) -> Result(QueueRuntime, StartError) {
  let scheduler_builder =
    actor.new_with_initialiser(1000, fn(subject) {
      Ok(
        actor.initialised(State(
          subject:,
          config:,
          store:,
          source: None,
          pending: [],
          report:,
          store_available: True,
          stopping: False,
        ))
        |> actor.returning(subject),
      )
    })
    |> actor.on_message(handle_message)
  use scheduler <- result.try(
    actor.start(scheduler_builder)
    |> result.map_error(SchedulerStart),
  )
  let pool_config =
    worker_pool.each(
      size: config.concurrency,
      prefetch: config.prefetch,
      initial_state: fn(_) { Nil },
      handle_event: fn(state, claimed_job) {
        job_executor.execute(
          config.name,
          config.worker,
          config.lease_ms,
          store,
          claimed_job,
          report,
        )
        state
      },
    )
    |> worker_pool.with_timeout(shutdown_timeout)
  case
    worker_pool.start_with_source_link(pool_config, fn(event) {
      process.send(scheduler.data, SourceEvent(event))
    })
  {
    Error(error) -> {
      process.send(scheduler.data, StopScheduler(process.new_subject()))
      Error(PoolStart(error))
    }
    Ok(#(pool, attached_source)) -> {
      process.send(scheduler.data, AttachSource(attached_source))
      process.send_after(scheduler.data, config.poll_interval_ms, Tick)
      Ok(QueueRuntime(scheduler.data, pool, shutdown_timeout))
    }
  }
}

pub fn wake(runtime: QueueRuntime) -> Nil {
  process.send(runtime.scheduler, Wake)
}

pub fn stop(runtime: QueueRuntime) {
  let reply = process.new_subject()
  process.send(runtime.scheduler, StopScheduler(reply))
  let stopped = process.receive(reply, within: runtime.timeout)
  let drained = worker_pool.stop(runtime.pool)
  case stopped, drained {
    Ok(_), Ok(_) -> Ok(Nil)
    _, _ -> Error(error.Unavailable)
  }
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    AttachSource(attached_source) ->
      actor.continue(process_pending(
        State(..state, source: Some(attached_source)),
      ))
    SourceEvent(source.DemandGranted(grant)) if !state.stopping -> {
      let pending =
        list.append(state.pending, [
          PendingGrant(grant, 0, source.grant_amount(grant)),
        ])
      actor.continue(process_pending(State(..state, pending:)))
    }
    SourceEvent(source.GrantRevoked(grant)) ->
      actor.continue(
        State(
          ..state,
          pending: list.filter(state.pending, fn(item) { item.grant != grant }),
        ),
      )
    SourceEvent(source.SourceStopped) ->
      actor.continue(State(..state, stopping: True, pending: []))
    SourceEvent(source.DemandGranted(_)) -> actor.continue(state)
    Wake if !state.stopping -> actor.continue(process_pending(state))
    Tick if !state.stopping -> {
      let state = case state.source, state.store_available {
        Some(attached_source), False -> {
          source.available(attached_source)
          State(..state, store_available: True)
        }
        _, _ -> state
      }
      process.send_after(state.subject, state.config.poll_interval_ms, Tick)
      actor.continue(process_pending(state))
    }
    Wake | Tick -> actor.continue(state)
    StopScheduler(reply) -> {
      case state.source {
        Some(attached) -> source.unavailable(attached)
        None -> Nil
      }
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

fn process_pending(state: State) -> State {
  case state.source {
    None -> state
    Some(attached_source) ->
      case supply_pending(state, attached_source, state.pending, []) {
        Ok(pending) -> State(..state, pending:, store_available: True)
        Error(reason) -> {
          state.report(QueueClaimFailed(state.config.name, reason))
          source.unavailable(attached_source)
          State(..state, pending: [], store_available: False)
        }
      }
  }
}

fn supply_pending(
  state: State,
  attached_source: Source(Job),
  pending: List(PendingGrant),
  kept: List(PendingGrant),
) -> Result(List(PendingGrant), store.Error) {
  case pending {
    [] -> Ok(list.reverse(kept))
    [grant, ..rest] -> {
      let claim_started = monotonic_milliseconds()
      use jobs <- result.try(store.claim(
        state.store,
        state.config.name,
        grant.remaining,
        state.config.name,
        system_milliseconds(),
        state.config.lease_ms,
      ))
      state.report(QueueClaimCompleted(
        state.config.name,
        grant.remaining,
        list.length(jobs),
        monotonic_milliseconds() - claim_started,
      ))
      list.each(jobs, fn(item) {
        state.report(JobClaimed(
          job.id(item),
          state.config.name,
          job.attempt(item),
        ))
      })
      case jobs {
        [] -> supply_pending(state, attached_source, rest, [grant, ..kept])
        _ ->
          case source.supply(attached_source, grant.grant, grant.offset, jobs) {
            Ok(source.Accepted(next_offset, remaining)) ->
              supply_pending(state, attached_source, rest, case remaining > 0 {
                True -> [
                  PendingGrant(grant.grant, next_offset, remaining),
                  ..kept
                ]
                False -> kept
              })
            Ok(source.Duplicate) | Ok(source.StaleGrant) ->
              supply_pending(state, attached_source, rest, kept)
            Error(_) -> supply_pending(state, attached_source, rest, kept)
          }
      }
    }
  }
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int
