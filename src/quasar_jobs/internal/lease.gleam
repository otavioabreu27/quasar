//// Heartbeat for one fenced execution.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/otp/actor
import gleam/result
import quasar_jobs/event.{type Event, JobPersistenceFailed, LeaseRenewed}
import quasar_jobs/job
import quasar_jobs/store.{type Store}

pub type HeartbeatMessage {
  Heartbeat
  StopHeartbeat
}

type HeartbeatState {
  HeartbeatState(
    subject: Subject(HeartbeatMessage),
    interval: Int,
    store: Store,
    token: job.ExecutionToken,
    queue: String,
    lease_ms: Int,
    report: fn(Event) -> Nil,
  )
}

pub fn stop(subject: Subject(HeartbeatMessage)) {
  process.send(subject, StopHeartbeat)
}

pub fn start(
  store: Store,
  token: job.ExecutionToken,
  queue: String,
  lease_ms: Int,
  lease_expires_at: Int,
  report: fn(Event) -> Nil,
  extended: fn(Int) -> Nil,
) -> Result(Subject(HeartbeatMessage), actor.StartError) {
  let owner = process.self()
  let assert Ok(interval_base) = int.divide(lease_ms, 3)
  let margin = int.max(1, interval_base)
  let interval = int.max(1, lease_ms - margin)
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      let first = int.max(1, lease_expires_at - system_milliseconds() - margin)
      process.send_after(subject, first, Heartbeat)
      Ok(
        actor.initialised(HeartbeatState(
          subject,
          interval,
          store,
          token,
          queue,
          lease_ms,
          report,
        ))
        |> actor.returning(subject),
      )
    })
    |> actor.on_message(fn(state, message) {
      case message {
        StopHeartbeat -> actor.stop()
        Heartbeat -> {
          let expires_at = system_milliseconds() + state.lease_ms
          case store.renew_lease(state.store, state.token, expires_at) {
            Ok(_) -> {
              extended(expires_at)
              state.report(LeaseRenewed(
                job.token_id(state.token),
                state.queue,
                expires_at,
              ))
              process.send_after(state.subject, state.interval, Heartbeat)
              actor.continue(state)
            }
            Error(reason) -> {
              state.report(JobPersistenceFailed(
                job.token_id(state.token),
                state.queue,
                "renew_lease",
                reason,
              ))
              // A fenced heartbeat cannot recover ownership. Stop writing for
              // this execution; its final acknowledgement will also be fenced.
              process.kill(owner)
              actor.stop()
            }
          }
        }
      }
    })
  actor.start(builder) |> result.map(fn(started) { started.data })
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int
