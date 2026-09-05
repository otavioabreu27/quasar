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
  HeartbeatState(subject: Subject(HeartbeatMessage))
}

pub fn stop(subject: Subject(HeartbeatMessage)) {
  process.send(subject, StopHeartbeat)
}

pub fn start(
  store: Store,
  token: job.ExecutionToken,
  queue: String,
  lease_ms: Int,
  report: fn(Event) -> Nil,
) -> Result(Subject(HeartbeatMessage), actor.StartError) {
  let assert Ok(interval_base) = int.divide(lease_ms, 3)
  let interval = int.max(1, interval_base)
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      process.send_after(subject, interval, Heartbeat)
      Ok(actor.initialised(HeartbeatState(subject)) |> actor.returning(subject))
    })
    |> actor.on_message(fn(state, message) {
      case message {
        StopHeartbeat -> actor.stop()
        Heartbeat -> {
          let expires_at = system_milliseconds() + lease_ms
          case store.renew_lease(store, token, expires_at) {
            Ok(_) ->
              report(LeaseRenewed(job.token_id(token), queue, expires_at))
            Error(reason) ->
              report(JobPersistenceFailed(
                job.token_id(token),
                queue,
                "renew_lease",
                reason,
              ))
          }
          process.send_after(state.subject, interval, Heartbeat)
          actor.continue(state)
        }
      }
    })
  actor.start(builder) |> result.map(fn(started) { started.data })
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int
