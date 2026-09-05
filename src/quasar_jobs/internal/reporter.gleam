//// Asynchronous reporting isolated from execution processes.

import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/result
import quasar_jobs/event.{type Event}

pub type Message {
  Report(Event)
  Stop
}

pub type Reporter =
  Subject(Message)

pub fn start(report: fn(Event) -> Nil) {
  actor.new(Nil)
  |> actor.on_message(fn(state, message) {
    case message {
      Report(event) -> {
        let _ = safely(fn() { report(event) })
        actor.continue(state)
      }
      Stop -> actor.stop()
    }
  })
  |> actor.start
  |> result.map(fn(started) { started.data })
}

pub fn emit(reporter: Reporter, event: Event) {
  process.send(reporter, Report(event))
}

pub fn stop(reporter: Reporter) {
  process.send(reporter, Stop)
}

@external(erlang, "quasar_jobs_ffi", "run_safely")
fn safely(run: fn() -> output) -> Result(output, Nil)
