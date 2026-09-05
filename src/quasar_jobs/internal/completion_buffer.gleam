//// Short-lived batching of durable execution results.

import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import quasar_jobs/event.{
  type Event, JobCompleted, JobCompletionPersisted, JobDiscarded,
  JobPersistenceFailed, JobRetryScheduled,
}
import quasar_jobs/job.{type Job}
import quasar_jobs/store.{type Store}

const max_batch_size = 100

const flush_interval_ms = 5

pub opaque type Buffer {
  Buffer(subject: Subject(Message))
}

pub type Result {
  Complete(Job)
  Fail(Job, String, Int)
}

type State {
  State(
    subject: Subject(Message),
    store: Store,
    pending: List(Result),
    count: Int,
    flush_scheduled: Bool,
    report: fn(Event) -> Nil,
  )
}

type Message {
  Add(Result)
  Flush
  Stop(Subject(Nil))
}

pub fn start(store: Store, report: fn(Event) -> Nil) {
  actor.new_with_initialiser(1000, fn(subject) {
    Ok(
      actor.initialised(State(subject, store, [], 0, False, report))
      |> actor.returning(Buffer(subject)),
    )
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn submit(buffer: Buffer, result: Result) -> Nil {
  process.send(buffer.subject, Add(result))
}

pub fn stop(buffer: Buffer) -> Nil {
  let reply = process.new_subject()
  process.send(buffer.subject, Stop(reply))
  let _ = process.receive(reply, within: 5000)
  Nil
}

fn handle_message(state: State, message: Message) {
  case message {
    Add(result) -> {
      let next =
        State(
          ..state,
          pending: [result, ..state.pending],
          count: state.count + 1,
        )
      case next.count >= max_batch_size {
        True -> actor.continue(flush(next))
        False -> actor.continue(schedule_flush(next))
      }
    }
    Flush -> actor.continue(flush(state))
    Stop(reply) -> {
      let _ = flush(state)
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

fn schedule_flush(state: State) -> State {
  case state.flush_scheduled {
    True -> state
    False -> {
      process.send_after(state.subject, flush_interval_ms, Flush)
      State(..state, flush_scheduled: True)
    }
  }
}

fn flush(state: State) -> State {
  case state.pending {
    [] -> State(..state, flush_scheduled: False)
    pending -> {
      let pending = list.reverse(pending)
      persist_completions(state.store, pending, state.report)
      persist_failures(state.store, pending, state.report)
      State(..state, pending: [], count: 0, flush_scheduled: False)
    }
  }
}

fn persist_completions(store: Store, pending: List(Result), report) -> Nil {
  let jobs =
    list.filter_map(pending, fn(item) {
      case item {
        Complete(job) -> Ok(job)
        Fail(_, _, _) -> Error(Nil)
      }
    })
  case jobs {
    [] -> Nil
    jobs -> {
      let started = monotonic_milliseconds()
      let completions =
        list.map(jobs, fn(item) {
          let assert Ok(token) = job.execution_token(item)
          store.Completion(token, system_milliseconds())
        })
      case store.complete_many(store, completions) {
        Ok(_) ->
          list.each(jobs, fn(item) {
            report(JobCompletionPersisted(
              job.id(item),
              job.queue(item),
              monotonic_milliseconds() - started,
            ))
            report(JobCompleted(job.id(item), job.queue(item)))
          })
        Error(reason) ->
          list.each(jobs, fn(item) {
            report(JobPersistenceFailed(
              job.id(item),
              job.queue(item),
              "complete_many",
              reason,
            ))
          })
      }
    }
  }
}

fn persist_failures(store: Store, pending: List(Result), report) -> Nil {
  let failures =
    list.filter_map(pending, fn(item) {
      case item {
        Complete(_) -> Error(Nil)
        Fail(job, message, available_at) -> Ok(#(job, message, available_at))
      }
    })
  case failures {
    [] -> Nil
    failures -> {
      let operations =
        list.map(failures, fn(item) {
          let #(claimed, message, available_at) = item
          let assert Ok(token) = job.execution_token(claimed)
          store.Failure(
            token,
            job.JobError("execution_failed", message),
            available_at,
          )
        })
      case store.fail_many(store, operations) {
        Ok(updated) ->
          list.each(updated, fn(item) {
            case job.status(item) {
              job.Discarded ->
                report(JobDiscarded(job.id(item), job.queue(item)))
              _ ->
                report(JobRetryScheduled(
                  job.id(item),
                  job.queue(item),
                  job.available_at(item),
                ))
            }
          })
        Error(reason) ->
          list.each(failures, fn(item) {
            let #(claimed, _, _) = item
            report(JobPersistenceFailed(
              job.id(claimed),
              job.queue(claimed),
              "fail_many",
              reason,
            ))
          })
      }
    }
  }
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int
