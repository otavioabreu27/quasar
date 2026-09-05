//// In-memory implementation of the Store contract.
////
//// `memory` is intended for tests. SQLite and PostgreSQL adapters can preserve
//// this contract while providing single-node and clustered durability.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/order.{type Order}
import gleam/otp/actor
import gleam/result
import quasar_jobs/job.{
  type ExecutionToken, type Job, type JobError, type JobId, type NewJob,
}
import quasar_jobs/store.{
  type Error, type Store, InvalidTransition, NotFound, StaleExecution, Timeout,
  Unavailable,
}

type State {
  State(jobs: List(Job))
}

type Message {
  Insert(
    NewJob,
    queue: String,
    available_at: Int,
    now: Int,
    reply: Subject(Result(JobId, Error)),
  )
  Fetch(JobId, Subject(Result(Job, Error)))
  Claim(
    queue: String,
    limit: Int,
    owner: String,
    now: Int,
    lease_ms: Int,
    reply: Subject(Result(List(Job), Error)),
  )
  Complete(ExecutionToken, now: Int, reply: Subject(Result(Job, Error)))
  Fail(
    ExecutionToken,
    JobError,
    available_at: Int,
    reply: Subject(Result(Job, Error)),
  )
  Cancel(JobId, Subject(Result(Job, Error)))
  Retry(JobId, now: Int, reply: Subject(Result(Job, Error)))
  RenewLease(
    ExecutionToken,
    expires_at: Int,
    reply: Subject(Result(Job, Error)),
  )
  Close(Subject(Result(Nil, Error)))
}

const default_timeout = 5000

/// Starts a deterministic in-memory Store for tests and local development.
pub fn new() -> Result(Store, Error) {
  let builder = actor.new(State(jobs: [])) |> actor.on_message(handle_message)
  case actor.start(builder) {
    Error(_) -> Error(Unavailable)
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(from_subject(started.data, default_timeout))
    }
  }
}

fn from_subject(subject: Subject(Message), timeout: Int) -> Store {
  store.from_operations(
    insert: fn(new_job, queue, available_at, now) {
      call_subject(subject, timeout, fn(reply) {
        Insert(new_job, queue, available_at, now, reply)
      })
    },
    get: fn(id) {
      call_subject(subject, timeout, fn(reply) { Fetch(id, reply) })
    },
    claim: fn(queue, limit, owner, now, lease_ms) {
      call_subject(subject, timeout, fn(reply) {
        Claim(queue, limit, owner, now, lease_ms, reply)
      })
    },
    complete: fn(id, now) {
      call_subject(subject, timeout, fn(reply) { Complete(id, now, reply) })
    },
    fail: fn(id, error, available_at) {
      call_subject(subject, timeout, fn(reply) {
        Fail(id, error, available_at, reply)
      })
    },
    cancel: fn(id) {
      call_subject(subject, timeout, fn(reply) { Cancel(id, reply) })
    },
    retry: fn(id, now) {
      call_subject(subject, timeout, fn(reply) { Retry(id, now, reply) })
    },
    renew_lease: fn(id, expires_at) {
      call_subject(subject, timeout, fn(reply) {
        RenewLease(id, expires_at, reply)
      })
    },
    close: fn() { call_subject(subject, timeout, Close) },
  )
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    Insert(new_job, queue, available_at, now, reply) -> {
      let id = job.new_id(next_id())
      case job.materialise(new_job, id, queue, available_at, now) {
        Error(error) -> {
          process.send(reply, Error(InvalidTransition(error)))
          actor.continue(state)
        }
        Ok(stored) -> {
          process.send(reply, Ok(id))
          actor.continue(State(jobs: list.append(state.jobs, [stored])))
        }
      }
    }
    Fetch(id, reply) -> {
      process.send(reply, find_job(state.jobs, id))
      actor.continue(state)
    }
    Claim(queue, limit, owner, now, lease_ms, reply) -> {
      let recovered =
        list.map(state.jobs, fn(item) { job.recover_if_expired(item, now) })
      let selected =
        recovered
        |> list.filter(fn(item) { job.ready(item, queue, now) })
        |> list.sort(compare_jobs)
        |> list.take(int.max(0, limit))
      case
        list.try_map(selected, fn(item) {
          job.claim(item, owner, now, now + lease_ms)
        })
      {
        Error(error) -> {
          process.send(reply, Error(InvalidTransition(error)))
          actor.continue(State(jobs: recovered))
        }
        Ok(claimed) -> {
          process.send(reply, Ok(claimed))
          actor.continue(State(jobs: replace_many(recovered, claimed)))
        }
      }
    }
    Complete(id, now, reply) ->
      fenced_transition(state, id, reply, fn(item) { job.complete(item, now) })
    Fail(id, error, available_at, reply) ->
      fenced_transition(state, id, reply, fn(item) {
        job.fail(item, error, available_at)
      })
    Cancel(id, reply) -> transition(state, id, reply, job.cancel)
    Retry(id, now, reply) ->
      transition(state, id, reply, fn(item) { job.retry(item, now) })
    RenewLease(token, expires_at, reply) ->
      fenced_transition(state, token, reply, fn(item) {
        Ok(job.set_lease_expiry(item, expires_at))
      })
    Close(reply) -> {
      process.send(reply, Ok(Nil))
      actor.stop()
    }
  }
}

fn transition(
  state: State,
  id: JobId,
  reply: Subject(Result(Job, Error)),
  apply: fn(Job) -> Result(Job, job.TransitionError),
) -> actor.Next(State, Message) {
  case find_job(state.jobs, id) {
    Error(error) -> {
      process.send(reply, Error(error))
      actor.continue(state)
    }
    Ok(item) ->
      case apply(item) {
        Error(error) -> {
          process.send(reply, Error(InvalidTransition(error)))
          actor.continue(state)
        }
        Ok(updated) -> {
          process.send(reply, Ok(updated))
          actor.continue(State(jobs: replace(state.jobs, updated)))
        }
      }
  }
}

fn find_job(jobs: List(Job), id: JobId) -> Result(Job, Error) {
  list.find(jobs, fn(item) { job.id(item) == id })
  |> result.map_error(fn(_) { NotFound })
}

fn replace(jobs: List(Job), updated: Job) -> List(Job) {
  list.map(jobs, fn(item) {
    case job.id(item) == job.id(updated) {
      True -> updated
      False -> item
    }
  })
}

fn replace_many(jobs: List(Job), updates: List(Job)) -> List(Job) {
  list.fold(updates, jobs, fn(current, updated) { replace(current, updated) })
}

fn compare_jobs(left: Job, right: Job) -> Order {
  case int.compare(job.priority(right), job.priority(left)) {
    order.Eq ->
      int.compare(job.id_value(job.id(left)), job.id_value(job.id(right)))
    ordering -> ordering
  }
}

fn call_subject(
  subject: Subject(Message),
  timeout: Int,
  make_message: fn(Subject(Result(value, Error))) -> Message,
) -> Result(value, Error) {
  let reply = process.new_subject()
  process.send(subject, make_message(reply))
  case process.receive(reply, within: timeout) {
    Ok(result) -> result
    Error(_) -> Error(Timeout)
  }
}

@external(erlang, "quasar_jobs_ffi", "next_id")
fn next_id() -> Int

fn fenced_transition(state: State, token: ExecutionToken, reply, apply) {
  case find_job(state.jobs, job.token_id(token)) {
    Ok(item) ->
      case job.owns(item, token) {
        True -> transition(state, job.token_id(token), reply, apply)
        False -> {
          process.send(reply, Error(StaleExecution))
          actor.continue(state)
        }
      }
    Error(_) -> {
      process.send(reply, Error(StaleExecution))
      actor.continue(state)
    }
  }
}
