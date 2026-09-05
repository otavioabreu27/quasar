//// Owns process lifetimes and resolves capabilities. Never performs Store I/O.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision.{type ChildSpecification}
import gleam/result
import gleam/string
import quasar_jobs/error.{
  type ConfigError, type ExecuteError, type StartError, InvalidConfig,
  PoolNotFound, RuntimeStartFailed, ShuttingDown, Unavailable,
}
import quasar_jobs/event
import quasar_jobs/internal/config
import quasar_jobs/internal/durable
import quasar_jobs/internal/local
import quasar_jobs/internal/reporter.{type Reporter}
import quasar_jobs/store.{type Store}

pub opaque type Runtime {
  Runtime(subject: Subject(Message), timeout: Int)
}

pub type DurableAccess {
  DurableAccess(
    store: Store,
    queues: Dict(String, durable.QueueRuntime),
    configs: List(durable.QueueConfig),
    reporter: Reporter,
  )
}

type State {
  State(
    subject: Subject(Message),
    pools: Dict(String, local.Pool),
    queues: Dict(String, durable.QueueRuntime),
    configs: List(durable.QueueConfig),
    store: Option(Store),
    reporter: Reporter,
    stopping: Bool,
    waiter: Option(Subject(Result(Nil, ExecuteError))),
  )
}

type Message {
  GetPool(String, Subject(Result(local.Executor, ExecuteError)))
  GetDurable(Subject(Result(DurableAccess, ExecuteError)))
  Stop(Subject(Result(Nil, ExecuteError)))
  Drained(Result(Nil, ExecuteError))
}

pub fn start(
  pools,
  queues,
  store,
  timeout,
  report,
) -> Result(Runtime, StartError) {
  use _ <- result.try(
    config.validate(pools, queues, store, timeout)
    |> result.map_error(InvalidConfig),
  )
  start_link(pools, queues, store, timeout, report, process.new_name("quasar"))
  |> result.map(fn(started) {
    process.unlink(started.pid)
    started.data
  })
  |> result.map_error(RuntimeStartFailed)
}

pub fn supervised(
  pools,
  queues,
  store,
  timeout,
  report,
) -> Result(ChildSpecification(Runtime), ConfigError) {
  use _ <- result.try(config.validate(pools, queues, store, timeout))
  // Allocated once when the child specification is built, reused on every restart.
  let name = process.new_name("quasar")
  Ok(
    supervision.worker(fn() {
      start_link(pools, queues, store, timeout, report, name)
    })
    |> supervision.timeout(ms: timeout),
  )
}

fn start_link(pools, queues, store, timeout, report, name) {
  actor.new_with_initialiser(timeout, fn(public_subject) {
    // Drain completion belongs to this incarnation, never its replacement.
    let subject = process.new_subject()
    use reporter <- result.try(
      reporter.start(report) |> result.map_error(string.inspect),
    )
    use started_pools <- result.try(start_pools(
      pools,
      timeout,
      reporter,
      dict.new(),
    ))
    use started_queues <- result.try(start_queues(
      queues,
      store,
      timeout,
      reporter,
      dict.new(),
    ))
    Ok(
      actor.initialised(State(
        subject,
        started_pools,
        started_queues,
        queues,
        store,
        reporter,
        False,
        None,
      ))
      |> actor.selecting(
        process.new_selector()
        |> process.select(public_subject)
        |> process.select(subject),
      )
      |> actor.returning(Runtime(process.named_subject(name), timeout)),
    )
  })
  |> actor.named(name)
  |> actor.on_message(handle)
  |> actor.start
}

pub fn get_pool(
  runtime: Runtime,
  name: String,
) -> Result(local.Executor, ExecuteError) {
  request(runtime, fn(reply) { GetPool(name, reply) })
}

pub fn durable_access(runtime: Runtime) -> Result(DurableAccess, ExecuteError) {
  request(runtime, GetDurable)
}

pub fn stop(runtime: Runtime) -> Result(Nil, ExecuteError) {
  request(runtime, Stop)
}

fn request(runtime: Runtime, make_message) {
  case process.subject_owner(runtime.subject) {
    Error(_) -> Error(Unavailable)
    Ok(_) -> {
      let reply = process.new_subject()
      process.send(runtime.subject, make_message(reply))
      case process.receive(reply, within: runtime.timeout) {
        Ok(value) -> value
        Error(_) -> Error(Unavailable)
      }
    }
  }
}

fn handle(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    GetPool(_, reply) if state.stopping -> {
      process.send(reply, Error(ShuttingDown))
      actor.continue(state)
    }
    GetPool(name, reply) -> {
      process.send(
        reply,
        dict.get(state.pools, name)
          |> result.map(fn(pool) { local.Executor(pool, state.reporter) })
          |> result.map_error(fn(_) { PoolNotFound }),
      )
      actor.continue(state)
    }
    GetDurable(reply) -> {
      let access = case state.stopping, state.store {
        True, _ -> Error(ShuttingDown)
        _, None -> Error(Unavailable)
        False, Some(store) ->
          Ok(DurableAccess(store, state.queues, state.configs, state.reporter))
      }
      process.send(reply, access)
      actor.continue(state)
    }
    Stop(reply) if state.stopping -> {
      process.send(reply, Error(ShuttingDown))
      actor.continue(state)
    }
    Stop(reply) -> {
      reporter.emit(state.reporter, event.RuntimeStopping)
      process.spawn(fn() { process.send(state.subject, Drained(drain(state))) })
      actor.continue(State(..state, stopping: True, waiter: Some(reply)))
    }
    Drained(outcome) -> {
      case outcome {
        Ok(Nil) -> reporter.emit(state.reporter, event.RuntimeStopped)
        Error(_) -> Nil
      }
      case state.waiter {
        Some(reply) -> process.send(reply, outcome)
        None -> Nil
      }
      reporter.stop(state.reporter)
      case outcome {
        Ok(Nil) -> actor.stop()
        Error(_) -> actor.stop_abnormal("runtime drain failed")
      }
    }
  }
}

fn drain(state: State) -> Result(Nil, ExecuteError) {
  let queues = list.map(dict.values(state.queues), durable.stop)
  let pools = list.map(dict.values(state.pools), local.stop)
  list.try_each(list.append(queues, pools), fn(outcome) { outcome })
}

fn start_pools(configs, timeout, reporter, started) {
  case configs {
    [] -> Ok(started)
    [config, ..rest] -> {
      use pool <- result.try(
        local.start(config, timeout, reporter)
        |> result.map_error(string.inspect),
      )
      start_pools(
        rest,
        timeout,
        reporter,
        dict.insert(started, config.name, pool),
      )
    }
  }
}

fn start_queues(configs, store, timeout, reporter, started) {
  case configs, store {
    [], _ -> Ok(started)
    [config, ..rest], Some(store) -> {
      use queue <- result.try(
        durable.start(config, store, timeout, fn(event) {
          reporter.emit(reporter, event)
        })
        |> result.map_error(string.inspect),
      )
      start_queues(
        rest,
        Some(store),
        timeout,
        reporter,
        dict.insert(started, durable.name(config), queue),
      )
    }
    _, None -> Error("durable queues require a Store")
  }
}
