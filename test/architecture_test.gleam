import gleam/erlang/process
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import quasar_jobs as quasar
import quasar_jobs/error
import quasar_jobs/event
import quasar_jobs/internal/job_executor
import quasar_jobs/job
import quasar_jobs/request_id
import quasar_jobs/store
import quasar_jobs/store/memory
import quasar_jobs/worker

fn config() {
  quasar.new()
  |> quasar.local_pool(
    name: "local",
    workers: 1,
    prefetch: 1,
    buffer_capacity: 1,
  )
}

pub fn old_shutdown_completion_cannot_stop_replacement_runtime_test() {
  let starts = process.new_subject()
  let claiming = process.new_subject()
  let stopping = process.new_subject()
  let stop_result = process.new_subject()
  let assert Ok(backing) = memory.new()
  let blocked =
    store.from_operations(
      insert: fn(item, queue, at, now) {
        store.insert(backing, item, queue, at, now)
      },
      get: store.get(backing, _),
      claim: fn(queue, limit, owner, now, lease) {
        let gate = process.new_subject()
        process.send(claiming, gate)
        let _ = process.receive(gate, within: 2000)
        store.claim(backing, queue, limit, owner, now, lease)
      },
      complete: fn(token, now) { store.complete(backing, token, now) },
      fail: fn(token, reason, at) { store.fail(backing, token, reason, at) },
      cancel: store.cancel(backing, _),
      retry: fn(id, now) { store.retry(backing, id, now) },
      renew_lease: fn(token, expiry) {
        store.renew_lease(backing, token, expiry)
      },
      close: fn() { store.close(backing) },
    )
  let worker =
    worker.new("worker", fn(x) { x }, fn(x) { Ok(x) }, fn(_, _) { Ok(Nil) })
  let assert Ok(child) =
    config()
    |> quasar.with_store(blocked)
    |> quasar.queue("queue", worker, 1, 1)
    |> quasar.with_shutdown_timeout(200)
    |> quasar.with_reporter(fn(message) {
      case message {
        event.RuntimeStopping -> process.send(stopping, Nil)
        _ -> Nil
      }
    })
    |> quasar.supervised
  let watched =
    supervision.ChildSpecification(..child, start: fn() {
      child.start()
      |> result.map(fn(started) {
        process.send(starts, started)
        started
      })
    })
  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(watched)
    |> static_supervisor.start
  process.unlink(supervisor.pid)
  let assert Ok(first) = process.receive(starts, within: 1000)
  let assert Ok(_) = process.receive(claiming, within: 1000)
  process.spawn(fn() { process.send(stop_result, quasar.stop(first.data)) })
  assert process.receive(stopping, within: 1000) == Ok(Nil)
  process.kill(first.pid)
  let assert Ok(second) = process.receive(starts, within: 1000)
  assert first.pid != second.pid
  let assert Ok(_) = process.receive(claiming, within: 1000)
  let assert Ok(Error(_)) = process.receive(stop_result, within: 1000)
  // The old scheduler stop times out at 200 ms. Its delayed Drained message
  // must target the dead incarnation, not this replacement's public name.
  process.sleep(350)
  assert process.receive(starts, within: 0) == Error(Nil)
  assert quasar.call(first.data, on: "local", timeout: 1000, run: fn() { 42 })
    == Ok(42)
  process.kill(supervisor.pid)
  assert store.close(backing) == Ok(Nil)
}

pub fn stale_prefetched_claim_does_not_invoke_user_handler_test() {
  let assert Ok(database) = memory.new()
  let assert Ok(id) =
    store.insert(database, job.new_job("worker", "p", 0, 3), "queue", 100, 100)
  let assert Ok([stale]) = store.claim(database, "queue", 1, "node", 100, 10)
  let assert Ok([fresh]) = store.claim(database, "queue", 1, "node", 111, 10)
  let performed = process.new_subject()
  let reports = process.new_subject()
  let worker =
    worker.new("worker", fn(x) { x }, fn(x) { Ok(x) }, fn(_, _) {
      process.send(performed, Nil)
      Ok(Nil)
    })
  job_executor.execute(
    "queue",
    worker.erase(worker),
    30_000,
    database,
    stale,
    fn(message) { process.send(reports, message) },
  )
  assert process.receive(reports, within: 1000)
    == Ok(event.JobPersistenceFailed(id, "queue", "begin", store.StaleExecution))
  assert process.receive(performed, within: 0) == Error(Nil)
  let assert Ok(current) = store.get(database, id)
  let assert Ok(token) = job.execution_token(fresh)
  assert job.owns(current, token)
  assert store.close(database) == Ok(Nil)
}

pub fn fresh_claim_does_not_write_an_eager_lease_renewal_test() {
  let assert Ok(database) = memory.new()
  let now = system_milliseconds()
  let assert Ok(_) =
    store.insert(database, job.new_job("worker", "p", 0, 3), "queue", now, now)
  let assert Ok([fresh]) =
    store.claim(database, "queue", 1, "node", now, 30_000)
  let renewals = process.new_subject()
  let performed = process.new_subject()
  let observed =
    store.from_operations(
      insert: fn(item, queue, at, now) {
        store.insert(database, item, queue, at, now)
      },
      get: store.get(database, _),
      claim: fn(queue, limit, owner, now, lease) {
        store.claim(database, queue, limit, owner, now, lease)
      },
      complete: fn(token, now) { store.complete(database, token, now) },
      fail: fn(token, reason, at) { store.fail(database, token, reason, at) },
      cancel: store.cancel(database, _),
      retry: fn(id, now) { store.retry(database, id, now) },
      renew_lease: fn(token, expiry) {
        process.send(renewals, Nil)
        store.renew_lease(database, token, expiry)
      },
      close: fn() { Ok(Nil) },
    )
  let worker =
    worker.new("worker", fn(x) { x }, fn(x) { Ok(x) }, fn(_, _) {
      process.send(performed, Nil)
      Ok(Nil)
    })
  job_executor.execute(
    "queue",
    worker.erase(worker),
    30_000,
    observed,
    fresh,
    fn(_) { Nil },
  )
  assert process.receive(performed, within: 1000) == Ok(Nil)
  assert process.receive(renewals, within: 0) == Error(Nil)
  assert store.close(database) == Ok(Nil)
}

pub fn blocking_store_does_not_block_local_execution_or_shutdown_test() {
  let entered = process.new_subject()
  let done = process.new_subject()
  let assert Ok(backing) = memory.new()
  let blocked =
    store.from_operations(
      insert: fn(item, queue, at, now) {
        store.insert(backing, item, queue, at, now)
      },
      get: fn(id) {
        let gate = process.new_subject()
        process.send(entered, gate)
        let assert Ok(Nil) = process.receive(gate, within: 2000)
        store.get(backing, id)
      },
      claim: fn(queue, limit, owner, now, lease) {
        store.claim(backing, queue, limit, owner, now, lease)
      },
      complete: fn(token, now) { store.complete(backing, token, now) },
      fail: fn(token, reason, at) { store.fail(backing, token, reason, at) },
      cancel: store.cancel(backing, _),
      retry: fn(id, now) { store.retry(backing, id, now) },
      renew_lease: fn(token, expiry) {
        store.renew_lease(backing, token, expiry)
      },
      close: fn() { store.close(backing) },
    )
  let assert Ok(runtime) =
    config() |> quasar.with_store(blocked) |> quasar.start
  process.spawn(fn() {
    process.send(done, quasar.get_job(runtime, job.new_id(-1)))
  })
  let assert Ok(gate) = process.receive(entered, within: 1000)
  assert quasar.call(runtime, on: "local", timeout: 500, run: fn() { 42 })
    == Ok(42)
  assert quasar.stop(runtime) == Ok(Nil)
  process.send(gate, Nil)
  assert process.receive(done, within: 1000)
    == Ok(Error(error.StoreFailure(store.NotFound)))
  assert store.close(backing) == Ok(Nil)
}

pub fn original_handle_resolves_restarted_supervised_runtime_test() {
  let starts = process.new_subject()
  let assert Ok(child) = config() |> quasar.supervised
  let watched =
    supervision.ChildSpecification(..child, start: fn() {
      child.start()
      |> result.map(fn(started) {
        process.send(starts, started)
        started
      })
    })
  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(watched)
    |> static_supervisor.start
  process.unlink(supervisor.pid)
  let assert Ok(first) = process.receive(starts, within: 1000)
  let handle = first.data
  assert quasar.call(handle, on: "local", timeout: 1000, run: fn() { 1 })
    == Ok(1)
  process.kill(first.pid)
  let assert Ok(second) = process.receive(starts, within: 2000)
  assert first.pid != second.pid
  assert quasar.call(handle, on: "local", timeout: 1000, run: fn() { 2 })
    == Ok(2)
  process.kill(supervisor.pid)
}

pub fn stopping_runtime_rejects_calls_while_accepted_work_drains_test() {
  let stopping = process.new_subject()
  let started = process.new_subject()
  let stopped = process.new_subject()
  let assert Ok(runtime) =
    config()
    |> quasar.with_reporter(fn(message) {
      case message {
        event.RuntimeStopping -> process.send(stopping, Nil)
        _ -> Nil
      }
    })
    |> quasar.start
  assert quasar.cast(runtime, on: "local", run: fn() {
      let gate = process.new_subject()
      process.send(started, gate)
      let assert Ok(Nil) = process.receive(gate, within: 2000)
      Nil
    })
    == Ok(Nil)
  let assert Ok(gate) = process.receive(started, within: 1000)
  process.spawn(fn() { process.send(stopped, quasar.stop(runtime)) })
  assert process.receive(stopping, within: 1000) == Ok(Nil)
  assert quasar.call(runtime, on: "local", timeout: 500, run: fn() { 42 })
    == Error(error.ShuttingDown)
  process.send(gate, Nil)
  assert process.receive(stopped, within: 2000) == Ok(Ok(Nil))
}

pub fn explicit_queue_routes_a_shared_worker_and_rejects_mismatch_test() {
  let assert Ok(database) = memory.new()
  let worker =
    worker.new("worker", fn(x) { x }, fn(x) { Ok(x) }, fn(_, _) { Ok(Nil) })
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(database)
    |> quasar.queue("first", worker, 1, 1)
    |> quasar.queue("second", worker, 1, 1)
    |> quasar.start
  let assert Ok(id) =
    quasar.schedule(
      worker.job(worker, "p"),
      runtime,
      on: "second",
      at: 9_999_999_999_999,
    )
  let assert Ok(item) = quasar.get_job(runtime, id)
  assert job.queue(item) == "second"
  assert quasar.enqueue(worker.job(worker, "p"), runtime, on: "missing")
    == Error(error.QueueNotFound)
  assert quasar.enqueue(job.new_job("other", "p", 0, 1), runtime, on: "first")
    == Error(error.WorkerQueueMismatch)
  assert quasar.stop(runtime) == Ok(Nil)
  assert store.close(database) == Ok(Nil)
}

pub fn caller_request_id_reaches_telemetry_test() {
  let ids = process.new_subject()
  let assert Ok(runtime) =
    config()
    |> quasar.with_reporter(fn(message) {
      case message {
        event.RequestCompleted(id, _, _, _) -> process.send(ids, id)
        _ -> Nil
      }
    })
    |> quasar.start
  let id = request_id.from_string("http-request-123")
  assert quasar.call_with_id(
      runtime,
      on: "local",
      id: id,
      timeout: 1000,
      run: fn() { 42 },
    )
    == Ok(42)
  assert process.receive(ids, within: 1000) == Ok(id)
  assert quasar.stop(runtime) == Ok(Nil)
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int
