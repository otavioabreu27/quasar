import gleam/erlang/process
import gleeunit
import quasar_jobs as quasar
import quasar_jobs/error

pub fn main() -> Nil {
  gleeunit.main()
}

fn runtime() {
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.local_pool(
      name: "default",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 1,
    )
    |> quasar.start
  runtime
}

pub fn call_preserves_result_type_test() {
  let runtime = runtime()
  assert quasar.call(runtime, on: "default", timeout: 1000, run: fn() { 42 })
    == Ok(42)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn missing_pool_is_typed_test() {
  let runtime = runtime()
  assert quasar.call(runtime, on: "missing", timeout: 1000, run: fn() { 42 })
    == Error(error.PoolNotFound)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn runtime_exposes_an_otp_child_specification_test() {
  let result =
    quasar.new()
    |> quasar.local_pool(
      name: "default",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 1,
    )
    |> quasar.supervised
  let assert Ok(_) = result
}

pub fn shutdown_timeout_is_validated_test() {
  let result =
    quasar.new()
    |> quasar.with_shutdown_timeout(0)
    |> quasar.start
  assert result == Error(error.InvalidConfig(error.InvalidShutdownTimeout(0)))
}

pub fn handler_panic_is_captured_and_worker_remains_healthy_test() {
  let runtime = runtime()
  assert quasar.call(runtime, on: "default", timeout: 1000, run: fn() {
      panic as "expected handler failure"
    })
    == Error(error.HandlerFailed)
  assert quasar.call(runtime, on: "default", timeout: 1000, run: fn() { 42 })
    == Ok(42)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn deadline_may_complete_later_test() {
  let runtime = runtime()
  let completed = process.new_subject()
  assert quasar.call(runtime, on: "default", timeout: 10, run: fn() {
      process.sleep(50)
      process.send(completed, Nil)
    })
    == Error(error.DeadlineExceededMayComplete)
  assert process.receive(completed, within: 1000) == Ok(Nil)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn overflow_is_rejected_test() {
  let runtime = runtime()
  let started = process.new_subject()
  assert quasar.cast(runtime, on: "default", run: fn() {
      process.send(started, Nil)
      process.sleep(100)
      Nil
    })
    == Ok(Nil)
  assert process.receive(started, within: 1000) == Ok(Nil)
  assert quasar.cast(runtime, on: "default", run: fn() { Nil }) == Ok(Nil)
  assert quasar.cast(runtime, on: "default", run: fn() { Nil })
    == Error(error.Overloaded)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn one_worker_does_not_renew_demand_before_handler_returns_test() {
  let runtime = runtime()
  let first_started = process.new_subject()
  let second_started = process.new_subject()

  assert quasar.cast(runtime, on: "default", run: fn() {
      let gate = process.new_subject()
      process.send(first_started, gate)
      let _ = process.receive(gate, within: 1000)
      Nil
    })
    == Ok(Nil)
  let assert Ok(gate) = process.receive(first_started, within: 1000)
  assert quasar.cast(runtime, on: "default", run: fn() {
      process.send(second_started, Nil)
    })
    == Ok(Nil)

  assert process.receive(second_started, within: 20) == Error(Nil)
  process.send(gate, Nil)
  assert process.receive(second_started, within: 1000) == Ok(Nil)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn local_buffer_is_fifo_test() {
  let runtime = runtime()
  let first_started = process.new_subject()
  let order = process.new_subject()

  assert quasar.cast(runtime, on: "default", run: fn() {
      let gate = process.new_subject()
      process.send(first_started, gate)
      let _ = process.receive(gate, within: 1000)
      Nil
    })
    == Ok(Nil)
  let assert Ok(gate) = process.receive(first_started, within: 1000)
  assert quasar.cast(runtime, on: "default", run: fn() {
      process.send(order, 1)
    })
    == Ok(Nil)
  // The pool has room for only one buffered task, so this also proves that
  // FIFO is tested without an unbounded auxiliary queue in Quasar.
  assert quasar.cast(runtime, on: "default", run: fn() {
      process.send(order, 2)
    })
    == Error(error.Overloaded)

  process.send(gate, Nil)
  assert process.receive(order, within: 1000) == Ok(1)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn named_pools_are_isolated_test() {
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.local_pool(
      name: "blocked",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 1,
    )
    |> quasar.local_pool(
      name: "free",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 1,
    )
    |> quasar.start
  let started = process.new_subject()
  assert quasar.cast(runtime, on: "blocked", run: fn() {
      let gate = process.new_subject()
      process.send(started, gate)
      let _ = process.receive(gate, within: 1000)
      Nil
    })
    == Ok(Nil)
  let assert Ok(gate) = process.receive(started, within: 1000)

  assert quasar.call(runtime, on: "free", timeout: 1000, run: fn() { 42 })
    == Ok(42)
  process.send(gate, Nil)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn shutdown_drains_an_accepted_local_task_test() {
  let runtime = runtime()
  let finished = process.new_subject()
  assert quasar.cast(runtime, on: "default", run: fn() {
      process.sleep(50)
      process.send(finished, Nil)
    })
    == Ok(Nil)

  assert quasar.stop(runtime) == Ok(Nil)
  assert process.receive(finished, within: 100) == Ok(Nil)
  assert quasar.cast(runtime, on: "default", run: fn() { Nil })
    == Error(error.Unavailable)
}

pub fn slow_reporter_does_not_delay_handler_test() {
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.local_pool(
      name: "default",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 1,
    )
    |> quasar.with_reporter(fn(_) {
      process.sleep(100)
      Nil
    })
    |> quasar.start

  let started_at = monotonic_milliseconds()
  assert quasar.call(runtime, on: "default", timeout: 1000, run: fn() { 42 })
    == Ok(42)
  assert monotonic_milliseconds() - started_at < 100
  assert quasar.stop(runtime) == Ok(Nil)
}

@external(erlang, "quasar_jobs_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int
