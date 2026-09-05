import gleam/erlang/process
import gleam/int
import gleam/result
import quasar_jobs as quasar
import quasar_jobs/error
import quasar_jobs/job
import quasar_jobs/store
import quasar_jobs/store/memory
import quasar_jobs/worker

fn int_worker(perform) {
  worker.new(
    name: "int-worker",
    encode: int.to_string,
    decode: fn(payload) {
      int.parse(payload) |> result.map_error(fn(_) { "invalid integer" })
    },
    perform:,
  )
}

fn durable_runtime(durable_worker) {
  let assert Ok(memory) = memory.new()
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(memory)
    |> quasar.queue(
      name: "jobs",
      worker: durable_worker,
      concurrency: 2,
      prefetch: 2,
    )
    |> quasar.start
  #(runtime, memory)
}

pub fn enqueue_runs_a_typed_job_to_completion_test() {
  let completed = process.new_subject()
  let durable_worker =
    int_worker(fn(value, context) {
      process.send(completed, #(value, context.attempt))
      Ok(Nil)
    })
  let #(runtime, memory) = durable_runtime(durable_worker)
  let assert Ok(id) =
    worker.job(durable_worker, 42) |> quasar.enqueue(runtime, on: "jobs")

  assert process.receive(completed, within: 1000) == Ok(#(42, 1))
  let assert Ok(stored) = wait_for_status(runtime, id, job.Completed, 100)
  assert job.attempt(stored) == 1
  assert quasar.stop(runtime) == Ok(Nil)
  assert store.close(memory) == Ok(Nil)
}

pub fn failed_job_retries_with_backoff_test() {
  let attempts = process.new_subject()
  let durable_worker =
    int_worker(fn(_, context) {
      process.send(attempts, context.attempt)
      case context.attempt {
        1 -> Error("temporary")
        _ -> Ok(Nil)
      }
    })
  let #(runtime, memory) = durable_runtime(durable_worker)
  let assert Ok(id) =
    worker.job(durable_worker, 1) |> quasar.enqueue(runtime, on: "jobs")

  assert process.receive(attempts, within: 1000) == Ok(1)
  assert process.receive(attempts, within: 2500) == Ok(2)
  let assert Ok(stored) = wait_for_status(runtime, id, job.Completed, 100)
  assert job.attempt(stored) == 2
  assert quasar.stop(runtime) == Ok(Nil)
  assert store.close(memory) == Ok(Nil)
}

pub fn exhausted_job_is_discarded_test() {
  let durable_worker = int_worker(fn(_, _) { Error("permanent") })
  let #(runtime, memory) = durable_runtime(durable_worker)
  let new_job =
    worker.job(durable_worker, 1)
    |> job.with_max_attempts(1)
  let assert Ok(id) = quasar.enqueue(new_job, runtime, on: "jobs")

  let assert Ok(stored) = wait_for_status(runtime, id, job.Discarded, 100)
  assert job.attempt(stored) == 1
  assert quasar.stop(runtime) == Ok(Nil)
  assert store.close(memory) == Ok(Nil)
}

pub fn scheduled_job_waits_until_available_test() {
  let completed = process.new_subject()
  let durable_worker =
    int_worker(fn(_, _) {
      process.send(completed, Nil)
      Ok(Nil)
    })
  let #(runtime, memory) = durable_runtime(durable_worker)
  let available_at = system_milliseconds() + 150
  let assert Ok(id) =
    quasar.schedule(
      worker.job(durable_worker, 1),
      runtime,
      on: "jobs",
      at: available_at,
    )

  assert process.receive(completed, within: 50) == Error(Nil)
  assert process.receive(completed, within: 1500) == Ok(Nil)
  let assert Ok(_) = wait_for_status(runtime, id, job.Completed, 100)
  assert quasar.stop(runtime) == Ok(Nil)
  assert store.close(memory) == Ok(Nil)
}

fn wait_for_status(runtime, id, expected, attempts: Int) {
  case quasar.get_job(runtime, id) {
    Ok(stored) ->
      case job.status(stored) == expected, attempts > 0 {
        True, _ -> Ok(stored)
        False, True -> {
          process.sleep(10)
          wait_for_status(runtime, id, expected, attempts - 1)
        }
        False, False -> Error(error.RuntimeUnavailable)
      }
    result -> result
  }
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int
