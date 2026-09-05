import gleam/erlang/process
import gleam/int
import gleam/result
import quasar_jobs as quasar
import quasar_jobs/error
import quasar_jobs/job
import quasar_jobs/store
import quasar_jobs/worker
import quasar_sqlite as sqlite_store
import store_contract

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

import gleeunit

pub fn main() {
  gleeunit.main()
}

pub fn sqlite_fences_every_acknowledgement_test() {
  let assert Ok(database) = sqlite_store.open(":memory:")
  store_contract.fencing(database, "fencing")
  assert store.close(database) == Ok(Nil)
}

pub fn sqlite_validates_and_discards_exhausted_leases_test() {
  let assert Ok(database) = sqlite_store.open(":memory:")
  store_contract.validation_and_exhaustion(database, "exhaustion")
  assert store.close(database) == Ok(Nil)
}

pub fn sqlite_store_runs_jobs_and_persists_terminal_state_test() {
  let completed = process.new_subject()
  let durable_worker =
    int_worker(fn(value, _) {
      process.send(completed, value)
      Ok(Nil)
    })
  let assert Ok(sqlite) = sqlite_store.open(":memory:")
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(sqlite)
    |> quasar.queue(
      name: "jobs",
      worker: durable_worker,
      concurrency: 1,
      prefetch: 1,
    )
    |> quasar.start
  let assert Ok(id) =
    worker.job(durable_worker, 99) |> quasar.enqueue(runtime, on: "jobs")

  assert process.receive(completed, within: 1000) == Ok(99)
  let assert Ok(stored) = wait_for_status(runtime, id, job.Completed, 100)
  assert job.status(stored) == job.Completed
  assert quasar.stop(runtime) == Ok(Nil)
  assert store.close(sqlite) == Ok(Nil)
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
