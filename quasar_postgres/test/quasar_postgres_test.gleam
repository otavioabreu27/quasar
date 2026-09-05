import gleeunit
import quasar_jobs/error

pub fn main() {
  gleeunit.main()
}

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/result
import gleam/set
import pog
import quasar_jobs as quasar
import quasar_jobs/job
import quasar_jobs/request_id
import quasar_jobs/worker
import quasar_postgres as postgres_store
import store_contract

/// Opt-in integration test. Set QUASAR_POSTGRES_URL to run it.
pub fn two_runtimes_claim_each_postgres_job_once_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(database_url) -> run_multi_instance_test(database_url)
  }
}

fn run_multi_instance_test(database_url: String) -> Nil {
  let assert Ok(config) =
    pog.url_config(process.new_name("quasar-postgres-test"), database_url)
    |> result.map(fn(config) { pog.pool_size(config, 8) })
  let assert Ok(started) = pog.start(config)
  process.unlink(started.pid)
  let connection = started.data
  let assert Ok(Nil) = migrate_when_ready(connection, 50)
  let queue = "postgres-jobs-" <> request_id.to_string(request_id.new())

  let completed = process.new_subject()
  let durable_worker =
    worker.new(
      name: "postgres-integration-worker",
      encode: int.to_string,
      decode: fn(payload) {
        int.parse(payload) |> result.map_error(fn(_) { "invalid integer" })
      },
      perform: fn(_, context) {
        process.send(completed, job.id_to_string(context.job_id))
        Ok(Nil)
      },
    )
  let store = postgres_store.new(connection)
  let assert Ok(first) = durable_runtime(store, durable_worker, queue)
  let assert Ok(second) = durable_runtime(store, durable_worker, queue)
  let count = 50
  let assert Ok(ids) =
    int.range(from: 1, to: count + 1, with: [], run: list.prepend)
    |> list.try_map(fn(value) {
      worker.job(durable_worker, value)
      |> quasar.enqueue(first, on: queue)
    })

  let executions = receive_ids(completed, count, set.new())
  assert set.size(executions) == count
  list.each(ids, fn(id) {
    let assert Ok(stored) = wait_for_completed(first, id, 100)
    assert job.attempt(stored) == 1
  })

  assert quasar.stop(first) == Ok(Nil)
  assert quasar.stop(second) == Ok(Nil)
  process.kill(started.pid)
}

fn durable_runtime(store, durable_worker, queue) {
  quasar.new()
  |> quasar.with_store(store)
  |> quasar.queue(
    name: queue,
    worker: durable_worker,
    concurrency: 2,
    prefetch: 2,
  )
  |> quasar.start
}

fn migrate_when_ready(connection, attempts: Int) {
  case postgres_store.migrate(connection), attempts > 0 {
    Ok(Nil), _ -> Ok(Nil)
    Error(_), True -> {
      process.sleep(100)
      migrate_when_ready(connection, attempts - 1)
    }
    Error(error), False -> Error(error)
  }
}

fn receive_ids(subject, remaining: Int, ids) {
  case remaining {
    0 -> ids
    _ -> {
      let assert Ok(id) = process.receive(subject, within: 5000)
      receive_ids(subject, remaining - 1, set.insert(ids, id))
    }
  }
}

fn wait_for_completed(runtime, id, attempts: Int) {
  case quasar.get_job(runtime, id) {
    Ok(stored) ->
      case job.status(stored), attempts > 0 {
        job.Completed, _ -> Ok(stored)
        _, True -> {
          process.sleep(10)
          wait_for_completed(runtime, id, attempts - 1)
        }
        _, False -> Error(error.RuntimeUnavailable)
      }
    result -> result
  }
}

@external(erlang, "quasar_jobs_test_ffi", "getenv")
fn getenv(name: String) -> Result(String, Nil)

pub fn postgres_store_contract_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> {
      let assert Ok(config) =
        pog.url_config(process.new_name("quasar-contract"), url)
      let assert Ok(started) = pog.start(config)
      process.unlink(started.pid)
      let assert Ok(Nil) = migrate_when_ready(started.data, 50)
      let database = postgres_store.new(started.data)
      let queue = request_id.to_string(request_id.new())
      store_contract.fencing(database, queue <> "-fencing")
      store_contract.validation_and_exhaustion(database, queue <> "-validation")
      process.kill(started.pid)
    }
  }
}

pub fn migrations_are_versioned_and_idempotent_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> {
      let assert Ok(config) =
        pog.url_config(process.new_name("quasar-migrations"), url)
      let assert Ok(started) = pog.start(config)
      process.unlink(started.pid)
      let assert Ok(Nil) = migrate_when_ready(started.data, 50)
      assert postgres_store.migrate(started.data) == Ok(Nil)
      let decoder = {
        use count <- decode.field(0, decode.int)
        decode.success(count)
      }
      let query =
        pog.query(
          "SELECT count(*) FROM quasar_jobs_migrations WHERE (version, name) IN ((1, 'create_quasar_jobs'), (2, 'create_quasar_jobs_fetch'), (3, 'create_quasar_jobs_leases'))",
        )
        |> pog.returning(decoder)
      let assert Ok(returned) = pog.execute(query, on: started.data)
      assert returned.rows == [3]
      process.kill(started.pid)
    }
  }
}
