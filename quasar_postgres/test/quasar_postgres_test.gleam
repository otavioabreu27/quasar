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
import quasar_jobs/retention
import quasar_jobs/worker
import quasar_postgres as postgres_store
import quasar_postgres/retention as postgres_retention
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
          "SELECT count(*) FROM quasar_jobs_migrations WHERE (version, name) IN ((1, 'create_quasar_jobs'), (2, 'create_quasar_jobs_fetch'), (3, 'create_quasar_jobs_leases'), (4, 'online_create_quasar_jobs_ready'), (5, 'online_create_quasar_jobs_active_leases'), (6, 'online_drop_quasar_jobs_fetch'), (7, 'online_drop_quasar_jobs_leases'), (8, 'add_quasar_jobs_finished_at'), (9, 'online_create_quasar_jobs_retention_completed'), (10, 'online_create_quasar_jobs_retention_cancelled'), (11, 'online_create_quasar_jobs_retention_discarded'))",
        )
        |> pog.returning(decoder)
      let assert Ok(returned) = pog.execute(query, on: started.data)
      assert returned.rows == [11]
      let indexes =
        pog.query(
          "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'quasar_jobs'::regclass AND c.relname IN ('quasar_jobs_ready', 'quasar_jobs_active_leases') AND i.indpred IS NOT NULL AND i.indisvalid AND i.indisready",
        )
        |> pog.returning(decoder)
      let assert Ok(returned_indexes) = pog.execute(indexes, on: started.data)
      assert returned_indexes.rows == [2]
      let retention_indexes =
        pog.query(
          "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'quasar_jobs'::regclass AND c.relname IN ('quasar_jobs_retention_completed', 'quasar_jobs_retention_cancelled', 'quasar_jobs_retention_discarded') AND i.indpred IS NOT NULL AND i.indisvalid AND i.indisready",
        )
        |> pog.returning(decoder)
      let assert Ok(returned_retention_indexes) =
        pog.execute(retention_indexes, on: started.data)
      assert returned_retention_indexes.rows == [3]
      let removed =
        pog.query(
          "SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname IN ('quasar_jobs_fetch', 'quasar_jobs_leases')",
        )
        |> pog.returning(decoder)
      let assert Ok(removed_indexes) = pog.execute(removed, on: started.data)
      assert removed_indexes.rows == [0]
      process.kill(started.pid)
    }
  }
}

pub fn retention_deletes_only_expired_terminal_jobs_in_batches_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> {
      let assert Ok(config) =
        pog.url_config(process.new_name("quasar-retention"), url)
      let assert Ok(started) = pog.start(config)
      process.unlink(started.pid)
      let assert Ok(Nil) = migrate_when_ready(started.data, 50)
      let queue = "retention-" <> request_id.to_string(request_id.new())
      let now = system_milliseconds()
      let old = now - 40 * 86_400_000
      let recent = now - 1000
      insert_terminal(started.data, queue, "completed", old, 5)
      insert_terminal(started.data, queue, "cancelled", old, 3)
      insert_terminal(started.data, queue, "discarded", old, 4)
      insert_terminal(started.data, queue, "completed", recent, 1)
      insert_terminal(started.data, queue, "cancelled", recent, 1)
      insert_terminal(started.data, queue, "discarded", recent, 1)
      insert_available(started.data, queue, old)

      let reports = process.new_subject()
      let policy =
        retention.new()
        |> retention.completed_for(days: 7)
        |> retention.cancelled_for(days: 30)
        |> retention.discarded_for(days: 30)
      let assert Ok(cleaner) =
        postgres_retention.new(started.data, policy)
        |> postgres_retention.with_batch_size(rows: 2)
        |> postgres_retention.with_pause(milliseconds: 1)
        |> postgres_retention.with_interval(milliseconds: 60_000)
        |> postgres_retention.with_reporter(fn(event) {
          process.send(reports, event)
        })
        |> postgres_retention.start
      let deleted = receive_retention_cycle(reports, 0)
      // The cleaner is intentionally global across queues. Other integration
      // tests may also leave expired fixtures in the shared test database.
      assert deleted >= 12
      assert count_queue_jobs(started.data, queue) == 4
      postgres_retention.stop(cleaner)
      process.kill(started.pid)
    }
  }
}

fn receive_retention_cycle(reports, batches: Int) -> Int {
  let assert Ok(event) = process.receive(reports, within: 5000)
  case event {
    postgres_retention.BatchCompleted(_, requested, deleted, _) -> {
      assert deleted <= requested
      assert requested == 2
      receive_retention_cycle(reports, batches + deleted)
    }
    postgres_retention.BatchFailed(_, _) -> panic as "retention batch failed"
    postgres_retention.CycleCompleted(deleted) -> {
      assert deleted == batches
      deleted
    }
  }
}

fn insert_terminal(connection, queue, status, timestamp, count) -> Nil {
  let query =
    pog.query(
      "INSERT INTO quasar_jobs (queue, worker, payload, status, priority, attempt, max_attempts, available_at, inserted_at, attempted_at, completed_at, finished_at) SELECT $1, 'retention-test', '0', $2::text, 0, 1, 1, $3::bigint, $3::bigint, $3::bigint, CASE WHEN $2::text = 'completed' THEN $3::bigint ELSE NULL END, $3::bigint FROM generate_series(1, $4)",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(status))
    |> pog.parameter(pog.int(timestamp))
    |> pog.parameter(pog.int(count))
  let assert Ok(_) = pog.execute(query, on: connection)
  Nil
}

fn insert_available(connection, queue, timestamp) -> Nil {
  let query =
    pog.query(
      "INSERT INTO quasar_jobs (queue, worker, payload, status, priority, attempt, max_attempts, available_at, inserted_at) VALUES ($1, 'retention-test', '0', 'available', 0, 0, 1, $2, $2)",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(timestamp))
  let assert Ok(_) = pog.execute(query, on: connection)
  Nil
}

fn count_queue_jobs(connection, queue) -> Int {
  let decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(count)
  }
  let query =
    pog.query("SELECT count(*)::int FROM quasar_jobs WHERE queue = $1")
    |> pog.parameter(pog.text(queue))
    |> pog.returning(decoder)
  let assert Ok(returned) = pog.execute(query, on: connection)
  let assert [count] = returned.rows
  count
}

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int
