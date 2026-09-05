import gleeunit
import quasar_jobs/error

pub fn main() {
  gleeunit.main()
}

pub fn expired_claim_is_not_resurrected_and_reaper_is_bounded_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> {
      let assert Ok(config) =
        pog.url_config(process.new_name("quasar-reaper-test"), url)
      let assert Ok(started) = pog.start(config)
      process.unlink(started.pid)
      let assert Ok(_) = migrate_when_ready(started.data, 50)
      let database = postgres_store.new(started.data)
      let queue = "reaper-" <> request_id.to_string(request_id.new())
      let now = system_milliseconds()
      let assert Ok(_) =
        store.insert_many(
          database,
          list.repeat(job.new_job("test", "p", 0, 3), 3),
          queue,
          now,
          now,
        )
      let assert Ok(claimed) = store.claim(database, queue, 3, "test", now, 20)
      process.sleep(30)
      let assert [first, ..] = claimed
      let assert Ok(token) = job.execution_token(first)
      assert store.renew_lease(database, token, system_milliseconds() + 30_000)
        == Error(store.StaleExecution)
      assert store.complete(database, token, system_milliseconds())
        == Error(store.StaleExecution)
      assert store.fail(
          database,
          token,
          job.JobError("test", "test"),
          system_milliseconds(),
        )
        == Error(store.StaleExecution)
      assert store.claim(
          database,
          queue,
          3,
          "test",
          system_milliseconds(),
          30_000,
        )
        == Ok([])
      let assert Ok(count) =
        reaper.batch(started.data, system_milliseconds(), 2)
      assert count <= 2
      // Recover all remaining old fixtures as well, then reclaim only this queue.
      let assert Ok(_) = reaper.batch(started.data, system_milliseconds(), 500)
      let assert Ok(recovered) =
        store.claim(database, queue, 3, "test", system_milliseconds(), 30_000)
      assert list.length(recovered) == 3
      assert list.all(recovered, fn(item) { job.attempt(item) == 2 })
      process.kill(started.pid)
    }
  }
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
import quasar_jobs/store
import quasar_jobs/worker
import quasar_postgres as postgres_store
import quasar_postgres/listener as postgres_listener
import quasar_postgres/reaper
import quasar_postgres/retention as postgres_retention
import store_contract

/// Opt-in integration test. Set QUASAR_POSTGRES_URL to run it.
pub fn two_runtimes_claim_each_postgres_job_once_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(database_url) -> run_multi_instance_test(database_url)
  }
}

pub fn notify_wakes_every_runtime_without_waiting_for_poll_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(database_url) -> run_distributed_wake_test(database_url)
  }
}

fn run_distributed_wake_test(database_url: String) -> Nil {
  let assert Ok(config) =
    pog.url_config(process.new_name("quasar-notify-test"), database_url)
    |> result.map(fn(config) { pog.pool_size(config, 4) })
  let assert Ok(started) = pog.start(config)
  process.unlink(started.pid)
  let connection = started.data
  let assert Ok(Nil) = migrate_when_ready(connection, 50)
  let queue = "notify-" <> request_id.to_string(request_id.new())
  let completed = process.new_subject()
  let notified = process.new_subject()
  let durable_worker =
    worker.new(
      name: "postgres-notify-worker",
      encode: int.to_string,
      decode: fn(payload) {
        int.parse(payload) |> result.map_error(fn(_) { "invalid integer" })
      },
      perform: fn(value, _) {
        process.send(completed, value)
        Ok(Nil)
      },
    )
  let database = postgres_store.new(connection)
  let assert Ok(first) =
    durable_runtime_with_poll(database, durable_worker, queue, 60_000)
  let assert Ok(second) =
    durable_runtime_with_poll(database, durable_worker, queue, 60_000)
  let assert Ok(first_listener) =
    postgres_listener.start(config, fn(received_queue) {
      process.send(notified, #(1, received_queue))
      let _ = quasar.wake(first, on: received_queue)
      Nil
    })
  let assert Ok(second_listener) =
    postgres_listener.start(config, fn(received_queue) {
      process.send(notified, #(2, received_queue))
      let _ = quasar.wake(second, on: received_queue)
      Nil
    })

  // Give both dedicated connections time to establish LISTEN before enqueue.
  process.sleep(200)
  let now = system_milliseconds()
  let assert Ok(_) =
    store.insert(database, worker.job(durable_worker, 42), queue, now, now)
  let first_notification = process.receive(notified, within: 1000)
  let second_notification = process.receive(notified, within: 1000)
  let assert Ok(#(first_id, first_queue)) = first_notification
  let assert Ok(#(second_id, second_queue)) = second_notification
  assert set.from_list([first_id, second_id]) == set.from_list([1, 2])
  assert first_queue == queue
  assert second_queue == queue
  assert process.receive(completed, within: 1000) == Ok(42)

  postgres_listener.stop(first_listener)
  postgres_listener.stop(second_listener)
  assert quasar.stop(first) == Ok(Nil)
  assert quasar.stop(second) == Ok(Nil)
  process.kill(started.pid)
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
  durable_runtime_with_poll(store, durable_worker, queue, 1000)
}

fn durable_runtime_with_poll(store, durable_worker, queue, poll_interval) {
  quasar.new()
  |> quasar.with_store(store)
  |> quasar.with_poll_interval(poll_interval)
  |> quasar.queue_with_prefetch(
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
      let reap = fn(now) {
        let assert Ok(_) = reaper.batch(started.data, now, 500)
        Nil
      }
      store_contract.fencing(database, queue <> "-fencing", reap)
      store_contract.validation_and_exhaustion(
        database,
        queue <> "-validation",
        reap,
      )
      process.kill(started.pid)
    }
  }
}

pub fn postgres_batch_completion_is_atomic_when_one_token_is_stale_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> {
      let assert Ok(config) =
        pog.url_config(process.new_name("quasar-batch-atomic"), url)
      let assert Ok(started) = pog.start(config)
      process.unlink(started.pid)
      let assert Ok(Nil) = migrate_when_ready(started.data, 50)
      let database = postgres_store.new(started.data)
      let queue = "batch-atomic-" <> request_id.to_string(request_id.new())
      let assert Ok(_) =
        store.insert(
          database,
          job.new_job("worker", "first", 0, 3),
          queue,
          100,
          100,
        )
      let assert Ok(second_id) =
        store.insert(
          database,
          job.new_job("worker", "second", 0, 3),
          queue,
          100,
          100,
        )
      let assert Ok([first, second]) =
        store.claim(database, queue, 2, "node", system_milliseconds(), 30_000)
      let assert Ok(first_token) = job.execution_token(first)
      let assert Ok(second_token) = job.execution_token(second)
      let assert Ok(_) = store.complete(database, first_token, 101)

      assert store.complete_many(database, [
          store.Completion(first_token, 102),
          store.Completion(second_token, 102),
        ])
        == Error(store.StaleExecution)
      let assert Ok(still_executing) = store.get(database, second_id)
      assert job.status(still_executing) == job.Executing
      let assert Ok(_) = store.complete(database, second_token, 103)
      process.kill(started.pid)
    }
  }
}

pub fn transactional_worker_commits_effect_and_completion_together_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> run_transactional_worker_test(url)
  }
}

pub fn postgres_enqueue_many_uses_one_atomic_insert_and_one_wake_test() {
  case getenv("QUASAR_POSTGRES_URL") {
    Error(_) -> Nil
    Ok(url) -> run_enqueue_many_test(url)
  }
}

fn run_enqueue_many_test(url: String) -> Nil {
  let assert Ok(config) =
    pog.url_config(process.new_name("quasar-enqueue-many"), url)
    |> result.map(fn(config) { pog.pool_size(config, 2) })
  let assert Ok(started) = pog.start(config)
  process.unlink(started.pid)
  let assert Ok(Nil) = migrate_when_ready(started.data, 50)
  let database = postgres_store.new(started.data)
  let queue = "enqueue-many-" <> request_id.to_string(request_id.new())
  let notifications = process.new_subject()
  let assert Ok(listener) =
    postgres_listener.start(config, fn(received_queue) {
      process.send(notifications, received_queue)
    })
  process.sleep(200)
  let jobs =
    int.range(from: 1, to: 101, with: [], run: list.prepend)
    |> list.map(fn(value) {
      job.new_job("batch-worker", int.to_string(value), 0, 3)
    })
  let assert Ok(ids) = store.insert_many(database, jobs, queue, 100, 100)
  assert list.length(ids) == 100
  assert set.size(set.from_list(ids)) == 100
  assert count_queue_jobs(started.data, queue) == 100
  assert process.receive(notifications, within: 1000) == Ok(queue)
  assert process.receive(notifications, within: 50) == Error(Nil)

  let invalid_queue = queue <> "-invalid"
  assert store.insert_many(
      database,
      [
        job.new_job("batch-worker", "valid", 0, 3),
        job.new_job("batch-worker", "invalid", 0, 0),
      ],
      invalid_queue,
      100,
      100,
    )
    == Error(
      store.InvalidTransition(job.InvalidJob("max_attempts must be positive")),
    )
  assert count_queue_jobs(started.data, invalid_queue) == 0
  let delete =
    pog.query("DELETE FROM quasar_jobs WHERE queue = $1")
    |> pog.parameter(pog.text(queue))
  let assert Ok(_) = pog.execute(delete, on: started.data)
  postgres_listener.stop(listener)
  process.kill(started.pid)
}

fn run_transactional_worker_test(url: String) -> Nil {
  let assert Ok(config) =
    pog.url_config(process.new_name("quasar-transactional-worker"), url)
    |> result.map(fn(config) { pog.pool_size(config, 1) })
  let assert Ok(started) = pog.start(config)
  process.unlink(started.pid)
  let assert Ok(Nil) = migrate_when_ready(started.data, 50)
  let assert Ok(_) =
    pog.query(
      "CREATE TEMP TABLE quasar_transactional_effects (job_id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
    )
    |> pog.execute(on: started.data)
  let queue = "transactional-" <> request_id.to_string(request_id.new())
  let transactional =
    postgres_store.transactional_worker(
      started.data,
      name: "postgres-transactional-worker",
      encode: fn(value) { value },
      decode: Ok,
      perform: fn(value, context, transaction) {
        use _ <- result.try(
          pog.query(
            "INSERT INTO quasar_transactional_effects (job_id, value) VALUES ($1, $2)",
          )
          |> pog.parameter(pog.int(job.id_value(context.job_id)))
          |> pog.parameter(pog.text(value))
          |> pog.execute(on: transaction)
          |> result.map_error(fn(_) { "effect failed" }),
        )
        case value {
          "rollback" -> Error("rollback requested")
          _ -> Ok(Nil)
        }
      },
    )
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(postgres_store.new(started.data))
    |> quasar.queue(queue, transactional, 1)
    |> quasar.start
  let assert Ok(committed_id) =
    quasar.enqueue(worker.job(transactional, "commit"), runtime, on: queue)
  let rollback_job =
    worker.job(transactional, "rollback") |> job.with_max_attempts(1)
  let assert Ok(rolled_back_id) =
    quasar.enqueue(rollback_job, runtime, on: queue)

  let assert Ok(_) = wait_for_completed(runtime, committed_id, 300)
  let assert Ok(discarded) =
    wait_for_status(runtime, rolled_back_id, job.Discarded, 300)
  assert job.status(discarded) == job.Discarded
  assert count_transactional_effects(started.data, committed_id) == 1
  assert count_transactional_effects(started.data, rolled_back_id) == 0
  assert quasar.stop(runtime) == Ok(Nil)
  process.kill(started.pid)
}

fn count_transactional_effects(connection, id) -> Int {
  let decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(count)
  }
  let query =
    pog.query(
      "SELECT count(*)::int FROM quasar_transactional_effects WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(id)))
    |> pog.returning(decoder)
  let assert Ok(returned) = pog.execute(query, on: connection)
  let assert [count] = returned.rows
  count
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
