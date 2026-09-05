import gleam/erlang/process
import gleam/int
import gleam/io
import mist
import pog
import quasar_jobs as quasar
import quasar_jobs/retention
import quasar_postgres
import quasar_postgres/listener as postgres_listener
import quasar_postgres/reaper
import quasar_postgres/retention as postgres_retention
import report_service/api
import report_service/config
import report_service/database
import report_service/metrics
import report_service/reports

pub fn main() {
  let assert Ok(config) = config.load()
  // Never print the config: DATABASE_URL contains credentials.
  let assert Ok(db_config) =
    pog.url_config(process.new_name("reports-db"), config.database_url)
  let assert Ok(db) =
    db_config
    |> pog.pool_size(config.db_pool_size)
    |> pog.start
  let assert Ok(worker_config) =
    pog.url_config(process.new_name("reports-worker-db"), config.database_url)
  let assert Ok(worker_db) =
    worker_config
    |> pog.pool_size(config.db_worker_pool_size)
    |> pog.start
  let assert Ok(control_config) =
    pog.url_config(process.new_name("reports-control-db"), config.database_url)
  let assert Ok(control_db) =
    control_config
    |> pog.pool_size(config.db_control_pool_size)
    |> pog.start
  let report = metrics.start()
  let assert Ok(Nil) = wait_for_database(db.data, 50)
  let assert Ok(Nil) = quasar_postgres.migrate(db.data)
  let assert Ok(Nil) = database.migrate(db.data)
  let retention_policy =
    retention.new()
    |> retention.completed_for(days: config.retention_completed_days)
    |> retention.cancelled_for(days: config.retention_cancelled_days)
    |> retention.discarded_for(days: config.retention_discarded_days)
  let assert Ok(_retention) =
    postgres_retention.new(control_db.data, retention_policy)
    |> postgres_retention.with_batch_size(rows: config.retention_batch_size)
    |> postgres_retention.with_pause(milliseconds: config.retention_pause_ms)
    |> postgres_retention.with_interval(
      milliseconds: config.retention_interval_ms,
    )
    |> postgres_retention.start
  let assert Ok(_reaper) =
    reaper.start(
      control_db.data,
      config.reaper_interval_ms,
      metrics.reaper_event,
    )
  let report_worker = case config.completion_mode {
    "buffered" ->
      reports.buffered_worker(
        worker_db.data,
        config.instance_id,
        config.simulated_work_ms,
      )
    _ ->
      reports.worker(
        worker_db.data,
        config.instance_id,
        config.simulated_work_ms,
      )
  }
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(quasar_postgres.new_with_pools(
      db.data,
      worker_db.data,
      control_db.data,
    ))
    |> quasar.with_reporter(report)
    |> quasar.with_poll_interval(config.queue_poll_interval_ms)
    |> quasar.local_pool(
      name: "http",
      workers: config.http_pool_workers,
      prefetch: config.http_pool_prefetch,
      buffer_capacity: config.http_buffer_capacity,
    )
    |> quasar.queue_with_prefetch(
      name: reports.queue,
      worker: report_worker,
      concurrency: config.worker_concurrency,
      prefetch: config.worker_prefetch,
    )
    |> quasar.start
  let assert Ok(_listener) =
    postgres_listener.start(db_config, fn(queue) {
      let _ = quasar.wake(runtime, on: queue)
      Nil
    })
  let assert Ok(_) =
    api.handler_with_readiness(runtime, db.data, control_db.data, report_worker)
    |> mist.new
    |> mist.bind(config.host)
    |> mist.port(config.port)
    |> mist.start
  io.println(
    "Reports API: http://" <> config.host <> ":" <> int.to_string(config.port),
  )
  // Linked startup is deliberately fail-fast. A process manager can restart
  // this example; durable jobs remain in PostgreSQL across application exits.
  process.sleep_forever()
}

fn wait_for_database(connection, remaining) {
  case database.ready(connection), remaining > 0 {
    True, _ -> Ok(Nil)
    False, True -> {
      process.sleep(100)
      wait_for_database(connection, remaining - 1)
    }
    False, False -> Error(Nil)
  }
}
