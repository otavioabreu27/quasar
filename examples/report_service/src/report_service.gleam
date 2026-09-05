import gleam/erlang/process
import gleam/int
import gleam/io
import mist
import pog
import quasar_jobs as quasar
import quasar_postgres
import report_service/api
import report_service/config
import report_service/database
import report_service/reports

pub fn main() {
  let assert Ok(config) = config.load()
  // Never print the config: DATABASE_URL contains credentials.
  let assert Ok(db_config) =
    pog.url_config(process.new_name("reports-db"), config.database_url)
  let assert Ok(db) =
    db_config |> pog.pool_size(config.db_pool_size) |> pog.start
  let assert Ok(Nil) = wait_for_database(db.data, 50)
  let assert Ok(Nil) = quasar_postgres.migrate(db.data)
  let assert Ok(Nil) = database.migrate(db.data)
  let report_worker =
    reports.worker(db.data, config.instance_id, config.simulated_work_ms)
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(quasar_postgres.new(db.data))
    |> quasar.local_pool(
      name: "http",
      workers: config.http_pool_workers,
      prefetch: config.http_pool_prefetch,
      buffer_capacity: config.http_buffer_capacity,
    )
    |> quasar.queue(
      name: reports.queue,
      worker: report_worker,
      concurrency: config.worker_concurrency,
      prefetch: config.worker_prefetch,
    )
    |> quasar.start
  let assert Ok(_) =
    api.handler(runtime, db.data, report_worker)
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
