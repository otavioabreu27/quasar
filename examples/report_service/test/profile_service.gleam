//// Profiling-only entry point; the normal service does not enable BEAM tracing.

import envoy
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/result
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
  let workers = setting("PROFILE_WORKERS", 8)
  let connections = setting("PROFILE_CONNECTIONS", 20)
  let prefetch = setting("PROFILE_PREFETCH", 2)
  let assert Ok(db_config) =
    pog.url_config(process.new_name("profile-db"), config.database_url)
  let assert Ok(db) = db_config |> pog.pool_size(connections) |> pog.start
  let assert Ok(Nil) = wait_for_database(db.data, 50)
  let assert Ok(Nil) = quasar_postgres.migrate(db.data)
  let assert Ok(Nil) = database.migrate(db.data)
  case envoy.get("PROFILE_TRACE") {
    Ok("1") -> start_trace()
    _ -> Nil
  }
  let worker =
    reports.worker(db.data, config.instance_id, config.simulated_work_ms)
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.with_store(quasar_postgres.new(db.data))
    |> quasar.local_pool(
      name: "http",
      workers: 8,
      prefetch: 1,
      buffer_capacity: 32,
    )
    |> quasar.queue(
      name: reports.queue,
      worker:,
      concurrency: workers,
      prefetch: prefetch,
    )
    |> quasar.start
  let assert Ok(_) =
    api.handler(runtime, db.data, worker)
    |> mist.new
    |> mist.bind(config.host)
    |> mist.port(config.port)
    |> mist.start
  io.println("PROFILE_READY")
  process.sleep_forever()
}

fn setting(name, default) {
  let assert Ok(value) =
    envoy.get(name) |> result.unwrap(int.to_string(default)) |> int.parse
  assert value > 0
  value
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

@external(erlang, "profile_trace_ffi", "start")
fn start_trace() -> Nil
