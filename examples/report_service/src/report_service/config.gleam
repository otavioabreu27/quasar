import envoy
import gleam/int
import gleam/result

pub type Config {
  Config(
    database_url: String,
    host: String,
    port: Int,
    instance_id: String,
    db_pool_size: Int,
    db_worker_pool_size: Int,
    db_control_pool_size: Int,
    reaper_interval_ms: Int,
    completion_mode: String,
    worker_concurrency: Int,
    worker_prefetch: Int,
    queue_poll_interval_ms: Int,
    http_pool_workers: Int,
    http_pool_prefetch: Int,
    http_buffer_capacity: Int,
    simulated_work_ms: Int,
    retention_completed_days: Int,
    retention_cancelled_days: Int,
    retention_discarded_days: Int,
    retention_batch_size: Int,
    retention_pause_ms: Int,
    retention_interval_ms: Int,
  )
}

pub fn load() -> Result(Config, String) {
  use database_url <- result.try(
    envoy.get("DATABASE_URL")
    |> result.map_error(fn(_) { "Set DATABASE_URL before starting the service" }),
  )
  use port <- result.try(
    envoy.get("PORT")
    |> result.unwrap("8080")
    |> int.parse
    |> result.map_error(fn(_) { "PORT must be an integer" }),
  )
  use db_pool_size <- result.try(
    envoy.get("DB_POOL_SIZE")
    |> result.unwrap("3")
    |> int.parse
    |> result.map_error(fn(_) { "DB_POOL_SIZE must be an integer" }),
  )
  use worker_concurrency <- result.try(
    envoy.get("WORKER_CONCURRENCY")
    |> result.unwrap("8")
    |> int.parse
    |> result.map_error(fn(_) { "WORKER_CONCURRENCY must be an integer" }),
  )
  use db_worker_pool_size <- result.try(positive_env("DB_WORKER_POOL_SIZE", "4"))
  use db_control_pool_size <- result.try(positive_env(
    "DB_CONTROL_POOL_SIZE",
    "1",
  ))
  use reaper_interval_ms <- result.try(positive_env(
    "REAPER_INTERVAL_MS",
    "1000",
  ))
  let completion_mode =
    envoy.get("COMPLETION_MODE") |> result.unwrap("transactional")
  use worker_prefetch <- result.try(
    envoy.get("WORKER_PREFETCH")
    |> result.unwrap("1")
    |> int.parse
    |> result.map_error(fn(_) { "WORKER_PREFETCH must be an integer" }),
  )
  use queue_poll_interval_ms <- result.try(positive_env(
    "QUEUE_POLL_INTERVAL_MS",
    "5000",
  ))
  use http_pool_workers <- result.try(
    envoy.get("HTTP_POOL_WORKERS")
    |> result.unwrap("8")
    |> int.parse
    |> result.map_error(fn(_) { "HTTP_POOL_WORKERS must be an integer" }),
  )
  use http_pool_prefetch <- result.try(
    envoy.get("HTTP_POOL_PREFETCH")
    |> result.unwrap("1")
    |> int.parse
    |> result.map_error(fn(_) { "HTTP_POOL_PREFETCH must be an integer" }),
  )
  use http_buffer_capacity <- result.try(
    envoy.get("HTTP_BUFFER_CAPACITY")
    |> result.unwrap("32")
    |> int.parse
    |> result.map_error(fn(_) { "HTTP_BUFFER_CAPACITY must be an integer" }),
  )
  use simulated_work_ms <- result.try(
    envoy.get("SIMULATED_WORK_MS")
    |> result.unwrap("0")
    |> int.parse
    |> result.map_error(fn(_) { "SIMULATED_WORK_MS must be an integer" }),
  )
  use retention_completed_days <- result.try(positive_env(
    "RETENTION_COMPLETED_DAYS",
    "7",
  ))
  use retention_cancelled_days <- result.try(positive_env(
    "RETENTION_CANCELLED_DAYS",
    "30",
  ))
  use retention_discarded_days <- result.try(positive_env(
    "RETENTION_DISCARDED_DAYS",
    "30",
  ))
  use retention_batch_size <- result.try(positive_env(
    "RETENTION_BATCH_SIZE",
    "1000",
  ))
  use retention_interval_ms <- result.try(positive_env(
    "RETENTION_INTERVAL_MS",
    "60000",
  ))
  use retention_pause_ms <- result.try(non_negative_env(
    "RETENTION_PAUSE_MS",
    "100",
  ))
  case
    port > 0
    && port <= 65_535
    && db_pool_size > 0
    && worker_concurrency > 0
    && worker_prefetch > 0
    && http_pool_workers > 0
    && http_pool_prefetch > 0
    && http_buffer_capacity > 0
    && simulated_work_ms >= 0
    && { completion_mode == "transactional" || completion_mode == "buffered" }
  {
    False ->
      Error(
        "PORT must be 1..65535, simulated_work_ms must be >= 0, and all concurrency, pool, and buffer values must be positive integers",
      )
    True ->
      Ok(Config(
        database_url:,
        host: envoy.get("HOST") |> result.unwrap("0.0.0.0"),
        port:,
        instance_id: envoy.get("INSTANCE_ID")
          |> result.unwrap("reports-" <> int.to_string(port)),
        db_pool_size:,
        db_worker_pool_size:,
        db_control_pool_size:,
        reaper_interval_ms:,
        completion_mode:,
        worker_concurrency:,
        worker_prefetch:,
        queue_poll_interval_ms:,
        http_pool_workers:,
        http_pool_prefetch:,
        http_buffer_capacity:,
        simulated_work_ms:,
        retention_completed_days:,
        retention_cancelled_days:,
        retention_discarded_days:,
        retention_batch_size:,
        retention_pause_ms:,
        retention_interval_ms:,
      ))
  }
}

fn positive_env(name: String, default: String) -> Result(Int, String) {
  use value <- result.try(
    envoy.get(name)
    |> result.unwrap(default)
    |> int.parse
    |> result.map_error(fn(_) { name <> " must be an integer" }),
  )
  case value > 0 {
    True -> Ok(value)
    False -> Error(name <> " must be positive")
  }
}

fn non_negative_env(name: String, default: String) -> Result(Int, String) {
  use value <- result.try(
    envoy.get(name)
    |> result.unwrap(default)
    |> int.parse
    |> result.map_error(fn(_) { name <> " must be an integer" }),
  )
  case value >= 0 {
    True -> Ok(value)
    False -> Error(name <> " must be non-negative")
  }
}
