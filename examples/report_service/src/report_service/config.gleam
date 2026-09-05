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
    worker_concurrency: Int,
    worker_prefetch: Int,
    http_pool_workers: Int,
    http_pool_prefetch: Int,
    http_buffer_capacity: Int,
    simulated_work_ms: Int,
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
    |> result.unwrap("20")
    |> int.parse
    |> result.map_error(fn(_) { "DB_POOL_SIZE must be an integer" }),
  )
  use worker_concurrency <- result.try(
    envoy.get("WORKER_CONCURRENCY")
    |> result.unwrap("8")
    |> int.parse
    |> result.map_error(fn(_) { "WORKER_CONCURRENCY must be an integer" }),
  )
  use worker_prefetch <- result.try(
    envoy.get("WORKER_PREFETCH")
    |> result.unwrap("2")
    |> int.parse
    |> result.map_error(fn(_) { "WORKER_PREFETCH must be an integer" }),
  )
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
        worker_concurrency:,
        worker_prefetch:,
        http_pool_workers:,
        http_pool_prefetch:,
        http_buffer_capacity:,
        simulated_work_ms:,
      ))
  }
}
