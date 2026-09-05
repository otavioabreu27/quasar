//// Configuration validation; no process or database effects.

import gleam/list
import gleam/option.{type Option, None}
import gleam/result
import quasar_jobs/error.{
  type ConfigError, DuplicatePool, DuplicateQueue, EmptyPoolName,
  InvalidBufferCapacity, InvalidPrefetch, InvalidQueue, InvalidShutdownTimeout,
  InvalidWorkers, StoreRequired,
}
import quasar_jobs/internal/durable
import quasar_jobs/internal/local.{type PoolConfig, PoolConfig}
import quasar_jobs/store.{type Store}

pub fn validate(
  configs: List(PoolConfig),
  queues: List(durable.QueueConfig),
  durable_store: Option(Store),
  shutdown_timeout: Int,
) -> Result(Nil, ConfigError) {
  use _ <- result.try(case shutdown_timeout > 0 {
    True -> Ok(Nil)
    False -> Error(InvalidShutdownTimeout(shutdown_timeout))
  })
  use _ <- result.try(validate_loop(configs, []))
  use _ <- result.try(validate_queues(queues, []))
  case queues, durable_store {
    [_, ..], None -> Error(StoreRequired)
    _, _ -> Ok(Nil)
  }
}

fn validate_queues(
  queues: List(durable.QueueConfig),
  names: List(String),
) -> Result(Nil, ConfigError) {
  case queues {
    [] -> Ok(Nil)
    [queue, ..rest] ->
      case durable.valid(queue), list.contains(names, durable.name(queue)) {
        False, _ -> Error(InvalidQueue(durable.name(queue)))
        _, True -> Error(DuplicateQueue(durable.name(queue)))
        True, False -> validate_queues(rest, [durable.name(queue), ..names])
      }
  }
}

fn validate_loop(
  configs: List(PoolConfig),
  names: List(String),
) -> Result(Nil, ConfigError) {
  case configs {
    [] -> Ok(Nil)
    [PoolConfig(name, workers, prefetch, capacity), ..rest] ->
      case name, workers, prefetch, capacity, list.contains(names, name) {
        "", _, _, _, _ -> Error(EmptyPoolName)
        _, workers, _, _, _ if workers <= 0 ->
          Error(InvalidWorkers(name, workers))
        _, _, prefetch, _, _ if prefetch <= 0 ->
          Error(InvalidPrefetch(name, prefetch))
        _, _, _, capacity, _ if capacity <= 0 ->
          Error(InvalidBufferCapacity(name, capacity))
        _, _, _, _, True -> Error(DuplicatePool(name))
        _, _, _, _, False -> validate_loop(rest, [name, ..names])
      }
  }
}
