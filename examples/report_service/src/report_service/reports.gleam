//// Domain and durable worker: calculate the sum of integers from 1 to N.

import gleam/erlang/process
import gleam/int
import gleam/result
import pog
import quasar_jobs/job
import quasar_jobs/worker
import quasar_postgres
import report_service/database

pub const queue = "reports"

pub fn parse_size(value: String) -> Result(Int, String) {
  use size <- result.try(
    int.parse(value) |> result.map_error(fn(_) { "size must be an integer" }),
  )
  case size >= 1 && size <= 1_000_000 {
    True -> Ok(size)
    False -> Error("size must be between 1 and 1000000")
  }
}

pub fn total(size: Int) -> Int {
  size * { size + 1 } / 2
}

pub fn worker(
  connection: pog.Connection,
  instance_id: String,
  simulated_work_ms: Int,
) -> worker.Worker(Int) {
  quasar_postgres.transactional_worker(
    connection,
    name: "reports.sum.v1",
    encode: int.to_string,
    decode: parse_size,
    perform: fn(size, context, transaction) {
      case simulated_work_ms > 0 {
        True -> process.sleep(simulated_work_ms)
        False -> Nil
      }
      database.save(
        transaction,
        job.id_value(context.job_id),
        size,
        total(size),
        instance_id,
      )
      |> result.map_error(fn(_) { "could not persist report" })
    },
  )
}

/// Explicit benchmark alternative: idempotent business INSERT, then a runtime
/// buffered completion. Unlike transactional mode these are separate commits.
pub fn buffered_worker(
  connection: pog.Connection,
  instance_id: String,
  simulated_work_ms: Int,
) {
  worker.new(
    name: "reports.sum.v1",
    encode: int.to_string,
    decode: parse_size,
    perform: fn(size, context) {
      case simulated_work_ms > 0 {
        True -> process.sleep(simulated_work_ms)
        False -> Nil
      }
      database.save(
        connection,
        job.id_value(context.job_id),
        size,
        total(size),
        instance_id,
      )
      |> result.map_error(fn(_) { "could not persist report" })
    },
  )
}
