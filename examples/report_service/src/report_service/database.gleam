//// Parameterized SQL only. Business results are separate from Quasar's schema.

import gleam/dynamic/decode
import gleam/option.{type Option, None, Some}
import gleam/result
import pog

pub fn migrate(connection: pog.Connection) -> Result(Nil, pog.QueryError) {
  pog.query(
    "CREATE TABLE IF NOT EXISTS example_reports (
    job_id BIGINT PRIMARY KEY REFERENCES quasar_jobs(id),
    size INTEGER NOT NULL,
    total BIGINT NOT NULL,
    processed_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )",
  )
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
}

/// A replay after the INSERT but before Quasar's acknowledgement is harmless.
/// This is not an exactly-once guarantee for arbitrary external side effects.
pub fn save(
  connection: pog.Connection,
  id: Int,
  size: Int,
  total: Int,
  instance_id: String,
) {
  pog.query(
    "INSERT INTO example_reports (job_id, size, total, processed_by)
    VALUES ($1, $2, $3, $4) ON CONFLICT (job_id) DO NOTHING",
  )
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.int(size))
  |> pog.parameter(pog.int(total))
  |> pog.parameter(pog.text(instance_id))
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
}

pub fn get_report(
  connection: pog.Connection,
  id: Int,
) -> Result(Option(#(Int, String)), pog.QueryError) {
  let decoder = {
    use total <- decode.field(0, decode.int)
    use instance <- decode.field(1, decode.string)
    decode.success(#(total, instance))
  }
  use rows <- result.try(
    pog.query(
      "SELECT total, processed_by FROM example_reports WHERE job_id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning(decoder)
    |> pog.execute(on: connection),
  )
  Ok(case rows.rows {
    [total] -> Some(total)
    _ -> None
  })
}

pub fn ready(connection: pog.Connection) -> Bool {
  case
    pog.query("SELECT 1") |> pog.timeout(1000) |> pog.execute(on: connection)
  {
    Ok(_) -> True
    Error(_) -> False
  }
}
