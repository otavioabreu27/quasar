//// Parameterized SQL only. Business results are separate from Quasar's schema.

import gleam/dynamic/decode
import gleam/option.{type Option, None, Some}
import gleam/result
import pog

pub fn migrate(connection: pog.Connection) -> Result(Nil, pog.QueryError) {
  use _ <- result.try(
    pog.query(
      "CREATE TABLE IF NOT EXISTS example_reports (
    job_id BIGINT PRIMARY KEY REFERENCES quasar_jobs(id) ON DELETE CASCADE,
    size INTEGER NOT NULL,
    total BIGINT NOT NULL,
    processed_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )",
    )
    |> pog.execute(on: connection),
  )
  use _ <- result.try(
    pog.query(
      "DO $$
     BEGIN
       IF EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conname = 'example_reports_job_id_fkey'
           AND conrelid = 'example_reports'::regclass
           AND confdeltype <> 'c'
       ) THEN
         ALTER TABLE example_reports
           DROP CONSTRAINT example_reports_job_id_fkey;
         ALTER TABLE example_reports
           ADD CONSTRAINT example_reports_job_id_fkey
           FOREIGN KEY (job_id) REFERENCES quasar_jobs(id)
           ON DELETE CASCADE NOT VALID;
       END IF;
     END $$",
    )
    |> pog.execute(on: connection),
  )
  pog.query(
    "ALTER TABLE example_reports
     VALIDATE CONSTRAINT example_reports_job_id_fkey",
  )
  |> pog.execute(on: connection)
  |> result.replace(Nil)
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
