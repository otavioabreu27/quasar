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

pub type JobView {
  JobView(
    status: String,
    attempt: Int,
    inserted_at: Int,
    completed_at: Option(Int),
    total: Option(Int),
    processed_by: Option(String),
  )
}

/// A single snapshot avoids both a second pool checkout and inconsistent reads
/// between job state and business result during transactional completion.
pub fn get_job_view(connection: pog.Connection, id: Int, queue: String) {
  let decoder = {
    use status <- decode.field(0, decode.string)
    use attempt <- decode.field(1, decode.int)
    use inserted <- decode.field(2, decode.int)
    use completed <- decode.field(3, decode.optional(decode.int))
    use total <- decode.field(4, decode.optional(decode.int))
    use instance <- decode.field(5, decode.optional(decode.string))
    decode.success(JobView(
      status,
      attempt,
      inserted,
      completed,
      total,
      instance,
    ))
  }
  pog.query(
    "SELECT q.status, q.attempt, q.inserted_at, q.completed_at, r.total, r.processed_by
    FROM quasar_jobs q LEFT JOIN example_reports r ON r.job_id = q.id
    WHERE q.id = $1 AND q.queue = $2",
  )
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.text(queue))
  |> pog.returning(decoder)
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [item] -> Some(item)
      _ -> None
    }
  })
}

/// Classification deliberately excludes driver messages and SQL parameters.
pub fn error_code(reason: pog.QueryError) -> String {
  case reason {
    pog.QueryTimeout -> "database_timeout"
    pog.ConnectionUnavailable -> "database_connection_unavailable"
    pog.PostgresqlError(_, _, _) -> "database_query_error"
    pog.ConstraintViolated(_, _, _) -> "database_constraint"
    _ -> "database_protocol_error"
  }
}
