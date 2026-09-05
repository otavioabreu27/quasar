//// Shared positional row contract for SQL adapters. Column order is part of
//// the storage boundary; malformed or unknown statuses fail decoding.

import gleam/dynamic/decode
import quasar_jobs/job

pub fn row() {
  use id <- decode.field(0, decode.int)
  use queue <- decode.field(1, decode.string)
  use worker <- decode.field(2, decode.string)
  use payload <- decode.field(3, decode.string)
  use status <- decode.field(
    4,
    decode.string
      |> decode.then(fn(value) {
        case job.status_from_string(value) {
          Ok(status) -> decode.success(status)
          Error(_) -> decode.failure(job.Available, "valid job status")
        }
      }),
  )
  use priority <- decode.field(5, decode.int)
  use attempt <- decode.field(6, decode.int)
  use max_attempts <- decode.field(7, decode.int)
  use available_at <- decode.field(8, decode.int)
  use inserted_at <- decode.field(9, decode.int)
  use attempted_at <- decode.field(10, decode.optional(decode.int))
  use completed_at <- decode.field(11, decode.optional(decode.int))
  use lease_owner <- decode.field(12, decode.optional(decode.string))
  use lease_expires_at <- decode.field(13, decode.optional(decode.int))
  use error_kind <- decode.field(14, decode.optional(decode.string))
  use error_message <- decode.field(15, decode.optional(decode.string))
  decode.success(job.restore(
    job.new_id(id),
    queue,
    worker,
    payload,
    status,
    priority,
    attempt,
    max_attempts,
    available_at,
    inserted_at,
    attempted_at,
    completed_at,
    lease_owner,
    lease_expires_at,
    error_kind,
    error_message,
  ))
}
