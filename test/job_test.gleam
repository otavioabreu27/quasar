import gleam/dynamic
import gleam/dynamic/decode
import gleam/result
import quasar_jobs/job
import quasar_jobs/store/codec

pub fn unknown_status_is_rejected_test() {
  assert job.status_from_string("future_status")
    == Error("unknown job status: future_status")
  let row =
    dynamic.array([
      dynamic.int(1),
      dynamic.string("queue"),
      dynamic.string("worker"),
      dynamic.string("payload"),
      dynamic.string("future_status"),
      dynamic.int(0),
      dynamic.int(0),
      dynamic.int(3),
      dynamic.int(100),
      dynamic.int(100),
      dynamic.nil(),
      dynamic.nil(),
      dynamic.nil(),
      dynamic.nil(),
      dynamic.nil(),
      dynamic.nil(),
    ])
  assert decode.run(row, codec.row()) |> result.is_error
}

pub fn job_state_machine_rejects_invalid_completion_test() {
  let new_job = job.new_job("worker", "payload", 0, 3)
  let assert Ok(stored) =
    job.materialise(new_job, job.new_id(1), "queue", 100, 100)

  assert job.complete(stored, 101)
    == Error(job.InvalidTransition(job.Available, "complete"))
}

pub fn expired_lease_becomes_retryable_test() {
  let new_job = job.new_job("worker", "payload", 0, 3)
  let assert Ok(stored) =
    job.materialise(new_job, job.new_id(1), "queue", 100, 100)
  let assert Ok(executing) = job.claim(stored, "instance", 100, 110)
  let recovered = job.recover_if_expired(executing, 111)

  assert job.status(recovered) == job.Retryable
  assert job.attempt(recovered) == 1
}
