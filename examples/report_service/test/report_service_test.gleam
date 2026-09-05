import gleeunit
import quasar_jobs/job
import report_service/api
import report_service/reports

pub fn main() {
  gleeunit.main()
}

pub fn validates_report_size_test() {
  assert reports.parse_size("100") == Ok(100)
  assert reports.parse_size("1") == Ok(1)
  assert reports.parse_size("1000000") == Ok(1_000_000)
  assert reports.parse_size("0") == Error("size must be between 1 and 1000000")
  assert reports.parse_size("1000001")
    == Error("size must be between 1 and 1000000")
  assert reports.parse_size("hello") == Error("size must be an integer")
}

pub fn calculates_total_test() {
  assert reports.total(1) == 1
  assert reports.total(100) == 5050
  assert reports.total(1_000_000) == 500_000_500_000
}

pub fn serializes_job_states_test() {
  assert api.status_name(job.Available) == "available"
  assert api.status_name(job.Scheduled) == "scheduled"
  assert api.status_name(job.Executing) == "executing"
  assert api.status_name(job.Completed) == "completed"
  assert api.status_name(job.Retryable) == "retryable"
  assert api.status_name(job.Discarded) == "discarded"
  assert api.status_name(job.Cancelled) == "cancelled"
}
