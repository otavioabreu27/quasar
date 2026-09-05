//// Bounded in-process counters; no payloads, credentials or per-job labels.

import quasar_jobs/event.{type Event}
import quasar_postgres/reaper

@external(erlang, "report_metrics_ffi", "start")
pub fn start() -> fn(Event) -> Nil

@external(erlang, "report_metrics_ffi", "snapshot")
pub fn snapshot() -> String

@external(erlang, "report_metrics_ffi", "reaper_event")
pub fn reaper_event(event: reaper.Event) -> Nil
