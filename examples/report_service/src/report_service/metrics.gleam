//// Bounded in-process counters; no payloads, credentials or per-job labels.

import quasar_jobs/event.{type Event}
import quasar_postgres/reaper

@external(erlang, "report_metrics_ffi", "start")
pub fn start() -> fn(Event) -> Nil

@external(erlang, "report_metrics_ffi", "snapshot")
pub fn snapshot() -> String

@external(erlang, "report_metrics_ffi", "reaper_event")
pub fn reaper_event(event: reaper.Event) -> Nil

/// Diagnostic-only timing, enabled with BENCHMARK_HTTP_TIMING=1.
/// Use fixed labels, never request paths or user data.
@external(erlang, "report_metrics_ffi", "measure")
pub fn measure(label: String, action: fn() -> a) -> a
