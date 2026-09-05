//// Aggregates Quasar's public runtime events for the isolated profiler.

import quasar_jobs/event.{type Event}

pub fn start() -> fn(Event) -> Nil {
  start_metrics()
}

@external(erlang, "profile_metrics_ffi", "start")
fn start_metrics() -> fn(Event) -> Nil
