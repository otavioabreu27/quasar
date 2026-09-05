//// Distributed PostgreSQL wake listener.
////
//// Notifications are hints only. Quasar's periodic polling remains the
//// recovery path whenever a notification is delayed or lost.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}
import gleam/result
import pog

const channel = "quasar_jobs"

pub opaque type Listener {
  Listener(pid: Pid)
}

/// Starts a dedicated LISTEN connection with automatic reconnect/backoff.
///
/// `wake` should signal the matching queue in the local Quasar runtime. The
/// listener monitors its owner and shuts itself down when the owner exits.
pub fn start(
  config: pog.Config,
  wake: fn(String) -> Nil,
) -> Result(Listener, Dynamic) {
  start_listener(config, channel, wake) |> result.map(Listener)
}

pub fn stop(listener: Listener) -> Nil {
  stop_listener(listener.pid)
}

@external(erlang, "quasar_postgres_listener_ffi", "start")
fn start_listener(
  config: pog.Config,
  channel: String,
  wake: fn(String) -> Nil,
) -> Result(Pid, Dynamic)

@external(erlang, "quasar_postgres_listener_ffi", "stop")
fn stop_listener(pid: Pid) -> Nil
