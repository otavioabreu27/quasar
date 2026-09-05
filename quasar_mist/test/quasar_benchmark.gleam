//// Development-only HTTP benchmark fixture; excluded from published src.

import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request
import gleam/http/response
import gleam/io
import mist
import quasar_jobs as quasar
import quasar_mist

const port = 19_090

pub fn main() -> Nil {
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.local_pool(
      name: "http",
      workers: 16,
      prefetch: 1,
      buffer_capacity: 256,
    )
    |> quasar.local_pool(
      name: "saturated",
      workers: 2,
      prefetch: 1,
      buffer_capacity: 8,
    )
    |> quasar.start

  let managed =
    fixed_response
    |> quasar_mist.manage(using: runtime, pool: "http", timeout: 5000)
  let saturated =
    slow_response
    |> quasar_mist.manage(using: runtime, pool: "saturated", timeout: 5000)
  let handler = fn(request) {
    case request.path_segments(request) {
      ["managed"] -> managed(request)
      ["saturated"] -> saturated(request)
      _ -> fixed_response(request)
    }
  }

  let assert Ok(_) =
    handler
    |> mist.new
    |> mist.port(port)
    |> mist.start
  io.println("quasar benchmark listening on 127.0.0.1:19090")
  process.sleep_forever()
}

fn fixed_response(_request) {
  response.new(200)
  |> response.set_body(mist.Bytes(bytes_tree.from_string("ok")))
}

fn slow_response(request) {
  process.sleep(20)
  fixed_response(request)
}
