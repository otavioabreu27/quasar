import gleam/bytes_tree
import gleam/http/request
import gleam/http/response
import gleeunit
import mist
import quasar_jobs as quasar
import quasar_jobs/error
import quasar_mist
import wisp
import wisp/wisp_mist

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn overload_maps_to_503_with_retry_after_test() {
  let response = quasar_mist.error_response(error.Overloaded, "request-1")
  assert response.status == 503
  assert response.get_header(response, "retry-after") == Ok("1")
  assert response.get_header(response, "x-request-id") == Ok("request-1")
}

pub fn deadline_maps_to_504_test() {
  let response =
    quasar_mist.error_response(error.DeadlineExceededMayComplete, "request-2")
  assert response.status == 504
}

pub fn handler_failure_maps_to_500_test() {
  let response = quasar_mist.error_response(error.HandlerFailed, "request-3")
  assert response.status == 500
}

pub fn manage_has_a_mist_handler_signature_test() {
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.local_pool(
      name: "http",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 10,
    )
    |> quasar.start
  let handler = fn(_request) {
    response.new(200)
    |> response.set_body(mist.Bytes(bytes_tree.new()))
  }
  let _managed: fn(request.Request(mist.Connection)) ->
    response.Response(mist.ResponseData) =
    quasar_mist.manage(handler, using: runtime, pool: "http", timeout: 1000)
  assert quasar.stop(runtime) == Ok(Nil)
}

pub fn wisp_handler_composes_through_wisp_mist_test() {
  let assert Ok(runtime) =
    quasar.new()
    |> quasar.local_pool(
      name: "http",
      workers: 1,
      prefetch: 1,
      buffer_capacity: 10,
    )
    |> quasar.start
  let wisp_handler = fn(_request: wisp.Request) { wisp.response(200) }
  let _managed: fn(request.Request(mist.Connection)) ->
    response.Response(mist.ResponseData) =
    wisp_handler
    |> wisp_mist.handler(
      "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    )
    |> quasar_mist.manage(using: runtime, pool: "http", timeout: 1000)
  assert quasar.stop(runtime) == Ok(Nil)
}
