//// Managed request execution for Mist handlers.
////
//// Mist continues to own sockets, TLS, parsing, protocol handling, and byte
//// transmission. Quasar manages only the synchronous execution that produces
//// a response value. WebSockets, SSE, streaming, chunked responses, and
//// connection ownership transfers must bypass `manage` in the MVP.

import quasar_jobs/error
import quasar_jobs/request_id as identity

import gleam/bytes_tree
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import mist
import quasar_jobs as quasar
import quasar_jobs/job

/// Wraps a fixed-response Mist handler in a named Quasar local pool.
///
/// Requests are ephemeral, local to the BEAM node, at-most-once, and never
/// retried. A 504 deadline response does not prove the handler stopped.
pub fn manage(
  handler: fn(Request(mist.Connection)) -> Response(mist.ResponseData),
  using runtime: quasar.Runtime,
  pool pool: String,
  timeout timeout: Int,
) -> fn(Request(mist.Connection)) -> Response(mist.ResponseData) {
  fn(request) {
    let request_id = request_id(request)
    let request = request.set_header(request, "x-request-id", request_id)
    case
      quasar.call_with_id(
        runtime,
        on: pool,
        id: identity.from_string(request_id),
        timeout: timeout,
        run: fn() { handler(request) },
      )
    {
      Ok(response) -> response.set_header(response, "x-request-id", request_id)
      Error(error) -> error_response(error, request_id)
    }
  }
}

/// Builds a 202 response for a successfully inserted durable job.
pub fn accepted(
  result: Result(job.JobId, quasar.JobOperationError),
) -> Response(mist.ResponseData) {
  case result {
    Ok(id) ->
      text_response(202, "{\"job_id\":\"" <> job.id_to_string(id) <> "\"}")
      |> response.set_header("content-type", "application/json")
    Error(_) -> text_response(503, "service unavailable")
  }
}

/// Converts Quasar execution failures to conservative HTTP responses.
pub fn error_response(
  error: quasar.ExecuteError,
  request_id: String,
) -> Response(mist.ResponseData) {
  let response = case error {
    error.Overloaded ->
      text_response(503, "service unavailable")
      |> response.set_header("retry-after", "1")
    error.PoolNotFound | error.Unavailable | error.ShuttingDown ->
      text_response(503, "service unavailable")
    error.DeadlineExceededMayComplete -> text_response(504, "gateway timeout")
    error.HandlerFailed -> text_response(500, "internal server error")
  }
  response.set_header(response, "x-request-id", request_id)
}

fn request_id(request: Request(body)) -> String {
  case request.get_header(request, "x-request-id") {
    Ok(id) -> id
    Error(_) -> identity.new() |> identity.to_string
  }
}

fn text_response(status: Int, body: String) -> Response(mist.ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}
