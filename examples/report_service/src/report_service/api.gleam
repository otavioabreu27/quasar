import gleam/bytes_tree
import gleam/http
import gleam/http/request
import gleam/http/response
import gleam/int
import gleam/json
import gleam/option
import mist
import pog
import quasar_jobs as quasar
import quasar_jobs/error
import quasar_jobs/job
import quasar_jobs/store
import quasar_jobs/worker
import quasar_mist
import report_service/database
import report_service/reports

pub fn handler(
  runtime: quasar.Runtime,
  connection: pog.Connection,
  report_worker: worker.Worker(Int),
) {
  let managed =
    fn(req) { route(req, runtime, connection, report_worker) }
    |> quasar_mist.manage(using: runtime, pool: "http", timeout: 3000)
  fn(req: request.Request(mist.Connection)) {
    // Liveness must not wait for the database or a saturated execution pool.
    case req.method, request.path_segments(req) {
      http.Get, ["health", "live"] -> message(200, "alive")
      _, _ -> managed(req)
    }
  }
}

fn route(
  req: request.Request(mist.Connection),
  runtime,
  connection,
  report_worker,
) {
  case req.method, request.path_segments(req) {
    http.Get, ["health", "ready"] ->
      case database.ready(connection) {
        True -> message(200, "ready")
        False -> message(503, "database unavailable")
      }
    http.Post, ["reports", size] ->
      case reports.parse_size(size) {
        Error(reason) -> message(400, reason)
        Ok(size) -> {
          let inserted =
            worker.job(report_worker, size)
            |> job.with_max_attempts(5)
            |> quasar.enqueue(runtime, on: reports.queue)
          let accepted = quasar_mist.accepted(inserted)
          case inserted {
            Ok(id) ->
              response.set_header(
                accepted,
                "location",
                "/jobs/" <> job.id_to_string(id),
              )
            Error(_) -> accepted
          }
        }
      }
    http.Get, ["jobs", id] ->
      case int.parse(id) {
        Ok(id) if id > 0 -> get_job(runtime, connection, id)
        _ -> message(400, "invalid job id")
      }
    _, _ -> message(404, "route not found")
  }
}

fn get_job(runtime, connection, id) {
  case quasar.get_job(runtime, job.new_id(id)) {
    Error(error.StoreFailure(store.NotFound)) -> message(404, "job not found")
    Error(_) -> message(503, "job store unavailable")
    Ok(item) ->
      case job.queue(item) == reports.queue {
        False -> message(404, "job not found")
        True ->
          case database.get_report(connection, id) {
            Error(_) -> message(503, "report store unavailable")
            Ok(report) ->
              json_response(
                200,
                json.object([
                  #("job_id", json.string(job.id_to_string(job.id(item)))),
                  #("status", json.string(status_name(job.status(item)))),
                  #("attempt", json.int(job.attempt(item))),
                  #(
                    "total",
                    json.nullable(option.map(report, fn(r) { r.0 }), json.int),
                  ),
                  #(
                    "processed_by",
                    json.nullable(
                      option.map(report, fn(r) { r.1 }),
                      json.string,
                    ),
                  ),
                ]),
              )
          }
      }
  }
}

pub fn status_name(status: job.Status) -> String {
  case status {
    job.Available -> "available"
    job.Scheduled -> "scheduled"
    job.Executing -> "executing"
    job.Completed -> "completed"
    job.Retryable -> "retryable"
    job.Discarded -> "discarded"
    job.Cancelled -> "cancelled"
  }
}

fn message(status, message) {
  json_response(status, json.object([#("message", json.string(message))]))
}

fn json_response(status, body) {
  response.new(status)
  |> response.set_header("content-type", "application/json")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(json.to_string(body))))
}
