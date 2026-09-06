import gleam/bytes_tree
import gleam/http
import gleam/http/request
import gleam/http/response
import gleam/int
import gleam/json
import gleam/list
import gleam/option
import mist
import pog
import quasar_jobs as quasar
import quasar_jobs/job
import quasar_jobs/worker
import quasar_mist
import report_service/database
import report_service/metrics
import report_service/reports

pub fn handler(
  runtime: quasar.Runtime,
  connection: pog.Connection,
  report_worker: worker.Worker(Int),
) {
  handler_with_readiness(runtime, connection, connection, report_worker)
}

pub fn handler_with_readiness(
  runtime: quasar.Runtime,
  connection: pog.Connection,
  readiness: pog.Connection,
  report_worker: worker.Worker(Int),
) {
  let managed =
    fn(req) { route(req, runtime, connection, report_worker) }
    |> quasar_mist.manage(using: runtime, pool: "http", timeout: 3000)
  fn(req: request.Request(mist.Connection)) {
    // Liveness must not wait for the database or a saturated execution pool.
    case req.method, request.path_segments(req) {
      http.Get, ["health", "live"] -> message(200, "alive")
      http.Get, ["health", "ready"] ->
        case database.ready(readiness) {
          True -> message(200, "ready")
          False -> message(503, "database unavailable")
        }
      http.Get, ["internal", "metrics"] ->
        response.new(200)
        |> response.set_header("content-type", "application/json")
        |> response.set_body(
          mist.Bytes(bytes_tree.from_string(metrics.snapshot())),
        )
      _, _ -> {
        let label = case req.method {
          http.Post -> "http_post_handler"
          http.Get -> "http_get_handler"
          _ -> "http_other_handler"
        }
        // Handler entry through response construction, not socket send time.
        metrics.measure(label, fn() { managed(req) })
      }
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
    http.Post, ["reports", "batch", size, count] ->
      enqueue_batch(runtime, report_worker, size, count)
    http.Post, ["reports", size] ->
      case reports.parse_size(size) {
        Error(reason) -> message(400, reason)
        Ok(size) -> {
          let inserted =
            metrics.measure("http_enqueue", fn() {
              worker.job(report_worker, size)
              |> job.with_max_attempts(5)
              |> quasar.enqueue(runtime, on: reports.queue)
            })
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
        Ok(id) if id > 0 -> get_job(connection, id)
        _ -> message(400, "invalid job id")
      }
    _, _ -> message(404, "route not found")
  }
}

fn enqueue_batch(runtime, report_worker, size_text, count_text) {
  case reports.parse_size(size_text), int.parse(count_text) {
    Error(reason), _ -> message(400, reason)
    _, Error(_) -> message(400, "count must be an integer")
    _, Ok(count) if count < 1 || count > 1000 ->
      message(400, "count must be between 1 and 1000")
    Ok(size), Ok(count) -> {
      let new_job = worker.job(report_worker, size) |> job.with_max_attempts(5)
      case
        quasar.enqueue_many(
          list.repeat(new_job, count),
          runtime,
          on: reports.queue,
        )
      {
        Ok(ids) ->
          json_response(
            202,
            json.object([
              #(
                "job_ids",
                json.array(ids, fn(id) { json.string(job.id_to_string(id)) }),
              ),
            ]),
          )
        Error(_) -> message(503, "job store unavailable")
      }
    }
  }
}

fn get_job(connection, id) {
  let view =
    metrics.measure("http_status_query", fn() {
      database.get_job_view(connection, id, reports.queue)
    })
  case view {
    Error(reason) ->
      json_response(
        503,
        json.object([
          #("message", json.string("job store unavailable")),
          #("code", json.string(database.error_code(reason))),
        ]),
      )
    Ok(option.None) -> message(404, "job not found")
    Ok(option.Some(item)) ->
      json_response(
        200,
        json.object([
          #("job_id", json.string(int.to_string(id))),
          #("status", json.string(item.status)),
          #("attempt", json.int(item.attempt)),
          #("inserted_at", json.int(item.inserted_at)),
          #("completed_at", json.nullable(item.completed_at, json.int)),
          #("total", json.nullable(item.total, json.int)),
          #("processed_by", json.nullable(item.processed_by, json.string)),
        ]),
      )
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
