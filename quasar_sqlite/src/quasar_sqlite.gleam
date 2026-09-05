//// Single-node SQLite durable Store.

import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import gleam/result
import quasar_jobs/job.{
  type ExecutionToken, type Job, type JobError, type JobId, type NewJob,
}
import quasar_jobs/store.{type Store}
import quasar_jobs/store/codec
import sqlight.{type Connection}

pub type OpenError {
  Database(sqlight.Error)
  Process(actor.StartError)
  MigrationUnavailable
}

type State {
  State(connection: Connection)
}

type Message {
  Insert(NewJob, String, Int, Int, Subject(Result(JobId, store.Error)))
  Get(JobId, Subject(Result(Job, store.Error)))
  Claim(String, Int, String, Int, Int, Subject(Result(List(Job), store.Error)))
  Complete(ExecutionToken, Int, Subject(Result(Job, store.Error)))
  Fail(ExecutionToken, JobError, Int, Subject(Result(Job, store.Error)))
  Cancel(JobId, Subject(Result(Job, store.Error)))
  Retry(JobId, Int, Subject(Result(Job, store.Error)))
  Renew(ExecutionToken, Int, Subject(Result(Job, store.Error)))
  Close(Subject(Result(Nil, store.Error)))
}

const timeout = 5000

const columns = "
id, queue, worker, payload, status, priority, attempt, max_attempts,
available_at, inserted_at, attempted_at, completed_at, lease_owner,
lease_expires_at, error_kind, error_message
"

/// Opens and migrates a SQLite Store.
///
/// SQLite is supported for a single Quasar node. It is not a cluster
/// coordination mechanism.
pub fn open(path: String) -> Result(Store, OpenError) {
  use migration <- result.try(
    migration_sql() |> result.map_error(fn(_) { MigrationUnavailable }),
  )
  use connection <- result.try(sqlight.open(path) |> result.map_error(Database))
  case sqlight.exec(migration, connection) {
    Error(error) -> {
      let _ = sqlight.close(connection)
      Error(Database(error))
    }
    Ok(_) -> start(connection)
  }
}

fn start(connection: Connection) {
  let builder =
    actor.new(State(connection:)) |> actor.on_message(handle_message)
  case actor.start(builder) {
    Error(error) -> {
      let _ = sqlight.close(connection)
      Error(Process(error))
    }
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(from_subject(started.data))
    }
  }
}

fn from_subject(subject: Subject(Message)) -> Store {
  store.from_operations(
    insert: fn(new_job, queue, available_at, now) {
      call(subject, fn(reply) {
        Insert(new_job, queue, available_at, now, reply)
      })
    },
    get: fn(id) { call(subject, fn(reply) { Get(id, reply) }) },
    claim: fn(queue, limit, owner, now, lease_ms) {
      call(subject, fn(reply) {
        Claim(queue, limit, owner, now, lease_ms, reply)
      })
    },
    complete: fn(id, now) {
      call(subject, fn(reply) { Complete(id, now, reply) })
    },
    fail: fn(id, error, available_at) {
      call(subject, fn(reply) { Fail(id, error, available_at, reply) })
    },
    cancel: fn(id) { call(subject, fn(reply) { Cancel(id, reply) }) },
    retry: fn(id, now) { call(subject, fn(reply) { Retry(id, now, reply) }) },
    renew_lease: fn(id, expires_at) {
      call(subject, fn(reply) { Renew(id, expires_at, reply) })
    },
    close: fn() { call(subject, Close) },
  )
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    Insert(new_job, queue, available_at, now, reply) -> {
      process.send(
        reply,
        insert(state.connection, new_job, queue, available_at, now),
      )
      actor.continue(state)
    }
    Get(id, reply) -> {
      process.send(reply, get(state.connection, id))
      actor.continue(state)
    }
    Claim(queue, limit, owner, now, lease_ms, reply) -> {
      process.send(
        reply,
        claim(state.connection, queue, limit, owner, now, lease_ms),
      )
      actor.continue(state)
    }
    Complete(id, now, reply) -> {
      process.send(
        reply,
        update_one(
          state.connection,
          "UPDATE quasar_jobs SET status = 'completed', completed_at = ?, lease_owner = NULL, lease_expires_at = NULL, error_kind = NULL, error_message = NULL WHERE id = ? AND status = 'executing' AND lease_owner = ? AND attempt = ? RETURNING "
            <> columns,
          [
            sqlight.int(now),
            sqlight.int(job.id_value(job.token_id(id))),
            sqlight.text(job.token_owner(id)),
            sqlight.int(job.token_generation(id)),
          ],
        ),
      )
      actor.continue(state)
    }
    Fail(id, error, available_at, reply) -> {
      process.send(
        reply,
        update_one(
          state.connection,
          "UPDATE quasar_jobs SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END, available_at = ?, lease_owner = NULL, lease_expires_at = NULL, error_kind = ?, error_message = ? WHERE id = ? AND status = 'executing' AND lease_owner = ? AND attempt = ? RETURNING "
            <> columns,
          [
            sqlight.int(available_at),
            sqlight.text(job.error_kind(error)),
            sqlight.text(job.error_message(error)),
            sqlight.int(job.id_value(job.token_id(id))),
            sqlight.text(job.token_owner(id)),
            sqlight.int(job.token_generation(id)),
          ],
        ),
      )
      actor.continue(state)
    }
    Cancel(id, reply) -> {
      process.send(
        reply,
        admin_update(
          state.connection,
          id,
          "cancel",
          "UPDATE quasar_jobs SET status = 'cancelled', lease_owner = NULL, lease_expires_at = NULL WHERE id = ? AND status NOT IN ('completed', 'discarded', 'cancelled') RETURNING "
            <> columns,
          [sqlight.int(job.id_value(id))],
        ),
      )
      actor.continue(state)
    }
    Retry(id, now, reply) -> {
      process.send(
        reply,
        admin_update(
          state.connection,
          id,
          "retry",
          "UPDATE quasar_jobs SET status = 'available', attempt = 0, available_at = ?, attempted_at = NULL, completed_at = NULL, lease_owner = NULL, lease_expires_at = NULL, error_kind = NULL, error_message = NULL WHERE id = ? AND status IN ('discarded', 'cancelled', 'retryable') RETURNING "
            <> columns,
          [sqlight.int(now), sqlight.int(job.id_value(id))],
        ),
      )
      actor.continue(state)
    }
    Renew(id, expires_at, reply) -> {
      process.send(
        reply,
        update_one(
          state.connection,
          "UPDATE quasar_jobs SET lease_expires_at = ? WHERE id = ? AND status = 'executing' AND lease_owner = ? AND attempt = ? RETURNING "
            <> columns,
          [
            sqlight.int(expires_at),
            sqlight.int(job.id_value(job.token_id(id))),
            sqlight.text(job.token_owner(id)),
            sqlight.int(job.token_generation(id)),
          ],
        ),
      )
      actor.continue(state)
    }
    Close(reply) -> {
      process.send(
        reply,
        sqlight.close(state.connection)
          |> result.map_error(fn(_) { store.Unavailable }),
      )
      actor.stop()
    }
  }
}

fn insert(
  connection: Connection,
  new_job: NewJob,
  queue: String,
  available_at: Int,
  now: Int,
) -> Result(JobId, store.Error) {
  use _ <- result.try(
    job.validate(new_job) |> result.map_error(store.InvalidTransition),
  )
  let status = case available_at > now {
    True -> "scheduled"
    False -> "available"
  }
  let id_decoder = {
    use id <- decode.field(0, decode.int)
    decode.success(id)
  }
  use rows <- result.try(
    sqlight.query(
      "INSERT INTO quasar_jobs (queue, worker, payload, status, priority, attempt, max_attempts, available_at, inserted_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?) RETURNING id",
      on: connection,
      with: [
        sqlight.text(queue),
        sqlight.text(job.worker_name(new_job)),
        sqlight.text(job.encoded_payload(new_job)),
        sqlight.text(status),
        sqlight.int(job.new_job_priority(new_job)),
        sqlight.int(job.new_job_max_attempts(new_job)),
        sqlight.int(available_at),
        sqlight.int(now),
      ],
      expecting: id_decoder,
    )
    |> result.map_error(fn(_) { store.Unavailable }),
  )
  case rows {
    [id] -> Ok(job.new_id(id))
    _ -> Error(store.Unavailable)
  }
}

fn get(connection: Connection, id: JobId) -> Result(Job, store.Error) {
  use rows <- result.try(
    query_jobs(
      connection,
      "SELECT " <> columns <> " FROM quasar_jobs WHERE id = ?",
      [sqlight.int(job.id_value(id))],
    ),
  )
  first(rows)
}

fn claim(
  connection: Connection,
  queue: String,
  limit: Int,
  owner: String,
  now: Int,
  lease_ms: Int,
) -> Result(List(Job), store.Error) {
  use _ <- result.try(
    sqlight.query(
      "UPDATE quasar_jobs SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END, available_at = ?, lease_owner = NULL, lease_expires_at = NULL, error_kind = 'lease_expired', error_message = 'execution lease expired' WHERE status = 'executing' AND lease_expires_at <= ?",
      on: connection,
      with: [sqlight.int(now), sqlight.int(now)],
      expecting: decode.success(Nil),
    )
    |> result.map_error(fn(_) { store.Unavailable }),
  )
  query_jobs(
    connection,
    "UPDATE quasar_jobs SET status = 'executing', attempt = attempt + 1, attempted_at = ?, lease_owner = ?, lease_expires_at = ? WHERE id IN (SELECT id FROM quasar_jobs WHERE queue = ? AND status IN ('available', 'scheduled', 'retryable') AND available_at <= ? ORDER BY priority DESC, id LIMIT ?) RETURNING "
      <> columns,
    [
      sqlight.int(now),
      sqlight.text(owner),
      sqlight.int(now + lease_ms),
      sqlight.text(queue),
      sqlight.int(now),
      sqlight.int(limit),
    ],
  )
}

fn update_one(
  connection: Connection,
  sql: String,
  parameters: List(sqlight.Value),
) -> Result(Job, store.Error) {
  use rows <- result.try(query_jobs(connection, sql, parameters))
  case rows {
    [item] -> Ok(item)
    _ -> Error(store.StaleExecution)
  }
}

fn query_jobs(
  connection: Connection,
  sql: String,
  parameters: List(sqlight.Value),
) -> Result(List(Job), store.Error) {
  sqlight.query(sql, on: connection, with: parameters, expecting: codec.row())
  |> result.map_error(fn(_) { store.Unavailable })
}

fn first(items: List(Job)) -> Result(Job, store.Error) {
  list.first(items) |> result.map_error(fn(_) { store.NotFound })
}

fn call(
  subject: Subject(Message),
  make_message: fn(Subject(Result(value, store.Error))) -> Message,
) -> Result(value, store.Error) {
  let reply = process.new_subject()
  process.send(subject, make_message(reply))
  case process.receive(reply, within: timeout) {
    Ok(result) -> result
    Error(_) -> Error(store.Timeout)
  }
}

@external(erlang, "quasar_sqlite_ffi", "migration_sql")
fn migration_sql() -> Result(String, Nil)

// Administrative commands are not execution acknowledgements. A failed
// predicate reports the observed state (or NotFound), never a stale lease.
fn admin_update(
  connection: Connection,
  id: JobId,
  operation: String,
  sql: String,
  parameters: List(sqlight.Value),
) -> Result(Job, store.Error) {
  case update_one(connection, sql, parameters) {
    Error(store.StaleExecution) -> {
      use item <- result.try(get(connection, id))
      Error(
        store.InvalidTransition(job.InvalidTransition(
          job.status(item),
          operation,
        )),
      )
    }
    outcome -> outcome
  }
}
