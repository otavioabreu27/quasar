//// Cluster-safe PostgreSQL durable Store.

import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import pog.{type Connection, type QueryError, type Value}
import quasar_jobs/job.{type Job, type JobId, type NewJob}
import quasar_jobs/store.{type Store}
import quasar_jobs/store/codec
import quasar_jobs/worker

const columns = "
q.id, q.queue, q.worker, q.payload, q.status, q.priority, q.attempt,
q.max_attempts, q.available_at, q.inserted_at, q.attempted_at,
q.completed_at, q.lease_owner, q.lease_expires_at, q.error_kind,
q.error_message
"

pub type MigrationError {
  MigrationUnavailable
  Database(QueryError)
}

type Migration {
  Migration(version: Int, name: String, sql: String, online: Bool)
}

type TransactionalWorkerError {
  BusinessError(String)
  CompletionError(store.Error)
}

/// Builds a worker whose PostgreSQL business effect and fenced job completion
/// commit in the same transaction.
///
/// This is intentionally PostgreSQL-specific. Use a regular `worker.new` when
/// effects live outside this database and make those effects idempotent.
pub fn transactional_worker(
  connection: Connection,
  name name: String,
  encode encode: fn(input) -> String,
  decode decode: fn(String) -> Result(input, String),
  perform perform: fn(input, worker.Context, Connection) -> Result(Nil, String),
) -> worker.Worker(input) {
  worker.new(name:, encode:, decode:, perform: fn(input, context) {
    case
      pog.transaction(connection, fn(transaction) {
        use _ <- result.try(
          perform(input, context, transaction)
          |> result.map_error(BusinessError),
        )
        complete_one(
          transaction,
          context.execution_token,
          system_milliseconds(),
        )
        |> result.map_error(CompletionError)
      })
    {
      Ok(_) -> Ok(Nil)
      Error(pog.TransactionRolledBack(BusinessError(message))) -> Error(message)
      Error(pog.TransactionRolledBack(CompletionError(_))) ->
        Error("could not persist transactional job completion")
      Error(pog.TransactionQueryError(_)) ->
        Error("PostgreSQL transaction failed")
    }
  })
  |> worker.with_managed_completion
}

/// Builds a Store backed by an existing Pog connection pool.
///
/// The Pog pool remains application-owned and is not stopped when this Store
/// closes. Run `migrate` once before starting Quasar.
pub fn new(connection: Connection) -> Store {
  store.from_operations_with_all_batches(
    insert: fn(new_job, queue, available_at, now) {
      insert(connection, new_job, queue, available_at, now)
    },
    insert_many: fn(jobs, queue, available_at, now) {
      insert_many(connection, jobs, queue, available_at, now)
    },
    get: fn(id) { get(connection, id) },
    claim: fn(queue, limit, owner, now, lease_ms) {
      claim(connection, queue, limit, owner, now, lease_ms)
    },
    complete: fn(id, now) { complete_one(connection, id, now) },
    complete_many: fn(completions) { complete_many(connection, completions) },
    fail: fn(id, error, available_at) {
      fail_one(connection, id, error, available_at)
    },
    fail_many: fn(failures) { fail_many(connection, failures) },
    cancel: fn(id) {
      admin_update(
        connection,
        id,
        "cancel",
        "UPDATE quasar_jobs q SET status = 'cancelled', finished_at = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint, lease_owner = NULL, lease_expires_at = NULL WHERE q.id = $1 AND q.status NOT IN ('completed', 'discarded', 'cancelled') RETURNING "
          <> columns,
        [pog.int(job.id_value(id))],
      )
    },
    retry: fn(id, now) {
      admin_update(
        connection,
        id,
        "retry",
        "UPDATE quasar_jobs q SET status = 'available', attempt = 0, available_at = $1, attempted_at = NULL, completed_at = NULL, finished_at = NULL, lease_owner = NULL, lease_expires_at = NULL, error_kind = NULL, error_message = NULL WHERE q.id = $2 AND q.status IN ('discarded', 'cancelled', 'retryable') RETURNING "
          <> columns,
        [pog.int(now), pog.int(job.id_value(id))],
      )
    },
    renew_lease: fn(id, expires_at) {
      update_one(
        connection,
        "UPDATE quasar_jobs q SET lease_expires_at = $1 WHERE q.id = $2 AND q.status = 'executing' AND q.lease_owner = $3 AND q.attempt = $4 RETURNING "
          <> columns,
        [
          pog.int(expires_at),
          pog.int(job.id_value(job.token_id(id))),
          pog.text(job.token_owner(id)),
          pog.int(job.token_generation(id)),
        ],
      )
    },
    close: fn() { Ok(Nil) },
  )
}

/// Applies all pending packaged migrations in version order.
///
/// The migration history and a shared advisory lock make concurrent startup
/// safe when multiple application instances share one database. Migrations are
/// applied strictly in version order; online migrations run outside a database
/// transaction so PostgreSQL can execute `CONCURRENTLY` statements.
pub fn migrate(connection: Connection) -> Result(Nil, MigrationError) {
  use packaged <- result.try(
    migration_files() |> result.map_error(fn(_) { MigrationUnavailable }),
  )
  let migrations =
    list.map(packaged, fn(item) { Migration(item.0, item.1, item.2, item.3) })
  list.try_each(migrations, fn(migration) {
    case migration.online {
      True ->
        apply_online_migration(
          connection,
          migration.version,
          migration.name,
          string.trim(migration.sql),
        )
        |> result.map_error(fn(_) { MigrationUnavailable })
      False -> apply_transactional_migration(connection, migration)
    }
  })
}

fn apply_transactional_migration(connection: Connection, migration: Migration) {
  case pog.transaction(connection, fn(tx) { migrate_locked(tx, [migration]) }) {
    Ok(Nil) -> Ok(Nil)
    Error(pog.TransactionQueryError(error)) -> Error(Database(error))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

fn migrate_locked(connection: Connection, migrations: List(Migration)) {
  use _ <- result.try(
    acquire_migration_lock(connection) |> result.map_error(Database),
  )
  use _ <- result.try(
    execute_nil(
      connection,
      "CREATE TABLE IF NOT EXISTS quasar_jobs_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
      [],
    )
    |> result.map_error(Database),
  )
  use applied <- result.try(
    applied_migration_versions(connection) |> result.map_error(Database),
  )
  list.try_each(migrations, fn(migration) {
    case list.contains(applied, migration.version) {
      True -> Ok(Nil)
      False -> apply_migration(connection, migration)
    }
  })
}

fn acquire_migration_lock(connection: Connection) -> Result(Nil, QueryError) {
  // PostgreSQL's advisory lock returns void. Cast it so Pog can decode the row.
  let decoder = {
    use _ <- decode.field(0, decode.string)
    decode.success(Nil)
  }
  let query =
    pog.query("SELECT pg_advisory_xact_lock(1903527851)::text")
    |> pog.returning(decoder)
  pog.execute(query, on: connection) |> result.replace(Nil)
}

fn applied_migration_versions(
  connection: Connection,
) -> Result(List(Int), QueryError) {
  let decoder = {
    use version <- decode.field(0, decode.int)
    decode.success(version)
  }
  let query =
    pog.query("SELECT version FROM quasar_jobs_migrations ORDER BY version")
    |> pog.returning(decoder)
  pog.execute(query, on: connection)
  |> result.map(fn(returned) { returned.rows })
}

fn apply_migration(connection: Connection, migration: Migration) {
  use _ <- result.try(
    execute_nil(connection, string.trim(migration.sql), [])
    |> result.map_error(Database),
  )
  execute_nil(
    connection,
    "INSERT INTO quasar_jobs_migrations (version, name) VALUES ($1, $2)",
    [pog.int(migration.version), pog.text(migration.name)],
  )
  |> result.map_error(Database)
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
  let query =
    pog.query(
      "WITH inserted AS (INSERT INTO quasar_jobs (queue, worker, payload, status, priority, attempt, max_attempts, available_at, inserted_at) VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8) RETURNING id, queue), notified AS (SELECT id, pg_notify('quasar_jobs', queue) FROM inserted) SELECT id FROM notified",
    )
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(job.worker_name(new_job)))
    |> pog.parameter(pog.text(job.encoded_payload(new_job)))
    |> pog.parameter(pog.text(status))
    |> pog.parameter(pog.int(job.new_job_priority(new_job)))
    |> pog.parameter(pog.int(job.new_job_max_attempts(new_job)))
    |> pog.parameter(pog.int(available_at))
    |> pog.parameter(pog.int(now))
    |> pog.returning(id_decoder)
  use returned <- result.try(
    pog.execute(query, on: connection)
    |> result.map_error(fn(_) { store.Unavailable }),
  )
  case returned.rows {
    [id] -> Ok(job.new_id(id))
    _ -> Error(store.Unavailable)
  }
}

fn insert_many(
  connection: Connection,
  jobs: List(NewJob),
  queue: String,
  available_at: Int,
  now: Int,
) -> Result(List(JobId), store.Error) {
  case jobs {
    [] -> Ok([])
    jobs -> {
      let status = case available_at > now {
        True -> "scheduled"
        False -> "available"
      }
      let id_decoder = {
        use id <- decode.field(0, decode.int)
        decode.success(job.new_id(id))
      }
      let query =
        pog.query(
          "WITH input AS (SELECT * FROM unnest($2::text[], $3::text[], $4::integer[], $5::integer[]) WITH ORDINALITY AS i(worker, payload, priority, max_attempts, ordinal)), inserted AS (INSERT INTO quasar_jobs (queue, worker, payload, status, priority, attempt, max_attempts, available_at, inserted_at) SELECT $1, worker, payload, $6, priority, 0, max_attempts, $7, $8 FROM input ORDER BY ordinal RETURNING id), notified AS (SELECT pg_notify('quasar_jobs', $1) FROM inserted LIMIT 1) SELECT id FROM inserted WHERE (SELECT count(*) FROM notified) >= 0 ORDER BY id",
        )
        |> pog.parameter(pog.text(queue))
        |> pog.parameter(pog.array(pog.text, list.map(jobs, job.worker_name)))
        |> pog.parameter(pog.array(
          pog.text,
          list.map(jobs, job.encoded_payload),
        ))
        |> pog.parameter(pog.array(
          pog.int,
          list.map(jobs, job.new_job_priority),
        ))
        |> pog.parameter(pog.array(
          pog.int,
          list.map(jobs, job.new_job_max_attempts),
        ))
        |> pog.parameter(pog.text(status))
        |> pog.parameter(pog.int(available_at))
        |> pog.parameter(pog.int(now))
        |> pog.returning(id_decoder)
      use returned <- result.try(
        pog.execute(query, on: connection)
        |> result.map_error(fn(_) { store.Unavailable }),
      )
      case list.length(returned.rows) == list.length(jobs) {
        True -> Ok(returned.rows)
        False -> Error(store.Unavailable)
      }
    }
  }
}

fn get(connection: Connection, id: JobId) -> Result(Job, store.Error) {
  use rows <- result.try(
    execute_jobs(
      connection,
      "SELECT " <> columns <> " FROM quasar_jobs q WHERE q.id = $1",
      [pog.int(job.id_value(id))],
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
  pog.transaction(connection, fn(tx) {
    use _ <- result.try(
      execute_nil(
        tx,
        "UPDATE quasar_jobs SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END, available_at = $1::bigint, finished_at = CASE WHEN attempt >= max_attempts THEN $1::bigint ELSE NULL END, lease_owner = NULL, lease_expires_at = NULL, error_kind = 'lease_expired', error_message = 'execution lease expired' WHERE status = 'executing' AND lease_expires_at <= $2",
        [pog.int(now), pog.int(now)],
      )
      |> result.map_error(fn(_) { store.Unavailable }),
    )
    execute_jobs(
      tx,
      "WITH claimable AS (SELECT id FROM quasar_jobs WHERE queue = $1 AND status IN ('available', 'scheduled', 'retryable') AND available_at <= $2 ORDER BY priority DESC, id FOR UPDATE SKIP LOCKED LIMIT $3) UPDATE quasar_jobs q SET status = 'executing', attempt = attempt + 1, attempted_at = $2, lease_owner = $4, lease_expires_at = $5 FROM claimable WHERE q.id = claimable.id RETURNING "
        <> columns,
      [
        pog.text(queue),
        pog.int(now),
        pog.int(limit),
        pog.text(owner),
        pog.int(now + lease_ms),
      ],
    )
  })
  |> result.map_error(fn(_) { store.Unavailable })
}

fn complete_one(connection, token, completed_at) {
  update_one(
    connection,
    "UPDATE quasar_jobs q SET status = 'completed', completed_at = $1, finished_at = $1, lease_owner = NULL, lease_expires_at = NULL, error_kind = NULL, error_message = NULL WHERE q.id = $2 AND q.status = 'executing' AND q.lease_owner = $3 AND q.attempt = $4 RETURNING "
      <> columns,
    [
      pog.int(completed_at),
      pog.int(job.id_value(job.token_id(token))),
      pog.text(job.token_owner(token)),
      pog.int(job.token_generation(token)),
    ],
  )
}

fn fail_one(connection, token, error, available_at) {
  update_one(
    connection,
    "UPDATE quasar_jobs q SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END, available_at = $1, finished_at = CASE WHEN attempt >= max_attempts THEN (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint ELSE NULL END, lease_owner = NULL, lease_expires_at = NULL, error_kind = $2, error_message = $3 WHERE q.id = $4 AND q.status = 'executing' AND q.lease_owner = $5 AND q.attempt = $6 RETURNING "
      <> columns,
    [
      pog.int(available_at),
      pog.text(job.error_kind(error)),
      pog.text(job.error_message(error)),
      pog.int(job.id_value(job.token_id(token))),
      pog.text(job.token_owner(token)),
      pog.int(job.token_generation(token)),
    ],
  )
}

fn complete_many(
  connection: Connection,
  completions: List(store.Completion),
) -> Result(List(Job), store.Error) {
  case completions {
    [] -> Ok([])
    completions ->
      pog.transaction(connection, fn(tx) {
        let tokens = list.map(completions, store.completion_token)
        use updated <- result.try(
          execute_jobs(
            tx,
            "WITH completion AS (SELECT * FROM unnest($1::bigint[], $2::text[], $3::integer[], $4::bigint[]) AS c(id, owner, generation, completed_at)) UPDATE quasar_jobs q SET status = 'completed', completed_at = c.completed_at, finished_at = c.completed_at, lease_owner = NULL, lease_expires_at = NULL, error_kind = NULL, error_message = NULL FROM completion c WHERE q.id = c.id AND q.status = 'executing' AND q.lease_owner = c.owner AND q.attempt = c.generation RETURNING "
              <> columns,
            [
              pog.array(
                pog.int,
                list.map(tokens, fn(token) { job.id_value(job.token_id(token)) }),
              ),
              pog.array(pog.text, list.map(tokens, job.token_owner)),
              pog.array(pog.int, list.map(tokens, job.token_generation)),
              pog.array(pog.int, list.map(completions, store.completion_time)),
            ],
          ),
        )
        case list.length(updated) == list.length(completions) {
          True -> Ok(updated)
          False -> Error(store.StaleExecution)
        }
      })
      |> transaction_result
  }
}

fn fail_many(
  connection: Connection,
  failures: List(store.Failure),
) -> Result(List(Job), store.Error) {
  case failures {
    [] -> Ok([])
    failures ->
      pog.transaction(connection, fn(tx) {
        let tokens = list.map(failures, store.failure_token)
        let errors = list.map(failures, store.failure_error)
        use updated <- result.try(
          execute_jobs(
            tx,
            "WITH failure AS (SELECT * FROM unnest($1::bigint[], $2::text[], $3::integer[], $4::bigint[], $5::text[], $6::text[]) AS f(id, owner, generation, available_at, error_kind, error_message)) UPDATE quasar_jobs q SET status = CASE WHEN q.attempt >= q.max_attempts THEN 'discarded' ELSE 'retryable' END, available_at = f.available_at, finished_at = CASE WHEN q.attempt >= q.max_attempts THEN (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint ELSE NULL END, lease_owner = NULL, lease_expires_at = NULL, error_kind = f.error_kind, error_message = f.error_message FROM failure f WHERE q.id = f.id AND q.status = 'executing' AND q.lease_owner = f.owner AND q.attempt = f.generation RETURNING "
              <> columns,
            [
              pog.array(
                pog.int,
                list.map(tokens, fn(token) { job.id_value(job.token_id(token)) }),
              ),
              pog.array(pog.text, list.map(tokens, job.token_owner)),
              pog.array(pog.int, list.map(tokens, job.token_generation)),
              pog.array(pog.int, list.map(failures, store.failure_available_at)),
              pog.array(pog.text, list.map(errors, job.error_kind)),
              pog.array(pog.text, list.map(errors, job.error_message)),
            ],
          ),
        )
        case list.length(updated) == list.length(failures) {
          True -> Ok(updated)
          False -> Error(store.StaleExecution)
        }
      })
      |> transaction_result
  }
}

fn transaction_result(result) {
  case result {
    Ok(value) -> Ok(value)
    Error(pog.TransactionRolledBack(error)) -> Error(error)
    Error(pog.TransactionQueryError(_)) -> Error(store.Unavailable)
  }
}

fn update_one(
  connection: Connection,
  sql: String,
  parameters: List(Value),
) -> Result(Job, store.Error) {
  use rows <- result.try(execute_jobs(connection, sql, parameters))
  case rows {
    [item] -> Ok(item)
    _ -> Error(store.StaleExecution)
  }
}

fn execute_jobs(
  connection: Connection,
  sql: String,
  parameters: List(Value),
) -> Result(List(Job), store.Error) {
  let query =
    list.fold(
      parameters,
      pog.query(sql) |> pog.returning(codec.row()),
      fn(query, parameter) { pog.parameter(query, parameter) },
    )
  pog.execute(query, on: connection)
  |> result.map(fn(returned) { returned.rows })
  |> result.map_error(fn(_) { store.Unavailable })
}

fn execute_nil(
  connection: Connection,
  sql: String,
  parameters: List(Value),
) -> Result(Nil, QueryError) {
  let query = list.fold(parameters, pog.query(sql), pog.parameter)
  pog.execute(query, on: connection) |> result.replace(Nil)
}

fn first(items: List(Job)) -> Result(Job, store.Error) {
  list.first(items) |> result.map_error(fn(_) { store.NotFound })
}

@external(erlang, "quasar_postgres_ffi", "migration_files")
fn migration_files() -> Result(List(#(Int, String, String, Bool)), Nil)

@external(erlang, "quasar_postgres_ffi", "apply_online_migration")
fn apply_online_migration(
  connection: Connection,
  version: Int,
  name: String,
  sql: String,
) -> Result(Nil, Nil)

// Administrative commands are not execution acknowledgements. A failed
// predicate reports the observed state (or NotFound), never a stale lease.
fn admin_update(
  connection: Connection,
  id: JobId,
  operation: String,
  sql: String,
  parameters: List(Value),
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

@external(erlang, "quasar_jobs_ffi", "system_milliseconds")
fn system_milliseconds() -> Int
