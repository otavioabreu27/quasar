//// Cluster-safe PostgreSQL durable Store.

import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import pog.{type Connection, type QueryError, type Value}
import quasar_jobs/job.{type Job, type JobId, type NewJob}
import quasar_jobs/store.{type Store}
import quasar_jobs/store/codec

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

/// Builds a Store backed by an existing Pog connection pool.
///
/// The Pog pool remains application-owned and is not stopped when this Store
/// closes. Run `migrate` once before starting Quasar.
pub fn new(connection: Connection) -> Store {
  store.from_operations(
    insert: fn(new_job, queue, available_at, now) {
      insert(connection, new_job, queue, available_at, now)
    },
    get: fn(id) { get(connection, id) },
    claim: fn(queue, limit, owner, now, lease_ms) {
      claim(connection, queue, limit, owner, now, lease_ms)
    },
    complete: fn(id, now) {
      update_one(
        connection,
        "UPDATE quasar_jobs q SET status = 'completed', completed_at = $1, finished_at = $1, lease_owner = NULL, lease_expires_at = NULL, error_kind = NULL, error_message = NULL WHERE q.id = $2 AND q.status = 'executing' AND q.lease_owner = $3 AND q.attempt = $4 RETURNING "
          <> columns,
        [
          pog.int(now),
          pog.int(job.id_value(job.token_id(id))),
          pog.text(job.token_owner(id)),
          pog.int(job.token_generation(id)),
        ],
      )
    },
    fail: fn(id, error, available_at) {
      update_one(
        connection,
        "UPDATE quasar_jobs q SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END, available_at = $1, finished_at = CASE WHEN attempt >= max_attempts THEN (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint ELSE NULL END, lease_owner = NULL, lease_expires_at = NULL, error_kind = $2, error_message = $3 WHERE q.id = $4 AND q.status = 'executing' AND q.lease_owner = $5 AND q.attempt = $6 RETURNING "
          <> columns,
        [
          pog.int(available_at),
          pog.text(job.error_kind(error)),
          pog.text(job.error_message(error)),
          pog.int(job.id_value(job.token_id(id))),
          pog.text(job.token_owner(id)),
          pog.int(job.token_generation(id)),
        ],
      )
    },
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
