PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS quasar_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue TEXT NOT NULL,
  worker TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'available', 'scheduled', 'executing', 'completed',
    'retryable', 'discarded', 'cancelled'
  )),
  priority INTEGER NOT NULL DEFAULT 0,
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  available_at INTEGER NOT NULL,
  inserted_at INTEGER NOT NULL,
  attempted_at INTEGER,
  completed_at INTEGER,
  lease_owner TEXT,
  lease_expires_at INTEGER,
  error_kind TEXT,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS quasar_jobs_fetch
  ON quasar_jobs (queue, status, available_at, priority DESC, id);
CREATE INDEX IF NOT EXISTS quasar_jobs_leases
  ON quasar_jobs (status, lease_expires_at);
