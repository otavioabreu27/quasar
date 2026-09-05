CREATE TABLE IF NOT EXISTS quasar_jobs (
  id BIGSERIAL PRIMARY KEY,
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
  available_at BIGINT NOT NULL,
  inserted_at BIGINT NOT NULL,
  attempted_at BIGINT,
  completed_at BIGINT,
  lease_owner TEXT,
  lease_expires_at BIGINT,
  error_kind TEXT,
  error_message TEXT
);
