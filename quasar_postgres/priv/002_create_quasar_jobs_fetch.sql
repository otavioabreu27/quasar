CREATE INDEX IF NOT EXISTS quasar_jobs_fetch
  ON quasar_jobs (queue, status, available_at, priority DESC, id);
