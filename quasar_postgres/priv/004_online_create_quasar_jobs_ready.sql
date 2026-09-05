CREATE INDEX CONCURRENTLY IF NOT EXISTS quasar_jobs_ready
  ON quasar_jobs (queue, priority DESC, id)
  WHERE status IN ('available', 'scheduled', 'retryable')
