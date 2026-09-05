CREATE INDEX CONCURRENTLY IF NOT EXISTS quasar_jobs_retention_completed
  ON quasar_jobs (COALESCE(finished_at, completed_at, inserted_at), id)
  WHERE status = 'completed'
