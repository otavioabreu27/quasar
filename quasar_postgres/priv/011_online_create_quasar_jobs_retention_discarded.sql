CREATE INDEX CONCURRENTLY IF NOT EXISTS quasar_jobs_retention_discarded
  ON quasar_jobs (COALESCE(finished_at, attempted_at, inserted_at), id)
  WHERE status = 'discarded'
