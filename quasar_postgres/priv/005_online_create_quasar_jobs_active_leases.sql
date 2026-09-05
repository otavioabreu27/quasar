CREATE INDEX CONCURRENTLY IF NOT EXISTS quasar_jobs_active_leases
  ON quasar_jobs (lease_expires_at)
  WHERE status = 'executing'
