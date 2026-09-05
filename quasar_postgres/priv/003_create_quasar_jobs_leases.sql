CREATE INDEX IF NOT EXISTS quasar_jobs_leases
  ON quasar_jobs (status, lease_expires_at);
