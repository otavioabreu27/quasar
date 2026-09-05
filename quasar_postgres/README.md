# quasar_postgres

Optional cluster-coordinated PostgreSQL Store for Quasar.
Requires `quasar_jobs >= 0.1.0 and < 0.2.0`.

See [Store contract and ownership](../docs/stores.md) and
[publishing prerequisites](../docs/publishing.md).

The migration in `priv/001_create_quasar_jobs.sql` is loaded at runtime;
it must be included in the published package. Never edit an applied migration
for future schema changes: add a versioned migration and runner step instead.
