# quasar_postgres

Optional cluster-coordinated PostgreSQL Store for Quasar.
Requires `quasar_jobs >= 0.1.0 and < 0.2.0`.

See [Store contract and ownership](../docs/stores.md) and
[publishing prerequisites](../docs/publishing.md).

Migrations in `priv/NNN_name.sql` are loaded and applied in version order inside
a transaction protected by a PostgreSQL advisory lock. Applied versions are
recorded in `quasar_jobs_migrations`, so concurrent application startup is safe
and repeated calls are idempotent. Migration files must be included in the
published package. Never edit an applied migration; add the next numbered file.
