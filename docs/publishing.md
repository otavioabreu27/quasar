# Publishing preflight

Current blocker: Hex does not yet contain a `constellation` version in the
required `>= 0.2.0 and < 0.3.0` range. The prepared Constellation 0.2.0
tarball passes locally, but it has not been published automatically.

1. Publish Constellation 0.2.0 only after its formatter, tests, docs, interface
   export, and clean Hex tarball pass.
2. Confirm `quasar_jobs` depends on `constellation = ">= 0.2.0 and < 0.3.0"` and
   contains no path, Git, submodule, or copied Constellation dependency.
3. Run the PostgreSQL integration suite against two application instances.
4. Re-run the controlled Mist benchmark baseline recorded in
   `docs/benchmarks.md` on release hardware.
5. Validate all four package tarballs and inspect their contents.
6. Publish `quasar_jobs` before `quasar_mist`, `quasar_sqlite`, and
   `quasar_postgres`. Adapter dependencies must use the Hex version range,
   never the temporary local development path.

No repository automation publishes any package.

Local validation uses temporary paths to the prepared Constellation and Quasar
core. Remove every local-path manifest and restore versioned dependencies before
release. A path-linked test run is not a clean Hex release validation.
