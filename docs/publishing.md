# Publishing preflight

The candidate is 0.3.0 for all four Quasar packages. Constellation stays in the
`>= 0.2.0 and < 0.3.0` range; no new Constellation publication is required.
Publishing is manual. Preparing versions or exporting a tarball does not publish it.

1. Review the 0.3.0 changelog and explicit PostgreSQL reaper startup requirement.
2. Confirm `quasar_jobs` depends on `constellation = ">= 0.2.0 and < 0.3.0"` and
   contains no path, Git, submodule, or copied Constellation dependency.
3. Run the PostgreSQL integration suite against two application instances.
4. Re-run the controlled Mist benchmark baseline recorded in
   `docs/benchmarks.md` on release hardware.
5. Validate all four package tarballs and inspect their contents.
6. Publish `quasar_jobs` first, then `quasar_postgres`, `quasar_mist`, and
   `quasar_sqlite`. Adapter dependencies must use `>= 0.3.0 and < 0.4.0`,
   never the temporary local development path.

No repository automation publishes any package.

Local integration validation can temporarily link the Quasar core. Before
publishing, restore Hex requirements in the adapters and, after publishing the
core, run `gleam deps update quasar_jobs` to update the locked core version;
never ship a local-path release dependency. Run
`gleam clean` when switching Hex/local dependencies to avoid stale Erlang FFI.
A path-linked test run is not a clean Hex release validation.

Use `mise x rebar@3.27.0 -- gleam publish` in each package directory. Do not use
`--replace`: 0.2.0 is already published and must not be overwritten. Finish the
core publication before resolving/publishing the adapters. Keep credentials out
of scripts, commits, terminal transcripts and chat.
