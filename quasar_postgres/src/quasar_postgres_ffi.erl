-module(quasar_postgres_ffi).
-export([migration_sql/0]).
migration_sql() ->
    case file:read_file(filename:join(code:priv_dir(quasar_postgres), "001_create_quasar_jobs.sql")) of
        {ok, Sql} -> {ok, Sql};
        {error, _} -> {error, nil}
    end.
