-module(quasar_sqlite_ffi).
-export([migration_sql/0]).
migration_sql() ->
    case file:read_file(filename:join(code:priv_dir(quasar_sqlite), "001_create_quasar_jobs.sql")) of
        {ok, Sql} -> {ok, Sql};
        {error, _} -> {error, nil}
    end.
