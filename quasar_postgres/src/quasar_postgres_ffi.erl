-module(quasar_postgres_ffi).
-export([migration_files/0, apply_online_migration/4]).

-define(MIGRATION_LOCK, 1903527851).

migration_files() ->
    Priv = code:priv_dir(quasar_postgres),
    case file:list_dir(Priv) of
        {ok, Files} -> read_migrations(Priv, lists:sort(Files), []);
        {error, _} -> {error, nil}
    end.

read_migrations(_Priv, [], Acc) ->
    {ok, lists:reverse(Acc)};
read_migrations(Priv, [File | Rest], Acc) ->
    case re:run(File, "^([0-9]{3})_([a-z0-9_]+)\\.sql$", [{capture, [1, 2], list}]) of
        {match, [VersionText, NameText]} ->
            case file:read_file(filename:join(Priv, File)) of
                {ok, Sql} ->
                    Name = unicode:characters_to_binary(NameText),
                    Migration = {
                        list_to_integer(VersionText),
                        Name,
                        Sql,
                        is_online(Name)
                    },
                    read_migrations(Priv, Rest, [Migration | Acc]);
                {error, _} ->
                    {error, nil}
            end;
        nomatch ->
            read_migrations(Priv, Rest, Acc)
    end.

is_online(<<"online_", _/binary>>) -> true;
is_online(_) -> false.

apply_online_migration(Connection, Version, Name, Sql) ->
    with_connection(Connection, fun(Conn) ->
        Lock = integer_to_binary(?MIGRATION_LOCK),
        LockSql = <<"SELECT pg_advisory_lock(", Lock/binary, ")::text">>,
        UnlockSql = <<"SELECT pg_advisory_unlock(", Lock/binary, ")">>,
        try
            #{rows := _} = pgo:query(LockSql, [], #{}, Conn),
            apply_online_locked(Conn, Version, Name, Sql)
        catch
            _:_ -> {error, nil}
        after
            _ = pgo:query(UnlockSql, [], #{}, Conn)
        end
    end).

with_connection({pool, Pool}, Run) ->
    case pgo:checkout(Pool) of
        {ok, Ref, Conn} ->
            try Run(Conn)
            after pgo:checkin(Ref, Conn)
            end;
        {error, _} ->
            {error, nil}
    end;
with_connection({single_connection, Conn}, Run) ->
    Run(Conn).

apply_online_locked(Conn, Version, Name, Sql) ->
    Check = iolist_to_binary(io_lib:format(
        "SELECT 1 FROM quasar_jobs_migrations WHERE version = ~B", [Version])),
    case pgo:query(Check, [], #{}, Conn) of
        #{rows := [_]} ->
            {ok, nil};
        #{rows := []} ->
            case pgo:query(Sql, [], #{}, Conn) of
                #{command := _} ->
                    Insert = iolist_to_binary(io_lib:format(
                        "INSERT INTO quasar_jobs_migrations (version, name) VALUES (~B, '~s') ON CONFLICT (version) DO NOTHING",
                        [Version, Name])),
                    case pgo:query(Insert, [], #{}, Conn) of
                        #{command := insert} -> {ok, nil};
                        _ -> {error, nil}
                    end;
                _ ->
                    {error, nil}
            end;
        _ ->
            {error, nil}
    end.
