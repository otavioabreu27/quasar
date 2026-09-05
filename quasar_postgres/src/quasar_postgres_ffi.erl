-module(quasar_postgres_ffi).
-export([migration_files/0]).

migration_files() ->
    Priv = code:priv_dir(quasar_postgres),
    case file:list_dir(Priv) of
        {ok, Files} -> read_migrations(Priv, lists:sort(Files), []);
        {error, _} -> {error, nil}
    end.

read_migrations(_Priv, [], Acc) ->
    {ok, lists:reverse(Acc)};
read_migrations(Priv, [File | Rest], Acc) ->
    case re:run(File, "^([0-9]+)_(.+)\\.sql$", [{capture, [1, 2], list}]) of
        {match, [VersionText, NameText]} ->
            case file:read_file(filename:join(Priv, File)) of
                {ok, Sql} ->
                    Migration = {
                        list_to_integer(VersionText),
                        unicode:characters_to_binary(NameText),
                        Sql
                    },
                    read_migrations(Priv, Rest, [Migration | Acc]);
                {error, _} ->
                    {error, nil}
            end;
        nomatch ->
            read_migrations(Priv, Rest, Acc)
    end.
