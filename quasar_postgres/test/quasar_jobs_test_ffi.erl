-module(quasar_jobs_test_ffi).
-export([getenv/1]).
-export([crash_notifications/1]).

crash_notifications({listener, Listener}) ->
    {links, [Child]} = process_info(Listener, links),
    exit(Child, kill),
    nil.

getenv(Name) ->
    case os:getenv(binary_to_list(Name)) of
        false -> {error, nil};
        Value -> {ok, unicode:characters_to_binary(Value)}
    end.
