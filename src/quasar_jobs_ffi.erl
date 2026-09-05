-module(quasar_jobs_ffi).
-export([run_safely/1, monotonic_milliseconds/0, system_milliseconds/0, next_id/0, unique_id/0]).

run_safely(Fun) ->
    try
        {ok, Fun()}
    catch
        _:_ -> {error, nil}
    end.

monotonic_milliseconds() ->
    erlang:monotonic_time(millisecond).

system_milliseconds() ->
    erlang:system_time(millisecond).

next_id() ->
    erlang:unique_integer([positive, monotonic]).

unique_id() ->
    base64:encode(crypto:strong_rand_bytes(24)).
