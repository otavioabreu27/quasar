-module(quasar_jobs_ffi).
-export([run_safely/1, monotonic_milliseconds/0, system_milliseconds/0, next_id/0, unique_id/0]).
-export([run_fenced/2]).

%% The parent holds the pool's capacity. Deadlines remain enforceable while a
%% heartbeat is blocked in a query. Child death also takes its linked heartbeat
%% down. A watcher prevents orphan effects if the pool worker itself is killed.
run_fenced(Run, ExpiresAt) ->
    Parent = self(),
    Tag = make_ref(),
    {Child, Mon} = spawn_monitor(fun() ->
        Self = self(),
        Watcher = spawn(fun() ->
            M = monitor(process, Parent),
            C = monitor(process, Self),
            receive
                {'DOWN', M, process, Parent, _} -> exit(Self, kill);
                {'DOWN', C, process, Self, _} -> ok
            end
        end),
        try
            Outcome = run_safely(fun() ->
                Run(fun(Expiry) -> Parent ! {Tag, extended, Expiry}, nil end)
            end),
            Parent ! {Tag, result, Outcome}
        after exit(Watcher, kill) end
    end),
    fenced_wait(Tag, Child, Mon, ExpiresAt).

fenced_wait(Tag, Child, Mon, ExpiresAt) ->
    Remaining = max(0, ExpiresAt - system_milliseconds()),
    receive
        {Tag, extended, NewExpiry} -> fenced_wait(Tag, Child, Mon, NewExpiry);
        {Tag, result, Outcome} -> demonitor(Mon, [flush]), Outcome;
        {'DOWN', Mon, process, Child, _} -> {error, nil}
    after Remaining ->
        exit(Child, kill),
        receive {'DOWN', Mon, process, Child, _} -> ok end,
        {error, nil}
    end.

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
