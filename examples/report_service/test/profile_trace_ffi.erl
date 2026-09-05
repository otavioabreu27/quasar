%% Test-only, OTP 27+ (json module). Never logs SQL arguments, results or credentials.
%% Timings include tracing overhead. Run an untraced control for comparison.
-module(profile_trace_ffi).
-export([start/0]).

start() ->
    MFAs = [{pgo_pool, checkout, 2},
            {pgo_handler, extended_query, 5},
            {'quasar_jobs@store', claim, 6},
            {'quasar_jobs@internal@job_executor', execute, 6}],
    lists:foreach(fun({M, _, _}) -> {module, M} = code:ensure_loaded(M) end, MFAs),
    Pid = spawn_link(fun() ->
        erlang:send_after(500, self(), snapshot),
        loop(#{}, #{})
    end),
    lists:foreach(fun(MFA) ->
        erlang:trace_pattern(MFA, [{'_', [], [{return_trace}]}], [local])
    end, MFAs),
    erlang:trace(all, true, [call, arity, monotonic_timestamp, {tracer, Pid}]),
    nil.

label({pgo_pool, checkout, 2}) -> <<"pool_checkout">>;
label({pgo_handler, extended_query, 5}) -> <<"driver_query">>;
label({'quasar_jobs@store', claim, 6}) -> <<"store_claim">>;
label({'quasar_jobs@internal@job_executor', execute, 6}) -> <<"worker_execute">>.

empty() -> #{count => 0, total_us => 0, max_us => 0, histogram => #{},
             active => 0, peak => 0, first_us => 0, last_us => 0}.

loop(Pending, Stats) ->
    receive
        {trace_ts, Pid, call, MFA, Native} ->
            Name = label(MFA),
            Time = erlang:convert_time_unit(Native, native, microsecond),
            S = maps:get(Name, Stats, empty()),
            Active = maps:get(active, S) + 1,
            First = case maps:get(first_us, S) of 0 -> Time; T -> T end,
            Next = S#{active := Active, peak := max(Active, maps:get(peak, S)), first_us := First},
            loop(Pending#{{Pid, MFA} => Time}, Stats#{Name => Next});
        {trace_ts, Pid, return_from, MFA, _Result, Native} ->
            case maps:take({Pid, MFA}, Pending) of
                error -> loop(Pending, Stats);
                {Start, Rest} ->
                    Name = label(MFA),
                    Time = erlang:convert_time_unit(Native, native, microsecond),
                    Duration = max(0, Time - Start),
                    S = maps:get(Name, Stats),
                    %% 100us bins, capped at 60s; max_us remains uncapped.
                    Bucket = min(60000000, ((Duration + 99) div 100) * 100),
                    Hist = maps:update_with(Bucket, fun(N) -> N + 1 end, 1, maps:get(histogram, S)),
                    Next = S#{count := maps:get(count, S) + 1,
                              total_us := maps:get(total_us, S) + Duration,
                              max_us := max(Duration, maps:get(max_us, S)),
                              active := maps:get(active, S) - 1,
                              last_us := Time, histogram := Hist},
                    loop(Rest, Stats#{Name => Next})
            end;
        snapshot ->
            %% Keys converted to strings so consumers can merge histograms.
            JsonStats = maps:map(fun(_, S) ->
                S#{histogram := maps:from_list([
                    {integer_to_binary(K), V} || {K, V} <- maps:to_list(maps:get(histogram, S))])}
            end, Stats),
            io:format("PROFILE ~s~n", [json:encode(JsonStats)]),
            erlang:send_after(500, self(), snapshot),
            loop(Pending, Stats)
    end.
