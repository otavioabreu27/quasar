-module(report_metrics_ffi).
-export([start/0, snapshot/0, reaper_event/1]).

start() ->
    ets:new(?MODULE, [named_table, public, set, {write_concurrency, true}]),
    ets:insert(?MODULE, {started_epoch_ms, erlang:system_time(millisecond)}),
    ets:insert(?MODULE, [{K, 0} || K <- [claims, claim_requested, claim_returned, empty_claims,
        claim_failures, jobs_started, jobs_completed, lease_renewals, persistence_failures,
        reaper_batches, reaped_jobs, reaper_failures, http_rejections, wakes_received, wakes_coalesced]]),
    case os:getenv("BENCHMARK_POOL_TRACE") of
        "1" -> start_pool_trace();
        _ -> nil
    end,
    fun record/1.

%% Opt-in diagnostic run only: checkout includes waiting for the pool. Trace
%% arguments/results are never printed or stored, only durations and counts.
%% Run an untraced control because tracing itself has overhead.
start_pool_trace() ->
    ets:insert(?MODULE, [{pool_checkout_count, 0}, {pool_checkout_failures, 0}, {<<"pool_checkout_sum_ms">>, 0}]),
    {module, pgo_pool} = code:ensure_loaded(pgo_pool),
    Pid = spawn_link(fun() -> pool_trace_loop(#{}) end),
    erlang:trace_pattern({pgo_pool, checkout, 2}, [{'_', [], [{exception_trace}]}], [local]),
    erlang:trace(all, true, [call, arity, monotonic_timestamp, {tracer, Pid}]),
    add(pool_trace_enabled, 1).

pool_trace_loop(Pending) ->
    receive
        {trace_ts, Pid, call, {pgo_pool, checkout, 2}, At} ->
            Next = Pending#{Pid => At},
            ets:insert(?MODULE, {pool_checkouts_inflight, map_size(Next)}),
            pool_trace_loop(Next);
        {trace_ts, Pid, Kind, {pgo_pool, checkout, 2}, _Ignored, At}
          when Kind =:= return_from; Kind =:= exception_from ->
            case maps:take(Pid, Pending) of
                error -> pool_trace_loop(Pending);
                {Start, Rest} ->
                    ets:insert(?MODULE, {pool_checkouts_inflight, map_size(Rest)}),
                    hist(<<"pool_checkout">>, erlang:convert_time_unit(At - Start, native, millisecond)),
                    add(pool_checkout_count, 1),
                    case Kind of exception_from -> add(pool_checkout_failures, 1); _ -> nil end,
                    pool_trace_loop(Rest)
            end
    after 1000 ->
        Next = maps:filter(fun(Pid, _) -> is_process_alive(Pid) end, Pending),
        ets:insert(?MODULE, {pool_checkouts_inflight, map_size(Next)}),
        pool_trace_loop(Next)
    end.

add(Key, N) -> ets:update_counter(?MODULE, Key, N, {Key, 0}), nil.

hist(Name, Duration) ->
    %% Fixed bucket cardinality, unlike one key per observed millisecond.
    Bucket = case Duration of
        N when N =< 1 -> <<"1">>;
        N when N =< 5 -> <<"5">>;
        N when N =< 20 -> <<"20">>;
        N when N =< 100 -> <<"100">>;
        N when N =< 1000 -> <<"1000">>;
        _ -> <<"inf">>
    end,
    add(<<Name/binary, "_bucket_", Bucket/binary>>, 1),
    add(<<Name/binary, "_sum_ms">>, Duration).

record({queue_claim_completed, _, Requested, Returned, Duration}) ->
    add(claims, 1), add(claim_requested, Requested), add(claim_returned, Returned),
    case Returned of 0 -> add(empty_claims, 1); _ -> nil end,
    hist(<<"claim">>, Duration);
record({queue_claim_failed, _, _}) -> add(claim_failures, 1);
record({queue_wake_received, _, Coalesced}) ->
    add(wakes_received, 1),
    case Coalesced of true -> add(wakes_coalesced, 1); false -> nil end;
record({job_started, _, _, _}) -> add(jobs_started, 1);
record({job_completed, _, _}) -> add(jobs_completed, 1);
record({lease_renewed, _, _, _}) -> add(lease_renewals, 1);
record({job_completion_persisted, _, _, Duration}) -> hist(<<"completion">>, Duration);
record({job_persistence_failed, _, _, _, _}) -> add(persistence_failures, 1);
record({request_started, _, _, Duration}) -> hist(<<"http_queue_wait">>, Duration);
record({request_rejected, _, _, _}) -> add(http_rejections, 1);
record(_) -> nil.

reaper_event({reaped, Rows, Duration}) ->
    add(reaper_batches, 1), add(reaped_jobs, Rows), hist(<<"reaper">>, Duration);
reaper_event(reap_failed) -> add(reaper_failures, 1).

snapshot() ->
    Stats = case ets:whereis(?MODULE) of
        undefined -> #{};
        _ -> maps:from_list(ets:tab2list(?MODULE))
    end,
    iolist_to_binary(json:encode(Stats#{timestamp_epoch_ms => erlang:system_time(millisecond),
                                      beam_run_queue => erlang:statistics(run_queue),
                                      cgroup_cpu => cgroup_cpu()})).

cgroup_cpu() ->
    case file:read_file("/sys/fs/cgroup/cpu.stat") of
        {ok, Bytes} ->
            maps:from_list([begin [K, V] = binary:split(Line, <<" ">>),
                                 {K, binary_to_integer(V)} end
                           || Line <- binary:split(Bytes, <<"\n">>, [global]), Line =/= <<>>]);
        _ -> #{}
    end.
