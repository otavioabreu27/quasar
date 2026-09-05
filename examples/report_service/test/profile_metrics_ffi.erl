%% Profiling-only aggregation of Quasar's public event stream.
-module(profile_metrics_ffi).
-export([start/0]).

start() ->
    Pid = spawn_link(fun() ->
        erlang:send_after(500, self(), snapshot),
        loop(empty())
    end),
    fun(Event) -> Pid ! {event, Event}, nil end.

empty() ->
    #{claims => 0, claim_requested => 0, claim_returned => 0,
      empty_claims => 0, claim_duration_ms => #{},
      jobs_started => 0, jobs_completed => 0,
      jobs_executing => 0, jobs_executing_peak => 0,
      lease_renewals => 0, completion_duration_ms => #{},
      persistence_failures => 0, claim_failures => 0}.

loop(Stats) ->
    receive
        {event, Event} ->
            loop(record(Event, Stats));
        snapshot ->
            io:format("RUNTIME_METRICS ~s~n", [json:encode(Stats)]),
            erlang:send_after(500, self(), snapshot),
            loop(Stats)
    end.

record({queue_claim_completed, _Queue, Requested, Returned, Duration}, Stats) ->
    Stats#{
      claims := maps:get(claims, Stats) + 1,
      claim_requested := maps:get(claim_requested, Stats) + Requested,
      claim_returned := maps:get(claim_returned, Stats) + Returned,
      empty_claims := maps:get(empty_claims, Stats) + case Returned of 0 -> 1; _ -> 0 end,
      claim_duration_ms := histogram(Duration, maps:get(claim_duration_ms, Stats))
    };
record({job_started, _Id, _Queue, _Attempt}, Stats) ->
    Executing = maps:get(jobs_executing, Stats) + 1,
    Stats#{jobs_started := maps:get(jobs_started, Stats) + 1,
           jobs_executing := Executing,
           jobs_executing_peak := max(Executing, maps:get(jobs_executing_peak, Stats))};
record({job_completed, _Id, _Queue}, Stats) ->
    finish(Stats#{jobs_completed := maps:get(jobs_completed, Stats) + 1});
record({job_retry_scheduled, _Id, _Queue, _At}, Stats) ->
    finish(Stats);
record({job_discarded, _Id, _Queue}, Stats) ->
    finish(Stats);
record({lease_renewed, _Id, _Queue, _ExpiresAt}, Stats) ->
    Stats#{lease_renewals := maps:get(lease_renewals, Stats) + 1};
record({job_completion_persisted, _Id, _Queue, Duration}, Stats) ->
    Stats#{completion_duration_ms := histogram(
      Duration, maps:get(completion_duration_ms, Stats))};
record({job_persistence_failed, _Id, _Queue, Operation, _Reason}, Stats) ->
    Next = Stats#{persistence_failures := maps:get(persistence_failures, Stats) + 1},
    case Operation of <<"complete">> -> finish(Next); _ -> Next end;
record({queue_claim_failed, _Queue, _Reason}, Stats) ->
    Stats#{claim_failures := maps:get(claim_failures, Stats) + 1};
record(_Event, Stats) ->
    Stats.

finish(Stats) ->
    Stats#{jobs_executing := max(0, maps:get(jobs_executing, Stats) - 1)}.

histogram(Duration, Histogram) ->
    Bucket = min(60000, max(0, Duration)),
    maps:update_with(integer_to_binary(Bucket), fun(N) -> N + 1 end, 1, Histogram).
