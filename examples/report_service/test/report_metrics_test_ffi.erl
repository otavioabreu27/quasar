-module(report_metrics_test_ffi).
-export([verify/0]).

verify() ->
    Original = os:getenv("BENCHMARK_HTTP_TIMING"),
    OriginalTrace = os:getenv("BENCHMARK_POOL_TRACE"),
    os:putenv("BENCHMARK_HTTP_TIMING", "1"),
    os:putenv("BENCHMARK_POOL_TRACE", "0"),
    try
        report_metrics_ffi:start(),
        preserved = report_metrics_ffi:measure(<<"http_post_handler">>, fun() -> preserved end),
        try
            report_metrics_ffi:measure(<<"http_get_handler">>, fun() -> error(expected) end),
            error(exception_not_propagated)
        catch error:expected -> ok end,
        [{_, 1}] = ets:lookup(report_metrics_ffi, <<"http_post_handler_count">>),
        [{_, 1}] = ets:lookup(report_metrics_ffi, <<"http_get_handler_count">>),
        ets:insert(report_metrics_ffi, {http_timing_enabled, 0}),
        unchanged = report_metrics_ffi:measure(<<"http_post_handler">>, fun() -> unchanged end),
        [{_, 1}] = ets:lookup(report_metrics_ffi, <<"http_post_handler_count">>),
        nil
    after
        ets:delete(report_metrics_ffi),
        restore("BENCHMARK_HTTP_TIMING", Original),
        restore("BENCHMARK_POOL_TRACE", OriginalTrace)
    end.

restore(Name, false) -> os:unsetenv(Name);
restore(Name, Value) -> os:putenv(Name, Value).
