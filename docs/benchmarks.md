# Controlled HTTP benchmark

The development-only `test/quasar_benchmark.gleam` fixture serves three endpoints on port
19090:

- `/pure`: a fixed response from Mist without Quasar;
- `/managed`: the same response through a 16-worker Quasar pool;
- `/saturated`: a 20 ms handler through 2 workers and a buffer of 8.

Run the fixture and ApacheBench from the `quasar_mist` directory:

```sh
gleam run -m quasar_benchmark
ab -n 5000 -c 64 http://127.0.0.1:19090/pure
ab -n 5000 -c 64 http://127.0.0.1:19090/managed
ab -n 1000 -c 100 http://127.0.0.1:19090/saturated
ab -n 5000 -c 64 http://127.0.0.1:19090/managed
```

The final managed run checks that throughput and latency stabilize after the
overload run. ApacheBench reports requests per second, non-2xx responses, and
latency percentiles. Sample the BEAM process RSS before and after each run.

Never infer that Quasar improves raw throughput from this benchmark. Its value
is bounded admission, workload isolation, and explicit overload behavior.

## Pre-refactor baseline — 2026-09-04

These historical measurements precede the architecture refactor. They are not
measurements of the current implementation; rerun before performance claims.

Environment: x86_64, 16 logical CPUs, Gleam 1.18.1, Erlang/OTP 27.3.4.17,
ApacheBench 2.3. Each normal run used 5,000 requests at concurrency 64. The
saturation run used 1,000 requests at concurrency 100.

| Route | req/s | p50 | p95 | p99 | non-2xx |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mist `/pure` | 4,618.63 | 15 ms | 17 ms | 17 ms | 0 |
| Mist + Quasar `/managed` | 4,291.53 | 16 ms | 17 ms | 19 ms | 0 |
| Quasar `/saturated` | 2,536.86 | 25 ms | 32 ms | 127 ms | 965 |
| `/managed` after saturation | 4,271.47 | 15 ms | 17 ms | 19 ms | 0 |

The saturated pool had 2 active slots and a hard buffer cap of 8; rejected
requests received 503 and the buffer could not exceed 8. Of the 1,000 requests,
35 completed with 200 while 965 were rejected. Managed throughput after the
overload run was within 0.5% of the preceding managed run, with identical p95
and p99, indicating recovery in this sample.

BEAM RSS was 77,292 KiB before the runs and 87,360 KiB afterwards, a 10,068 KiB
increase. These are single-machine development measurements, not general
performance claims. Repeat on deployment hardware and with representative
handlers before choosing pool sizes.
