
>>> Running: 1. Steady-State (3000 jobs, 16 threads, 300.0 TPS)
[-] Progress: 896/3000 completed (29.9%) | Pending: 2104 | Elapsed: 10.5s | Speed: 85.0 jobs/s[-] Progress: 2048/3000 completed (68.3%) | Pending: 952 | Elapsed: 11.1s | Speed: 184.5 jobs/s[✔] Finished in 11.57s (259.3 jobs/s)!

>>> Running: 2. Peak Burst (5000 jobs, 32 threads, burst)
[-] Progress: 112/5000 completed (2.2%) | Pending: 4888 | Elapsed: 4.6s | Speed: 24.2 jobs/s[-] Progress: 144/5000 completed (2.9%) | Pending: 4856 | Elapsed: 5.3s | Speed: 27.2 jobs/s[-] Progress: 165/5000 completed (3.3%) | Pending: 4835 | Elapsed: 5.9s | Speed: 28.0 jobs/s[-] Progress: 191/5000 completed (3.8%) | Pending: 4809 | Elapsed: 6.5s | Speed: 29.4 jobs/s[-] Progress: 234/5000 completed (4.7%) | Pending: 4766 | Elapsed: 7.1s | Speed: 33.0 jobs/s[-] Progress: 298/5000 completed (6.0%) | Pending: 4702 | Elapsed: 7.7s | Speed: 38.9 jobs/s[-] Progress: 390/5000 completed (7.8%) | Pending: 4610 | Elapsed: 8.3s | Speed: 47.1 jobs/s[-] Progress: 533/5000 completed (10.7%) | Pending: 4467 | Elapsed: 8.8s | Speed: 60.3 jobs/s[-] Progress: 733/5000 completed (14.7%) | Pending: 4267 | Elapsed: 9.4s | Speed: 77.9 jobs/s[-] Progress: 969/5000 completed (19.4%) | Pending: 4031 | Elapsed: 10.1s | Speed: 96.3 jobs/s[-] Progress: 1342/5000 completed (26.8%) | Pending: 3658 | Elapsed: 10.7s | Speed: 125.7 jobs/s[-] Progress: 2010/5000 completed (40.2%) | Pending: 2990 | Elapsed: 11.2s | Speed: 178.9 jobs/s[-] Progress: 3034/5000 completed (60.7%) | Pending: 1966 | Elapsed: 11.8s | Speed: 258.1 jobs/s[-] Progress: 4314/5000 completed (86.3%) | Pending: 686 | Elapsed: 12.4s | Speed: 348.7 jobs/s[✔] Finished in 12.70s (393.8 jobs/s)!

>>> Running: 3. Single Ingress (3000 jobs, 24 threads, burst)
[-] Progress: 104/3000 completed (3.5%) | Pending: 2896 | Elapsed: 3.0s | Speed: 34.3 jobs/s[-] Progress: 159/3000 completed (5.3%) | Pending: 2841 | Elapsed: 3.6s | Speed: 44.3 jobs/s[-] Progress: 252/3000 completed (8.4%) | Pending: 2748 | Elapsed: 4.2s | Speed: 60.5 jobs/s[-] Progress: 357/3000 completed (11.9%) | Pending: 2643 | Elapsed: 4.8s | Speed: 75.0 jobs/s[-] Progress: 574/3000 completed (19.1%) | Pending: 2426 | Elapsed: 5.3s | Speed: 107.4 jobs/s[-] Progress: 1033/3000 completed (34.4%) | Pending: 1967 | Elapsed: 5.9s | Speed: 174.0 jobs/s[-] Progress: 1991/3000 completed (66.4%) | Pending: 1009 | Elapsed: 6.5s | Speed: 306.1 jobs/s[✔] Finished in 6.95s (431.5 jobs/s)!

>>> Running: 4. High-Volume Sustained (10000 jobs, 32 threads, 400.0 TPS)
[-] Progress: 768/10000 completed (7.7%) | Pending: 9232 | Elapsed: 25.5s | Speed: 30.1 jobs/s[-] Progress: 2048/10000 completed (20.5%) | Pending: 7952 | Elapsed: 26.1s | Speed: 78.4 jobs/s[-] Progress: 3328/10000 completed (33.3%) | Pending: 6672 | Elapsed: 26.7s | Speed: 124.5 jobs/s[-] Progress: 4352/10000 completed (43.5%) | Pending: 5648 | Elapsed: 27.2s | Speed: 159.8 jobs/s[-] Progress: 5632/10000 completed (56.3%) | Pending: 4368 | Elapsed: 27.9s | Speed: 202.2 jobs/s[-] Progress: 6656/10000 completed (66.6%) | Pending: 3344 | Elapsed: 28.4s | Speed: 234.5 jobs/s[-] Progress: 7680/10000 completed (76.8%) | Pending: 2320 | Elapsed: 28.9s | Speed: 265.4 jobs/s[-] Progress: 8960/10000 completed (89.6%) | Pending: 1040 | Elapsed: 29.5s | Speed: 303.3 jobs/s[✔] Finished in 30.01s (333.2 jobs/s)!

>>> Running: 5. Chaos Crash & Recovery (1000 jobs, 16 threads, burst)

[💥 CHAOS] Killed instance on port 18083 (PID 1555633) with SIGKILL!
[-] Progress: 91/1000 completed (9.1%) | Pending: 909 | Elapsed: 1.6s | Speed: 56.2 jobs/s[-] Progress: 139/1000 completed (13.9%) | Pending: 861 | Elapsed: 2.1s | Speed: 65.4 jobs/s[-] Progress: 229/1000 completed (22.9%) | Pending: 771 | Elapsed: 2.6s | Speed: 86.8 jobs/s[-] Progress: 494/1000 completed (49.4%) | Pending: 506 | Elapsed: 3.2s | Speed: 154.2 jobs/s[✔] Finished in 3.65s (274.2 jobs/s)!
[*] Restoring node on port 18083...
[✔] Node on port 18083 restored and healthy!

┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 1. Steady-State                                                                     │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  Throughput:       259.3 jobs/s     │  Total Time:        11.57 s              │
│  Enqueue Mean:      3.32 ms         │  Enqueue P95:        4.44 ms              │
│  Enqueue P99:       8.21 ms         │  HTTP Requests:      6000                 │
│  Attempts:      Att 1: 3000            │  Cluster RAM:    363.38 MB (avg: 90.84 MB/node) │
└────────────────────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 2. Peak Burst                                                                       │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  Throughput:       393.8 jobs/s     │  Total Time:        12.70 s              │
│  Enqueue Mean:     25.07 ms         │  Enqueue P95:       53.79 ms              │
│  Enqueue P99:      62.96 ms         │  HTTP Requests:     17206                 │
│  Attempts:      Att 1: 5000            │  Cluster RAM:    368.8 MB (avg: 92.2 MB/node) │
└────────────────────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 3. Single Ingress                                                                   │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  Throughput:       431.5 jobs/s     │  Total Time:         6.95 s              │
│  Enqueue Mean:     19.29 ms         │  Enqueue P95:       41.53 ms              │
│  Enqueue P99:      50.87 ms         │  HTTP Requests:      9577                 │
│  Attempts:      Att 1: 3000            │  Cluster RAM:    368.97 MB (avg: 92.24 MB/node) │
└────────────────────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 4. High-Volume Sustained                                                            │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  Throughput:       333.2 jobs/s     │  Total Time:        30.01 s              │
│  Enqueue Mean:      5.16 ms         │  Enqueue P95:       18.25 ms              │
│  Enqueue P99:      32.94 ms         │  HTTP Requests:     20000                 │
│  Attempts:      Att 1: 10000           │  Cluster RAM:    366.69 MB (avg: 91.67 MB/node) │
└────────────────────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 5. Chaos Crash & Recovery                                                           │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  Throughput:       274.2 jobs/s     │  Total Time:         3.65 s              │
│  Enqueue Mean:     16.20 ms         │  Enqueue P95:       39.07 ms              │
│  Enqueue P99:      43.35 ms         │  HTTP Requests:      5481                 │
│  Attempts:      Att 1: 1000            │  Cluster RAM:    290.65 MB (avg: 72.66 MB/node) │
└────────────────────────────────────────────────────────────────────────────────────────┘


==========================================================================================
🏆 CONSOLIDATED BENCHMARK SUMMARY TABLE
==========================================================================================

| Scenario | Jobs | Throughput | Enqueue Mean | Enqueue P95 | Enqueue P99 | Cluster RAM | Attempts / Reliability |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1. Steady-State** | 3,000 | **259.3 jobs/s** | 3.32 ms | 4.44 ms | 8.21 ms | 363.38 MB | 100% (1st att) |
| **2. Peak Burst** | 5,000 | **393.8 jobs/s** | 25.07 ms | 53.79 ms | 62.96 ms | 368.8 MB | 100% (1st att) |
| **3. Single Ingress** | 3,000 | **431.5 jobs/s** | 19.29 ms | 41.53 ms | 50.87 ms | 368.97 MB | 100% (1st att) |
| **4. High-Volume Sustained** | 10,000 | **333.2 jobs/s** | 5.16 ms | 18.25 ms | 32.94 ms | 366.69 MB | 100% (1st att) |
| **5. Chaos Crash & Recovery** | 1,000 | **274.2 jobs/s** | 16.20 ms | 39.07 ms | 43.35 ms | 290.65 MB | 100% (1st att) |


