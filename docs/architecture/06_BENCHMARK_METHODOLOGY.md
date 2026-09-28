# 06 — Benchmark and Experimental Methodology

## 1. Principle

The benchmark system is part of the product. A tail-latency controller without trustworthy measurement is not a valid project.

Headline latency comes from the **load generator's end-to-end timing**, not from Prometheus scrape histograms or trace samples.

## 2. Open-loop workload generator

The generator schedules arrivals independently of response completion.

Supported arrival processes:

- constant-rate;
- Poisson;
- step changes;
- ramps;
- burst trains;
- deterministic scripted arrival sequences.

Each request records:

```text
scheduled_arrival_time
actual_issue_time
completion_time
logical_outcome
backend_attempt_count
```

Primary user-facing latency is measured from scheduled arrival to completion where appropriate, preventing a slow system from hiding demand by merely delaying request issuance.

## 3. Coordinated omission

Closed-loop generators can under-report tail latency because they stop generating intended load while waiting for slow responses. ARTC therefore treats coordinated omission as a benchmark-invalidating risk.

Requirements:

- open-loop scheduling for headline load tests;
- HdrHistogram-compatible high-dynamic-range recording;
- explicit scheduled-vs-actual issue lag metric;
- generator CPU saturation detection;
- discard/invalidate runs where the generator cannot sustain the requested schedule within defined tolerance.

## 4. Canonical outcome metrics

Always report at least:

```text
offered_rps
issued_rps
accepted_rps
logical_success_rps
deadline_goodput_rps
rejection_rate
error_rate
deadline_miss_rate
p50
p95
p99
p999
total_backend_attempts
hedge_rate
retry_rate
attempt_amplification
ARTC CPU
ARTC RSS
backend CPU
```

### Goodput

Canonical definition:

```text
deadline_goodput = successful logical requests completed within their deadline / second
```

### Attempt amplification

```text
attempt_amplification = total backend attempts / logical requests issued
```

Examples:

```text
1.00 = no duplicate/replacement work
1.05 = 5% extra backend attempts
1.20 = 20% extra backend attempts
```

## 5. Baseline ladder

Do not compare only against a weak baseline.

Required configurations:

1. direct backend path where meaningful — proxy overhead reference;
2. round robin;
3. least inflight;
4. EWMA latency-aware routing;
5. P2C-style latency/inflight policy;
6. fixed concurrency cap;
7. adaptive concurrency only;
8. adaptive concurrency + replica routing;
9. routing + concurrency + bounded hedging;
10. full ARTC including retry containment and deadline admission.

Optional external comparator: a carefully matched Envoy adaptive-concurrency configuration for selected scenarios, documented as a comparator rather than a claimed apples-to-apples replacement.

## 6. Ablation methodology

Every major mechanism is independently switchable.

Canonical ablation sequence:

```text
baseline
+ replica selector
+ adaptive admission
+ deadline feasibility
+ hedging
+ retry containment
full system
```

If one mechanism explains most of the benefit, report that fact.

## 7. Mandatory flagship experiments

### E-001 Healthy overhead

Compare direct backend, simple proxy/baseline, and ARTC under healthy replicas.

Measure proxy-added latency, CPU/request, memory, allocations if instrumented, and maximum sustainable throughput.

### E-002 Isolated straggler

One replica receives fixed or distributional added latency. Compare selection policies and full ARTC.

### E-003 Cluster-wide slowdown

All replicas become slow. Validate that hedging does not create unsafe amplification.

### E-004 Burst load

Run 1x -> 5x -> 1x offered load. Plot concurrency limit, inflight, latency, goodput, and rejection over time.

### E-005 Retry storm

Inject retryable failures and verify bounded attempt RPS with budget/backoff/jitter.

### E-006 Failure recovery

Degrade a replica, restore it, and measure detection, traffic reduction, recovery detection, and reintegration.

### E-007 Hedge cost curve

Sweep hedge thresholds/budgets and plot tail-latency improvement versus attempt amplification/backend cost.

### E-008 Cancellation-ignorant server

Compare cancellation-aware versus cancellation-ignoring backends to quantify wasted speculative work.

### E-009 Mixed service-time workload

Use cheap and expensive calls to show why QPS alone is not capacity.

## 8. Negative-result experiments

Actively search for cases where ARTC adds overhead or provides little value:

- very low traffic;
- uniformly healthy replicas;
- extremely short calls;
- all replicas equally saturated;
- extremely short deadlines;
- hedge-unsafe workloads;
- conditions where extra policy computation outweighs benefit.

Negative results belong in the final report.

## 9. Statistical discipline

For important result points:

- warm up before measurement;
- use long enough runs to collect meaningful tail samples;
- repeat independent trials, typically 5–10 for headline scenarios;
- record seed/config for every trial;
- report median across runs plus dispersion/confidence representation;
- preserve per-run histograms rather than averaging percentiles incorrectly.

For p999, ensure sample counts are large enough that the tail contains a meaningful number of observations. Do not publish p999 from tiny runs as if it were stable.

## 10. Run validity checks

Invalidate a run if any of the following occurs unexpectedly:

- generator cannot sustain intended arrival schedule;
- CPU/resource isolation differs from manifest;
- fault injection fails or cleanup is incomplete;
- telemetry/benchmark collector loses required raw data;
- warmup conditions were not reached;
- container/process restart occurs outside the scenario;
- clock source or build differs from the recorded manifest.

## 11. Resource isolation

On a suitable Linux host, use cgroups/cpusets or equivalent to reduce benchmark self-interference among:

- load generator;
- ARTC;
- replicas;
- dependency B;
- observability stack;
- fault tools.

Exact pinning depends on host CPU topology and is recorded per run.

## 12. Environment manifest

Every benchmark writes a machine-readable manifest containing:

```text
git SHA
build type
compiler + version
compiler flags
gRPC version
kernel/OS
CPU model/topology
CPU governor or frequency policy
memory
container runtime version
container/resource limits
ARTC configuration
workload configuration
fault configuration
seed
warmup duration
measurement duration
sample count
```

## 13. Raw artifacts

A benchmark result directory contains:

```text
manifest.json
logical_latency.hdr
service_latency.hdr (optional diagnostic)
outcomes.csv/parquet or compact equivalent
controller_timeseries.csv
resource_timeseries.csv
fault_events.jsonl
summary.json
```

Plots are generated from these artifacts, never hand-edited data.

## 14. Performance-regression baselines

Once stable, track:

- healthy-path p50/p99 overhead;
- maximum sustainable goodput;
- CPU per logical request;
- RSS at defined concurrency;
- attempt amplification in canonical scenarios.

Regression thresholds are configured and updated only with documented justification.
