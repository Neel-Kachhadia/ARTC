# 12 — Requirement and Evidence Traceability Matrix

This file prevents ARTC from becoming a pile of disconnected tests. Every critical claim maps to a safety property, telemetry signal, verification method, and release evidence.

## Core matrix

| Requirement | Invariant(s) | Telemetry | Verification | Release evidence |
|---|---|---|---|---|
| Exactly one logical result | I-REQ-001, I-ATT-005 | logical outcomes, attempt outcomes | state-machine + deterministic completion races + lifecycle fuzz | zero duplicate terminals across suites |
| No work after rejection | I-REQ-004 | rejected + backend attempts | unit + E2E reject test | backend attempt delta = 0 |
| Deadline monotonicity | I-REQ-006/007 | remaining budget samples | fake-clock property tests + gRPC propagation integration | propagation test artifacts |
| Permit correctness | I-REQ-008, I-ADM-002 | admitted inflight, limit | property + TSan + stress | no leak/underflow/limit breach |
| Safe non-idempotent behavior | I-SAFE-001/002 | hedge/retry by method class | policy unit + E2E attempt observation + fuzz | 0 unsafe duplicates |
| Bounded retry work | I-BUD-001/003/006 | retry attempts/tokens/amplification | storm test + properties | amplification plot/table |
| Bounded hedge work | I-BUD-002/003/006 | hedge attempts/tokens/amplification | cluster-slow/hedge-storm test | amplification plot/table |
| Distinct hedge target | I-ATT-003 | primary/hedge target diagnostic | integration + property tests | zero same-target violations |
| Controller boundedness | I-ADM-001/004 | limit transitions | unit/property/fault injection | all values within bounds |
| Recovery/reintegration | I-REP-005 | replica state + traffic share | failure-recovery scenario | recovery timeline |
| Telemetry independence | I-TEL-001/002 | exporter failures/queue | collector outage + stress | request behavior unaffected within defined overhead |
| Resource boundedness | I-RES-001..005 | RSS/FD/thread/queue | stress + soak + churn | no unexplained drift |
| Shutdown safety | I-RES-006 + lifecycle invariants | active calls/timers | deterministic shutdown races + E2E | bounded shutdown, sanitizer clean |
| Honest latency measurement | benchmark rules | scheduled/actual/completion | generator tests + open-loop validation | raw histograms + manifest |
| No hidden load-shed win | reporting rule | goodput/rejections/errors | benchmark analysis checks | latency always adjacent to goodput/rejection |

## Phase 3 attempt-management matrix

| Requirement | Invariant(s) | Implementation | Verification | Metric / artifact | Interview question |
|---|---|---|---|---|---|
| Explicit idempotency and conservative unknown-method default | I-SAFE-001..003 | `MethodPolicy`, exact RPC-method map | `AttemptPolicyTest`, non-idempotent integration | per-run manifest attempt kinds | Why can’t the router infer idempotency from a method name? |
| One logical completion across backend attempts | I-REQ-001..003, I-ATT-005, I-ATT-008 | `AttemptManager` mutex terminal transition and server reactor | simultaneous completion, caller cancel, deadline, shutdown, seeded sequences | logical terminal count and winner trailers | What happens when primary and hedge finish together? |
| Bound total and active attempts | I-ATT-001, I-ATT-002, I-ATT-007 | runtime and method limits, fixed three-slot attempt array | `P+H+R` boundary and active-cap tests | primary/hedge/retry counts and amplification | What is the maximum amplification per admitted request? |
| Distinct-target, overload-aware hedging | I-ATT-003, I-BUD-002, I-BUD-007 | healthy-only secondary selector, target exclusion, and Phase 2 overload signals | probe-sequence unit test, distinct-target integration, all-replicas-slow integration, hedge-delay sweep | hedge rate, denial counters, latency estimates, p99 and service logs | Why can hedging worsen a cluster-wide slowdown? |
| Bounded, classified retries with backoff and jitter | I-BUD-001, I-BUD-005, I-BUD-007 | explicit status policy, all-used-target exclusion, capped exponential delay, seeded jitter | policy/deadline tests, distinct replacement target, retryable failure, budget denial and retry storm | retry counts, target diagnostics, budget state, interval manifests | How do backoff, jitter and the retry budget contain a storm? |
| Deadline and cancellation propagation | I-REQ-005..007, I-ATT-006, I-ATT-011 | monotonic request budget and manager-owned gRPC alarms | short-deadline integration, pending hedge/retry shutdown tests | zero-dispatch admissions and cancellation trailers | What changes when a backend ignores cancellation? |
| Logical admission permit and per-attempt replica leases | I-REQ-008..010, I-ADM-002, I-ATT-010 | one Phase 2 permit per logical request; a lease per attempt | concurrent-attempt tests, permit-drain checks and TSan | active attempts, terminal drain and controller counts | Do Phase 2 permits count requests or attempts? |
| Attempt amplification and wasted work | I-TEL-004, I-BUD-006 | attempt snapshots, censored-latency lower bounds, gRPC trailers and low-cardinality shutdown exposition | selector lower-bound unit test, loadgen manifest validator, cancellation-aware/ignoring backend | amplification, cancellation requests, censored attempts and aggregate loser time | What tail improvement justified the extra backend work? |
| Shutdown with outstanding attempts and timers | I-RES-002, I-RES-006, I-REQ-010 | dispatch gate, stop source, weak timer callbacks, reactor-owned attempt lifetimes, callback drain CV | callback-drain integration, pending hedge/retry timer tests and targeted TSan | pending backend callbacks and final attempt summary | How does shutdown prevent a late timer from creating work? |

Focused Phase 3 run directories are written under `artifacts/runs/<run-id>/` and remain ignored by Git. The canonical entry point is `lab/run_phase3_gates.sh`; `--code-only` runs build, test and sanitizer gates without Docker scenarios.

## Fault-model traceability

[`../failures/phase4_fault_coverage.csv`](../failures/phase4_fault_coverage.csv) reconciles every catalog fault/compound ID plus 13 selected pairwise interactions to status, test or job, artifact, and limitation. It separates PASS, PASS WITH LIMITATION, NOT APPLICABLE, and NOT RUN. Invalid or incomplete fault attempts remain preserved in their run directories and do not count as passing evidence.

## Phase 4 requirement traceability

| Requirement | Invariant | Implementation | Test / scenario | Metric | Canonical evidence | Limitation | Interview question |
|---|---|---|---|---|---|---|---|
| Deadline cannot create a late retry | I-REQ-006/007, I-BUD-001 | AttemptManager timer and dispatch deadline fences | `DeadlinePreventsQueuedAndDispatchingRetry` (pending timer and dispatch fence) | terminal count, retry starts/tokens, deadline misses, active callbacks | `tests/integration/attempt_management_test.cc`; targeted Debug/ASan/TSan logs | deterministic test controls callback order; wall-clock scheduling is covered separately | What if the retry timer and deadline fire together? |
| Retry and hedge work stays bounded | I-ATT-001/002, I-BUD-001/002/006 | Fixed attempt slots, separate token budgets, method policy | retry/hedge budget tests; C-002/C-003/C-011 | attempts/logical request, retry/hedge starts, token use, wasted work | `artifacts/reviews/phase4-hardening-evidence.txt`; `artifacts/runs/phase4_chaos_20261001_r10/chaos/` | Chaos and policy comparisons are single-host trials | How do budgets contain a cluster-wide retry or hedge storm? |
| Degradation recovers without losing capacity permanently | I-REP-005 | Replica health state, recovery probes, selector | A2 recovery/flapping; C-007 | health transitions, traffic share, recovery-probe success | `artifacts/runs/phase4_baseline_20261001/phase2/recovery-*`; `artifacts/runs/phase4_chaos_20261001_r4/chaos/` | One seeded flapping sequence; no long statistical false-recovery study | How does a recovered replica regain traffic safely? |
| Load failure is bounded and visible | I-ADM-001/002, I-RES-001 | Admission gate and adaptive concurrency | H global slowdown; three low/high load cycles | offered/admitted/goodput/rejected, route limit, inflight | `artifacts/runs/phase4_baseline_20261001/phase2/H-global-overload/`; `artifacts/runs/phase4_oscillation_20261001/phase2/` | 2,000 RPS with slow Service B shows shedding; healthy host saturation beyond that was not established | Does ARTC fail by queueing or shedding? |
| Healthy and faulted runs do not show monotonic resource drift | I-RES-001..006 | Bounded snapshots, attempts, timers, callback drain | One-hour healthy and 20-minute faulted soak | RSS, FD, threads, sockets, processes, samples, attempts | `artifacts/runs/phase4_healthy_soak_20261001/phase2-soak/`; `artifacts/runs/phase4_faulted_soak_20261001/phase2-soak/` | Short soak does not prove behavior over days; RSS rose modestly without monotonic growth flag | What evidence rules out permit/timer/resource leaks? |
| Invalid configuration fails before serving | I-CFG-001 | Startup validation and bounded policy parser | 31-case configuration matrix | startup exit/result and rejected cases | `artifacts/runs/phase4_config_matrix_20261001_r3/config-matrix/result.txt` | Runtime reconfiguration is not supported | Why reject rather than silently correct unsafe values? |
| Fault tooling cannot escape the ARTC lab | I-OPS-001 | Verified Compose labels/service identities and scoped cleanup | Seeded chaos, netem/process cleanup checks | leftover containers/networks/qdiscs/stress processes | `lab/run_phase4_chaos.sh`; final chaos event/result artifacts | Fault modes not exposed by the allowlist remain NOT APPLICABLE or NOT RUN | How does an incident runner avoid host-wide damage? |
| Benchmark conclusions account for offered load and uncertainty | I-BENCH-001/002 | Open-loop scheduler, issue-lag validity, HdrHistogram artifacts | Phase 4 baseline/ablation/soak runs | scheduled/issued/completed, issue lag, p50/p95/p99, goodput, amplification | `artifacts/runs/phase4_ablation_20261001/phase2/`; run manifests/histograms | Policy rows are one trial each; p999 is omitted below supported sample counts | How does the generator detect coordinated omission or saturation? |

The benchmark and fault coverage artifacts preserve unfavorable outcomes and invalid runs. README and interview claims must link to the exact manifest or deterministic test row rather than a lone percentile.

## Interview traceability

Every final resume/README claim must map to evidence.

Example:

```text
Claim:
"Reduced p99 under isolated straggler by X while amplification remained Y"

Evidence chain:
README table
  -> experiment E-002
  -> run manifest IDs
  -> raw HdrHistogram files
  -> analysis script
  -> exact git SHA
```

For correctness claims:

```text
Claim:
"Bounded hedging prevents duplicate-work explosion"

Evidence chain:
Invariant I-BUD-006
  -> hedge storm scenario
  -> property tests
  -> E2E metrics
  -> CI/release artifact
```

## Completion rule

A critical requirement with no evidence mapping is unfinished even if the code exists.
