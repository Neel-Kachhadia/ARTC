# 04 — Failure Model and Fault Laboratory

## 1. Philosophy

It is impossible to test every failure the universe can produce. Production-grade rigor means defining an explicit fault model, deterministically covering every modeled single failure, systematically covering pairwise interactions, selecting adversarial higher-order combinations, and fuzzing unexpected event sequences.

Every fault experiment has three phases:

```text
HEALTHY BASELINE -> FAILURE -> RECOVERY
```

Failure onset alone is insufficient.

## 2. Fault-lab requirements

Each injected fault must have:

- unique scenario ID;
- target component/link;
- parameters;
- start time or trigger;
- duration;
- random seed if stochastic;
- expected containment properties;
- expected recovery properties;
- automatic cleanup/rollback;
- machine-readable event log.

The fault controller must fail closed: a failed cleanup must be visible and must invalidate the benchmark run.

## 3. Request-lifecycle faults/races

Deterministically exercise:

```text
F-REQ-001  client cancels before admission
F-REQ-002  client cancels during routing
F-REQ-003  client cancels after primary dispatch
F-REQ-004  client cancellation races primary completion
F-REQ-005  client cancellation races hedge timer
F-REQ-006  deadline expires before admission
F-REQ-007  deadline races primary completion
F-REQ-008  deadline races hedge completion
F-REQ-009  hedge timer races primary completion
F-REQ-010  primary and hedge complete concurrently
F-REQ-011  retry scheduling races logical completion
F-REQ-012  shutdown races every active lifecycle stage
```

These are primarily deterministic scheduler/barrier tests, not network chaos tests.

## 4. Backend behavior faults

Each dummy backend supports configured behavior modes:

```text
F-BE-001  immediate success
F-BE-002  fixed additional latency
F-BE-003  random bounded latency
F-BE-004  bimodal latency distribution
F-BE-005  heavy/long-tail latency distribution
F-BE-006  periodic latency spikes
F-BE-007  gradual latency degradation
F-BE-008  CPU saturation
F-BE-009  memory pressure
F-BE-010  worker/thread starvation
F-BE-011  local queue saturation
F-BE-012  immediate UNAVAILABLE
F-BE-013  RESOURCE_EXHAUSTED
F-BE-014  INTERNAL/error response
F-BE-015  connection refusal
F-BE-016  abrupt connection reset
F-BE-017  server process crash
F-BE-018  server restart during active RPC
F-BE-019  server freeze/hang
F-BE-020  ignores cancellation and continues work
F-BE-021  honors cancellation promptly
F-BE-022  oversized response within configured protocol bounds
F-BE-023  slow downstream dependency B
F-BE-024  dependency B fails while A remains reachable
F-BE-025  replica repeatedly flaps healthy/unhealthy
```

## 5. Network faults

Using per-container/network-namespace targeting where practical:

```text
F-NET-001  fixed latency
F-NET-002  variable jitter
F-NET-003  independent packet loss
F-NET-004  burst loss
F-NET-005  packet duplication
F-NET-006  packet reordering
F-NET-007  bandwidth restriction
F-NET-008  temporary blackhole
F-NET-009  asymmetric latency
F-NET-010  asymmetric loss
F-NET-011  proxy-to-single-replica partition
F-NET-012  single-replica-to-dependency partition
F-NET-013  client-to-proxy degradation
```

Packet corruption is included only where the transport path allows a meaningful experiment; integrity checks may turn corruption into loss/reset before the application sees it.

## 6. Load faults

```text
F-LOAD-001  steady expected load
F-LOAD-002  step 1x -> 5x
F-LOAD-003  step 5x -> 1x
F-LOAD-004  sustained overload beyond capacity
F-LOAD-005  periodic burst train
F-LOAD-006  Poisson arrival workload
F-LOAD-007  synchronized client burst
F-LOAD-008  mixed cheap/expensive request service times
F-LOAD-009  traffic disappears then resumes suddenly
```

## 7. Controller/data faults

Testing hooks may inject:

```text
F-CTL-001  stale latency observations
F-CTL-002  delayed observation processing
F-CTL-003  missing observation interval
F-CTL-004  isolated extreme outlier
F-CTL-005  burst of extreme outliers
F-CTL-006  invalid NaN/inf sample at validation boundary
F-CTL-007  limit at configured minimum
F-CTL-008  limit at configured maximum
F-CTL-009  oscillating offered load
F-CTL-010  all replicas degrade simultaneously
F-CTL-011  multiple replicas recover simultaneously
F-CTL-012  controller thread stalls temporarily
```

Unsafe values must be rejected or sanitized before publication.

## 8. Telemetry faults

```text
F-TEL-001  OTel Collector unavailable at startup
F-TEL-002  collector dies under load
F-TEL-003  Prometheus unavailable
F-TEL-004  Tempo unavailable
F-TEL-005  exporter blocks/slows
F-TEL-006  telemetry queue reaches bound
F-TEL-007  trace sampling config changes
```

Expected property: request correctness continues and telemetry failure is itself observable locally.

## 9. Process/startup faults

```text
F-PROC-001  ARTC starts before all replicas
F-PROC-002  replicas start before ARTC
F-PROC-003  dependency B starts late
F-PROC-004  one replica starts late
F-PROC-005  replica is removed while attempts are active
F-PROC-006  configuration is invalid
F-PROC-007  graceful shutdown while idle
F-PROC-008  graceful shutdown under heavy load
F-PROC-009  forced termination followed by restart
```

## 10. Resource-exhaustion faults

```text
F-RES-001  high concurrent request count
F-RES-002  allocator pressure
F-RES-003  file-descriptor pressure
F-RES-004  connection churn
F-RES-005  telemetry-buffer pressure
F-RES-006  maximum configured queue pressure
F-RES-007  CPU starvation of ARTC itself
F-RES-008  memory pressure on ARTC process
```

Where deterministic allocation failure is feasible through testing hooks, explicitly inject it at important allocation boundaries.

## 11. Mandatory compound scenarios

Single faults do not model many real incidents. At minimum test:

```text
C-001 burst load + one slow replica
C-002 packet loss + retry enabled
C-003 isolated slow replica + hedge enabled
C-004 all replicas overloaded + incoming burst
C-005 B dependency slow + one A replica CPU saturated
C-006 short deadlines + load increase
C-007 backend recovery + immediate traffic spike
C-008 telemetry outage + high request volume
C-009 replica removal + hedge timer fires
C-010 shutdown + active retry + client cancellation
C-011 packet loss + backend restart
C-012 overload + retryable errors + reduced deadlines
```

## 12. Pairwise and higher-order strategy

- Every single modeled fault: deterministic coverage.
- Pairwise combinations: generated from relevant compatible dimensions and executed in CI/nightly tiers.
- Three-or-more faults: curated adversarial incident scenarios.
- Randomized chaos: seeded event sequence with bounded state space and reproducibility.

Do not claim exhaustive universal failure coverage. Claim exhaustive coverage of the documented single-fault model plus systematic combination coverage.

## 13. Recovery metrics

Every scenario records:

```text
time_to_detect
time_to_contain
peak_latency_degradation
peak_amplification
peak_rejection_rate
time_to_detect_recovery
time_to_reintegrate
time_to_steady_state
```

A mechanism that identifies failure but never restores recovered capacity is considered incorrect.
