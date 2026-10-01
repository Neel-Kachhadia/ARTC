# ADR-002: Phase 2 adaptive control core

## Context

Phase 1 routes one unary gRPC callback request to one statically configured
replica. `RouterCallState` owns the downstream request and replica lease through
completion. Phase 2 adds admission, deadline feasibility, replica feedback, and
adaptive routing without adding retries or hedges.

The architecture contract requires monotonic internal deadlines, immediate
bounded shedding, one permit per admitted request, bounded replica state,
slower controller updates, coherent snapshots, and recovery after faults.

## Alternatives

- Update every controller variable on each request. This couples expensive
  health and AIMD work to the callback path.
- Queue requests when the limit is full. This creates another wait queue and
  consumes caller deadlines while overloaded.
- Put controller reads behind one mutable global state lock. This serializes
  request selection with control updates.
- Publish immutable snapshots from an owned periodic worker and use a small
  mutex only for permit accounting. This gives a simple ownership boundary.

## Decision

- `RequestContext` is a value owned by the asynchronous router call. It stores
  the caller's absolute gRPC deadline and a steady-clock effective deadline.
  Internal expiry and feasibility use only `steady_clock`. At dispatch, ARTC
  passes the earlier of the original caller deadline and the current wall time
  plus the remaining steady budget.
- Admission is fail-fast. A mutex protects route inflight, current limit,
  accepting state, and exact acquire/release counters. A move-only RAII permit
  belongs to the router call and releases once on every terminal path.
- Replica completion observations update bounded per-replica state. A fixed
  rolling sample window supplies actual latency percentiles; EWMA remains a
  separate smoothed estimate. Invalid samples are discarded.
- Health states are `HEALTHY`, `DEGRADED`, `UNAVAILABLE`, and `RECOVERING`.
  Three consecutive transport/timeout failures make a replica unavailable.
  After a cooldown it re-enters as recovering and receives bounded probe
  traffic. Sustained successful observations restore healthy status.
- An owned controller thread ticks at a configured interval. It consumes route
  outcome windows, updates health and AIMD, then atomically publishes one
  `shared_ptr<const ControllerSnapshot>`. Request readers load one coherent
  snapshot; no telemetry exporter participates in correctness.
- AIMD adds a configured integer step after a sufficiently sampled healthy
  window. Overload signals apply `max(min_limit, floor(limit * beta))`.
  Admission rejects immediately while inflight is at or above the current
  limit, including while inflight drains below a newly reduced limit.
- The adaptive selector compares dimensionless components:
  `(latency / best_latency) * (1 + replica_inflight / route_limit) *
  (1 + 4 * error_ewma) * health_penalty`. Healthy, degraded, and recovering
  penalties are 1, 2, and 4; unavailable replicas are ineligible. Cold replicas
  receive deterministic exploration before latency scoring. Every 16 selections,
  a bounded deterministic probe prefers recovering, then degraded replicas;
  healthy replicas receive exploration when no cold replica remains.
- Phase 2 launches at most one backend call. No retry, hedge, or replacement
  attempt exists.

## Tradeoffs

The permit mutex adds a small serialized admission cost. The snapshot uses
atomic shared-pointer publication, which may internally lock on some standard
libraries. Both costs are measured before optimization. A limit reduction can
leave existing inflight above the new limit; the gate admits no new request
until the count drains. Static replicas and a bounded recovering probe share
avoid a separate discovery or probe service.

## Evidence

The Phase 1 canonical regression passed on pinned gRPC 1.84, including the four
baseline selectors, targeted A2 straggler/recovery, CPU saturation,
pause/resume, kill/restart, repeated startup, and interrupted-fault cleanup.
The latest Phase 2 validation passed 44 CTest cases under GCC Debug, GCC
Release, Clang, ASan/UBSan, LeakSanitizer, and TSan.

The raw Phase 2 experiment bundle remains local and ignored under
`artifacts/runs/`; it is not part of the public evidence package. The results
below are historical single-host measurements recorded here as an engineering
summary. The later Phase 4 evidence under `artifacts/evidence/phase4/` is the
canonical retained validation package. The analyzer ordered concurrent
decision samples by monotonic elapsed time before calculating time-series
metrics.

On the healthy 1,000 RPS overhead run, direct A1 p99 is 843 us, Phase 1
least-inflight is 1,095 us, round robin is 1,181 us, and adaptive Phase 2 is
1,197 us. Phase 2 adds 102 us p99 over Phase 1 least-inflight in this run.
Microbenchmark medians are 44 ns for selector scoring, 20 ns for admission,
21 ns for snapshot read, and 171 ns for controller update.

At 2,000 offered RPS with a 50 ms downstream delay, the global-overload run
admits 3,603 of 40,000 calls, rejects 36,397, completes 3,603 useful calls,
and records one backend attempt per admitted call. Its p99 is 53.535 ms and
the route limit falls from 512 to 6. Under all-replica 150 ms faults and a
100 ms caller deadline, feasibility rejects all 4,000 calls before backend
dispatch and reports zero caller deadline misses.

The paired deadline comparison uses an A2 straggler, 8,000 offered RPS, and a
300 ms caller deadline. Both runs have complete attempt metadata and zero
caller deadline misses. Deadline feasibility records zero
`REJECT_DEADLINE_INFEASIBLE` results in the ON run. ON goodput is 3,592 RPS
versus 4,533 RPS OFF; this workload does not demonstrate a feasibility benefit
or isolate the goodput difference, so no causal win is claimed.

The A2 recovery trace samples A2 traffic at 34.1% when healthy, 5.0% while
unavailable, and 29.0% after recovery. Sampled degradation and reintegration
detection are 1 s and 11 s. Failed bounded probes produce 66 errors during
the unavailable phase and 25 during reintegration; these remain visible
negative results.

The 120-second 12,000 RPS step/sustain/recovery trace has 7,551 and 7,040
deadline-goodput RPS during step-up and sustained overload. p99 is 424.959 ms
and 207.359 ms. The route limit undershoots 150 below its 153 high-load median,
changes direction 38 times, and never remains within 10% through the end of
the high-load phase. It returns to the 512 normal-load median in 18.39 s.
This is measured oscillatory AIMD behavior, not a settling claim.

The final-code 30-minute Phase 2 soak completed 900,000 requests at 500 RPS
with 900,000 deadline-goodput, zero errors, zero rejects, and one backend
attempt per admitted request. Across 162 resource samples, FD, thread, socket,
and PID counts stayed flat after startup; median RSS changed by 808 KiB and
container RSS by 1.125 MiB. The resource analyzer passed, recorded maximum
route inflight of 1, and reported zero dropped decision samples. Its raw run
directory remains local and ignored; these figures are preserved here as a
historical summary, not as a separately reproducible public artifact.

The measurements support the reduced Phase 2 gate. The deadline paired run
does not show a benefit, and the overload trace does not settle; both remain
negative results and are not presented as wins.
