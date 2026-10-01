# 13 — Interview Readiness

This document is an evidence guide, not a script. Every number below points to
retained Phase 4 artifacts; short comparisons are descriptive and scoped to
one shared Linux host.

## 30-second explanation

> I built ARTC, a C++23 unary-gRPC traffic layer for controlling tail latency and overload across a static replica pool. It combines deadline-aware admission, replica health, adaptive concurrency, and bounded idempotency-aware retries and hedges. A scoped open-loop fault lab records latency, goodput, rejections, resource samples, and backend amplification. A one-hour healthy run completed 900,000 requests at 250 per second with no rejections and 1.0 attempt amplification. The implementation is tested with deterministic callback races, compiler and sanitizer gates, seeded state sequences, and recovery experiments. It is a single-process prototype, not a fleet-wide service mesh.

## Two-minute explanation

Tail latency gets worse when a small number of backend requests become
stragglers, while unbounded concurrency and retries can turn that delay into a
larger outage. ARTC sits between clients and a configured set of unary gRPC
replicas. It uses request deadlines for admission, an adaptive controller to
bound route concurrency, and replica health/latency for selection.

The hardest lifecycle boundary is one logical request that can own multiple
backend attempts. `AttemptManager` owns the terminal decision, timers, attempt
accounting, and cancellation; a request can have at most two active attempts
and three total. Retries happen after an attempt group fails, hedges overlap a
slow attempt, separate budgets cap their aggregate use, and automatic duplicate
work requires an explicit idempotency policy. Deadlines and shutdown are
rechecked at dispatch boundaries.

I tested it with an open-loop generator that records scheduled arrival, actual
issue, completion, and issue lag, alongside attempt metadata and raw latency
histograms. The one-hour healthy run measured p50 1.123 ms, p95 1.529 ms, and
p99 1.732 ms over 900,000 observations. Under one slow-backend overload case,
ARTC shed most offered calls; I report that as bounded containment with low
goodput, not as a latency win. A single straggler trial showed lower p99 with a
5 ms hedge but 1.33x amplification, so that observation is not a stable causal
claim. The controller also kept increasing its limit during low demand, and a
deadline-feasibility experiment reduced goodput in some cases. Those are
limitations I would investigate before deployment-specific tuning.

## Architecture and correctness answers

### What problem does ARTC solve, and why a proxy?

It coordinates admission, routing, deadlines, and duplicate-attempt policy at
one request boundary. That makes amplification and logical completion visible
across replicas without putting a separate policy implementation into every
backend. The current deployment is one configured process; it does not provide
fleet-wide coordination.

### Why C++ and gRPC callbacks? Why no database?

The lab exercises asynchronous gRPC call lifetimes and bounded state in a
systems-language implementation. It uses gRPC's callback API and timers rather
than adding a second event loop. Controller state is ephemeral feedback state,
not durable business data; on restart it is rebuilt from observations. A
database would not add a correctness guarantee the current single-process
design needs.

### Why AIMD? What is its limitation?

AIMD provides a small, bounded controller with an explainable increase and
overload-decrease rule. Its response depends on sampling and traffic; the
oscillatory test showed the limit continuing to grow during low-demand waves
and reaching its cap by the second high wave. That is a measured settling and
demand-tracking limitation, not a reason to claim a new controller is already
validated.

### How do admission and recovery work?

Admission checks the monotonic deadline and current concurrency snapshot before
creating a logical request. One permit accounts for the request; each backend
attempt has its own replica lease. Failed replicas cool down and recover through
observed successful probes before they rejoin ordinary selection. Tests cover
restart/reintegration and a seeded slow/healthy/down/healthy sequence.

### Hedge versus retry? Why idempotency and separate budgets?

A hedge is concurrent work intended to escape an isolated straggler. A retry is
later work after the current attempt group fails. Both consume capacity, so
separate token budgets make their costs visible and bounded. Automatic
duplicate attempts require explicit idempotency; unknown methods default to
non-idempotent and one attempt. Neither mechanism creates capacity when all
replicas or a shared dependency are slow.

### How does exactly-one completion work?

The manager serializes terminal-state commitment under its synchronization
boundary. The winner commits one logical result, requests cancellation of
losers, and later callbacks only account their attempts; they cannot commit a
second result. Deterministic tests force completion/deadline/cancel/timer
orders, then assert one terminal transition and balanced permits, leases, and
callback drain.

### What if the backend ignores cancellation? What happens at shutdown?

Cancellation is best effort. The caller still gets at most one logical result,
but an ignoring server may keep doing work. The lab records cancelled attempts
and post-cancel/wasted work separately. Shutdown closes admission and dispatch,
cancels timers/calls, and drains registered callbacks before destroying shared
state; selected timer, dispatch-fence, primary, and hedge schedules are tested.

## Benchmarking and failure answers

### How does the harness address coordinated omission?

Arrivals are scheduled independently of previous completion. Each run records
scheduled arrival, actual issue, completion, issue-lag histogram, and the
maximum lag. The manifest marks generator saturation or excessive lag invalid;
invalid runs are retained but excluded from comparisons.

### Why report goodput with latency? Can ARTC route around global overload?

Fast rejections can make a latency percentile look small. For example, with
Service B slowed and 2,000 offered requests/s, the measured workload admitted
3,378 of 40,000 scheduled calls and rejected 36,622. In the earlier
all-replicas-slow trace, each replica had 150 ms egress delay and the caller
deadline was 100 ms; all 4,000 requests were rejected before dispatch as
`REJECT_DEADLINE_INFEASIBLE`. That demonstrates deadline-infeasibility
shedding, not general absence of capacity. In the separate clean-checkout case,
120 ms delay with a 5 s deadline admitted 50 of 50 sampled calls.

### What can you claim about p99 or p999?

The healthy soak has 900,000 completed observations and supports its reported
p50/p95/p99 summary. It is below the project's one-million-observation gate
for a serious RPC p999 claim, so I do not use RPC p999 as a headline. Policy
ladder and ablation rows are one trial each; close values are not ranked.

### What did you learn about deadline gating and hedging?

Earlier valid deadline-feasibility runs reduced goodput without reducing
deadline misses in some conditions. One straggler sweep favored a 5 ms hedge
over 20/50 ms delays, but it was one trial per setting and incurred 1.33x
amplification. These are workload-specific negative and positive observations,
not general settings.

### How are faults reproducible and safe?

The Compose fault runner records a seed and ordered events, verifies project
and service identity before changing a target, restores injected faults, and
checks scoped cleanup and recovery. It does not change host firewall rules or
global network interfaces. The environment is single-host and not resource
isolated, which limits causal interpretation of short performance trials.

## Real defect story: hedge suppression and a recovering replica

- **Symptom:** the controller started a hedge although every eligible healthy
  alternate was already slow.
- **Why it was subtle:** the global-overload scan considered a replica in
  `Recovering` state with a fast historical latency sample. The hedge selector
  itself only allowed `Healthy` replicas, so the two checks reasoned about
  different candidate sets.
- **Reproduction:** a deterministic test held the primary, set two healthy
  replicas above the latency target, and gave a third recovering replica a
  stale fast sample. The before-fix log showed a hedge was started.
- **Fix:** the overload scan now applies the same `Healthy` eligibility rule as
  secondary selection.
- **Regression:** `HedgeIsSuppressedWhenOnlyFastReplicaIsRecovering` asserts an
  overload denial, no hedge dispatch, and no hedge-token consumption.
- **Evidence:** `HedgeIsSuppressedWhenOnlyFastReplicaIsRecovering` in
  `tests/integration/attempt_management_test.cc`.

## Design evolution

1. Phase 1 built the reproducible unary-gRPC lab and open-loop generator.
2. Phase 2 added request deadlines, replica state, immutable controller
   snapshots, adaptive concurrency, and logical admission permits.
3. Phase 3 separated a logical request from backend attempts and bounded
   idempotency-aware hedging/retries with independent budgets.
4. Phase 4 kept the algorithms stable, corrected the demonstrated recovering
   replica hedge bug, added a deadline/retry timer race regression, and
   validated failures, cleanup, recovery, and measured limitations.

## Verified resume bullets

- Built a C++23 unary-gRPC traffic layer with deadline-aware admission,
  adaptive replica routing, and idempotency-aware bounded retries/hedges;
  completed 900,000 requests at 250 RPS in a one-hour single-attempt healthy
  soak with zero rejections and 1.00 attempt amplification, while validating
  retries and hedges separately with deterministic and short fault tests.
- Built a scoped seeded Docker fault lab and deterministic lifecycle tests;
  validated GCC/Clang Debug/Release builds (98/98 tests each), focused
  ASan/UBSan/LSan (47/47), targeted TSan (49/49), 32 deterministic
  state-sequence seeds, restart/recovery, and a 20-minute faulted soak while
  preserving invalid and unfavorable results.

## Evidence to open during an interview

- Healthy soak manifest and resources:
  `artifacts/evidence/phase4/soaks/healthy/`
- Faulted soak manifest and resources:
  `artifacts/evidence/phase4/soaks/faulted/`
- Baseline, overload, recovery, and load-step runs:
  `artifacts/evidence/phase4/scenarios/`
- Policy/ablation and focused control profile:
  `artifacts/evidence/phase4/benchmarks/` and
  `artifacts/evidence/phase4/profiles/`
- Fresh-checkout compiler, sanitizer, Compose, image, and cleanup record:
  `artifacts/evidence/phase4/reproducibility/clean-checkout-validation.txt`
- Explicit fault dispositions:
  `docs/failures/phase4_fault_coverage.csv`
- PRR and requirement mapping:
  `docs/architecture/15_PRODUCTION_READINESS_REVIEW.md` and
  `docs/architecture/12_TRACEABILITY_MATRIX.md`
