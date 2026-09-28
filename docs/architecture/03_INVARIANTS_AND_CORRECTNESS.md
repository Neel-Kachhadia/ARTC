# 03 — Invariants and Correctness

## 1. Correctness model

ARTC's correctness is defined by explicit invariants, not by example-based tests alone. Every invariant must map to at least one of:

- assertion or runtime guard;
- unit/state-machine test;
- property test;
- deterministic concurrency test;
- sanitizer/fuzzer coverage;
- integration evidence.

## 2. Request-lifecycle invariants

The following must hold for all executions:

```text
I-REQ-001  A logical request exposes at most one terminal result.
I-REQ-002  A completed request never transitions back to ACTIVE.
I-REQ-003  Every accepted request eventually reaches a terminal state or bounded shutdown cancellation.
I-REQ-004  A rejected request launches zero backend attempts.
I-REQ-005  A cancelled logical request cannot launch a new attempt afterward.
I-REQ-006  No attempt starts after the effective logical deadline.
I-REQ-007  Remaining deadline never increases.
I-REQ-008  Each acquired admission permit is released exactly once.
I-REQ-009  Inflight counters never become negative or wrap silently.
I-REQ-010  Request-owned state outlives every callback that can access it.
```

## 3. Attempt invariants

```text
I-ATT-001  Backend attempts are bounded by MethodPolicy.max_attempts.
I-ATT-002  Active attempts never exceed configured simultaneous-attempt bound.
I-ATT-003  A normal hedge target differs from its primary target.
I-ATT-004  A terminal attempt never re-enters IN_FLIGHT.
I-ATT-005  Loser cancellation cannot produce a second logical completion.
I-ATT-006  Retry scheduling cannot survive logical completion.
```

## 4. Idempotency and side-effect invariants

```text
I-SAFE-001  Non-idempotent methods never hedge unless an explicit deduplication contract exists.
I-SAFE-002  Non-idempotent methods never automatically retry unless explicitly declared retry-safe.
I-SAFE-003  Unsafe configuration is rejected before serving traffic.
```

Tests must inspect actual launched attempts, not merely policy return values.

## 5. Budget invariants

```text
I-BUD-001  Retry tokens cannot become negative.
I-BUD-002  Hedge tokens cannot become negative.
I-BUD-003  Every token consumption is attributable to an attempt.
I-BUD-004  Budget refill is bounded and monotonic according to configured policy.
I-BUD-005  Retry and hedge accounting remain independent.
I-BUD-006  Amplification cannot exceed the maximum implied by attempt and budget limits.
```

## 6. Admission invariants

```text
I-ADM-001  current_limit remains within [min_limit, max_limit].
I-ADM-002  admitted_inflight never exceeds the active limit except for explicitly documented transition tolerance.
I-ADM-003  pending admission queue is bounded or absent.
I-ADM-004  malformed controller samples cannot publish NaN/inf limits.
I-ADM-005  controller failure has a defined safe fallback.
```

## 7. Replica-state invariants

```text
I-REP-001  Removed replicas cannot be selected by new requests.
I-REP-002  Replica state memory remains valid for callbacks of already-issued attempts.
I-REP-003  Health transitions require defined evidence; one arbitrary sample cannot create an illegal state.
I-REP-004  Published policy snapshots are internally coherent.
I-REP-005  Recovery can return a replica to eligible service.
```

## 8. Telemetry invariants

```text
I-TEL-001  Telemetry failure cannot block the request path indefinitely.
I-TEL-002  Telemetry buffers are bounded.
I-TEL-003  User-controlled high-cardinality values are not exported as unrestricted metric labels.
I-TEL-004  Metrics counters reflect logical requests and backend attempts separately.
I-TEL-005  Diagnostic sampling cannot alter request semantics.
```

## 9. Resource invariants

```text
I-RES-001  Request queues are bounded.
I-RES-002  Per-request allocations/objects are released after terminal completion.
I-RES-003  File descriptors/sockets do not exhibit unbounded drift under churn.
I-RES-004  Thread count is bounded by configuration/runtime design.
I-RES-005  RSS growth under steady-state soak converges to an explainable plateau.
I-RES-006  Shutdown has a finite configured upper bound.
```

## 10. Configuration invariants

Reject startup/reload if any condition is unsafe or nonsensical:

```text
negative/overflow duration
NaN or infinity
min_limit > max_limit
max_attempts < 1
unbounded queue size
empty replica pool for a required route
duplicate replica identity
invalid target URI
hedging enabled while no alternate replica can exist
hedging/retry enabled on prohibited method policy
budget capacities outside accepted range
```

## 11. Ownership model

The request lifecycle must have one obvious ownership tree.

Recommended model:

```text
RPC reactor/call object
   owns shared/logically stable RequestState
       owns AttemptManager
           owns Attempt records
           owns timers/alarms
           owns downstream client contexts/reactors
```

Any cross-thread/callback reference must have documented lifetime semantics. Raw pointers are allowed only where ownership and lifetime are provably external/stable and tests cover teardown races.

## 12. Synchronization model

Use the simplest synchronization that proves correctness:

- atomics for independent counters/one-word state where semantics are clear;
- mutexes for multi-field invariants or compound state transitions;
- immutable snapshots for read-mostly policy publication;
- no lock-free data structure solely for prestige.

Every non-trivial atomic requires a comment or design note explaining why its memory ordering is sufficient. Defaulting to `seq_cst` is acceptable until profiling proves it is material.

## 13. Debug assertions and death tests

In debug/test builds, impossible states should fail loudly:

- double permit release;
- second logical completion;
- negative/underflow accounting;
- illegal state transition;
- attempt count beyond hard bound;
- invalid snapshot publication.

Death/assertion tests prove these guards work.

## 14. Traceability requirement

Every invariant ID in this document appears in `12_TRACEABILITY_MATRIX.md` with its corresponding test suite and telemetry/evidence source before release.
