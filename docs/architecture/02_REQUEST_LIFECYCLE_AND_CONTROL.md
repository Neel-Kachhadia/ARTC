# 02 — Request Lifecycle and Control

## 1. Why precedence is mandatory

ARTC combines multiple feedback mechanisms: admission control, replica routing, hedging, retrying, deadlines, and shedding. Without strict precedence they can form destructive positive-feedback loops.

Example of an unsafe design:

```text
latency rises
  -> hedge rate rises
  -> backend work rises
  -> timeouts rise
  -> retries rise
  -> queues rise
  -> latency rises further
```

The implementation therefore uses one request lifecycle and one attempt authority.

## 2. Canonical decision order

For every logical RPC:

1. Resolve method policy and effective deadline.
2. Check deadline feasibility.
3. Acquire route admission permit.
4. Select primary replica.
5. Start exactly one primary attempt.
6. Arm hedge timer only if the method and state permit hedging.
7. Accept the first valid terminal result according to response policy.
8. Retry only after a retryable failure and only if budget/deadline permit.
9. Cancel superseded attempts best-effort.
10. Release permits exactly once.
11. Emit observation event.
12. Update adaptive state outside the request critical path.

No hedge or retry may bypass deadline, budget, idempotency, or active-attempt limits.

## 3. Logical-request state machine

```text
CREATED
   │
   ▼
ADMISSION_CHECK
   │
   ├── reject ───────────────► TERMINAL_REJECTED
   │
   ▼
ACTIVE
   │
   ├── client cancel ────────► COMPLETING
   ├── deadline ─────────────► COMPLETING
   ├── valid response ───────► COMPLETING
   ├── terminal failure ─────► COMPLETING
   │
   ▼
COMPLETING
   │
   ├── cancel losers
   ├── release permit
   ├── record outcome
   │
   ▼
COMPLETED
```

Only one concurrent callback may win the transition from `ACTIVE` to `COMPLETING`. All other contenders observe that completion has already begun and perform no second terminal action.

## 4. Attempt state machine

```text
NOT_STARTED
    │
    ▼
IN_FLIGHT
    │
    ├── response success ──► SUCCEEDED
    ├── response failure ──► FAILED
    ├── deadline ──────────► TIMED_OUT
    └── cancellation ──────► CANCELLED
```

Attempts are immutable after reaching a terminal state.

## 5. Exactly-once logical completion

A request may have multiple backend attempts but only one externally visible logical completion.

Required invariant:

```text
terminal_transitions_per_logical_request == 1
```

Implementation may use a CAS/state transition or a small synchronized critical section. The choice is driven by correctness and measured contention, not by an assumption that lock-free is inherently better.

## 6. Deadline semantics

ARTC accepts an existing caller deadline or applies an explicit configured upper bound if the caller provides none.

Internally:

```text
remaining = request.deadline - steady_clock::now()
```

A downstream attempt receives only the remaining budget; it never receives a fresh full timeout.

Tests must prove that remaining budget monotonically decreases and that no new attempt starts after the effective deadline.

## 7. Deadline feasibility

A request may be rejected before backend dispatch when its remaining budget cannot plausibly cover expected queueing and service time.

V1 estimator inputs may include:

- route inflight/concurrency limit;
- rolling route or replica latency estimate;
- optional predicted queue delay;
- safety margin.

The estimator must expose:

- predicted completion;
- actual completion when accepted;
- false-admit count;
- false-reject approximation where observable.

## 8. Adaptive concurrency controller

V1 starts with AIMD because its behavior is understandable, testable, and interview-defensible.

Conceptual rule:

```text
healthy window:
    limit = min(limit + additive_step, max_limit)

overload/latency signal:
    limit = max(floor(limit * multiplicative_factor), min_limit)
```

Exact signals and constants are configuration, not hard-coded assumptions.

### Required safeguards

- update only at defined control intervals;
- clamp all values;
- prevent NaN/inf propagation;
- ignore insufficient-sample windows;
- define behavior after long idle periods;
- define cold-start limit;
- expose every limit transition with reason.

## 9. Controller timescales

Different signals update at different rates. One slow RPC must not instantly rewrite every control parameter.

Recommended conceptual separation:

| State | Update style |
|---|---|
| `inflight` | immediate atomic accounting |
| attempt outcome | immediate event capture |
| latency/error EWMA | short rolling interval |
| rolling percentiles | bounded multi-window aggregation |
| concurrency limit | slower control interval |
| health recovery | sustained evidence over multiple intervals |
| budget refill | continuous/time-based or success-coupled |

Exact durations are benchmarked and configured.

## 10. Replica scoring

A score may combine latency, inflight work, recent failures, and timeout history. The formula must remain inspectable.

Example form:

```text
score = latency_component
      * load_component
      * reliability_component
```

The system must preserve individual components in diagnostic output so a selection can be explained after the fact.

## 11. Hedging rules

V1 default bound:

```text
max simultaneously active attempts per logical RPC <= 2
```

A normal hedge creates at most one speculative attempt on a distinct replica.

Hedging must reduce or disable itself during cluster-wide overload. A mechanism intended to cure isolated stragglers must not double pressure when all replicas are unhealthy.

## 12. Retry rules

Retry failures are classified explicitly. Typical retryable classes may include transient transport/unavailable failures; application errors are not automatically retryable.

A retry uses:

```text
backoff = bounded_exponential_backoff(attempt_index)
          + jitter
```

A scheduled retry is cancelled if the logical request completes or the remaining deadline becomes insufficient.

## 13. Cancellation semantics

Cancellation is best effort. ARTC cancels superseded client-side attempts, but backend code may continue running unless it observes and honors cancellation.

The fault lab therefore provides two server modes:

- cancellation-aware;
- cancellation-ignoring.

ARTC measures wasted work in both.

## 14. Failure containment behavior

When all replicas degrade:

1. admission limit should contract;
2. hedging should not increase without bound;
3. retries remain budgeted;
4. queues remain bounded;
5. excess load is rejected before consuming large backend resources;
6. the system continues reporting offered load, accepted load, goodput, and amplification separately.

## 15. Recovery semantics

A failed/degraded replica is not permanently poisoned.

Recovery policy must define:

- minimum evidence before reintegration;
- probing or low-rate traffic strategy if needed;
- rate of restored traffic;
- hysteresis preventing flap-induced oscillation.

Every failure scenario includes a recovery phase and measures reintegration time.
