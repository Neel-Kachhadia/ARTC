# 05 — Testing and Verification Strategy

## 1. Testing objective

ARTC testing is designed to prove invariants and characterize failure behavior, not merely maximize a line-coverage number.

The test pyramid is replaced by a **verification matrix**: each critical behavior is exercised at the cheapest deterministic layer possible and then validated again at selected higher-level integration layers.

## 2. Test classes

### 2.1 Unit tests

Cover pure logic and local state transitions:

- method-policy validation;
- replica score calculation;
- AIMD/controller bounds;
- rolling histogram/window behavior;
- deadline feasibility estimator;
- retry classification;
- backoff/jitter bounds;
- token-bucket accounting;
- config validation;
- error mapping.

Use table-driven boundary tests, including integer/duration overflow edges.

### 2.2 State-machine tests

Model logical request and attempt transitions explicitly. Test every valid transition and verify every invalid transition is rejected.

Required paths include:

```text
primary success
primary terminal failure
primary fail -> retry success
primary slow -> hedge wins
primary slow -> primary wins after hedge starts
client cancellation
request deadline
shutdown cancellation
```

### 2.3 Deterministic concurrency tests

Do not use timing sleeps to hope for races. Use fake clocks, barriers, latches, controllable callbacks, and deterministic test executors where possible.

Force exact races:

```text
primary completion || hedge completion
primary completion || deadline
primary completion || client cancellation
hedge timer || primary completion
retry timer || deadline
permit release || controller limit update
replica removal || selection
config publication || new request
shutdown || all of the above
```

Run each forced interleaving repeatedly.

### 2.4 Integration tests

Use real gRPC channels against test services to verify:

- deadline propagation;
- cancellation behavior;
- status mapping;
- distinct hedge destination;
- retry and hedge budgets across many requests;
- replica recovery;
- telemetry-outage independence;
- process restart behavior.

### 2.5 End-to-end tests

Run generator -> ARTC -> replica cluster -> dependency B with production-like binaries/configuration.

E2E tests verify externally visible properties rather than internal implementation details.

## 3. Sanitizer gates

Maintain separate build presets:

```text
debug
asan-ubsan
tsan
release
benchmark
fuzz
```

### ASan

Zero tolerance for:

- heap/stack use-after-free;
- buffer overflow;
- use-after-scope;
- invalid frees.

### UBSan

Zero tolerance for untriaged undefined behavior, including arithmetic and invalid type/state operations caught by the selected configuration.

### TSan

Run more than unit tests. TSan coverage includes:

- lifecycle/state-machine stress;
- integration requests;
- cancellation/deadline races;
- controller publication;
- shutdown;
- randomized concurrent request runs.

Any unexplained TSan report blocks merge/release.

### Leak checking

Use LSan where compatible, plus process-level steady-state RSS/FD checks during soak testing.

## 4. Fuzzing

### Structured-input fuzz targets

- configuration parser;
- fault-scenario parser;
- method-policy parser;
- metadata handling boundaries;
- any custom serialization/configuration format.

### Stateful lifecycle fuzzing

Generate event sequences such as:

```text
ADMIT
PRIMARY_START
PRIMARY_OK
PRIMARY_FAIL
HEDGE_TIMER
HEDGE_OK
HEDGE_FAIL
RETRY_TIMER
DEADLINE
CLIENT_CANCEL
SHUTDOWN
```

For every sequence assert invariants from `03_INVARIANTS_AND_CORRECTNESS.md`.

The fuzzer should shrink/reproduce failures as a deterministic sequence.

## 5. Property-based testing

Important properties:

```text
P-001 remaining_deadline(t+1) <= remaining_deadline(t)
P-002 visible_terminal_results <= logical_requests
P-003 active_attempts <= configured_simultaneous_bound
P-004 total_attempts <= configured_total_attempt_bound
P-005 non_idempotent_hedges == 0 unless explicit safe contract
P-006 inflight never underflows
P-007 every admission permit acquired is eventually released once
P-008 every consumed budget token is accounted
P-009 completed request cannot launch later work
P-010 published limits remain within configured bounds
```

Property tests must include generated boundary configurations.

## 6. Fault-injection tests

Every fault ID in `../failures/04_FAILURE_MODEL_AND_FAULT_LAB.md` receives one of:

- deterministic unit/test-hook reproduction;
- real integration fault injection;
- benchmark-lab scenario.

A fault is not considered covered because a nearby scenario looks similar.

## 7. Stress testing

Stress tests discover the system's failure boundary.

Drive increasing offered load until saturation and beyond. Verify degradation follows the intended sequence:

```text
healthy admission
-> limit contraction
-> bounded rejection/shedding
-> stable resource consumption
```

and not:

```text
unbounded queue
-> timeout storm
-> retry/hedge storm
-> memory growth
-> crash
```

Track CPU, RSS, FDs, sockets, threads, queue depths, logical RPS, backend attempt RPS, and latency.

## 8. Spike testing

Abrupt transitions such as 1x -> 5x and 5x -> 1x measure controller response:

- rise/reaction time;
- overshoot;
- settling time;
- oscillation;
- restored throughput/goodput.

## 9. Soak testing

Progressively require:

```text
1 hour
6 hours
12 hours
24 hours
```

Long tests periodically inject seeded faults and recoveries.

Pass criteria include no unexplained monotonic drift in:

- RSS;
- file descriptors;
- socket count;
- thread count;
- pending timers;
- queue depth;
- controller state;
- unavailable replica state after recovery.

## 10. Retry-storm verification

Induce widespread retryable errors at controlled load. Plot:

```text
logical request RPS
backend attempt RPS
retry RPS
retry token level
error rate
goodput
```

Prove backend attempt amplification remains within the configured bound.

## 11. Hedge-storm verification

Make all replicas slow enough to cross the hedge threshold. Verify:

- hedge budget caps duplicate work;
- cluster-overload logic reduces unsafe hedging;
- attempt bound holds;
- goodput/rejection behavior remains interpretable;
- no exponential or recursive speculative behavior is possible.

## 12. Shutdown testing

Test graceful shutdown during:

- idle;
- high load;
- active primary;
- active hedge;
- scheduled retry;
- deadline race;
- cancellation race;
- telemetry export;
- controller update.

Expected sequence:

```text
stop accepting new logical requests
bound/drain or cancel existing work
cancel timers
best-effort bounded telemetry flush
release resources in defined ownership order
exit before shutdown deadline
```

## 13. Coverage policy

Line/branch coverage is tracked as a diagnostic, not as the primary quality target. Critical lifecycle, error, and invariant paths require explicit named tests even if aggregate coverage is already high.

## 14. Defect discipline

Every serious defect discovered during development produces:

1. minimal deterministic reproducer;
2. root-cause note;
3. code fix;
4. permanent regression test;
5. invariant/fault-model update if the defect exposed a missing assumption.

These defect stories become high-value interview evidence.
