# 13 — Interview Readiness

## 1. Interview objective

ARTC must be explainable as a real engineering system, not as a memorized feature list.

The expected signal is:

> This engineer understands latency distributions, overload, request amplification, concurrency ownership, failure containment, measurement integrity, and where the design stops being valid.

## 2. Three explanation depths

### 30-second version

> ARTC is a C++23 gRPC traffic controller for tail latency under stragglers and overload. It combines deadline-aware admission, adaptive concurrency, replica-aware routing, bounded hedging, and retry budgets. I built a deterministic fault lab and open-loop benchmark harness to measure p99/p999, deadline-goodput, recovery, and backend amplification, then validated the request lifecycle with deterministic race tests, sanitizers, fuzzing, and long-running stress/soak tests.

Do not add performance numbers until measured and committed.

### 2-minute version

Structure:

1. tail-latency problem;
2. request-path architecture;
3. why admission/routing/hedging/retries need coordination;
4. one difficult correctness problem;
5. benchmark methodology;
6. one measured tradeoff;
7. limitation/negative result.

### 15–30 minute deep dive

Be prepared to draw and defend:

- request lifecycle/state machine;
- adaptive concurrency feedback loop;
- retry/hedge amplification controls;
- benchmark methodology and coordinated omission;
- one production-style incident reproduction;
- one concurrency bug/root cause;
- one result where ARTC loses or provides little value.

## 3. Five flagship code areas

### AttemptManager — C++/concurrency centerpiece

Be able to explain:

- ownership;
- concurrent callback races;
- exactly-once completion;
- cancellation;
- timer lifecycle;
- shutdown;
- why a mutex/CAS choice is correct;
- how TSan and deterministic tests validate it.

### AdaptiveConcurrencyController — systems/control centerpiece

Explain:

- why unbounded concurrency creates queueing collapse;
- AIMD behavior;
- sampling window;
- overshoot/settling/oscillation;
- bounds and fallback;
- why more sophisticated Gradient-style control was not the starting point.

### ReplicaSelector — routing centerpiece

Explain:

- why round robin fails under heterogeneous stragglers;
- score inputs;
- stale-health risk;
- fairness/recovery;
- comparison with least-inflight, EWMA, P2C-style baselines.

### Benchmark generator — performance centerpiece

Explain:

- open-loop vs closed-loop;
- coordinated omission;
- scheduled arrival vs actual issue;
- HdrHistogram;
- p999 sample requirements;
- repeated runs and raw artifacts.

### Fault lab — reliability centerpiece

Explain:

- deterministic faults;
- link-specific netem;
- cgroup/resource isolation;
- failure + recovery phases;
- compound failures;
- why chaos without reproducibility is weak evidence.

## 4. Required tradeoff answers

For every mechanism answer four questions:

```text
Why does it exist?
What does it cost?
When does it help?
When does it hurt?
```

### Hedging

Why: reduce isolated straggler tail.

Cost: duplicate backend work.

Helps: one path/replica is slow while alternatives are healthy.

Hurts: cluster-wide overload.

Containment: budget + distinct target + deadline + global overload awareness.

### Retries

Why: recover transient failures.

Cost: extra attempts and delayed completion.

Helps: brief transport/backend unavailability.

Hurts: widespread dependency failure.

Containment: idempotency + bounded attempts + retry budget + backoff + jitter + deadline.

Phase 3 implementation facts to explain:

- `MethodPolicy` is looked up by the exact gRPC method. Unknown methods default
  to non-idempotent, with hedging and retry disabled and one total attempt.
- Automatic duplicates require explicit idempotency. Retry defaults to
  `UNAVAILABLE`; method-specific retryable statuses are validated.
- The hard runtime ceiling is three total attempts and two active attempts.
  Only one hedge is allowed, and a retry waits for the current attempt group to
  fail.
- The logical Phase 2 admission permit covers the request and remains held
  until every local backend callback drains. Each attempt holds its own replica
  lease. This avoids a second concurrency controller while bounding local
  attempt concurrency by the request limit times two.
- Cancellation stops timers and cancels active calls best effort. An ignoring
  backend can keep working after the caller receives its one result; the lab
  reports that wasted work separately.

### Adaptive admission

Why: keep queueing near useful capacity.

Cost: controlled rejections and controller complexity.

Helps: overload/bursts.

Hurts: poor estimator/controller tuning can underutilize healthy capacity.

Containment: bounds + conservative startup + stability measurement.

## 5. Questions ARTC should let you answer from evidence

### Architecture

- Why a proxy/data-plane layer instead of modifying each backend?
- Why keep controller state in memory?
- Why no Redis/database?
- Why use gRPC's own async machinery instead of adding Asio/io_uring to V1?
- Why unary RPC only?
- What changes for streaming?

### Distributed systems

- What is a straggler?
- Why does p99 matter when average latency looks fine?
- How can retries cause cascading failure?
- When does hedging make the system worse?
- What happens during a partial partition?
- What happens when every replica is slow?
- How do you reintroduce a recovered replica safely?
- What if health observations are stale?

### C++

- Who owns request state?
- Can gRPC callbacks run concurrently?
- What races exist between completion, deadline, hedge timer, and cancellation?
- Why use an atomic versus a mutex in a given location?
- What memory ordering is required and why?
- How do you avoid use-after-free during shutdown?
- How did ASan/TSan find or rule out classes of bugs?

### Performance

- What is coordinated omission?
- Why open-loop traffic?
- Why can throughput increase while user experience gets worse?
- What is deadline-goodput?
- How is request amplification measured?
- How many samples make p999 meaningful?
- How do you measure ARTC's healthy-path overhead?

### Reliability/testing

- Why isn't TSan enough?
- How do you deterministically reproduce a race?
- What does fuzzing add beyond unit tests?
- How are compound failures selected?
- What happens if observability is down?
- What happens during shutdown with active hedges/retries?

## 6. Design decisions worth documenting as ADRs

Prepare concise evidence for:

- gRPC callback API and native timers;
- no second event loop in V1;
- unary scope;
- in-memory state;
- AIMD first;
- explicit AttemptManager;
- independent hedge/retry budgets;
- open-loop benchmark generator;
- HdrHistogram/raw artifacts as benchmark truth.

## 7. Mandatory real defect stories

During implementation, preserve at least one meaningful defect story in sanitized technical form:

```text
symptom
production risk
minimal deterministic reproducer
root cause
why earlier tests missed it
fix
new invariant/regression test
```

Never invent a story for interviews. Use a real failure found during development.

## 8. Flagship experiment stories

The final repo should make these easy to discuss:

1. healthy proxy overhead;
2. isolated straggler;
3. all-replica overload;
4. burst/control-loop response;
5. retry storm;
6. failure and recovery;
7. hedge latency-vs-amplification curve;
8. cancellation-aware vs cancellation-ignoring backend.

Each experiment needs one graph with one clear takeaway.

## 9. Production-vs-project honesty

State clearly what V1 does and what a fleet production deployment would add.

Implemented V1:

```text
unary RPCs
single ARTC process per lab deployment
static replica set
in-memory adaptive state
deterministic fault environment
```

Potential production extensions:

```text
service discovery
dynamic configuration
mTLS/authn/authz
multi-instance/fleet rollout semantics
canarying
fleet-level control/telemetry
Kubernetes integration where deployment requires it
```

Knowing what was intentionally omitted is a senior signal.

## 10. Resume bullets

Do not finalize until benchmark numbers exist.

Template 1:

> Built a C++23 adaptive gRPC traffic layer combining deadline-aware admission, replica-aware routing, adaptive concurrency, and bounded speculative execution; reduced measured p99 latency by **X%** under reproducible straggler workloads while limiting backend attempt amplification to **Y%**.

Template 2:

> Designed a deterministic distributed-systems fault laboratory covering network delay/loss, CPU saturation, dependency failure, retry storms, recovery, and request-lifecycle races; validated correctness with ASan/UBSan/TSan, state-machine fuzzing, and multi-hour soak tests across **N** modeled scenarios.

Only use values generated by retained artifacts.

## 11. Interview completion gate

Before calling the project interview-ready, you must be able to:

- draw the architecture from memory in ~60 seconds;
- explain each controller's cost and failure mode;
- explain one race from actual code;
- explain one benchmark pitfall and how the harness avoids it;
- defend why excluded technologies were unnecessary;
- show raw evidence behind the headline result;
- identify where ARTC performs worse than simpler approaches;
- discuss how the design would change at much larger scale.
