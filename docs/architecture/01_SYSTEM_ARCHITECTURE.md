# 01 — System Architecture

## 1. System role

ARTC is a **request-path RPC traffic controller**. The data plane handles live calls. An in-process feedback controller continuously updates immutable/read-mostly policy state from observed outcomes.

The architecture deliberately distinguishes **decision execution** from **feedback calculation** so that request processing does not become coupled to slow telemetry or control-loop work.

```text
                              observations
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │ Feedback Controller      │
                    │                          │
                    │ replica health models    │
                    │ concurrency limit        │
                    │ hedge threshold/budget   │
                    │ retry budget             │
                    │ overload state           │
                    └────────────┬─────────────┘
                                 │ immutable/read-mostly snapshot
                                 ▼
Client ──► ARTC Data Plane ──► Backend replica A1/A2/A3 ──► Dependency B
                 │
                 └──► metrics/traces/events
```

ARTC must remain useful if the telemetry pipeline is down. Observability cannot be a correctness dependency.

## 2. Canonical request-path architecture

```text
Incoming unary RPC
       │
       ▼
RequestContext
(method policy, deadline, idempotency, request id)
       │
       ▼
Deadline Feasibility Gate
       │
       ├── impossible to finish ──► reject early
       │
       ▼
Route Admission Gate
(adaptive concurrency)
       │
       ├── no permit ──► bounded rejection
       │
       ▼
Replica Selector
       │
       ▼
AttemptManager
       │
       ├── primary attempt
       ├── optional hedge attempt
       └── optional bounded retry attempt(s)
       │
       ▼
First valid terminal outcome wins
       │
       ▼
Cancel superseded outstanding attempts
       │
       ▼
Return exactly one logical response
```

## 3. Major runtime components

### 3.1 Ingress / RPC server

Responsibilities:

- receive unary gRPC calls;
- create `RequestContext`;
- resolve `MethodPolicy`;
- derive effective request deadline;
- transfer lifecycle ownership to `AttemptManager`;
- never block worker/callback threads on downstream I/O.

It must not contain routing heuristics, retry rules, or controller math.

### 3.2 RequestContext

Carries immutable logical-request information:

```cpp
struct RequestContext {
    RequestId id;
    MethodId method;
    MethodPolicy policy;
    TimePoint accepted_at;
    TimePoint deadline;
    TraceContext trace;
};
```

Use a monotonic clock internally (`std::chrono::steady_clock`) for elapsed-time and remaining-budget calculations.

### 3.3 MethodPolicy

Per-RPC-method safety and behavior configuration:

```cpp
enum class Idempotency {
    kNonIdempotent,
    kIdempotent,
};

struct MethodPolicy {
    Idempotency idempotency;
    bool hedging_enabled;
    bool retry_enabled;
    uint32_t max_attempts;
    Duration minimum_useful_budget;
};
```

Configuration validation must reject unsafe combinations such as hedging enabled for a non-idempotent method unless the method has an explicitly implemented deduplication contract.

### 3.4 Deadline feasibility gate

Rejects requests that cannot plausibly finish before the caller's deadline.

Conceptually:

```text
remaining_budget = deadline - now
estimated_completion = predicted_queue_delay
                     + predicted_service_time
                     + safety_margin

reject if estimated_completion >= remaining_budget
```

The estimator does not need to be perfect. Its prediction error must be observable and benchmarked.

### 3.5 Adaptive admission controller

Maintains the route-level maximum useful concurrency. V1 uses a deliberately simple AIMD-style controller before considering more sophisticated algorithms.

Requirements:

- configured hard minimum and maximum;
- bounded update rate;
- hysteresis or windowing to avoid single-sample oscillation;
- explicit safe behavior when observations are missing;
- no unbounded pending queue.

### 3.6 Replica model

Per-replica state contains inexpensive request-path signals and rolling statistics maintained outside the hottest decision path.

```cpp
struct ReplicaState {
    std::atomic<uint32_t> inflight;
    double latency_ewma_us;
    double error_ewma;
    double timeout_ewma;
    RollingLatencyWindow latency_window;
    uint64_t completed;
    uint64_t failures;
    uint64_t timeouts;
    TimePoint last_success;
    TimePoint last_failure;
    Availability availability;
};
```

A percentile is never represented as a naive EWMA. Percentiles come from a bounded rolling distribution/histogram.

### 3.7 Replica selector

Produces a primary destination from the current snapshot. It must support pluggable policies for benchmark baselines:

- round robin;
- least inflight;
- EWMA latency-aware;
- power-of-two choices with latency/inflight score;
- ARTC composite selector.

Selection must be side-effect-free except for explicit reservation/inflight accounting.

### 3.8 AttemptManager

The central lifecycle authority for a logical request. It owns:

- primary attempt;
- hedge timer;
- hedge attempt;
- retries;
- downstream client contexts/reactors;
- deadline/cancellation transitions;
- exactly-once logical completion.

No independent module may launch an attempt without going through `AttemptManager`.

### 3.9 Hedge controller

A hedge is permitted only if all conditions hold:

```text
method is hedge-safe
primary is still unresolved
hedge delay/threshold has elapsed
remaining deadline is sufficient
global/route hedge budget has capacity
cluster is not in a state where speculative load is unsafe
distinct eligible replica exists
active-attempt bound will remain valid
```

The hedge target must differ from the primary.

### 3.10 Retry controller

Retry eligibility requires:

```text
retry-safe/idempotent method
retryable failure class
remaining attempt allowance
remaining deadline budget
retry token available
backoff elapsed
```

Backoff includes jitter. Retry and hedge budgets are independent.

### 3.11 Budget controllers

Use independently configurable token-bucket-style budgets:

- `RetryBudget` — bounds failure-driven replacement work;
- `HedgeBudget` — bounds speculative duplicate work.

Budget consumption/release/refill must be fully accounted and observable.

### 3.12 Feedback controller

Consumes completion observations and periodically updates policy state. It must not mutate request-owned objects.

Suggested division:

```text
fast path: inflight counters, timestamp capture, event enqueue
slow path: EWMA/window update, limit adjustment, threshold recomputation
```

The controller publishes coherent snapshots. Requests must not observe half-updated policy structures.

## 4. Runtime topology for the laboratory

```text
Open-loop Generator
       │
       ▼
      ARTC
   ┌───┼───────────┐
   ▼   ▼           ▼
  A1  A2          A3
   \   |          /
      Service B

Fault controller:
  - tc/netem per link/namespace
  - stress-ng per service/cgroup
  - process kill/restart/freeze

Telemetry:
  OTel SDK -> Collector -> Prometheus / Tempo -> Grafana

Benchmark truth:
  generator-side raw latency histograms + run manifest
```

## 5. Explicit non-goals for V1

- streaming RPC hedging/retries;
- global multi-region routing;
- persistent controller state;
- distributed consensus;
- custom transport or HTTP/2 implementation;
- dynamic service discovery;
- Kubernetes control-plane integration;
- production authentication stack;
- algorithmic novelty claims.

These may be discussed as production extensions but must not dilute completion of the core system.
