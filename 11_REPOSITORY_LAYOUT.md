# 11 — Repository Layout and Engineering Standards

## 1. Proposed layout

```text
artc/
├── CMakeLists.txt
├── CMakePresets.json
├── README.md
├── LICENSE
├── proto/
│   └── traffic.proto
│
├── include/artc/
│   ├── request_context.h
│   ├── method_policy.h
│   ├── attempt_manager.h
│   ├── replica_state.h
│   ├── selector.h
│   ├── admission_controller.h
│   ├── budgets.h
│   └── status.h
│
├── src/
│   ├── proxy/
│   ├── routing/
│   ├── admission/
│   ├── attempts/
│   ├── controller/
│   ├── telemetry/
│   └── common/
│
├── tests/
│   ├── unit/
│   ├── state_machine/
│   ├── concurrency/
│   ├── property/
│   ├── integration/
│   ├── lifecycle/
│   ├── recovery/
│   ├── shutdown/
│   ├── fault/
│   ├── e2e/
│   └── fuzz/
│
├── lab/
│   ├── services/
│   ├── compose/
│   ├── faults/
│   ├── cgroups/
│   └── scenarios/
│
├── bench/
│   ├── generator/
│   ├── baselines/
│   ├── workloads/
│   ├── manifests/
│   └── analysis/
│
├── observability/
│   ├── otel/
│   ├── prometheus/
│   ├── tempo/
│   └── grafana/
│
├── tools/
├── ci/
├── docs/
│   ├── architecture/
│   ├── adr/
│   ├── failures/
│   └── interview/
│
└── artifacts/          # generated/ignored locally; release-selected artifacts only
```

## 2. Module boundaries

### `routing/`

Replica scoring and selection only. It does not launch RPCs.

### `admission/`

Deadline feasibility and concurrency admission. It owns route permits but not backend RPC lifecycles.

### `attempts/`

Primary/hedge/retry/cancellation orchestration. `AttemptManager` lives here.

### `controller/`

Observation aggregation, concurrency updates, rolling windows, budget refill, immutable snapshot publication.

### `telemetry/`

Metrics/tracing/diagnostic events with bounded buffers and no correctness authority.

### `common/`

Only genuinely shared primitives. Do not turn it into a dumping ground.

## 3. Engineering style

- RAII for ownership and cleanup.
- Prefer value semantics and immutable configuration where practical.
- Avoid hidden global mutable state.
- No detached background threads without explicit lifecycle ownership.
- No blocking waits in gRPC callback paths.
- Avoid clever lock-free structures until profiling demonstrates need.
- Keep error/status handling explicit.
- Time values use strong chrono types rather than raw integer units at API boundaries.
- Every externally configurable limit is validated.

## 4. Build profiles

Recommended presets:

```text
debug
release
asan-ubsan
tsan
fuzz
benchmark
```

Benchmark profile disables debug instrumentation that changes hot-path behavior while retaining required correctness checks that are safe for measurement.

## 5. Warning policy

Start strict and intentionally suppress only understood noise. Candidate flags:

```text
-Wall
-Wextra
-Wpedantic
-Wshadow
-Wconversion
-Wsign-conversion
-Werror=return-type
```

Adopt incrementally if third-party/generated gRPC/protobuf code needs isolation from project warning policy.

## 6. Documentation discipline

Architecture documents describe contracts and invariants, not every implementation line.

Create short ADRs only for consequential choices, for example:

```text
ADR-001 gRPC callback API instead of extra Asio/io_uring event loop
ADR-002 unary RPC scope for V1
ADR-003 in-memory adaptive state; no Redis/database
ADR-004 AIMD as initial concurrency controller
ADR-005 independent hedge/retry budgets
ADR-006 open-loop benchmark generator
ADR-007 AttemptManager as sole attempt authority
```

Each ADR contains:

```text
Context
Options considered
Decision
Tradeoffs
Evidence / follow-up experiment
```

## 7. Generated artifacts

Do not commit huge raw benchmark data indiscriminately. Keep:

- small canonical samples in-repo;
- schemas and manifests in-repo;
- release/CI artifacts attached or stored through a reproducible artifact policy;
- generated plots reproducible from retained raw data.

## 8. No architecture theater

The repository should not contain empty abstraction layers, speculative interfaces, or infrastructure that exists only to make the tree look large. Every module must correspond to a real responsibility, invariant, or independent test surface.
