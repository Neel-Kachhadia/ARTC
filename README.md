# ARTC — Adaptive RPC Tail Controller

> **Mission:** Build a production-style C++23 RPC traffic-control component that reduces tail-latency failure amplification under stragglers, overload, transient faults, and burst traffic while preserving bounded resource use, measurable goodput, and explainable behavior.
>
> **Engineering bar:** Production correctness + research-grade evaluation + interview-grade explainability.

ARTC is not a generic reverse proxy and it is not a feature-count project. It is an experimental and production-oriented RPC data plane with an in-process feedback controller. It exists to answer a precise systems question:

**Can deadline-aware admission, replica-aware routing, adaptive concurrency, bounded hedging, and retry containment improve p99/p999 behavior under realistic failure modes without hiding the cost in rejection rate, backend amplification, or resource consumption?**

## Non-negotiable principles

1. **Correctness before performance.** A lower p99 is irrelevant if lifecycle races, duplicate side effects, leaks, or unbounded queues exist.
2. **Bound every amplifier.** Retries, hedges, queues, concurrency, telemetry buffers, and shutdown waits must have explicit limits.
3. **Measure end-to-end behavior honestly.** Open-loop traffic, coordinated-omission-safe latency accounting, raw histograms, repeated runs, and workload manifests are mandatory.
4. **Failure and recovery are both first-class.** Every injected failure must include detection, containment, and reintegration behavior.
5. **No hidden benchmark wins.** Latency results are always reported beside goodput, rejection rate, error rate, amplification, CPU, and memory.
6. **Every major mechanism must be isolatable.** Ablation experiments must show what routing, admission, hedging, and retries each contribute.
7. **The request path stays understandable.** Complexity is concentrated in a small number of components with explicit ownership and state machines.
8. **No technology for optics.** No database, Kafka, Kubernetes, custom allocator, io_uring, or extra service unless a measured requirement justifies it.
9. **All claims are reproducible.** A result must map to code revision, configuration, seed, environment manifest, raw artifacts, and analysis.
10. **Interview defensibility is a release requirement.** Important decisions, failures, and tradeoffs must be explainable from actual evidence.

## Architecture pack

- [System architecture](docs/architecture/01_SYSTEM_ARCHITECTURE.md) — boundaries, topology, responsibilities.
- [Request lifecycle](docs/architecture/02_REQUEST_LIFECYCLE_AND_CONTROL.md) — precedence, deadlines, attempts.
- [Invariants](docs/architecture/03_INVARIANTS_AND_CORRECTNESS.md) — safety properties and ownership.
- [Fault model](docs/failures/04_FAILURE_MODEL_AND_FAULT_LAB.md) — deterministic faults and recovery.
- [Testing](docs/architecture/05_TESTING_AND_VERIFICATION.md) — verification layers and release evidence.
- [Benchmark methodology](docs/architecture/06_BENCHMARK_METHODOLOGY.md) — open-loop load and statistical discipline.
- [Observability](docs/architecture/07_OBSERVABILITY_AND_OPERATIONS.md) — metrics, diagnostics, lifecycle.
- [Security](docs/architecture/08_SECURITY_AND_ROBUSTNESS.md) — input and resource bounds.
- [CI gates](docs/architecture/09_CI_RELEASE_GATES.md) — presubmit, nightly, and release criteria.
- [Implementation plan](docs/architecture/10_IMPLEMENTATION_PLAN.md) — phased scope and exit gates.
- [Repository layout](docs/architecture/11_REPOSITORY_LAYOUT.md) — module boundaries and standards.
- [Traceability](docs/architecture/12_TRACEABILITY_MATRIX.md) — requirements to tests and evidence.
- [Interview readiness](docs/interview/13_INTERVIEW_READINESS.md) — technical deep dives and evidence.
- [References](docs/architecture/14_REFERENCES.md) — systems and tooling sources.

## Scope for V1

ARTC V1 intentionally supports **unary request/response gRPC calls only**. It uses a statically configured replica set for deterministic experiments and keeps adaptive state in memory. Streaming RPCs, distributed controller coordination, dynamic service discovery, mTLS fleet rollout, and Kubernetes integration are explicitly outside V1 unless the core system is complete and evidence shows they are necessary.

## Primary stack

| Layer | Choice |
|---|---|
| Data plane | C++23 |
| RPC | gRPC C++ callback API + Protobuf |
| Timers | `grpc::Alarm` or equivalent gRPC-native async timer |
| Build | CMake + Ninja |
| Tests | GoogleTest/GoogleMock + custom deterministic harness |
| Correctness | ASan, UBSan, TSan, LSan where supported |
| Fuzzing | libFuzzer + sanitizer builds |
| Benchmark histograms | HdrHistogram-compatible recording |
| Telemetry | OpenTelemetry C++ |
| Metrics | Prometheus |
| Traces | Tempo |
| Visualization | Grafana |
| Fault lab | Docker Compose + Linux namespaces/cgroups + tc/netem + stress-ng |
| Analysis | Python + pandas + matplotlib, outside the data plane |

## Completion definition

ARTC is not complete when the implementation works on a happy path. It is complete when a clean machine can reproduce the benchmark lab, all modeled failure modes have deterministic tests, race/sanitizer/fuzz gates are clean, resource growth is bounded under long-duration runs, recovery behavior is measured, and every headline result can be traced to raw artifacts.
