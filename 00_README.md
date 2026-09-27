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

- `01_SYSTEM_ARCHITECTURE.md` — system boundaries, runtime topology, component responsibilities.
- `02_REQUEST_LIFECYCLE_AND_CONTROL.md` — precedence, state machine, controller timescales, deadlines, attempts.
- `03_INVARIANTS_AND_CORRECTNESS.md` — executable safety properties and ownership rules.
- `04_FAILURE_MODEL_AND_FAULT_LAB.md` — single faults, compound faults, recovery, deterministic injection.
- `05_TESTING_AND_VERIFICATION.md` — unit, deterministic concurrency, sanitizers, fuzzing, property testing, soak/stress.
- `06_BENCHMARK_METHODOLOGY.md` — open-loop generator, coordinated omission, baselines, ablations, statistics.
- `07_OBSERVABILITY_AND_OPERATIONS.md` — metrics, tracing, diagnostics, startup/shutdown, resource accounting.
- `08_SECURITY_AND_ROBUSTNESS.md` — malformed input, cardinality, resource exhaustion, configuration safety.
- `09_CI_RELEASE_GATES.md` — PR, nightly, release candidate, performance-regression gates.
- `10_IMPLEMENTATION_PLAN.md` — four implementation phases with hard exit criteria.
- `11_REPOSITORY_LAYOUT.md` — source/test/lab/artifact structure and engineering standards.
- `12_TRACEABILITY_MATRIX.md` — requirement → invariant → telemetry → tests → evidence.
- `13_INTERVIEW_READINESS.md` — deep-dive paths, experiment stories, design questions, resume evidence.
- `14_REFERENCES.md` — foundational systems and tooling references.

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
