# 10 — Four-Phase Implementation Plan

## Phase 1 — Reproducible RPC and fault laboratory

### Build

- C++23 gRPC unary service protocol;
- three Service-A replicas;
- downstream Service-B dependency;
- open-loop load generator;
- high-dynamic-range latency histograms;
- run manifest/artifact writer;
- Docker Compose laboratory;
- targeted tc/netem fault scripts;
- stress-ng/cgroup CPU pressure;
- process kill/restart/freeze controls;
- round-robin, least-inflight, EWMA/P2C-style baselines.

### Tests

- protocol/config unit tests;
- generator scheduling correctness;
- histogram correctness;
- fault-target correctness;
- fault cleanup correctness;
- clean startup/shutdown.

### Exit gate

One command from a clean environment reproduces a known straggler scenario, captures raw artifacts, and shows a real tail-latency change without benchmark self-saturation.

---

## Phase 2 — Request lifecycle, routing, and admission

### Build

- `RequestContext` and `MethodPolicy`;
- monotonic deadline accounting;
- deadline feasibility gate;
- admission permit lifecycle;
- AIMD concurrency controller;
- replica state and rolling observations;
- pluggable selector interface;
- explicit immutable/read-mostly controller snapshots;
- sampled decision diagnostics.

### Tests

- all request/admission invariants;
- deterministic primary/deadline/cancel races;
- controller bound/property tests;
- stale/missing/outlier observation tests;
- replica removal/recovery tests;
- ASan/UBSan/TSan gates.

### Exit gate

No known race or permit/accounting bug, no unbounded queue, deterministic deadline behavior, and at least one scenario where adaptive admission/routing is measurably characterized against stronger baselines.

---

## Phase 3 — Attempt management and failure containment

### Build

- central `AttemptManager`;
- primary/hedge/retry state machine;
- explicit idempotency rules;
- distinct hedge target enforcement;
- independent retry/hedge token budgets;
- bounded exponential backoff + jitter;
- best-effort loser cancellation;
- cancellation-aware/ignoring backend modes;
- amplification/wasted-work accounting.

### Tests

- primary vs hedge completion race;
- deadline vs hedge/retry races;
- client cancellation races;
- retry storm;
- hedge storm;
- non-idempotent safety;
- token accounting properties;
- lifecycle state-machine fuzzing;
- shutdown during active attempts.

### Exit gate

No configuration can exceed the defined logical/attempt amplification bounds; non-idempotent methods cannot accidentally duplicate; all major lifecycle races are deterministic regression tests; sanitizer/fuzzer gates remain clean.

---

## Phase 4 — Adversarial validation and publication

### Build/execute

- full single-fault catalog;
- pairwise fault matrix;
- selected multi-fault incidents;
- recovery experiments;
- spike/stress tests;
- multi-hour soak tests progressing toward 24h;
- telemetry dashboards;
- controller stability plots;
- ablation suite;
- repeated-run statistical analysis;
- healthy proxy overhead study;
- negative-result scenarios;
- performance-regression baselines.

### Publication artifacts

- architecture diagrams;
- reproducibility instructions;
- benchmark manifests/raw histograms;
- concise results table;
- failure/recovery graphs;
- tradeoff graphs;
- limitations;
- documented defect stories;
- interview deep-dive guide.

### Exit gate

A technically skeptical reviewer can reproduce the results from a clean environment, inspect raw evidence, understand where ARTC helps and hurts, and trace all critical safety claims to tests.

## Scope discipline

Do not add streaming RPCs, Kubernetes, dynamic discovery, Redis, Kafka, custom transport, or io_uring during these phases unless a measured blocker proves the existing architecture insufficient.
