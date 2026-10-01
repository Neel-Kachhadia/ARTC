# 15 — Phase 4 Production Readiness Review

## Decision

**Validated within ARTC's documented scope, with limitations.** This is a single-process Linux C++23 unary-gRPC prototype with a static replica pool and in-memory controller state. The evidence does not establish readiness for an arbitrary production fleet or deployment environment.

## Review status

| Category | Status | Evidence and boundary |
|---|---|---|
| Architecture | PASS WITH LIMITATION | Request, control, attempt, and lab diagrams are in `README.md`; support is one ARTC process, static replicas, unary gRPC. |
| Correctness | PASS WITH LIMITATION | Exactly-one terminal result, deadline fencing, attempt bounds, permits, timers, and budgets have deterministic unit/integration assertions. The full single-fault catalog also records unrun orders. |
| Concurrency and lifetime | PASS WITH LIMITATION | GCC/Clang builds and ASan/UBSan/LSan/TSan gates passed; focused post-change sanitizer runs include the deadline/retry race. TSan remains bounded to the documented suite. |
| Failure containment | PASS WITH LIMITATION | Overload, retry/hedge pressure, process failure, packet loss, dependency slowdown, and selected compounds were exercised. See `docs/failures/phase4_fault_coverage.csv`; unsupported and unrun cases remain explicit. |
| Recovery | PASS WITH LIMITATION | Replica restart/reintegration, global slowdown recovery, flapping, and a recovery-plus-load incident passed. These are finite seeded trials, not a long-term fleet study. |
| Resource bounds | PASS WITH LIMITATION | One-hour healthy soak and 20-minute faulted soak passed resource analysis; FD/thread/socket/process counts stayed stable and neither analyzer found monotonic growth. RSS rose 1,076 KiB and 2,360 KiB between healthy/faulted analyzer medians respectively. No deliberate ARTC memory or FD exhaustion campaign was run. |
| Performance | PASS WITH LIMITATION | Open-loop manifests, histograms, baseline ladder, ablation, stress, spike, oscillation, and Callgrind evidence exist. Short policy comparisons are single-run and same-host; no general ranking or causal claim is justified. |
| Observability | PASS WITH LIMITATION | Deterministic snapshots/counters and bounded labels are tested; the process emits a shutdown summary and Prometheus text on stdout. No external exporter, collector, queue, or live scrape endpoint exists, so exporter-outage behavior is not applicable. |
| Security and robustness | PASS WITH LIMITATION | Fault targets are label-checked and cleanup is scoped; no host network/firewall/socket permission changes are used. The malformed-input matrix is partial; oversized payload and hostile high-cardinality metadata campaigns were not run. |
| Configuration | PASS | 31 malformed configurations were rejected before serving; default startup succeeded. Runtime reconfiguration is unsupported. Evidence: `artifacts/runs/phase4_config_matrix_20261001_r3/config-matrix/result.txt`. |
| Deployment assumptions | PASS WITH LIMITATION | Linux, unary gRPC, static pool, one process, local in-memory state. Dynamic discovery, cross-process coordination, mTLS deployment policy, and Kubernetes integration are outside scope. |
| Rollback and restart | PASS WITH LIMITATION | Scoped router/backend restart and recovery were exercised. Controller state is intentionally ephemeral and rebuilds after restart; production rollout/rollback orchestration is not implemented. |
| Testing | PASS WITH LIMITATION | GCC/Clang, Debug/Release, unit/integration, selected E2E, sanitizers, deterministic state sequences, startup/shutdown, and fault tests passed. The 32-seed sequence campaign is not coverage-guided fuzzing. |
| Benchmark credibility | PASS WITH LIMITATION | Open-loop scheduled/issued/completed and issue-lag fields are preserved; saturated/invalid results are excluded. RPC p999 is not claimed below one million observations. Most policy/ablation comparisons have one trial and are descriptive. |
| Reproducibility | BLOCKED | A clean worktree configure/build/test/lab run is the remaining release gate. Do not treat the existing build tree as clean-checkout evidence. |
| Operational safety | PASS WITH LIMITATION | Docker was used as `neel` via the verified Docker group/ACL path. Fault scripts verify scoped target identity and cleanup; hosted privileged netem CI is not claimed. |
| Documentation and traceability | PASS WITH LIMITATION | README, fault catalog, traceability table, and this review link evidence and limitations. Final clean-checkout evidence must be added after it runs. |
| Interview evidence | PASS WITH LIMITATION | `docs/interview/13_INTERVIEW_READINESS.md` contains measured claims, a reproducible defect story, tradeoffs, and explicit scope. Resume claims remain limited to retained artifacts. |

## Request and amplification invariants

- One logical request commits at most one terminal outcome.
- Automatic duplicate attempts require an explicit idempotency policy; unknown methods default to one non-idempotent attempt.
- At most two attempts are active and at most three total attempts are started per request. Hedge and retry budgets are separate; a retry follows failure of the current attempt group.
- Every new attempt rechecks deadline, shutdown, target eligibility, and its budget at the dispatch boundary. Phase 4 added deterministic coverage for a deadline racing a queued retry callback and the retry dispatch fence.
- A backend that ignores cancellation can continue work after logical completion. Cancellation is best effort and that work is measured as wasted or censored work, not described as stopped.

The request-level ceilings bound amplification to at most 3 attempts per logical request; token buckets further bound aggregate speculative attempts over their configured window. They do not create capacity during a cluster-wide outage. The all-replicas-slow scenario rejected all 4,000 requests and produced zero goodput; that is a containment result, not successful service.

## Evidence and open boundaries

- Fault rows use `PASS`, `PASS WITH LIMITATION`, `NOT APPLICABLE`, or `NOT RUN` with a reason in `docs/failures/phase4_fault_coverage.csv`. Twenty-seven catalog/pair rows are explicitly `NOT RUN`; they are not counted as passes.
- Pairwise selection is risk-based. Replica removal plus queued hedge, one shutdown/retry/caller-cancel three-way schedule, and overload plus retryable errors plus shortened deadlines remain unrun.
- The seeded chaos artifacts keep failed/invalid attempts. The router SIGKILL load-window result has missing expected attempt-trailer metadata and is not a valid workload measurement; recovery and cleanup were still checked.
- The 32-seed state-sequence replay completed without unresolved invariant failures. It is deterministic bounded sequence testing, not exhaustive state-space exploration or libFuzzer.
- Telemetry has no remote sink to fail. The stdout snapshot is best-effort operational output; no claim is made about an external collector.
- The host uses the `powersave` governor; generator, router, backends, and local monitoring share one host. Short-run comparisons are descriptive. `perf` counters were blocked by host policy; focused Callgrind profiles are retained.
- The healthy soak had 900,000 latency samples. The sample count is below the project's one-million observation threshold for a serious RPC p999 claim.
- The faulted soak lasted 20 minutes and repeatedly samples resources; no multi-hour faulted soak was run because no observed drift required it.

## Release gate

The final decision becomes **PASS WITH LIMITATION** after clean-checkout reproducibility succeeds and is recorded. Any remaining `NOT RUN` catalog rows stay visible as scope limitations. This review does not claim “production ready everywhere.”
