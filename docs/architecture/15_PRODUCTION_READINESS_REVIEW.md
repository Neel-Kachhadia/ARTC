# 15 — Phase 4 Production Readiness Review

## Decision

**PASS WITH LIMITATION.** ARTC was validated against its documented production-readiness criteria within the implemented scope. It is a single-process Linux C++23 unary-gRPC prototype with a static replica pool and in-memory controller state; the evidence does not establish readiness for every production fleet or deployment environment.

## Review status

| Category | Status | Evidence and boundary |
|---|---|---|
| Architecture | PASS WITH LIMITATION | Request, control, attempt, and lab diagrams are in `README.md`; support is one ARTC process, static replicas, unary gRPC. |
| Correctness | PASS WITH LIMITATION | Exactly-one terminal result, deadline fencing, attempt bounds, permits, timers, and budgets have deterministic unit/integration assertions. The full single-fault catalog also records unrun orders. |
| Concurrency and lifetime | PASS WITH LIMITATION | GCC/Clang builds and ASan/UBSan/LSan/TSan gates passed; focused post-change sanitizer runs include the deadline/retry race. TSan remains bounded to the documented suite. |
| Failure containment | PASS WITH LIMITATION | Overload, retry/hedge pressure, process failure, packet loss, dependency slowdown, and selected compounds were exercised. See `docs/failures/phase4_fault_coverage.csv`; unsupported and unrun cases remain explicit. |
| Recovery | PASS WITH LIMITATION | Replica restart/reintegration, global slowdown recovery, flapping, and a recovery-plus-load incident passed. These are finite seeded trials, not a long-term fleet study. |
| Resource bounds | PASS WITH LIMITATION | One-hour healthy soak and 20-minute faulted soak passed resource analysis; FD/thread/socket/process counts stayed stable and neither analyzer found monotonic growth. RSS rose 1,076 KiB and 2,360 KiB between healthy/faulted analyzer medians respectively. Both soaks used non-idempotent single-attempt policy with hedge/retry disabled, so they do not measure hedge/retry timer or loser-cancellation churn; the shorter deterministic/Compose cases cover those paths. Soak manifests are from `15d08f4` with an unrecorded dirty diff, while final clean-checkout short validation used `f6d9349`. No deliberate ARTC memory or FD exhaustion campaign was run. |
| Performance | PASS WITH LIMITATION | Open-loop manifests, histograms, baseline ladder, ablation, stress, spike, oscillation, and Callgrind evidence exist. Short policy comparisons are single-run and same-host; no general ranking or causal claim is justified. |
| Observability | PASS WITH LIMITATION | Deterministic snapshots/counters and bounded labels are tested; the process emits a shutdown summary and Prometheus text on stdout. No external exporter, collector, queue, or live scrape endpoint exists, so exporter-outage behavior is not applicable. |
| Security and robustness | PASS WITH LIMITATION | Fault targets are label-checked and cleanup is scoped; no host network/firewall/socket permission changes are used. The malformed-input matrix is partial; oversized payload and hostile high-cardinality metadata campaigns were not run. |
| Configuration | PASS | 31 malformed configurations were rejected before serving; default startup succeeded. Runtime reconfiguration is unsupported. Evidence: `artifacts/evidence/phase4/startup-config/config-matrix-result.txt`. |
| Deployment assumptions | PASS WITH LIMITATION | Linux, unary gRPC, static pool, one process, local in-memory state. Dynamic discovery, cross-process coordination, mTLS deployment policy, and Kubernetes integration are outside scope. |
| Rollback and restart | PASS WITH LIMITATION | Scoped router/backend restart and recovery were exercised. Controller state is intentionally ephemeral and rebuilds after restart; production rollout/rollback orchestration is not implemented. |
| Testing | PASS WITH LIMITATION | Fresh GCC/Clang Debug/Release runs passed 98/98 tests each; selected ASan/UBSan/LSan passed 47/47 and targeted TSan passed 49/49. Compose release matrix and earlier startup/shutdown/fault campaigns passed. The 32-seed sequence campaign is not coverage-guided fuzzing. |
| Benchmark credibility | PASS WITH LIMITATION | Open-loop scheduled/issued/completed and issue-lag fields are preserved; saturated/invalid results are excluded. RPC p999 is not claimed below one million observations. Most policy/ablation comparisons have one trial and are descriptive. |
| Reproducibility | PASS WITH LIMITATION | Fresh isolated checkout built all compiler/sanitizer configurations and ran the Compose release matrix successfully. The checkout was based on `f6d9349`; final lifecycle-test and TSan-runner diffs were applied before the full run, so manifests truthfully say `worktree_dirty=true`. The full run had a separate Debug configure prerequisite; after review, the canonical script was made self-configuring and its code-only matrix passed. Compose evidence is reused because only the configure launch changed. Source SHA, image digest, and run details are in `artifacts/evidence/phase4/reproducibility/clean-checkout-validation.txt`. |
| Operational safety | PASS WITH LIMITATION | Docker was used as `neel` via the verified Docker group/ACL path. Fault scripts verify scoped target identity and cleanup; hosted privileged netem CI is not claimed. |
| Documentation and traceability | PASS WITH LIMITATION | README, fault catalog, traceability table, and this review link positive, negative, invalid, and clean-checkout evidence with scope limits. |
| Interview evidence | PASS WITH LIMITATION | `docs/interview/13_INTERVIEW_READINESS.md` contains measured claims, a reproducible defect story, tradeoffs, and explicit scope. Resume claims remain limited to retained artifacts. |

## Request and amplification invariants

- One logical request commits at most one terminal outcome.
- Automatic duplicate attempts require an explicit idempotency policy; unknown methods default to one non-idempotent attempt.
- At most two attempts are active and at most three total attempts are started per request. Hedge and retry budgets are separate; a retry follows failure of the current attempt group.
- Every new attempt rechecks deadline, shutdown, target eligibility, and its budget at the dispatch boundary. Phase 4 added deterministic coverage for a deadline racing a queued retry callback and the retry dispatch fence.
- A backend that ignores cancellation can continue work after logical completion. Cancellation is best effort and that work is measured as wasted or censored work, not described as stopped.

The request-level ceilings bound amplification to at most 3 attempts per logical request; token buckets further bound aggregate speculative attempts over their configured window. They do not create capacity during a cluster-wide outage. In the earlier all-replicas-slow trace, 150 ms egress delay exceeded the 100 ms caller deadline; all 4,000 requests were rejected before dispatch as `REJECT_DEADLINE_INFEASIBLE`. That is deadline-infeasibility shedding, not evidence that no capacity exists under longer deadlines.

## Evidence and open boundaries

- Fault rows use `PASS`, `PASS WITH LIMITATION`, `NOT APPLICABLE`, or `NOT RUN` with a reason in `docs/failures/phase4_fault_coverage.csv`. Thirty-nine catalog/pair rows are explicitly `NOT RUN`; they are not counted as passes. This includes in-scope fault modes without an injector and process-level graceful shutdown under load.
- Pairwise selection is risk-based. Replica removal plus queued hedge, one shutdown/retry/caller-cancel three-way schedule, and overload plus retryable errors plus shortened deadlines remain unrun.
- The seeded chaos artifacts keep failed/invalid attempts. The router SIGKILL load-window result has missing expected attempt-trailer metadata and is not a valid workload measurement; recovery and cleanup were still checked.
- The 32-seed state-sequence replay completed without unresolved invariant failures. It is deterministic bounded sequence testing, not exhaustive state-space exploration or libFuzzer.
- Both soaks ran with hedging/retries disabled and at most one attempt per logical request; their resource result does not cover loser cancellation or hedge/retry timer churn. They are Phase 4 evidence from revision `15d08f4` with an unrecoverable dirty diff. The tracked source diff to `f6d9349` is summarized at `artifacts/evidence/phase4/reproducibility/soak-source-delta.txt`; the final clean-checkout short validation does not claim to reproduce the soak.
- The separate final clean-checkout all-replica run used 120 ms delay and 5 s deadlines; it admitted all 50 sampled calls with 1.10 amplification. The earlier 4,000-request run used 150 ms per-replica egress delay and a 100 ms caller deadline, rejecting all requests as `REJECT_DEADLINE_INFEASIBLE`.
- Process-level SIGTERM-under-load shutdown, including `wait_for_shutdown` exit/drain capture, was not run. In-process deterministic shutdown races are tested; the router SIGKILL/restart experiment is not graceful-shutdown evidence.
- Focused `clang-tidy` static analysis found one nonmaterial dead initialization in `src/control/phase2.cc:648`; it is recorded as optional cleanup and does not affect correctness. The selected checks reported no other diagnostic.
- Telemetry has no remote sink to fail. The stdout snapshot is best-effort operational output; no claim is made about an external collector.
- The host uses the `powersave` governor; generator, router, backends, and local monitoring share one host. Short-run comparisons are descriptive. `perf` counters were blocked by host policy; focused Callgrind profiles are retained.
- The healthy soak had 900,000 latency samples. The sample count is below the project's one-million observation threshold for a serious RPC p999 claim.
- The faulted soak lasted 20 minutes and repeatedly samples resources; no multi-hour faulted soak was run because no observed drift required it.

## Release gate

The final decision is **PASS WITH LIMITATION** within the documented scope. The fresh-checkout release command, compiler/sanitizer matrix, Compose matrix, and cleanup checks completed; remaining `NOT RUN` catalog rows stay visible as scope limitations. The first clean attempt exposed a sanitizer-sensitive test threshold and a TSan-instrumented `protoc` ASLR startup constraint; both harness issues were corrected and the final run passed. This review does not claim “production ready everywhere.”
