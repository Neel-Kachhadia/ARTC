# ARTC Phase 4 early production-validation review

Act as the principal engineer challenging this Phase 4 validation plan. This is a bounded review of production risks and evidence coverage, not a request to redesign ARTC or add features. Use the repository artifacts as evidence; treat the status summary below as a claim to check.

## System and frozen contracts

ARTC is a C++23 unary gRPC traffic controller with one router process, a statically configured three-replica A pool, and an optional shared Service B dependency. Its in-process Phase 2 controller provides replica health/routing, deadline-feasibility admission, AIMD concurrency, immutable snapshots, and goodput accounting. Phase 3 centralizes every backend launch in `AttemptManager`.

Frozen Phase 3 safety contracts:

- At most 2 active and 3 total backend attempts per logical request; at most one hedge.
- Unknown methods default to non-idempotent and one primary attempt. Automatic retries/hedges require explicit idempotency.
- One AttemptManager owns launch and exactly-one logical completion. Hedge and retry budgets are independent and route-wide.
- One Phase 2 admission permit belongs to a logical request and remains through local callback accounting; each dispatched attempt owns one replica lease. Remote cancellation remains best effort.
- Deadlines use a monotonic effective budget; no new attempt may start after expiry. Shutdown closes the dispatch boundary, cancels managers, stops the server, and waits up to five seconds for backend callbacks.
- Phase 1/2/3 milestones and their contracts are frozen.

## Current implementation boundaries

- Request/attempt/controller callbacks and local attempt accounting are concentrated in `src/app/services.cc`; control and replica health live in `src/control/phase2.cc`; primary/secondary selectors live in `src/routing/selector.cc`.
- Startup configuration is strict environment/CLI parsing in `src/app/lab_main.cc`; there is no structured config-file parser or reload path.
- `artc_loadgen` supports open-loop constant, Poisson, step, ramp, burst, and scripted arrivals. It records scheduled/actual issue/completion timing and rejects runs that exceed issue-lag limits.
- The lab supports fixed Service A/B delays, first-N UNAVAILABLE responses, cancellation-aware/ignoring Service A, per-container `tc netem` delay, stress-ng CPU load, pause/resume, process kill/restart, and Compose-scoped cleanup. Existing Phase 1 and Phase 3 scripts verify some target and cleanup state.
- Attempt counters are exposed in bounded-cardinality shutdown text/trailers and benchmark artifacts. There is no OpenTelemetry SDK/exporter, collector, dashboard, or remote metrics queue; do not claim those exist.
- There is no Phase 4 runner, fuzz target, `ci/`, or `.github/` configuration yet. Existing tests use deterministic barriers/timers and include seeded event sequences.
- The load generator has no command-line caller-cancellation mode; cancellation storms require the existing deterministic held-back-backend test harness or a narrowly scoped test-driver extension.
- Existing decision-sample output is capped at 10,000 samples. At 500 RPS with one sample per 100 requests, it fills in about 33 minutes; a one-hour soak needs sparser sampling or segmented artifacts.
- The checked-in Graphify map has 1,039 nodes but predates AttemptManager; it is useful only as an older Phase 1/2 map. The source tree was clean at baseline `15d08f4`, one commit after `milestone-3-bounded-attempt-management`; that extra commit tracks generated Graphify output and concise Phase 3 review/evidence files. The Phase 4 review brief, focused source packet, and Sol High plan note are new untracked review artifacts; implementation files remain unchanged. Local ignored Phase 1–3 run evidence is preserved.

## Existing evidence and negative results

The Phase 3 evidence report records GCC Debug/Release and Clang test gates, targeted ASan/UBSan/LSan and TSan passes, Phase 1 regression, and Docker fault scenarios. The source is unchanged since the Phase 3 milestone; no Phase 4 result has been run yet.

Existing measured limits to preserve:

- A 450-request A2 straggler scenario measured p99 154,367 us unhedged versus 9,367 us with a 5 ms hedge delay, at 1.333 attempt amplification and 1.11 s aggregate wasted attempt time. 20/50 ms delays were worse. p999 is unavailable at this sample count.
- A 300-request all-replica-slow scenario was bounded by a five-token hedge budget (amplification 1.0167); hedges stopped after budget exhaustion.
- Transient UNAVAILABLE used 30 retries per 200 logical calls (amplification 1.15); cluster-wide UNAVAILABLE used only three retries per 200 (amplification 1.015).
- Cancellation-aware versus cancellation-ignoring handlers continued 641 us versus 9.36 s of post-cancel work; client cancellation does not prove remote work stopped.
- Phase 2 deadline feasibility did not improve goodput in the paired A2 workload and rejected no requests there. Phase 2 AIMD did not settle during the measured high-load window and showed oscillation; its return-to-baseline took 18.39 s.
- Pinned gRPC improved sanitizer/reproducibility confidence at a measurable local RPC latency cost. No Phase 4 healthy-path claim should ignore that baseline or compare incompatible environments.

Docker lab commands succeeded without sudo in the host-capable execution context. No host socket or network permissions were changed. Continue Docker lab work through the supported scoped execution path.

## Draft risk-based validation plan

### Modeled single faults

Use the existing fault model as the catalog, but mark unsupported modes explicitly `NOT APPLICABLE` with a reason. Reuse unchanged Phase 1–3 evidence where source/environment relevance is valid. Prioritize deterministic local tests for every lifecycle state and timer race; supported real faults for fixed delay, CPU saturation, UNAVAILABLE, process pause/kill/restart, cancellation-aware/ignoring work, dependency delay, and netem; and bounded load shapes already supported by the open-loop generator. Add a small machine-readable coverage file with `PASS`, `FAIL`, `NOT APPLICABLE`, or `NOT RUN` for every catalog ID. No unknown state and no claim of universal failure coverage.

Explicit boundaries expected to be out of model unless new repository support is justified: external telemetry exporter failure (no exporter exists), dynamic replica removal/discovery, memory-allocation fault injection, packet corruption/reordering/asymmetric link control, arbitrary malformed RPC responses that protobuf/gRPC rejects before ARTC, and host-wide faults. Do not mutate host networking or issue unscoped process/container commands.

### Pairwise set (draft)

Select a small, compatible subset with one distinct risk per case:

1. Burst load + single A2 straggler — local capacity imbalance.
2. Burst load + all replicas slow — global overload and shedding.
3. Packet loss + retries with retry budget nearly empty — transport failure amplification.
4. Added latency + hedging — tail benefit versus duplicate work.
5. Service B slowdown + A2 CPU saturation — shared dependency versus local health.
6. Short deadlines + high offered load — admission and no-useless-work behavior.
7. Retryable errors + depleted budget — strict retry ceiling.
8. Hedge pressure + route concurrency pressure — route permit versus active-attempt bounds.
9. Replica recovery + load spike — reintegration and control stability.
10. Caller cancellation + hedge completion — exactly-one completion and loser accounting.
11. Deadline + retry timer — no post-expiry retry.
12. Shutdown + active primary/hedge/retry cohorts across concurrent logical requests — dispatch closure, callback drain, bounded exit. One manager schedules a retry only after its active attempt group drains, so one request cannot hold both an active hedge and pending retry.

Treat 10–12 primarily as deterministic lifecycle tests; network chaos cannot force their exact interleavings.

### Selected multi-fault incidents (draft)

- 5x load burst + A2 CPU saturation + Service B slowdown.
- Netem loss + UNAVAILABLE responses + near-empty retry budget.
- A2 straggler + hedging + caller-cancellation burst.
- A2 recovery + load spike + controller limit recovery.
- Shutdown + active hedge + pending retry + caller cancellation across separate concurrent request cohorts (deterministic test sequence).

For the last incident, use concurrent logical requests: within one request, retry is only scheduled after its active attempt group drains, so an active hedge and a pending retry cannot coexist in that same AttemptManager.

Only run incidents supported by the lab; bound each duration, use unique verified Compose project/service identities, persist seed/event log, trap cleanup, and verify all qdiscs/process/pause/container state before the next case.

### Stability, measurement, and PRR plan (draft)

- Start with one 60-minute healthy soak because Phase 2 already has a clean 30-minute soak and Phase 3 has only short resource runs; use sparse/segmented decision samples so the existing 10,000-row cap does not silently truncate the hour. Extend to six hours only if drift/race suspicion warrants it and execution capacity permits. Collect offered/issued/goodput/errors/attempts, route limit/inflight, replica health/budgets, active attempts/timers, RSS/FD/thread/socket/CPU, and validate end-state callback/permit accounting.
- If the healthy soak is clean, run one seeded, bounded faulted soak (about 20 minutes) that repeatedly applies and removes only supported safe faults.
- Separately measure stepped 1x/2x/5x/10x stress up/down, one 1x→5x→1x spike, three low/high oscillation waves, A2 flapping/recovery, and shared Service B slowdown. Invalidate any run where generator lag exceeds its configured bound. Stop load increases when generator or host resources become the bottleneck.
- Reuse open-loop timing and raw Hdr artifacts. Run repeated independent p99 trials for a small canonical baseline/ablation set; only publish p999 with at least 1M successful observations per compared condition. Keep all negative cases.
- Add one bounded packet-loss + backend-restart recovery case or mark it `NOT RUN`; include overload + retryable errors + reduced deadlines in incident B or mark that compound `NOT RUN`. Treat transport loss and injected application `UNAVAILABLE` as separate observed causes.
- Prioritize P0 correctness/accounting/config/lifecycle tests; P1 recovery/fault cleanup/stability; P2 clean Release build, baseline/ablation/profiling; P3 traceability, README, interview evidence, PRR. Do not rerun unrelated expensive experiments after a local repair.
- PRR must distinguish passes from limitations and unsupported deployment concerns: static replicas, unary RPC, single process, in-memory controller, no production authentication/mTLS/fleet deployment, and no telemetry exporter.

## Independent design-review input to verify

Sol Max's read-only AttemptManager review raised a permit-lifetime question against ADR-003: `backend_done()` decrements `active_attempts_` and can release the route permit before telemetry, replica-lease release, `execute_plan()`, `OnDone()` return, reactor destruction, and the global callback-drain decrement. The existing `CallbackDrainWaitsThroughBackendOnDone` test does not pause a completed callback while a second request probes admission. Determine whether the frozen contract requires the permit to remain held until callback destruction/drain, and recommend the smallest regression test/fix if so. Treat this as a question to verify from code and ADR evidence, not a presumed defect.

Sol Max also found a likely mismatch in all-slow hedge suppression: the overload test loop skips `Unavailable` replicas but may count stale fast latency for `Degraded`/`Recovering` replicas, while secondary selection requires a `Healthy` target. Verify the state transition and whether the resulting slow-healthy hedge escapes all-slow suppression; propose only the smallest fix/test if reproducible.

Sol High recommends that the shutdown compound use concurrent request cohorts; cover packet loss + backend restart or mark it `NOT RUN`; include overload + retryable errors + reduced deadlines in multi-fault B or mark it unrun; and require post-fault recovery plus final timer/lease/callback drain in the soak result. Caller-cancellation storms belong in the deterministic held-back-backend harness because the load generator has no caller-cancel mode. Also inspect `lab/run_phase2_experiments.sh`: it suppresses qdisc and stress cleanup errors, which is incompatible with a fail-closed Phase 4 cleanup gate.

## Review request

Challenge only material plan and production-risk gaps. In particular:

1. Which high-risk failures or operational assumptions are missing from the supported model?
2. Are the proposed pairwise set and selected multi-fault scenarios compatible, nonredundant, and high-value? Which should be dropped or added?
3. What recovery, cleanup, shutdown, or bounded-resource check is still absent?
4. Is the soak progression, stress design, and benchmark repetition plan credible for this repository/host? What can be inferred only as a limitation?
5. Which config/input boundaries deserve Phase 4 tests without adding new product surface?
6. What claims must remain out of the PRR/README because evidence or implementation is absent?
7. What is the highest-risk scope expansion to avoid?

Return concrete recommendations with repository locations or scenario IDs. Separate required fixes, useful documentation limitations, and optional polish. Do not propose a new control algorithm or deployment platform.
