# Principal architecture review: ARTC Phase 3 bounded attempts

Review the current implementation as it exists in the accompanying source and focused diff. This is a design challenge, not a rewrite request. Return concrete findings with file/line references, the failure scenario, severity, and a verification suggestion. Separate confirmed architectural defects from tradeoffs or follow-up ideas. If you find no release-blocking architecture defect, say so explicitly.

## Goal and frozen boundary

ARTC Phase 1 provides a reproducible gRPC lab. Phase 2 is frozen at `milestone-2-adaptive-control-core` (`3abdf00`): it owns the request context/deadline, Phase 2 logical admission permit, adaptive selector, replica health/concurrency state, AIMD route limit, snapshots, and logical goodput. Existing Phase 1/2 behavior and negative performance findings must remain intact. Phase 3 adds method-policy-controlled unary hedging and retries with cancellation, hard attempt limits, independent route-wide budgets, and measurable amplification. No streaming, distributed budgets, transport changes, or Phase 4 rollout/soak work.

The intended hard bounds are at most 2 active attempts per logical request and at most 3 total. A hedge is only for idempotent work, at most one per logical request, delayed by the selected replica's measured p95 clamped to the method's configured min/max. The hedge target excludes the primary and must be healthy. Global route pressure and all-slow observed replicas suppress hedges. Retries are replacement work after all attempts in the current primary/hedge group fail, use explicit retryable statuses (default UNAVAILABLE), capped exponential backoff plus injected deterministic jitter, an alternate healthy target, current deadline feasibility, and a separate RetryBudget. Default/unknown policy is non-idempotent and cannot speculate.

## Current ownership and lifecycle design

`RouterService::State` owns backend stubs, selection/controller state, per-method policies, the two route-wide token buckets, retry RNG, metrics, and shutdown state. Admission remains one Phase 2 permit per logical request. The manager retains that permit until logical completion **and** all started backend attempt callbacks drain. Every backend attempt separately owns one replica lease, `ClientContext`, request/response storage, identity, timestamps, and its gRPC reactor callback lifetime. These callback-held shared references keep attempts/manager alive when best-effort cancellation is ignored.

One `AttemptManager` owns each accepted logical call. Its mutex serializes attempt states, timer callbacks, failure-group/retry decisions, winner choice, cancellation decisions, and the sole logical transition from ACTIVE to COMPLETED. It holds a fixed three-slot attempt array, one hedge `grpc::Alarm`, one retry `grpc::Alarm`, and one deadline `grpc::Alarm`. Alarm closures hold weak manager references; backend reactor callbacks hold strong manager/attempt references until terminal callback accounting. First successful backend result wins; failures do not complete while another attempt remains active. When the current group is exhausted, a deterministic permanent-failure precedence is used; retryable failures may schedule one replacement attempt subject to policy, budget, target, and deadline. Caller cancellation, deadline, shutdown, or success completes the logical request once and cancels active losers best effort. Late loser completions only release/account attempt resources.

The current implementation adds a short RouterService-wide `dispatch_mutex` plus atomic closed flag. It serializes the final backend StartCall commit with shutdown: shutdown closes the gate, releases it, then requests stop/cancels managers and closes admission. Attempt creation rechecks shutdown and deadline at the gate. The gate is held through gRPC async Execute/StartCall and timer scheduling. This gives shutdown a clear no-new-dispatch boundary but serializes dispatch commits across logical calls; challenge whether the correctness boundary is sound and whether the contention/callback/lock-order tradeoff is acceptable for this lab.

The logical result is emitted through the gRPC server reactor's once-only finish primitive. Backend cancellation is best effort. ServiceA has simulated cancellation-aware and cancellation-ignoring modes, including dependency work, to distinguish logical completion from backend CPU/work termination.

## Frozen Phase 2 interaction

Primary choice is still the Phase 2 selector. A hedge/retry reserves an independent `ReplicaLease`; selection accepts an excluded replica index. Secondary attempts cannot bypass Phase 2 admission: one logical admission permit remains held until every attempt callback drains, and hedge/retry creation also consumes its own bounded budget. Phase 2 accounting is split into backend-attempt observations and exactly-once logical completion/goodput so speculative losers do not count as extra logical success. The implementation rejects secondary-attempt policy combinations with legacy routing paths that lack Phase 2 logical admission.

## Review questions

Independently identify:

1. Any architectural mistake, missing state/invariant, or boundary likely to be expensive to repair later.
2. Any hidden ownership/lifetime issue across gRPC reactors, borrowed server contexts, attempts, alarms, stop callbacks, caller cancellation, and shutdown.
3. Whether exactly-once completion and loser cleanup hold for simultaneous completion, deadline, cancellation, and shutdown.
4. Whether timer ownership and the dispatch/shutdown linearization are coherent; identify deadlock or stale-callback cases.
5. Whether permit lifetime, per-attempt replica leases, budgets, and controller observations enforce bounded backend concurrency and correct goodput.
6. Whether the hard attempt bound actually holds under concurrent hedge/retry/deadline events and all supported policy combinations.
7. Whether retry/hedge precedence, failure-result semantics, idempotency defaults, and amplification containment have a gap.
8. Any unnecessary complexity, misleading metrics, or test gap that matters for this milestone.

Use the tests and executed evidence as evidence, not as a substitute for architectural reasoning. Do not request Phase 4 campaigns. Keep the review focused and rank only actionable findings.
