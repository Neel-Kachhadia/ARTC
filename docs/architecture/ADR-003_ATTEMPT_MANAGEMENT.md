# ADR-003: Bounded unary attempt management

## Context

Phase 2 admits one logical request and dispatches one backend attempt. Phase 3
adds speculative hedges and failure-driven retries, so a request can own several
gRPC client calls while still returning one logical result. Launch decisions,
completion, cancellation, deadlines, and shutdown must share one authority.

## Decision

- `MethodPolicy` is resolved once from the exact full RPC method. Unknown methods
  are non-idempotent and get one primary attempt. Idempotency is explicit; method
  names never imply retry or hedge safety.
- A single `AttemptManager` owns logical state and is the only code that can
  start an attempt or choose the terminal result. A mutex guards compound state
  changes. A success before the effective deadline wins; failure waits for all
  active attempts in the current attempt group. A retry is considered only
  after that group fails. If any group failure is not retryable under the method
  policy, the earliest such attempt's gRPC status wins. If retry cannot proceed
  and all failures are retryable, the latest attempt's gRPC status is returned.
- A policy may request at most three total attempts. At most two may be active,
  and at most one hedge is allowed. A retry never launches concurrently with an
  unresolved attempt. Thus primary + hedge + one retry is the maximum.
- Hedge and retry budgets are independent route-wide token buckets. They are
  consumed only when a selected, eligible secondary attempt is committed. A
  dispatched attempt does not refund its token after it loses or is cancelled.
- Hedge delay uses the primary replica's bounded rolling p95, clamped by
  configured minimum and maximum. A replica with no observations uses the
  maximum delay. Hedge target selection is a separate healthy-only pass: it
  excludes the primary, does not advance the primary selector's recovery-probe
  sequence, and never targets a degraded or recovering replica. Retry selection
  excludes every replica already used by the logical request; same-replica
  fallback is allowed only by explicit policy when no unused healthy target is
  available. Hedging is suppressed when other admitted work fills the current
  route limit (`route_inflight > 1 && route_inflight >= route_limit`) or every
  eligible replica's latency estimate exceeds the configured target.
- Retry defaults to `UNAVAILABLE`. Additional status codes require explicit
  idempotent method policy. A logical cancellation or exhausted logical
  deadline terminalizes the manager before a retry can be scheduled. Backoff is
  bounded exponential with deterministic-seedable jitter. It must leave the
  configured minimum useful attempt budget before a retry timer is armed and
  is checked again when it fires.
- A successful attempt is selected by the first completion callback that wins
  the manager mutex while the logical deadline remains live. This defines the
  simultaneous-success race: exactly one response is copied to the caller; the
  remaining attempts become losers and are cancelled best effort.
- A loser canceled by a successful sibling contributes its elapsed time at the
  cancellation request as a rolling, tagged latency lower bound only after the
  client reports `CANCELLED`. This is not a completed service-latency sample,
  failure, success, recovery, or goodput observation. The lower-bound floor
  informs routing and overload checks without claiming the
  canceled RPC's full duration. Wasted attempt time includes loser execution
  before logical completion and local callback drain after it.
- Phase 2's move-only admission permit remains one permit per logical request.
  A dispatched attempt keeps it held until that attempt's `OnDone` enters
  `backend_done()` and its manager accounting completes. The final callback
  releases the permit inside that accounting path, before its remaining local
  cleanup and callback destruction. Thus it counts backend-attempt completion
  and state accounting in route inflight, not the entire gRPC callback tail.
  This bounds active local backend calls by
  `max_active_attempts * route admission limit` without adding another
  concurrency controller. Each attempt owns its own `ReplicaLease` until its
  callback accounts exactly once. Best-effort cancellation does not prove a
  remote server stopped work.
- The manager owns hedge/retry/deadline timers. Timer callbacks retain only a
  weak manager reference and re-check terminal state and remaining deadline.
  Server and backend reactors retain the manager for their callback lifetimes.
  Only the terminal transition accesses the outer server context/response;
  late callbacks only account and release their own attempt.
- Router shutdown closes the dispatch gate, cancels managers, shuts down the
  inbound gRPC server, then waits up to five seconds for outbound attempt
  reactors to finish accounting and destroy themselves before printing the
  final attempt summary. A timeout is returned as a nonzero shutdown result and
  the pending callback count remains visible in the summary.
- Caller cancellation, deadline, or server shutdown terminalizes the logical
  request immediately and cancels timers/attempts best effort. The route permit
  is released after every dispatched attempt has entered and completed manager
  accounting in `OnDone`; local callback-tail cleanup may still be in progress.
  A cancellation-ignoring backend may continue remote work; the lab measures
  that separately.

## Alternatives considered

- Give every backend attempt another Phase 2 permit. This changes the frozen
  Phase 2 admission accounting and makes a one-slot route unable to hedge its
  already-admitted request.
- Release the logical permit at the winning response and add a second global
  attempt gate. That adds another configuration and admission controller; it is
  only justified if measurement shows callback drain materially harms admission.
- Let hedge and retry logic schedule independently. This creates simultaneous
  launch races and makes the total-attempt bound harder to enforce.
- Retry immediately or retry every non-OK status. Both create synchronized
  amplification and can duplicate side effects.

## Tradeoffs and evidence

Holding the logical permit until attempt accounting may conservatively delay
new admission when a dispatched backend callback has not entered `OnDone`.
The cost of the remaining callback tail is not independently measured.
Cancellation remains
best-effort across the RPC boundary; no client-side state can prove that a
remote cancellation-ignoring handler stopped consuming CPU.

The full Phase 3 gate passed on 2026-09-30 (`bash lab/run_phase3_gates.sh`, run
`phase3_20260930_160802_724820`, based on implementation commit `43e42a8`). It
ran the Phase 1 regression and the Docker runtime scenarios below. After adding
deterministic callback-drain and dispatch-fence test controls, the final
code-only gate passed at `396b7ba`: GCC Debug, GCC Release, and Clang each
passed 93 CTest cases; targeted ASan/UBSan/LSan passed 42 and targeted TSan
passed 44. The six directly affected callback, deadline, shutdown, and seeded
race tests also passed in GCC Debug and under TSan. Logs are
`artifacts/reviews/phase3-final-code-gate.log` and
`artifacts/reviews/phase3-current-repair-tsan.log`. The test controls compile
only into `artc_rpc_testing`; production builds use the standard steady clock
and gRPC alarms. Runtime scenarios were not repeated after this test-only seam.
The Phase 1 regression also passed. In the 450-request A2 straggler
run, p99 fell from 154,367 us without hedging to 9,367 us at a 5 ms delay, with
1.333 attempt amplification and 1.11 s aggregate wasted attempt time. Delays of
20 ms and 50 ms produced p99 of 23,695 us and 53,951 us at the same
amplification, so the shortest tested delay had the best measured tail result
for this fault. Across 300 all-replica-slow requests, a five-token hedge budget
limited hedges to five (1.0167 aggregate amplification); overload suppression
was observed. A transient UNAVAILABLE run used 30 retries for 200 requests
(1.15 amplification), while cluster-wide UNAVAILABLE used three retries for 200
requests (1.015 amplification). Non-idempotent and short-deadline runs launched
no extra attempts. Cancellation-aware and cancellation-ignoring backends
completed 67 losing hedge handlers with 641 us and 9.36 s post-cancel work,
respectively.

The complete per-scenario results are under
`artifacts/runs/phase3_20260930_160802_724820/`; the Phase 1 regression is under
`artifacts/runs/phase1-regression-phase3-20260930/`. p999 was unavailable at
these sample counts. Docker CPU/memory snapshots were captured around scenarios,
and two bounded 60-second resource samples were collected with the current
Phase 3 binary. During 6,000 requests with 130 actual hedges, the analyzer
captured 29 samples and passed its bounded-growth thresholds: router RSS rose
1,612 KiB between first and last decile medians, FD and socket counts rose by
two each, and thread count stayed at 21. The detailed measurements are under
`artifacts/runs/phase3-resource-stability-hedge-20260930/phase2-soak/`.
