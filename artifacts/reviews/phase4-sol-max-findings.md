# Sol Max read-only AttemptManager findings

## Finding 1: admission permit can release before backend callback drain

`AttemptManager::backend_done()` decrements `active_attempts_`, unlocks, then
calls `release_permit_if_drained_locked()` before telemetry, per-attempt lease
release, and `execute_plan()`. The `BackendReactor` remains registered in
`BackendCallbackDrain` until its destructor. This conflicts with ADR-003's
contract that the logical permit remains held until dispatched local backend
reactors drain. The existing drain test only checks while the backend is still
held, and does not pause a completed `OnDone` while another request attempts
admission.

Minimal regression: route limit one; complete a primary; pause its callback
after manager accounting/unlock but before callback cleanup; issue a second
logical call; require admission rejection and a pending callback drain; resume,
then verify drain and admission recover. A simple implementation candidate is
to keep the move-only permit in `AttemptManager` until destruction, because the
inbound reactor and each backend reactor already retain the manager. Validate
that ownership path and the test before fixing.

## Finding 2: stale non-Healthy latency may suppress all-slow hedge detection

`overloaded_for_hedging_locked()` excludes only Unavailable replicas, while
secondary selection requires Healthy. A fast historical sample on a Recovering
replica can make the whole-pool overload check report a fast candidate even
though that replica cannot receive a hedge; a slow Healthy alternate then
receives the hedge. Existing all-slow coverage uses two slow Healthy replicas
and misses the state mismatch.

Minimal regression: two Healthy replicas above the method target and a third
replica with a historical fast sample that transitions through Unavailable to
Recovering without a fresh success. Hold the primary, fire the hedge timer, and
require one overload denial, no hedge dispatch, and no consumed hedge token.
Candidate fix is for the suppression scan to use the same Healthy eligibility
predicate as secondary selection.

These were reported by code/ADR inspection. The following dispositions use
runtime evidence and the exact ADR lifecycle boundary.

## Phase 4 verification and disposition

### Finding 1: callback-tail permit release

Disposition: **valid boundary clarification; no production change**. ADR-003
requires the permit through attempt completion/accounting in `OnDone`; it does
not require holding admission through all remaining callback-tail cleanup.
`backend_done()` performs manager accounting before releasing the last permit.
`AdmissionPermitStaysHeldUntilLoserAttemptCallbackAccounts` now pauses the
loser's `OnDone` entry after a hedge wins, verifies a second request is rejected
while the callback has not accounted, resumes it, then verifies admission
recovers. This directly checks the contract without adding a second gate or
holding a permit through unrelated telemetry and lease cleanup. The remaining
callback-tail admission cost is not independently measured.

### Finding 2: recovering replica in overload scan

Disposition: **material defect fixed**. The scan now ignores every replica
whose health state is not `Healthy`, matching the separate hedge-target
selector. Before-fix reproduction is preserved in
`phase4-hedge-recovering-before-fix.log`; it observed two Healthy replicas with
p95 above target and one fast `Recovering` replica, then started a hedge instead
of suppressing it. The new deterministic test asserts those exact states,
expects an overload denial, and verifies no hedge token is consumed. Both
post-fix targeted tests passed in `phase4-p0-regressions-after-fix.log` after
rebuilding `artc_integration_tests`.
