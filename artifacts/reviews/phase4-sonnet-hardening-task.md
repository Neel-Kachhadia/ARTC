# Phase 4 checkpoint B: independent P0 hardening review

Review the focused implementation and evidence supplied with this packet. This is a correctness review of the existing ARTC design, not a redesign request.

## Frozen behavior and scope

- Single ARTC process, Linux/C++23, unary gRPC, statically configured replica pool.
- Phase 1/2/3 milestones are frozen. Do not recommend feature expansion.
- Attempt bounds remain max 2 active and max 3 total; automatic retries/hedges require idempotency; hedge/retry token budgets remain separate.
- Request completion remains exactly once. Cancellation is best effort, but admission permits and resources must be accounted for through backend `OnDone` accounting.
- The known Phase 3 finding was that hedge overload detection counted a fast Recovering replica. The implementation now considers only Healthy replicas; a deterministic regression test covers it.
- The callback/permit boundary was clarified with Opus A: permit release follows backend `OnDone` accounting and precedes destruction of the local callback tail. That semantic is documented; the callback-tail time is not claimed as separately measured.

## Review focus

Independently inspect the supplied focused diff, the complete affected lifecycle/config code, neighboring APIs, deterministic tests, fuzz harness, and sanitizer/Compose evidence. Identify only material findings in:

1. C++ ownership/lifetime and race risks, especially AttemptManager, callback completion, timer cancellation, caller cancellation, shutdown, and permit release.
2. Bounded resource/amplification invariants, including the Recovering-replica hedge fix.
3. Configuration/startup validation: integer/duration overflow, non-finite values, budgets/limits, strict router environment names, endpoint parsing, duplicate/empty pools, and rejection before serving.
4. Regression test correctness, meaningful negative-path coverage, fuzz-event coverage/reproducibility, and cleanup behavior.
5. Test-only hooks leaking into production builds or startup behavior.

For every finding give severity, file/line, a concrete failing scenario, why current checks miss it, and the smallest test or repair that would prove it. Separate a demonstrated defect from speculation. Do not request another review or recommend cosmetic changes.

## Executed checks supplied

- Full `bash lab/run_phase3_gates.sh`, including Debug/Release/Clang builds and CTest, targeted ASan/UBSan/LSan and TSan suites, plus Docker Phase 3 E2E: exit 0, `phase3_gates=pass`, run `phase3_20261001_121912_95857`.
- Debug CTest: 97/97 passed. Clang CTest: 97/97 passed. Release CTest and build passed in the same script.
- ASan/UBSan/LSan selected suite: 46/46 passed with leak detection and halt-on-error. TSan selected suite: 48/48 passed.
- New Phase 4 config matrix: 26 malformed configurations rejected with expected messages before serving; clean-default router startup reached shutdown summary. Exit 0, run `phase4_config_20261001_121753_95224`.
- Seeded AttemptManager state-event campaign: 16 recorded seeds, all passed; it checks logical completion, active/total attempt bounds, and budget accounting after generated event sequences. It is scoped model-based coverage, not exhaustive fuzzing.
- Docker E2E in the updated Phase 3 gate verified healthy traffic, isolated straggler with/without hedging, all-replica slowdown and recovery, transient UNAVAILABLE, retry-budget exhaustion, non-idempotent suppression, cancellation-aware/ignoring work, and deadline suppression. Generator was valid; Compose cleanup and netem cleanup passed.
- Prior Sol Max review and verified dispositions: `phase4-sol-max-findings.md`. Early Opus architecture review: `phase4-opus-early-result.json`.

## Known limits

- Full Phase 4 chaos/pairwise/stress/soak and benchmark campaign is still in progress; do not infer those gates from this packet.
- The new configuration ceilings are hard safety bounds, not benchmark-derived tuning recommendations.
- Do not claim comprehensive malformed protobuf/payload security testing from this packet.
