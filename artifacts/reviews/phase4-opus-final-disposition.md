# Phase 4 final Opus review disposition

- Model: `claude-opus-5-5`
- Requested effort: `medium`
- Provider/auth: `firstParty` / Claude Pro
- Completion: success, one call, no replay
- Verdict: PRR `PASS WITH LIMITATION` supported; no correctness or architecture blocker; documentation/evidence precision corrections required.

## Findings and disposition

| Finding | Decision | Disposition / evidence |
|---|---|---|
| Soaks had hedging, retry, and cancellation paths disabled | Accepted | README, PRR, and traceability now say both were non-idempotent, single-attempt runs. Hedge/retry timer and loser-cancellation resource churn is attributed to the deterministic and short Compose evidence, not the soak. |
| Soak manifests predate final source and are dirty | Accepted with limitation | Manifests retain `15d08f4` / `worktree_dirty=true`. Added `soak-source-delta.txt` with the exact tracked source diff to `f6d9349`, its relevant changes, and the unrecoverable historical dirty-diff caveat. Soaks remain workload evidence but are not described as an exact final-SHA build. |
| Eleven in-scope faults were mislabeled `NOT APPLICABLE` because no injector existed | Accepted | Reclassified five backend latency modes, five network modes, and allocator pressure as `NOT RUN` with exact injector gaps. |
| All-replica slow result lacked cause and configuration | Accepted | The 4,000-call run used 150 ms egress delay and a 100 ms caller deadline; all were `REJECT_DEADLINE_INFEASIBLE` before dispatch. README/PRR/interview now call this deadline-infeasibility shedding. Separate 120 ms / 5 s clean-checkout run admitted 50/50 calls at 1.10 amplification. |
| Fresh-checkout canonical runner had an undocumented Debug configure prerequisite | Accepted | Added `cmake --preset debug` before the Debug build. Updated runner passed the code-only compiler/sanitizer matrix from the isolated checkout; Compose evidence is reused because no Compose behavior changed. |
| Graceful shutdown under heavy load cited SIGKILL evidence | Accepted | `F-PROC-008` is now `NOT RUN` for process-level SIGTERM/drain capture; deterministic in-process races remain separately described. SIGKILL stays under `F-PROC-009`. |
| Configuration matrix CI invocation | Verified; no change | `.github/workflows/validation.yml` runs `lab/run_phase4_config_matrix.sh` as a separate presubmit step. |

Final fault-row counts after reclassification: **41 PASS, 28 PASS WITH LIMITATION, 39 NOT RUN, 12 NOT APPLICABLE** (120 total). No finding required a new experiment or product-code change. No specialist conclusion was found to contradict compiler, test, runtime, sanitizer, soak, or benchmark evidence.
