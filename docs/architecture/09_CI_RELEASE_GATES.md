# 09 — CI, Quality, and Release Gates

## 1. Principle

Passing compilation and unit tests is not sufficient for ARTC. CI is organized by cost and confidence.

The repository workflow in `.github/workflows/validation.yml` runs the
non-privileged code gates and configuration matrix on pull requests and pushes:
GCC/Clang Debug and Release, CTest unit/integration/lifecycle coverage, selected
ASan/UBSan/LSan and targeted TSan. Its scheduled run adds the fixed-seed
state-sequence campaign (300 seconds). Hosted CI does not run privileged netem,
long soaks, or the full policy/fault matrix; those remain explicit local/manual
release commands.

## 2. Presubmit / every PR

Mandatory:

```text
GCC Debug and Release builds + CTest
Clang Debug and Release builds + CTest
deterministic unit, integration, controller, attempt, timer, and shutdown tests
selected ASan/UBSan/LSan tests
targeted TSan tests
31-case startup configuration validation
```

`lab/run_phase3_gates.sh --code-only` is the workflow entry point. The default
form also builds the Compose image and runs the short integration/attempt
containment matrix for local release validation. CI currently does not run a
Docker benchmark smoke test or privileged fault injection.

The repository has no checked-in formatter configuration, so CI does not run
`clang-format`; style-only reformatting is kept out of Phase 4 correctness work.

No known sanitizer/race finding may be waived without a documented, narrowly scoped suppression and justification.

## 3. Compiler matrix

At least:

- recent GCC;
- recent Clang.

Where practical, test Debug and Release-like builds. Warnings are treated seriously; configure a deliberate set such as `-Wall -Wextra -Wpedantic` plus selected conversion/shadow checks after noise is reviewed.

## 4. Nightly validation

The scheduled workflow currently reruns the compiler/sanitizer/configuration
presubmit, then runs:

```text
fixed-seed state-sequence campaign, capped at 300 seconds
```

The broader deterministic fault, pairwise, chaos, stress, spike, resource
churn, and performance suites are documented/manual release work; the hosted
workflow does not claim to execute them.

Failures retain artifacts automatically.

## 5. Scheduled extended validation

Before tagged releases or at a lower cadence:

```text
one-hour healthy and bounded faulted soaks, when required by release risk
selected recovery, stress, spike, oscillation, and seeded chaos scenarios
canonical baseline and ablation matrix with limitations retained
fresh-checkout configure/build/test/Compose validation
```

## 6. Benchmark regression policy

Stable canonical scenarios establish historical baselines for:

- healthy-path overhead;
- goodput;
- p99/p999 where statistically valid;
- CPU/request;
- RSS at reference concurrency;
- amplification;
- controller settling behavior.

A regression beyond configured tolerance fails or requires an explicit reviewed baseline update with evidence.

## 7. Artifact retention

A failed CI benchmark/fault job retains:

```text
binary/build metadata
manifest
stdout/stderr
fault events
raw histograms
controller/resource time series
sanitizer logs
core dump/backtrace where available
```

A green summary without artifacts is insufficient for major benchmark claims.

## 8. Release-candidate checklist

A release candidate is blocked unless:

1. all mandatory functional suites pass;
2. ASan clean;
3. UBSan clean;
4. TSan clean;
5. leak/resource checks clean;
6. bounded fuzz duration completes without unresolved crash;
7. every modeled fault row is reconciled; required supported cases pass and NOT RUN stays explicit;
8. risk-selected compound scenarios pass or retain a documented limitation;
9. retry amplification bound is experimentally verified;
10. hedge amplification bound is experimentally verified;
11. non-idempotent safety tests pass;
12. deadline propagation/invariants pass;
13. recovery suite passes;
14. graceful shutdown suite passes;
15. telemetry outage is tested where a remote sink exists, otherwise marked not applicable;
16. canonical benchmark artifacts are valid and claims match sample/repetition limits;
17. fresh-checkout build/run succeeds and records source/image provenance;
18. known limitations are documented;
19. raw result artifacts are retained;
20. interview evidence pack is synchronized with actual implementation/results.

## 9. Production-readiness review mindset

Before a tagged "production-style" release, perform a written review asking:

- What can create unbounded work?
- What can create duplicate side effects?
- What happens when dependencies all fail?
- What happens when feedback is stale/wrong?
- What happens during process teardown?
- What data structure can grow forever?
- What resource is not measured?
- Which metric could lie?
- Which benchmark assumption can be violated?
- Which failure has no recovery test?

Every answer must point to implementation or an explicitly documented limitation.
