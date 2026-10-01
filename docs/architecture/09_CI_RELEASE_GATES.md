# 09 — CI, Quality, and Release Gates

## 1. Principle

Passing compilation and unit tests is not sufficient for ARTC. CI is organized by cost and confidence.

The repository workflow in `.github/workflows/validation.yml` runs the
non-privileged code gates on pull requests and pushes. It covers diff whitespace
validation, GCC Debug and Release, Clang Debug and Release, deterministic tests,
selected ASan/UBSan/LSan tests, and targeted TSan. A seeded state-event campaign runs nightly. Docker `NET_ADMIN`,
netem, long soaks, and full benchmark matrices remain local/manual release
commands because hosted runners do not provide the lab's verified network and
resource topology.

## 2. Presubmit / every PR

Mandatory:

```text
`git diff --check` whitespace validation
CMake configure + clean build
warnings policy
unit tests
state-machine tests
deterministic lifecycle race tests
ASan/UBSan suite
TSan targeted suite
short fuzz smoke tests
configuration-validation tests
integration smoke test
benchmark smoke test
```

The repository has no checked-in formatter configuration, so CI does not run
`clang-format`; style-only reformatting is kept out of Phase 4 correctness work.

No known sanitizer/race finding may be waived without a documented, narrowly scoped suppression and justification.

## 3. Compiler matrix

At least:

- recent GCC;
- recent Clang.

Where practical, test Debug and Release-like builds. Warnings are treated seriously; configure a deliberate set such as `-Wall -Wextra -Wpedantic` plus selected conversion/shadow checks after noise is reviewed.

## 4. Nightly validation

Nightly jobs run expensive suites:

```text
longer fuzz campaigns
full deterministic fault matrix
selected pairwise fault combinations
stress tests
spike/controller-stability tests
performance regression suite
resource leak/churn tests
multi-hour soak tier where infrastructure permits
```

Failures retain artifacts automatically.

## 5. Scheduled extended validation

Before tagged releases or at a lower cadence:

```text
6h/12h/24h soak
large-sample p999 benchmark scenarios
full recovery suite
broader compiler/dependency matrix
clean-machine reproducibility run
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
6. required fuzz duration completes without unresolved crash;
7. full documented single-fault catalog passes;
8. required compound scenarios pass;
9. retry amplification bound is experimentally verified;
10. hedge amplification bound is experimentally verified;
11. non-idempotent safety tests pass;
12. deadline propagation/invariants pass;
13. recovery suite passes;
14. graceful shutdown suite passes;
15. telemetry-outage suite passes;
16. canonical benchmark suite is reproducible;
17. clean-machine build/run succeeds;
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
