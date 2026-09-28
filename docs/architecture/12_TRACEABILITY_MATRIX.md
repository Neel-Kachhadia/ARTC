# 12 — Requirement and Evidence Traceability Matrix

This file prevents ARTC from becoming a pile of disconnected tests. Every critical claim maps to a safety property, telemetry signal, verification method, and release evidence.

## Core matrix

| Requirement | Invariant(s) | Telemetry | Verification | Release evidence |
|---|---|---|---|---|
| Exactly one logical result | I-REQ-001, I-ATT-005 | logical outcomes, attempt outcomes | state-machine + deterministic completion races + lifecycle fuzz | zero duplicate terminals across suites |
| No work after rejection | I-REQ-004 | rejected + backend attempts | unit + E2E reject test | backend attempt delta = 0 |
| Deadline monotonicity | I-REQ-006/007 | remaining budget samples | fake-clock property tests + gRPC propagation integration | propagation test artifacts |
| Permit correctness | I-REQ-008, I-ADM-002 | admitted inflight, limit | property + TSan + stress | no leak/underflow/limit breach |
| Safe non-idempotent behavior | I-SAFE-001/002 | hedge/retry by method class | policy unit + E2E attempt observation + fuzz | 0 unsafe duplicates |
| Bounded retry work | I-BUD-001/003/006 | retry attempts/tokens/amplification | storm test + properties | amplification plot/table |
| Bounded hedge work | I-BUD-002/003/006 | hedge attempts/tokens/amplification | cluster-slow/hedge-storm test | amplification plot/table |
| Distinct hedge target | I-ATT-003 | primary/hedge target diagnostic | integration + property tests | zero same-target violations |
| Controller boundedness | I-ADM-001/004 | limit transitions | unit/property/fault injection | all values within bounds |
| Recovery/reintegration | I-REP-005 | replica state + traffic share | failure-recovery scenario | recovery timeline |
| Telemetry independence | I-TEL-001/002 | exporter failures/queue | collector outage + stress | request behavior unaffected within defined overhead |
| Resource boundedness | I-RES-001..005 | RSS/FD/thread/queue | stress + soak + churn | no unexplained drift |
| Shutdown safety | I-RES-006 + lifecycle invariants | active calls/timers | deterministic shutdown races + E2E | bounded shutdown, sanitizer clean |
| Honest latency measurement | benchmark rules | scheduled/actual/completion | generator tests + open-loop validation | raw histograms + manifest |
| No hidden load-shed win | reporting rule | goodput/rejections/errors | benchmark analysis checks | latency always adjacent to goodput/rejection |

## Fault-model traceability

Before release, every fault ID from `../failures/04_FAILURE_MODEL_AND_FAULT_LAB.md` must appear in a generated or maintained coverage table:

```text
fault_id
scenario_file
test/job name
expected invariants
last passing commit
artifact location
```

## Interview traceability

Every final resume/README claim must map to evidence.

Example:

```text
Claim:
"Reduced p99 under isolated straggler by X while amplification remained Y"

Evidence chain:
README table
  -> experiment E-002
  -> run manifest IDs
  -> raw HdrHistogram files
  -> analysis script
  -> exact git SHA
```

For correctness claims:

```text
Claim:
"Bounded hedging prevents duplicate-work explosion"

Evidence chain:
Invariant I-BUD-006
  -> hedge storm scenario
  -> property tests
  -> E2E metrics
  -> CI/release artifact
```

## Completion rule

A critical requirement with no evidence mapping is unfinished even if the code exists.
