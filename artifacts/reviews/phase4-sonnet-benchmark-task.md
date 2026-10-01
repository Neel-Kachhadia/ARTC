# Sonnet checkpoint C — benchmark and evidence review

Review the current Phase 4 benchmark implementation and claims independently.
This is a measurement audit, not a request to tune ARTC or rewrite prose. Report
only material findings, with exact file/manifest references, the failure mode,
and the smallest validating check. Separate confirmed defects from uncertainty
or optional improvements.

Attack these points:

1. Open-loop scheduling and coordinated-omission handling: verify scheduled,
   actual issue, completion, and issue-lag accounting in the generator.
2. Generator saturation and validity: check thresholds, histogram failure,
   and whether error-allowed runs can be mistaken for successful runs.
3. Baseline and ablation fairness: direct backend, round robin, least inflight,
   EWMA, P2C, selector-only, concurrency-only, adaptive plus deadline gate,
   speculation-disabled Phase 3 path, and representative full ARTC.
4. Repetition, warmup, sample size, and uncertainty. Challenge every p999 or
   causal/headline claim that exceeds the evidence.
5. Attempt, goodput, deadline, rejection, cancellation, and wasted-work
   accounting against manifests and analyzers.
6. Shared-host self-interference and the CPU governor/affinity/resource
   topology.
7. Negative results, invalid runs, cherry-picking, and whether conclusions are
   narrowly scoped enough.

Context and known evidence:

- All workloads ran on one host with no CPU isolation; governor was powersave.
- The healthy soak is a valid 3,600-second run: 900,000/900,000 succeeded,
  250 RPS, zero rejection/deadline/error, amplification 1.0, max issue lag
  2,916 us under a 20,000 us validity limit. It has 900,000 latency samples,
  so do not promote RPC p999 as a headline.
- The faulted soak is a valid 1,200-second run: 300,000/300,000 succeeded,
  one 60-second A2 egress-delay fault, amplification 1.0, max issue lag
  1,150 us, cleanup/recovery verified.
- The Phase 4 policy ladder has one 5,000-request trial per policy. Keep it
  descriptive and do not rank close values.
- The isolated-straggler sweep has one trial per hedge delay. The observed
  5 ms hedge improved that run's p99 while adding 1.33x attempt amplification;
  20/50 ms delays were worse. Do not claim a repeated effect.
- Global overload at 2,000 RPS with 50 ms Service B delay shed most scheduled
  calls; report goodput and rejection alongside low latency. All-slow has zero
  goodput. Preserve both as negative/containment results.
- Three low/high cycles were valid, with no rejections; the AIMD limit kept
  increasing during low load and reached its cap by the second high interval.
- A high-load deadline-feasibility run is invalid: required attempt trailers
  were absent on 158,876 outcomes. It is excluded, not silently discarded.
- `perf` hardware counters were unavailable (`perf_event_paranoid=4`); focused
  Callgrind profiles exist, but they are not production flamegraphs.
- Earlier valid deadline-feasibility cases showed reduced goodput without
  fewer deadline misses. These are negative, condition-specific observations.

The required disposition is a short list:

- MATERIAL DEFECT: specify the invalid claim/accounting and evidence needed.
- NO MATERIAL DEFECT: state the specific claims that remain descriptive.
- OPTIONAL: items that do not block Phase 4.

Do not ask for more experiments unless one identified defect makes an existing
claim invalid and the smallest experiment resolves it. Do not request another
review loop.
