# Sonnet checkpoint C findings and disposition

Review completed once with Claude Sonnet 5.5 High using the first-party Pro
subscription path on 2026-10-01. Native result:
`artifacts/reviews/phase4-sonnet-benchmark-result.json`.

## Material findings

1. **Historical code/image provenance — accepted with a documented limitation.**
   The retained manifests record the base SHA with a dirty worktree. The current
   image ID and creation time, runtime source modification times, and manifest
   start times are in `phase4-benchmark-provenance.txt`. The old runner did not
   record a per-run image ID or dirty diff hash. Historical short-run
   comparisons remain descriptive; the clean-checkout release run is the exact
   provenance check. No historical benchmark or soak was repeated.
2. **CPU per request measured the generator — accepted.** `std::clock()` is
   sampled inside `artc_loadgen`; the README and methodology now say that this
   value is generator CPU and do not use it to claim ARTC overhead. The direct
   backend and router paths differ in trailer processing, and no normalized
   router CPU/request claim is made.
3. **Overload percentiles pool success and rejection — accepted.** The README
   labels them pooled and states that a success-only histogram is unavailable.
   It reports goodput/rejection and limits the observation to the measured AIMD
   configuration; there is no claim of improvement over an unbounded baseline.
4. **p999 reporting threshold — fixed.** `kP999MinimumSamples` now requires
   1,000,000 observations. The existing histogram regression passed at the new
   threshold. Historical sub-million manifest p999 fields are described as raw
   estimates only and are not published as claims.
5. **Ablation instrumentation/order — accepted as a limitation.** The README
   records one trial per row, fixed order, no separate warmup, and decision
   sampling every 100 calls on ARTC/adaptive rows versus zero on simple
   baselines. No cross-row p99 ranking or causal claim is made; no rerun was
   needed for the retained descriptive table.
6. **Deadline-feasibility accounting — accepted and scoped.** One earlier valid
   pair (`phase2-final-20260929-03`) measured 90,652 successes with the gate off
   and 71,842 on, with zero deadline misses in both; it is reported as one
   workload-specific negative result. The later run with 158,876
   `DEADLINE_EXCEEDED` outcomes is invalid due missing attempt trailers and is
   preserved/excluded, not used as a comparison.

## Other evidence cautions

- Faulted soak had 934 of 300,000 completions above 100 ms; its p99 alone hides
  the rare tail. The README includes this count.
- Router `docker stats` was sampled every second on the shared host; this is
  disclosed as self-interference risk.
- A2 CPU stress was injected without CPU pinning/quota; do not claim measured
  router or backend CPU saturation from that scenario.
- Cancellation-aware/ignoring evidence has separate server-side work summaries;
  attempt amplification alone is not a measure of post-cancel work.
- Policy and hedge sweeps remain single-trial descriptions. The host governor
  and same-host topology are documented.

No additional experiments or review loop were requested or started from these
findings.
