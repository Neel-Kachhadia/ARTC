# 07 — Observability and Operations

## 1. Observability principle

Observability is diagnostic and must not be a request-path dependency.
Benchmark comparisons use the same generator sampling and resource-monitoring
settings for each policy.

## 2. Current implementation

```text
ARTC counters/controller snapshots -> shutdown summary + Prometheus text on stdout
open-loop generator -> result manifest, latency histograms, sampled decisions
lab resource monitors -> CPU, memory, process, and container CSV artifacts
```

The benchmark generator remains the source of truth for headline latency
percentiles. The current runtime has no live scrape endpoint, OpenTelemetry
SDK/exporter, trace pipeline, collector, or dashboard. Those are deployment
integrations for future work, not current support claims.

## 3. Production integration signals

The following names describe useful signals for a future production telemetry
integration. They are a design target, not a claim that every signal is
currently emitted.

### Workload

```text
offered_requests_total
issued_requests_total
accepted_requests_total
rejected_requests_total
logical_requests_inflight
```

### Outcomes

```text
logical_success_total
logical_failure_total
deadline_miss_total
client_cancel_total
```

### Attempts

```text
backend_attempts_total
primary_attempts_total
hedge_attempts_total
retry_attempts_total
attempts_inflight
cancelled_loser_total
```

### Admission/controller

```text
concurrency_limit
admitted_inflight
admission_rejected_total
controller_updates_total
controller_limit_increase_total
controller_limit_decrease_total
```

### Replica

```text
replica_inflight
replica_latency_ewma
replica_error_ewma
replica_timeout_ewma
replica_selected_total
replica_availability_state
```

### Budgets

```text
hedge_tokens_available
retry_tokens_available
hedge_budget_denied_total
retry_budget_denied_total
```

### Resource

```text
process_cpu
process_rss
open_fds
thread_count
telemetry_queue_depth
```

Avoid unbounded labels such as raw request ID, arbitrary user value, or full URL metadata.

## 4. Latency dimensions

Distinguish:

- scheduled-arrival to completion — benchmark end-to-end;
- actual issue to completion;
- ARTC processing overhead;
- backend attempt latency;
- optional predicted queue delay;
- time spent waiting for admission if a bounded queue exists.

Do not collapse all timing into one histogram during diagnosis.

## 5. Decision diagnostics

The load generator writes bounded sampled decision rows for benchmark analysis.
ARTC does not currently emit per-request decision logs.

A sampled decision record may contain:

```text
request_id=<sampled/internal>
method=Foo/GetBar
remaining_deadline_ms=184
route_limit=64
route_inflight=51
primary=A1
primary_score_components={latency:..., inflight:..., reliability:...}
hedge_threshold_ms=21
hedge_started=true
hedge_target=A3
winner=A3
primary_cancel_requested=true
logical_latency_ms=31
attempt_count=2
```

Diagnostics must never leak secrets or create unbounded storage by default.

## 6. Tracing

Distributed tracing is not currently implemented. If added, spans may include:

```text
logical request
  admission
  primary attempt
  hedge attempt
  retry attempt
  downstream service work
```

Sampling policy is explicit. Full tracing is not required for benchmark truth.

## 7. Fault event correlation

Fault tooling records timestamped events with scenario ID. Offline analysis can
overlay:

```text
fault start
controller reaction
latency spike
limit change
hedge/retry behavior
fault removal
recovery
```

This is essential for explaining experiments and interviews.

## 8. Startup behavior

ARTC startup sequence:

1. parse configuration;
2. validate all safety constraints;
3. initialize gRPC channels/stubs;
4. initialize process-local counters and snapshots;
5. initialize controller snapshot;
6. start serving only after mandatory components are ready.

No telemetry backend is required to start or serve requests.

## 9. Graceful shutdown

Shutdown contract:

1. stop accepting new logical requests;
2. mark shutdown state visible to lifecycle code;
3. allow bounded drain or cancel active requests per configured policy;
4. cancel alarms/timers;
5. stop controller after request observations are safely quiesced;
6. print the final attempt summary and Prometheus text to stdout;
7. release channels/resources in ownership order;
8. exit within configured maximum shutdown interval.

Every step is tested under concurrency.

## 10. Health/readiness

If a health endpoint is added, it should distinguish:

- process alive;
- ready to accept traffic;
- degraded internal controller/telemetry state.

There is currently no external telemetry backend or health endpoint dependency.

## 11. Operational resource bounds

Document and expose configuration for:

- maximum route concurrency;
- maximum simultaneous attempts/request;
- maximum total attempts/request;
- shutdown grace period;

The runtime currently uses no telemetry queue or request queue. Future
diagnostic buffers and any pending queue must have explicit bounds.
