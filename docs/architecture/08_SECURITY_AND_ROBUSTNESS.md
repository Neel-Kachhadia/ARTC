# 08 — Security and Robustness

## 1. Scope

ARTC V1 is primarily a systems/performance project, not a complete internet-facing gateway. Nevertheless, request-path infrastructure must defend itself against malformed, adversarial, or resource-exhausting inputs.

## 2. Input boundaries

Test and enforce limits on:

- protobuf message size;
- metadata size/count;
- method-name handling;
- deadline values;
- malformed/unknown configuration fields;
- target addresses;
- diagnostic request identifiers;
- any admin/test-only endpoint.

Rely on gRPC/protobuf validation where appropriate but keep ARTC's own semantic constraints explicit.

## 3. High-cardinality telemetry defense

Never use unrestricted user-controlled values as Prometheus label values.

Prohibited examples:

```text
request_id
raw user id
arbitrary path/query
unbounded error message
full target supplied by client
```

Use bounded enumerations or normalized method/replica IDs.

## 4. Resource-exhaustion defense

ARTC must remain bounded under:

- connection churn;
- many concurrent calls;
- slow clients;
- slow/unresponsive backends;
- retryable-error floods;
- hedge-triggering latency floods;
- telemetry backend outage;
- malformed configuration causing extreme requested limits.

Hard bounds take precedence over adaptive decisions.

## 5. Integer/time safety

All externally/configured durations and counts are range-validated before conversion. Test:

```text
negative values
zero where illegal
maximum representable values
overflowing unit conversions
NaN/inf in floating configuration
```

Current startup checks are fail-closed. Router `ARTC_*` variables must be in
the explicit allowlist; unknown names are rejected before server startup.
Replica addresses must have a non-empty host and numeric port in `1..65535`;
IPv6 literals must be bracketed. DNS resolution and backend reachability remain
runtime health concerns and are not treated as syntax errors.

Current hard ceilings are safety bounds rather than tuning recommendations:

| Setting | Accepted range |
|---|---:|
| AIMD maximum route concurrency | configured minimum through 65,536 |
| AIMD control interval | positive through 24 hours |
| default request deadline | positive through 24 hours |
| deadline safety margin | zero through 24 hours |
| hedge/retry budget capacity | zero through 1,000,000 tokens |
| hedge/retry refill rate | finite and non-negative |

Method-policy parsing also enforces the Phase 3 attempt ceiling (2 active, 3
total), retry count, hedge-delay ordering, finite backoff inputs, status names,
and idempotency values. These are startup-only settings; there is no live reload.

Validation coverage includes five startup-order cases and 20 repeated
start/stop cycles, plus 31 rejected startup-config cases and a valid default
startup. Large payloads, metadata count/size, malformed protobuf wire data,
connection churn, and resolver-specific edge cases are not yet claimed as
comprehensively tested.

Runtime metric labels are fixed enums (`primary`/`hedge`/`retry`, bounded
outcomes and cancellation reasons). Request IDs, raw statuses, endpoint
strings, method names, and caller metadata are not used as labels. There is no
external exporter or telemetry queue to saturate; telemetry outage behavior is
therefore a documented support boundary rather than a simulated collector
failure.

## 6. Configuration publication

A reload, if implemented, follows:

```text
parse -> validate complete candidate -> construct snapshot -> atomic publish
```

Never mutate a live configuration piecemeal.

If reload is not necessary for V1, prefer startup-only configuration rather than introducing unneeded concurrent complexity.

## 7. Test-only fault hooks

Dangerous hooks are unavailable or disabled in release builds/configuration. Examples:

- allocation failure injection;
- callback delay/reordering hooks;
- forced stale controller state;
- internal state mutation endpoints.

Tests verify they cannot accidentally activate under normal production configuration.

## 8. Dependency discipline

Keep the request path dependency surface small. Pin/record dependency versions and run available vulnerability/dependency checks in CI.

Do not add a dependency for functionality trivially provided by the standard library or existing gRPC stack unless it materially improves correctness or maintainability.

## 9. Logging hygiene

Diagnostics must avoid sensitive payloads by default. Log structural metadata, policy decisions, statuses, and timing—not arbitrary request contents.

## 10. Deployment-security extension points

A real production deployment may require:

- mTLS identity;
- authentication/authorization;
- certificate rotation;
- signed configuration;
- restricted admin endpoint/network;
- hardened container/runtime privileges.

These are acknowledged but do not become V1 scope unless the project is otherwise complete.
