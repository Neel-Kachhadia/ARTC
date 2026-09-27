# 14 — Technical References

These references inform the engineering approach. ARTC should cite them where relevant without implying that their algorithms or claims are original to this project.

## Tail latency and speculative execution

- Dean, J. and Barroso, L. A. — **The Tail at Scale**. Communications of the ACM / Google Research.
  - https://research.google/pubs/the-tail-at-scale/

## gRPC behavior

- gRPC C++ callback API / best practices.
  - https://grpc.io/docs/languages/cpp/callback/
  - https://grpc.io/docs/languages/cpp/best_practices/
- gRPC deadlines.
  - https://grpc.io/docs/guides/deadlines/
- gRPC request hedging.
  - https://grpc.io/docs/guides/request-hedging/
- gRPC performance guidance.
  - https://grpc.io/docs/guides/performance/

## Overload, retries, and cascading failure

- Google SRE Book — **Handling Overload**.
  - https://sre.google/sre-book/handling-overload/
- Google SRE Book — **Addressing Cascading Failures**.
  - https://sre.google/sre-book/addressing-cascading-failures/
- AWS Builders' Library — **Making retries safe with idempotent APIs**.
  - https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/

## Adaptive concurrency

- Envoy adaptive concurrency filter.
  - https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter.html
- Netflix concurrency-limits.
  - https://github.com/Netflix/concurrency-limits

## Benchmarking / coordinated omission

- Gil Tene — HdrHistogram.
  - https://github.com/HdrHistogram/HdrHistogram
- `wrk2` constant-throughput HTTP benchmarking and coordinated-omission discussion.
  - https://github.com/giltene/wrk2

ARTC uses a custom RPC workload generator rather than `wrk2`, but the measurement issue is directly relevant.

## Observability

- OpenTelemetry C++.
  - https://opentelemetry.io/docs/languages/cpp/

## Usage rule

References support design rationale and comparison. The final README must distinguish:

- established techniques implemented by ARTC;
- ARTC-specific integration decisions;
- original measurements from the ARTC fault laboratory.
