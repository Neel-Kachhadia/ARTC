# ADR-001: Pin the gRPC C++ dependency stack

## Context

The Ubuntu 24.04 system packages used by the initial Phase 1 build were gRPC
1.51.1, Protobuf 3.21.12, and Abseil 20220623.1. The smallest failing ARTC
integration test reported a TSan race during `ServerBuilder::BuildAndStart()`,
before a service callback, channel, client request, reactor, or Alarm lifecycle
ran. The lifecycle audit found no ARTC ownership or shutdown defect on the
exercised paths.

The standalone callback-service reproducer under
[`tests/repro/tsan_grpc_startup`](../../tests/repro/tsan_grpc_startup/README.md)
reported a race in 30/30 fresh processes against those system libraries. The
reports were in `GraphCycles::GetId` in 22 runs and gRPC's epoll poller in 8
runs. The program contains no ARTC code. The prebuilt Ubuntu gRPC and GraphCycles
shared objects have no `__tsan_*` references, unlike the source-built pinned
GraphCycles archive.

To separate the package instrumentation from the old source versions, the
minimal reproducer was built against the official gRPC `v1.51.1` source and its
exact direct dependency gitlinks, with all gRPC C/C++ dependencies
TSan-instrumented. The dependencies used C++17, their supported source mode;
the reproducer remained C++23 because old Abseil 20220623 fails to compile as
C++23 with GCC 13.3 (`absl/time/clock.cc:199`, `constinit` with non-literal
`SpinLock`). Of 130 fresh processes, 124 completed cleanly and 6 reported the
same Abseil startup access: `Waiter::Post()` versus `LowLevelAlloc` while the
gRPC default executor initialized. The signature differs from the packaged
GraphCycles/epoll reports. This confirms an old-stack startup TSan report
independent of ARTC; it does not prove that the exact packaged GraphCycles
report is a source-level defect or exclude an instrumentation effect.

The reproducer was also built with TSan-instrumented gRPC 1.84.0, Abseil
20250512.1, and Protobuf 35.1.0, using the exact dependency commits pinned by
that gRPC release. It completed startup, one unary call, and shutdown in
130/130 fresh processes with no report. A separate GCC TSan positive control
detected its intentional data race. The formerly failing ARTC health test
passed in 30/30 fresh pinned-stack processes, and the complete ARTC TSan CTest
suite passed 26/26 without a report.

## Decision

Build gRPC and its C++ dependencies from the official gRPC `v1.84.0` release
checkout. CMake pins release commit
`3252a89f10d8e92997862167ca7d095ecda85973`, validates that commit, and uses the
release's submodule commits through gRPC's supported CMake `module` providers.
The source-built dependencies use C++23 and are linked statically. The runtime
image no longer installs Ubuntu's gRPC 1.51 package.

## Tradeoffs

The first configure and build fetches and compiles gRPC and its pinned
dependencies, so clean builds take longer and use more build storage. In
exchange, compiler and sanitizer builds use the same recorded dependency set,
and the lab image no longer depends on a host-selected gRPC/Abseil shared
library version. CMake 3.28 is now the minimum because the dependency is kept
out of the default aggregate target with `FetchContent`'s `EXCLUDE_FROM_ALL`.

The dependency change has a measurable local performance cost. In matched
10-second, 1,000-RPS GCC Release runs with 10,000 successful requests, the
router healthy p99 was 1,736us on the pinned stack versus 1,065us on the
preserved system-linked stack; load-generator CPU was 27.89% versus 20.92%.
Direct one-hop and two-hop tests also showed a 45–47% p99 increase, localizing
the change to the RPC runtime path. Throughput and generator validity remained
intact. A second matched full-router run with process RSS sampling completed
10,000/10,000 requests on each stack. The largest per-process RSS increase was
332 KiB for the router (about 1.9%); Service B, A1-A3, and the generator each
used less peak RSS on the pinned stack. The pinned run's issue-lag p99 was
98us versus 175us on the system stack, with both maximum issue lags below the
5,000us validity limit. Generator CPU and RPC latency increased again, in line
with the first matched comparison and direct one-hop/two-hop measurements.

The benchmark regression policy allows an intentional baseline update with
documented evidence and justification. No numeric latency tolerance is
configured for the Phase 1 gate. This ADR records the pinned canonical healthy
run as the Phase 1 reference for later comparisons and preserves the system
stack comparison as historical evidence; it does not claim the latency cost
is immaterial. The comparison and raw manifests are retained in
`tests/repro/tsan_grpc_startup/evidence/pinned-benchmark-comparison.txt` and
`artifacts/runs/`.

## Verification and limits

The pinned stack passed GCC Debug, GCC Release, Clang, ASan/UBSan (Clang 18),
and TSan. ASan/UBSan and TSan each passed all 26 CTest cases; no TSan
suppression or test exclusion was used. The canonical Compose run and all
benchmark, readiness, targeted-fault, recovery, cleanup, CPU saturation,
pause/resume, kill/restart, interrupted-run, repeated-startup, and artifact
gates passed. Exact per-run evidence is retained under `artifacts/runs/` and
`tests/repro/tsan_grpc_startup/evidence/`.

GCC 13.3 plus UBSan could not compile the pinned Abseil `flags/reflection.cc`
constexpr path before reaching ARTC sources; the supported Clang sanitizer
preset passes with LeakSanitizer enabled. This is recorded as a toolchain
limitation, not a sanitizer suppression.

The evidence rules out ARTC as necessary for the startup report: the standalone
reproducer has no ARTC code, and the original report occurs before ARTC invokes
the callback API. The old gRPC/Abseil startup stack produces TSan reports, while
the pinned supported stack is clean in repeated reproducer runs and the full
project suite. The exact packaged `GraphCycles::GetId` report was not reproduced
with the same stack in the instrumented old-source build; do not claim that
specific source location is a proven Abseil defect. The dependency decision
addresses the verified old-stack TSan behavior with a pinned, fully
instrumentable build. Phase 1 still requires the independent review findings
to be reconciled with this local evidence before its exit gate can be declared.
