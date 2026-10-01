# Standalone gRPC startup TSan reproducer

This program contains only a unary callback service, one client call, and
orderly server shutdown. It does not link ARTC code. The default configuration
uses installed gRPC and Protobuf packages. `ARTC_GRPC_SOURCE_DIR` instead builds
gRPC and the exact dependencies pinned by that gRPC checkout as part of this
reproducer.

Build against the installed system packages:

```sh
cmake -S tests/repro/tsan_grpc_startup -B build/tsan-grpc-system \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
  -DCMAKE_C_FLAGS='-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_CXX_FLAGS='-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread
cmake --build build/tsan-grpc-system --parallel
ldd build/tsan-grpc-system/tsan_grpc_startup
TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 --addr-no-randomize build/tsan-grpc-system/tsan_grpc_startup
```

Build directly against a source checkout with its pinned dependencies:

```sh
cmake -S tests/repro/tsan_grpc_startup -B build/tsan-grpc-pinned \
  -DARTC_GRPC_SOURCE_DIR=/path/to/grpc \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_STANDARD=23 \
  -DCMAKE_C_COMPILER=/usr/bin/gcc \
  -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
  -DCMAKE_C_FLAGS='-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_CXX_FLAGS='-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread
cmake --build build/tsan-grpc-pinned --target tsan_grpc_startup --parallel 6
ldd build/tsan-grpc-pinned/tsan_grpc_startup
TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 --addr-no-randomize build/tsan-grpc-pinned/tsan_grpc_startup
```

`setarch` disables address randomization for the process because this host's
TSan runtime otherwise exits at startup with `unexpected memory mapping`.

## Captured comparison

System packages on Ubuntu 24.04:

```text
gRPC       1.51.1-4.1build5
Protobuf   3.21.12-8.2ubuntu0.3
Abseil     20220623.1-3.1ubuntu3.2
Compiler   GCC 13.3.0
TSan       -fsanitize=thread -fno-omit-frame-pointer
```

Thirty fresh system-stack processes all reported a TSan race before
`BuildAndStart()` returned: 22 reports named
`GraphCycles::GetId`, and 8 named gRPC's `Epoll1Poller::DoEpollWait`. No ARTC
code is linked. The captured reports and complete `ldd` output are in
`evidence/`.

The pinned comparison used gRPC `v1.84.0` at
`3252a89f10d8e92997862167ca7d095ecda85973`, Abseil `20250512.1`, and Protobuf
`35.1.0`, with the dependency commits recorded by that gRPC release. All C and
C++ dependency sources were built with TSan. Across 130 fresh processes, all
completed server startup, one unary call, and orderly shutdown with exit code 0
and no TSan report. The binary links those libraries statically; its dynamic
dependencies contain no gRPC, Abseil, or Protobuf shared library.

The Ubuntu gRPC and GraphCycles shared objects contain no `__tsan_*` references;
the pinned source-built GraphCycles archive contains TSan hooks. Abseil warns
that mixed instrumented application code and uninstrumented prebuilt libraries
can produce incorrect sanitizer reports and recommends building dependencies
consistently from source ([sanitizer guidance](https://github.com/abseil/abseil-cpp/blob/master/FAQ.md#how-do-i-use-the-llvm-sanitizers-with-abseil)).

To separate the old source versions from the Ubuntu package instrumentation,
the same reproducer was also built against the gRPC `v1.51.1` source release and
its exact direct dependency gitlinks, with every C and C++ dependency
instrumented. The dependencies used their supported C++17 mode because the old
Abseil source fails a GCC 13.3 C++23 build at `absl/time/clock.cc:199`; the
minimal application target remained C++23. In 130 fresh processes, 124 were
clean and 6 reported the same Abseil `Waiter::Post` versus `LowLevelAlloc`
startup access. This program also contains no ARTC code. That report's stack
differs from the Ubuntu package's GraphCycles and epoll reports, so the
instrumentation gap may affect the packaged report signatures, but it is not
the only explanation: the old source stack itself produces a TSan startup
report. The evidence does not prove that the exact GraphCycles report is a
source-level defect. Full details and a representative complete report are in
`evidence/old-source-stack.txt` and
`evidence/old-source-startup-race.txt`; build steps are in
`evidence/old-source-build-recipe.txt`.

A standalone intentional data race was detected by GCC TSan as a positive
control. Its source and full report are `positive_control.cc` and
`evidence/tsan-positive-control-report.txt`.

The complete project was also rebuilt with the pinned stack and TSan. All 26
unit, load-generator, and live gRPC integration tests passed without a report.
An active Alarm-backed RPC was cancelled during SIGTERM shutdown, and a valid
open-loop generator run exercised concurrent router callbacks across three
replicas without a report. The lifecycle audit and captured results are in
`evidence/artc-lifecycle-audit.txt`, `evidence/pinned-artc-tsan-suite.log`,
`evidence/pinned-artc-shutdown.txt`, and
`evidence/pinned-artc-generator-concurrency.txt`. The formerly failing health
test also passed in 30 fresh pinned TSan processes; see
`evidence/pinned-health-repetitions.txt`. No TSan suppression or test exclusion
was used.

The pinned dependency stack adds measured latency and load-generator CPU on
this host compared with the preserved system-linked Release build. Matched
10-second runs at 1,000 RPS and direct one-hop/two-hop probes are recorded in
`evidence/pinned-benchmark-comparison.txt`; all had 10,000 successful requests
and no generator saturation. The comparison localizes the increase to the RPC
runtime path and keeps the measured tradeoff explicit.

The original Ubuntu package comparison used 30 fresh processes. The pinned
modern source and old-source controls used 130 each; counts and retained logs
are listed in their evidence files. The system-stack loop is:

```sh
mkdir -p /tmp/tsan-grpc-repro
for n in $(seq -w 1 30); do
  TSAN_OPTIONS=halt_on_error=1:history_size=7 \
    setarch x86_64 --addr-no-randomize build/tsan-grpc-system/tsan_grpc_startup \
    > "/tmp/tsan-grpc-repro/system-$n.log" 2>&1
done
```

For a source-built gRPC v1.51.1 comparison, use the exact commit and dependency
gitlinks listed in `evidence/old-source-stack.txt`. Build its C++ dependencies
with C++17 while keeping the reproducer target at C++23; the temporary CMake
file used for that control is summarized in the same evidence file. Use the
pinned binary path in place of `build/tsan-grpc-system/tsan_grpc_startup` and
change the log prefix to repeat the pinned comparison.

The TSan-only settings used for both builds were:

```text
CMAKE_BUILD_TYPE=Debug
CMAKE_CXX_STANDARD=23
CMAKE_C_FLAGS=-fsanitize=thread -fno-omit-frame-pointer
CMAKE_CXX_FLAGS=-fsanitize=thread -fno-omit-frame-pointer
CMAKE_EXE_LINKER_FLAGS=-fsanitize=thread
TSAN_OPTIONS=halt_on_error=1:history_size=7
```

The checked-in system TSan logs normalize machine-specific build paths to `<ARTC_WORKTREE>` for publication.
