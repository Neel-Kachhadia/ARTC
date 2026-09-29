#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
JOBS="${ARTC_BUILD_JOBS:-8}"
LOG_DIR="${ARTC_GATE_LOG_DIR:-artifacts/gates/phase2-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$LOG_DIR"

GRPC_SOURCE="$ROOT/build/debug/_deps/grpc-src"
HDR_SOURCE="$ROOT/build/debug/_deps/hdrhistogram_c-src"
GTEST_SOURCE="$ROOT/build/debug/_deps/googletest-src"
for source in "$GRPC_SOURCE" "$HDR_SOURCE" "$GTEST_SOURCE"; do
  if [[ ! -d "$source" ]]; then
    echo "pinned FetchContent source missing: $source" >&2
    exit 2
  fi
done

cmake_sources=(
  "-DFETCHCONTENT_SOURCE_DIR_GRPC=$GRPC_SOURCE"
  "-DFETCHCONTENT_SOURCE_DIR_HDRHISTOGRAM_C=$HDR_SOURCE"
  "-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=$GTEST_SOURCE"
  -DBUILD_TESTING=ON
)

build_and_test() {
  local label="$1"
  local build_dir="$2"
  shift 2
  cmake -S . -B "$build_dir" -G Ninja "${cmake_sources[@]}" "$@" \
    >"$LOG_DIR/$label-configure.log" 2>&1
  cmake --build "$build_dir" --parallel "$JOBS" \
    >"$LOG_DIR/$label-build.log" 2>&1
  ctest --test-dir "$build_dir" --output-on-failure \
    >"$LOG_DIR/$label-ctest.log" 2>&1
}

build_and_test_tsan() {
  local label="tsan"
  local build_dir="build/phase2-tsan"
  if ! command -v setarch >/dev/null; then
    echo "setarch is required to run TSan with this host's address randomization" >&2
    exit 2
  fi
  setarch "$(uname -m)" -R cmake -S . -B "$build_dir" -G Ninja \
    "${cmake_sources[@]}" \
    -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ \
    -DCMAKE_C_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
    -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" \
    >"$LOG_DIR/$label-configure.log" 2>&1
  setarch "$(uname -m)" -R cmake --build "$build_dir" --parallel "$JOBS" \
    >"$LOG_DIR/$label-build.log" 2>&1
  setarch "$(uname -m)" -R ctest --test-dir "$build_dir" --output-on-failure \
    >"$LOG_DIR/$label-ctest.log" 2>&1
}

build_and_test gcc-debug build/debug \
  -DCMAKE_BUILD_TYPE=Debug
build_and_test gcc-release build/release \
  -DCMAKE_BUILD_TYPE=Release

if ! command -v clang >/dev/null || ! command -v clang++ >/dev/null; then
  echo "Clang compiler is unavailable" >&2
  exit 2
fi
build_and_test clang build/phase2-clang \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
build_and_test asan-ubsan build/phase2-asan-ubsan \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_C_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
build_and_test_tsan

ASAN_OPTIONS="detect_leaks=1:halt_on_error=1" \
  ctest --test-dir build/phase2-asan-ubsan --output-on-failure \
  >"$LOG_DIR/asan-lsan-ctest.log" 2>&1

lab/run_phase1_gates.sh >"$LOG_DIR/phase1-regression.log" 2>&1
ARTC_RUN_ID="${ARTC_PHASE2_RUN_ID:-phase2-$(date -u +%Y%m%dT%H%M%SZ)-$$}" \
  lab/run_phase2_experiments.sh >"$LOG_DIR/phase2-experiments.log" 2>&1

echo "phase2_gate_logs=$LOG_DIR"
