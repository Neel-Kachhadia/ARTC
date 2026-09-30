#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CODE_ONLY=false
if [[ "${1:-}" == "--code-only" ]]; then CODE_ONLY=true; fi
if [[ "${1:-}" != "" && "$1" != "--code-only" ]]; then
  echo "usage: $0 [--code-only]" >&2
  exit 2
fi

JOBS="${ARTC_BUILD_JOBS:-2}"
BASE="${ARTC_RUN_ID:-phase3_$(date -u +%Y%m%d_%H%M%S)_$$}"
if [[ ! "$BASE" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "ARTC_RUN_ID must contain lowercase letters, digits, '_' or '-'" >&2
  exit 2
fi
OUT_ROOT="$ROOT/artifacts/runs/$BASE"
mkdir -p "$OUT_ROOT"
COMPOSE_FILE="$ROOT/lab/compose.yaml"
CURRENT_PROJECT=""
LOAD_DEADLINE_MS=5000
LOAD_DEPENDENCY=false

compose() {
  docker compose -f "$COMPOSE_FILE" -p "$CURRENT_PROJECT" "$@"
}

cleanup() {
  local result=$?
  trap - EXIT
  if [[ -n "$CURRENT_PROJECT" ]]; then
    docker compose -f "$COMPOSE_FILE" -p "$CURRENT_PROJECT" down --remove-orphans || result=1
  fi
  exit "$result"
}
trap cleanup EXIT

configure_defaults() {
  export ARTC_ROUTING_POLICY=artc_adaptive_no_deadline
  export ARTC_AIMD_INTERVAL_MS=10 ARTC_AIMD_MIN_SAMPLES=4
  export ARTC_AIMD_TARGET_LATENCY_US=50000 ARTC_AIMD_INITIAL_LIMIT=64
  export ARTC_DEFAULT_DEADLINE_MS=5000 ARTC_DECISION_SAMPLE_EVERY=0
  export ARTC_ATTEMPT_MAX_TOTAL=3 ARTC_ATTEMPT_MAX_ACTIVE=2
  export ARTC_ATTEMPT_MINIMUM_BUDGET_US=1000 ARTC_ATTEMPT_JITTER_SEED=31
  export ARTC_HEDGE_BUDGET_CAPACITY=8 ARTC_HEDGE_BUDGET_REFILL_PER_SECOND=0
  export ARTC_RETRY_BUDGET_CAPACITY=8 ARTC_RETRY_BUDGET_REFILL_PER_SECOND=0
  export ARTC_EXECUTE_IDEMPOTENCY=non_idempotent
  export ARTC_EXECUTE_HEDGING_ENABLED=false ARTC_EXECUTE_RETRY_ENABLED=false
  export ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY=false
  export ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS=1 ARTC_EXECUTE_MAX_RETRIES=0
  export ARTC_EXECUTE_HEDGE_DELAY_MIN_US=10000 ARTC_EXECUTE_HEDGE_DELAY_MAX_US=100000
  export ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS=10 ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS=100
  export ARTC_EXECUTE_RETRY_JITTER_MAX_MS=10 ARTC_EXECUTE_RETRYABLE_STATUSES=UNAVAILABLE
  export ARTC_SERVICE_B_DELAY_US=0 ARTC_SERVICE_A_UNAVAILABLE_FIRST_N=0
  export ARTC_SERVICE_A_HONOR_CANCELLATION=true
  export ARTC_SERVICE_A1_DELAY_US=0 ARTC_SERVICE_A2_DELAY_US=0 ARTC_SERVICE_A3_DELAY_US=0
  export ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=0 ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N=0
  export ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N=0
  export ARTC_SERVICE_A1_HONOR_CANCELLATION=true ARTC_SERVICE_A2_HONOR_CANCELLATION=true
  export ARTC_SERVICE_A3_HONOR_CANCELLATION=true
}

set_method() {
  local idempotency="$1" hedging="$2" retrying="$3" max_total="$4" max_retries="$5"
  local hedge_delay_us="$6" hedge_budget="$7" retry_budget="$8"
  export ARTC_EXECUTE_IDEMPOTENCY="$idempotency"
  export ARTC_EXECUTE_HEDGING_ENABLED="$hedging"
  export ARTC_EXECUTE_RETRY_ENABLED="$retrying"
  export ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS="$max_total"
  export ARTC_EXECUTE_MAX_RETRIES="$max_retries"
  export ARTC_EXECUTE_HEDGE_DELAY_MIN_US="$hedge_delay_us"
  export ARTC_EXECUTE_HEDGE_DELAY_MAX_US="$hedge_delay_us"
  export ARTC_HEDGE_BUDGET_CAPACITY="$hedge_budget"
  export ARTC_RETRY_BUDGET_CAPACITY="$retry_budget"
  export ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS=10
  export ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS=100
  export ARTC_EXECUTE_RETRY_JITTER_MAX_MS=10
}

capture_resources() {
  local name="$1" destination="$2"
  local -a ids=()
  while IFS= read -r id; do
    [[ -n "$id" ]] && ids+=("$id")
  done < <(compose ps -q router a1 a2 a3 service-b)
  if ((${#ids[@]})); then
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "${ids[@]}" \
      >"$destination/$name-docker-stats.csv"
  fi
}

run_load() {
  local run_name="$1" duration_ms="$2" rate_rps="$3" allow_errors="$4"
  local directory="$OUT_ROOT/$run_name"
  mkdir -p "$directory"
  mkdir -p "$OUT_ROOT/loadgen-logs"
  export ARTC_RUN_ID="$BASE-$run_name"
  export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
  if [[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
    export ARTC_WORKTREE_DIRTY=false
  else
    export ARTC_WORKTREE_DIRTY=true
  fi
  export ARTC_COMPOSE_PROJECT="$CURRENT_PROJECT"
  local -a error_flag=() dependency_flag=()
  [[ "$allow_errors" == true ]] && error_flag+=(--allow-errors)
  [[ "$LOAD_DEPENDENCY" == true ]] && dependency_flag+=(--invoke-dependency)
  compose run --rm --no-deps loadgen \
    --target router:50050 --output "/artifacts/$BASE/$run_name" \
    --mode constant --duration-ms "$duration_ms" --rate-rps "$rate_rps" \
    --max-inflight 256 --max-issue-lag-us 5000 \
    --deadline-ms "$LOAD_DEADLINE_MS" --work-units 1 \
    "${dependency_flag[@]}" "${error_flag[@]}" >"$OUT_ROOT/loadgen-logs/$run_name.stdout"
  local -a validate_flag=()
  [[ "$allow_errors" == true ]] && validate_flag+=(--allow-errors)
  python3 lab/validate_artifacts.py "${validate_flag[@]}" "$directory"
  capture_resources "after-$run_name" "$directory"
}

run_case() {
  local label="$1" fault="$2" allow_errors="$3" duration_ms="$4"
  local rate_rps="$5" warmup_ms="$6" repeats="$7"
  CURRENT_PROJECT="artc-phase3-$BASE-$label"
  export COMPOSE_PROJECT_NAME="$CURRENT_PROJECT"
  export ARTC_RUN_ID="$BASE-$label"
  export ARTC_WORKTREE_DIRTY=true
  mkdir -p "$OUT_ROOT/$label"
  compose up -d --wait --wait-timeout 90
  compose run --rm --no-deps loadgen --health --target router:50050 \
    >"$OUT_ROOT/$label-health.txt"
  if ((warmup_ms > 0)); then
    run_load "$label-warmup" "$warmup_ms" "$rate_rps" "$allow_errors"
  fi

  case "$fault" in
    none) ;;
    all-netem)
      for service in a1 a2 a3; do
        compose exec -T "$service" tc qdisc add dev eth0 root netem delay 120ms
        compose exec -T "$service" tc qdisc show dev eth0
      done >"$OUT_ROOT/$label-network-fault.txt"
      ;;
    *) echo "unknown fault: $fault" >&2; return 2 ;;
  esac

  mkdir -p "$OUT_ROOT/resources"
  capture_resources "$label-before-load" "$OUT_ROOT/resources"
  for ((batch = 1; batch <= repeats; ++batch)); do
    local run_name="$label"
    if ((batch > 1)); then run_name="$label-repeat$batch"; fi
    run_load "$run_name" "$duration_ms" "$rate_rps" "$allow_errors"
  done

  if [[ "$fault" == all-netem ]]; then
    for service in a1 a2 a3; do
      compose exec -T "$service" tc qdisc del dev eth0 root
      compose exec -T "$service" tc qdisc show dev eth0
    done >"$OUT_ROOT/$label/recovery-qdisc.txt"
    run_load "$label-recovery" 1000 100 false
  fi

  compose stop -t 8
  compose logs --no-color >"$OUT_ROOT/$label/compose.log"
  compose logs --no-color --no-log-prefix router \
    | rg 'ARTC_ATTEMPT_SUMMARY|artc_[a-z_]+(_total)?(\{| )' \
    >"$OUT_ROOT/$label/router-summary.txt" || true
  for service in a1 a2 a3; do
    compose logs --no-color --no-log-prefix "$service" | rg 'ARTC_SERVICE_WORK_SUMMARY' \
      >"$OUT_ROOT/$label/$service-work-summary.txt" || true
  done
  compose down --remove-orphans
  CURRENT_PROJECT=""
}

configure_defaults
python3 -m py_compile lab/validate_artifacts.py
docker compose -f "$COMPOSE_FILE" config --quiet
git diff --check

cmake --build build/debug -j"$JOBS"
ctest --test-dir build/debug --output-on-failure

expect_startup_reject() {
  local label="$1" pattern="$2"; shift 2
  local output="$OUT_ROOT/config-reject-$label.log"
  if env -i "PATH=$PATH" "$@" "$ROOT/build/debug/artc_lab_node" router \
      127.0.0.1:0 artc_adaptive_no_deadline 1 0.2 \
      A1=127.0.0.1:1 A2=127.0.0.1:2 A3=127.0.0.1:3 >"$output" 2>&1; then
    echo "invalid configuration unexpectedly started: $label" >&2
    return 1
  fi
  grep -q "$pattern" "$output"
}
expect_startup_reject active-limit 'invalid Phase 3 attempt limits' ARTC_ATTEMPT_MAX_ACTIVE=3
expect_startup_reject method-policy \
  'ARTC_EXECUTE_IDEMPOTENCY must be exactly' \
  ARTC_EXECUTE_IDEMPOTENCY=unknown
expect_startup_reject nonfinite-budget 'invalid floating-point argument' \
  ARTC_HEDGE_BUDGET_REFILL_PER_SECOND=nan

cmake --preset release
cmake --build --preset release -j"$JOBS"
ctest --test-dir build/release --output-on-failure

FETCH_SOURCE_ARGS=(
  "-DFETCHCONTENT_SOURCE_DIR_GRPC=$ROOT/build/debug/_deps/grpc-src"
  "-DFETCHCONTENT_SOURCE_DIR_HDRHISTOGRAM_C=$ROOT/build/debug/_deps/hdrhistogram_c-src"
  "-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=$ROOT/build/debug/_deps/googletest-src"
)
cmake -S . -B build/phase3-clang -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DBUILD_TESTING=ON \
  "${FETCH_SOURCE_ARGS[@]}"
cmake --build build/phase3-clang -j"$JOBS"
ctest --test-dir build/phase3-clang --output-on-failure

cmake -S . -B build/phase2-asan-ubsan -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_C_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer' \
  -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS='-fsanitize=address,undefined' "${FETCH_SOURCE_ARGS[@]}"
cmake --build build/phase2-asan-ubsan -j"$JOBS"
ASAN_OPTIONS='detect_leaks=1:halt_on_error=1' ctest --test-dir build/phase2-asan-ubsan \
  --output-on-failure -R 'AttemptManagerIntegrationTest|AttemptBudgetTest|AttemptPolicyOnly'

cmake -S . -B build/phase2-tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON -DCMAKE_C_FLAGS='-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_CXX_FLAGS='-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS='-fsanitize=thread' "${FETCH_SOURCE_ARGS[@]}"
cmake --build build/phase2-tsan -j"$JOBS"
setarch "$(uname -m)" -R ctest --test-dir build/phase2-tsan --output-on-failure \
  -R 'AttemptManagerIntegrationTest|AttemptBudgetTest|AttemptPolicyOnly|ControllerRaceTest'

rm -rf lab/__pycache__
if [[ "$CODE_ONLY" == true ]]; then
  printf 'phase3_code_gates=pass\n' | tee "$OUT_ROOT/gate-status.txt"
  exit 0
fi

docker compose -f "$COMPOSE_FILE" build

configure_defaults
set_method non_idempotent false false 1 0 100000 0 0
run_case phase2_healthy none false 2000 100 0 1

configure_defaults
set_method idempotent true true 3 1 100000 8 8
run_case phase3_healthy none false 2000 100 0 3
python3 lab/validate_artifacts.py --compare-hedge \
  "$OUT_ROOT/phase2_healthy" "$OUT_ROOT/phase3_healthy"

configure_defaults
export ARTC_ROUTING_POLICY=adaptive_concurrency_only ARTC_SERVICE_A2_DELAY_US=150000
set_method idempotent false false 1 0 10000 0 0
run_case straggler_unhedged none false 3000 150 0 1

for delay_ms in 5 20 50; do
  configure_defaults
  export ARTC_ROUTING_POLICY=adaptive_concurrency_only ARTC_SERVICE_A2_DELAY_US=150000
  delay_us=$((delay_ms * 1000))
  set_method idempotent true false 2 0 "$delay_us" 256 0
  hedge_run="straggler_hedge_${delay_ms}ms"
  run_case "$hedge_run" none false 3000 150 0 1
  python3 - "$OUT_ROOT/$hedge_run/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text())
assert manifest["hedge_attempts"] > 0, manifest
assert manifest["max_admitted_attempts_per_request"] <= 2, manifest
assert manifest["attempt_amplification_per_admitted"] <= 2, manifest
print("straggler_hedge_observed=true attempt_bound=2")
PY
  python3 lab/validate_artifacts.py --compare-hedge \
    "$OUT_ROOT/straggler_unhedged" "$OUT_ROOT/straggler_hedge_${delay_ms}ms"
done

configure_defaults
export ARTC_ROUTING_POLICY=adaptive_concurrency_only
set_method idempotent true false 2 0 10000 5 0
run_case all_replicas_slow all-netem false 500 100 0 4
python3 - "$OUT_ROOT/all_replicas_slow/router-summary.txt" <<'PY'
import sys
from pathlib import Path
line = next((line for line in Path(sys.argv[1]).read_text().splitlines()
             if line.startswith("ARTC_ATTEMPT_SUMMARY ")), "")
values = dict(part.split("=", 1) for part in line.split()[1:] if "=" in part)
assert int(values["hedge_attempts"]) <= 5, values
overload = next((line for line in Path(sys.argv[1]).read_text().splitlines()
                 if line.startswith('artc_hedges_total{result="overload_denied"} ')), "")
assert overload and int(overload.rsplit(" ", 1)[1]) > 0, overload
print("all_replicas_slow_hedges_bounded=true overload_suppression_observed=true")
PY
python3 - "$OUT_ROOT" <<'PY'
import csv, json, sys
from pathlib import Path
root = Path(sys.argv[1])
rows = []
remaining = 5
for name in ("all_replicas_slow", *(f"all_replicas_slow-repeat{i}" for i in range(2, 5))):
    manifest = json.loads((root / name / "manifest.json").read_text())
    seconds = manifest["duration_ms"] / 1000
    remaining -= manifest["hedge_attempts"]
    rows.append({
        "interval": name,
        "logical_rps": manifest["admitted"] / seconds,
        "backend_attempt_rps": manifest["backend_attempts"] / seconds,
        "hedge_rps": manifest["hedge_attempts"] / seconds,
        "hedge_rate": manifest["hedge_attempts"] / max(1, manifest["admitted"]),
        "amplification": manifest["attempt_amplification_per_admitted"],
        "hedge_budget_remaining": remaining,
    })
with (root / "all_replicas_slow" / "hedge-storm-series.csv").open("w", newline="") as out:
    writer = csv.DictWriter(out, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
assert remaining >= 0, rows
PY

configure_defaults
export ARTC_ROUTING_POLICY=adaptive_concurrency_only ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=30
set_method idempotent false true 2 1 10000 0 32
run_case transient_unavailable none true 2000 100 0 1

configure_defaults
export ARTC_ROUTING_POLICY=adaptive_concurrency_only
export ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=10000
export ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N=10000
export ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N=10000
set_method idempotent false true 3 2 10000 0 3
run_case retry_storm none true 500 100 0 4
python3 - "$OUT_ROOT/retry_storm/router-summary.txt" <<'PY'
import sys
from pathlib import Path
line = next((line for line in Path(sys.argv[1]).read_text().splitlines()
             if line.startswith("ARTC_ATTEMPT_SUMMARY ")), "")
values = dict(part.split("=", 1) for part in line.split()[1:] if "=" in part)
assert int(values["retry_attempts"]) <= 3, values
assert int(values["retry_budget_denied"]) > 0, values
print("retry_storm_budget_containment=true")
PY
python3 - "$OUT_ROOT" <<'PY'
import csv, json, sys
from pathlib import Path
root = Path(sys.argv[1])
rows = []
remaining = 3
for name in ("retry_storm", *(f"retry_storm-repeat{i}" for i in range(2, 5))):
    manifest = json.loads((root / name / "manifest.json").read_text())
    seconds = manifest["duration_ms"] / 1000
    remaining -= manifest["retry_attempts"]
    rows.append({
        "interval": name,
        "logical_rps": manifest["admitted"] / seconds,
        "backend_attempt_rps": manifest["backend_attempts"] / seconds,
        "retry_rps": manifest["retry_attempts"] / seconds,
        "retry_budget_remaining": remaining,
        "success_fraction": manifest["successful"] / max(1, manifest["issued"]),
        "p99_us": manifest["latency_us"].get("p99_us"),
    })
with (root / "retry_storm" / "retry-storm-series.csv").open("w", newline="") as out:
    writer = csv.DictWriter(out, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
assert remaining >= 0, rows
print(f"retry_storm_series_saved=true retry_tokens_remaining={remaining}")
PY

configure_defaults
export ARTC_ROUTING_POLICY=adaptive_concurrency_only ARTC_SERVICE_A_UNAVAILABLE_FIRST_N=10000
set_method non_idempotent false false 1 0 10000 8 8
run_case non_idempotent none true 1500 100 0 1
python3 - "$OUT_ROOT/non_idempotent/manifest.json" <<'PY'
import json, sys
from pathlib import Path
manifest = json.loads(Path(sys.argv[1]).read_text())
assert manifest["hedge_attempts"] == 0 and manifest["retry_attempts"] == 0, manifest
print("non_idempotent_speculative_attempts=0")
PY

for mode in aware ignoring; do
  configure_defaults
  export ARTC_ROUTING_POLICY=adaptive_concurrency_only ARTC_SERVICE_A2_DELAY_US=150000
  if [[ "$mode" == ignoring ]]; then export ARTC_SERVICE_A2_HONOR_CANCELLATION=false; fi
  set_method idempotent true false 2 0 10000 128 0
  run_case "cancellation_${mode}" none false 2000 100 0 1
done

configure_defaults
export ARTC_ROUTING_POLICY=adaptive_concurrency_only ARTC_SERVICE_A2_DELAY_US=80000
set_method idempotent true false 2 0 120000 8 0
LOAD_DEADLINE_MS=100
run_case short_deadline none true 1000 50 0 1
LOAD_DEADLINE_MS=5000
python3 - "$OUT_ROOT/short_deadline/router-summary.txt" <<'PY'
import sys
from pathlib import Path

lines = Path(sys.argv[1]).read_text().splitlines()
summary = next((line for line in lines if line.startswith("ARTC_ATTEMPT_SUMMARY ")), "")
values = dict(part.split("=", 1) for part in summary.split()[1:] if "=" in part)
denied = next((line for line in lines
               if line.startswith('artc_hedges_total{result="deadline_denied"} ')), "")
assert int(values["hedge_attempts"]) == 0, values
assert denied and int(denied.rsplit(" ", 1)[1]) > 0, denied
print("short_deadline_hedges=0 deadline_suppression_observed=true")
PY

printf 'phase3_gates=pass\nrun_id=%s\n' "$BASE" | tee "$OUT_ROOT/gate-status.txt"
rm -rf lab/__pycache__
