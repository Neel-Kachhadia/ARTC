#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${ARTC_RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-$$}"
PHASE2_START_AT="${ARTC_PHASE2_START_AT:-A}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
if [[ "$PHASE2_START_AT" != A && "$PHASE2_START_AT" != I && \
      "$PHASE2_START_AT" != DEADLINE && "$PHASE2_START_AT" != ABLATION && \
      "$PHASE2_START_AT" != OSCILLATION ]]; then
  echo "ARTC_PHASE2_START_AT must be A, I, DEADLINE, ABLATION, or OSCILLATION" >&2
  exit 2
fi
PROJECT="${COMPOSE_PROJECT_NAME:-artc-phase2-$RUN_ID}"
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/phase2"
COMPOSE_FILE="lab/compose.yaml"
mkdir -p "$OUT_DIR"
if [[ "$PHASE2_START_AT" == I ]]; then
  for label in A-healthy B-a2-straggler C-a2-cpu-saturation D-a2-down \
               E-a2-recovery F-downstream-b-slow G-one-to-five-burst H-global-overload; do
    python3 lab/validate_artifacts.py --allow-errors "$OUT_DIR/$label"
  done
fi
if [[ "$PHASE2_START_AT" == DEADLINE ]]; then
  for label in A-healthy B-a2-straggler C-a2-cpu-saturation D-a2-down \
               E-a2-recovery F-downstream-b-slow G-one-to-five-burst \
               H-global-overload I-all-replicas-slow J-load-normal-again \
               stability-00-30-normal stability-30-60-step-up \
               stability-60-90-sustained stability-90-120-recovery \
               recovery-00-30-healthy recovery-30-60-a2-unavailable \
               recovery-60-120-a2-reintegrated; do
    python3 lab/validate_artifacts.py --allow-errors "$OUT_DIR/$label"
  done
fi

export COMPOSE_PROJECT_NAME="$PROJECT"
export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
export ARTC_SERVICE_B_DELAY_US=0
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  export ARTC_WORKTREE_DIRTY=true
else
  export ARTC_WORKTREE_DIRTY=false
fi

LAB_UP=false
FAULT_SERVICES=()
STRESS_ACTIVE=false
LOAD_PID=""
MONITOR_PID=""

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

clear_faults() {
  local service queues result=0
  for service in "${FAULT_SERVICES[@]}"; do
    if ! compose exec -T "$service" tc qdisc del dev eth0 root >/dev/null; then
      echo "failed to remove netem from scoped target $service" >&2
      result=1
      continue
    fi
    if ! queues="$(compose exec -T "$service" tc qdisc show dev eth0)"; then
      echo "could not verify qdisc cleanup on scoped target $service" >&2
      result=1
    elif grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
      echo "netem remains on scoped target $service" >&2
      result=1
    fi
  done
  if ((result == 0)); then FAULT_SERVICES=(); fi
  assert_no_netem || result=1
  return "$result"
}

stop_stress() {
  if [[ "$STRESS_ACTIVE" == true ]]; then
    compose exec -T a2 sh -ec '
      pkill -TERM -x stress-ng >/dev/null 2>&1 || true
      for attempt in 1 2 3 4 5; do
        if ! pgrep -x stress-ng >/dev/null 2>&1; then exit 0; fi
        sleep 0.2
      done
      pgrep -ax stress-ng >&2 || true
      exit 1
    '
    STRESS_ACTIVE=false
  fi
}

assert_no_netem() {
  local service container_id queues
  for service in service-b a1 a2 a3; do
    container_id="$(compose ps -q "$service")"
    [[ -n "$container_id" ]] || continue
    queues="$(compose exec -T "$service" tc qdisc show dev eth0)"
    if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
      echo "unexpected netem qdisc on scoped target $service" >&2
      return 1
    fi
  done
}

assert_fault_cleanup() {
  if ((${#FAULT_SERVICES[@]} != 0)); then
    echo "tracked netem faults remain: ${FAULT_SERVICES[*]}" >&2
    return 1
  fi
  if [[ "$STRESS_ACTIVE" == true ]]; then
    echo "tracked stress-ng fault remains active" >&2
    return 1
  fi
  assert_no_netem
}

on_exit() {
  local result=$?
  trap - EXIT INT TERM
  if [[ -n "$LOAD_PID" ]]; then
    kill "$LOAD_PID" >/dev/null 2>&1 || true
    wait "$LOAD_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$MONITOR_PID" ]]; then
    kill "$MONITOR_PID" >/dev/null 2>&1 || true
    wait "$MONITOR_PID" >/dev/null 2>&1 || true
  fi
  stop_stress || result=1
  if [[ "$LAB_UP" == true ]]; then
    clear_faults || result=1
    assert_fault_cleanup || result=1
    compose down --remove-orphans || result=1
    LAB_UP=false
    local remaining
    remaining="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$PROJECT")" || result=1
    if [[ -n "$remaining" ]]; then
      echo "Compose project containers remain after cleanup: $PROJECT" >&2
      result=1
    fi
  fi
  exit "$result"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

check_health() {
  local target="$1"
  compose run --rm --no-deps loadgen --health --target "$target"
}

start_policy() {
  local policy="$1"
  local sample_every=0
  if [[ "$#" -ge 2 ]]; then
    sample_every="$2"
  elif [[ "$policy" == artc_* || "$policy" == adaptive_concurrency_only ]]; then
    sample_every="${ARTC_PHASE2_SAMPLE_EVERY:-100}"
  fi
  export ARTC_ROUTING_POLICY="$policy"
  export ARTC_DECISION_SAMPLE_EVERY="$sample_every"
  compose up -d --no-deps --force-recreate --wait --wait-timeout 60 router
  check_health router:50050
}

monitor_router() {
  local container_id="$1"
  local load_pid="$2"
  local output="$3"
  (
    while kill -0 "$load_pid" >/dev/null 2>&1; do
      printf '%s,' "$(date -u +%s)"
      docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.PIDs}}' \
        "$container_id" 2>/dev/null || true
      sleep "${ARTC_STATS_INTERVAL_SEC:-1}"
    done
  ) >"$output" &
  MONITOR_PID=$!
}

run_load() {
  local label="$1"
  local target="$2"
  local mode="$3"
  local rate="$4"
  local duration_ms="$5"
  local deadline_ms="$6"
  local initial_rate="${7:-0}"
  local allow_uninstrumented="${8:-false}"
  local max_issue_lag_us="${9:-${ARTC_PHASE2_MAX_ISSUE_LAG_US:-20000}}"
  local run_dir="$OUT_DIR/$label"
  local loadgen_log="$OUT_DIR/$label.loadgen.stdout"
  local container_id
  local target_service="${target%%:*}"
  mkdir -p "$run_dir"
  export ARTC_RUN_ID="$RUN_ID-$label"
  export ARTC_ROUTING_POLICY="${ARTC_ROUTING_POLICY:-round_robin}"
  container_id="$(compose ps -q "$target_service")"
  if [[ -z "$container_id" ]]; then
    echo "target container is missing: $target_service" >&2
    return 1
  fi

  compose run --rm --no-deps loadgen \
    --target "$target" \
    --output "/artifacts/$RUN_ID/phase2/$label" \
    --mode "$mode" \
    --duration-ms "$duration_ms" \
    --rate-rps "$rate" \
    --initial-rate-rps "$initial_rate" \
    --max-inflight "${ARTC_LOADGEN_MAX_INFLIGHT:-8192}" \
    --max-issue-lag-us "$max_issue_lag_us" \
    --deadline-ms "$deadline_ms" \
    --invoke-dependency --allow-errors \
    >"$loadgen_log" 2>&1 &
  LOAD_PID=$!
  monitor_router "$container_id" "$LOAD_PID" "$OUT_DIR/$label.router-resources.csv"
  local result=0
  wait "$LOAD_PID" || result=$?
  LOAD_PID=""
  kill "$MONITOR_PID" >/dev/null 2>&1 || true
  wait "$MONITOR_PID" >/dev/null 2>&1 || true
  MONITOR_PID=""
  if (( result != 0 )); then
    cat "$loadgen_log" >&2
    echo "loadgen failed label=$label status=$result" >&2
    return "$result"
  fi
  local validation=(python3 lab/validate_artifacts.py --allow-errors)
  if [[ "$allow_uninstrumented" == true ]]; then
    validation+=(--allow-uninstrumented)
  fi
  "${validation[@]}" "$run_dir"
  echo "run=$label policy=$ARTC_ROUTING_POLICY target=$target"
}

apply_netem() {
  local service="$1"
  local delay="$2"
  local queues
  queues="$(compose exec -T "$service" tc qdisc show dev eth0)"
  if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
    echo "refusing to stack a netem qdisc on scoped target $service" >&2
    return 1
  fi
  compose exec -T "$service" tc qdisc add dev eth0 root netem delay "$delay"
  FAULT_SERVICES+=("$service")
}

restart_service_b() {
  export ARTC_SERVICE_B_DELAY_US="$1"
  compose up -d --no-deps --force-recreate --wait --wait-timeout 60 service-b
  check_health service-b:50052
}

wait_for_a2() {
  compose start a2
  local attempt
  for attempt in $(seq 1 30); do
    if check_health a2:50051 >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "A2 did not recover within 30 seconds" >&2
  return 1
}

if ! docker image inspect artc-phase1:local >/dev/null 2>&1; then
  echo "missing artc-phase1:local; run lab/run_phase1_gates.sh first" >&2
  exit 2
fi
docker info >/dev/null
LAB_UP=true
compose up -d --wait --wait-timeout 90
assert_fault_cleanup
check_health service-b:50052
check_health a1:50051
check_health a2:50051
check_health a3:50051

# Required scenario matrix A-J.
if [[ "$PHASE2_START_AT" == A ]]; then
start_policy artc_adaptive
run_load A-healthy router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 5000

apply_netem a2 150ms
run_load B-a2-straggler router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 1000
clear_faults

compose exec -d -T a2 stress-ng --cpu 1 --timeout 25s --quiet
STRESS_ACTIVE=true
run_load C-a2-cpu-saturation router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 5000
stop_stress

compose stop a2
run_load D-a2-down router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 15000 1000
wait_for_a2
run_load E-a2-recovery router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 30000 5000

restart_service_b 20000
run_load F-downstream-b-slow router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 1000
restart_service_b 0

run_load G-one-to-five-burst router:50050 burst "${ARTC_PHASE2_NORMAL_RPS:-200}" 15000 1000

restart_service_b 50000
run_load H-global-overload router:50050 constant "${ARTC_PHASE2_OVERLOAD_RPS:-2000}" 20000 1000
fi

if [[ "$PHASE2_START_AT" != DEADLINE && "$PHASE2_START_AT" != ABLATION && \
      "$PHASE2_START_AT" != OSCILLATION ]]; then
restart_service_b 0
start_policy artc_adaptive
apply_netem a1 150ms
apply_netem a2 150ms
apply_netem a3 150ms
run_load I-all-replicas-slow-warmup router:50050 constant \
  "${ARTC_PHASE2_NORMAL_RPS:-200}" 10000 5000
run_load I-all-replicas-slow router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 100
clear_faults
run_load J-load-normal-again router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 5000

# Stability trace: 30s normal, 30s step-up, 30s sustained, 30s recovery.
restart_service_b "${ARTC_PHASE2_STABILITY_B_DELAY_US:-0}"
start_policy artc_adaptive
run_load stability-00-30-normal router:50050 constant "${ARTC_PHASE2_STABILITY_NORMAL_RPS:-200}" 30000 1000
run_load stability-30-60-step-up router:50050 constant "${ARTC_PHASE2_STABILITY_HIGH_RPS:-12000}" 30000 "${ARTC_PHASE2_STABILITY_RPC_DEADLINE_MS:-5000}" 0 false "${ARTC_PHASE2_STABILITY_MAX_ISSUE_LAG_US:-50000}"
run_load stability-60-90-sustained router:50050 constant "${ARTC_PHASE2_STABILITY_HIGH_RPS:-12000}" 30000 "${ARTC_PHASE2_STABILITY_RPC_DEADLINE_MS:-5000}" 0 false "${ARTC_PHASE2_STABILITY_MAX_ISSUE_LAG_US:-50000}"
run_load stability-90-120-recovery router:50050 constant "${ARTC_PHASE2_STABILITY_NORMAL_RPS:-200}" 30000 1000
restart_service_b 0

# Recovery trace: 30s healthy, 30s unavailable, 60s recovering/reintegrated.
start_policy artc_adaptive
run_load recovery-00-30-healthy router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 30000 1000
compose stop a2
run_load recovery-30-60-a2-unavailable router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 30000 1000
wait_for_a2
run_load recovery-60-120-a2-reintegrated router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 60000 1000
fi

# Deadline feasibility ablation: A2 is a learned straggler while A1/A3 remain
# fast enough to complete inside the caller deadline.
HAD_AIMD_TARGET="${ARTC_AIMD_TARGET_LATENCY_US+x}"
PREVIOUS_AIMD_TARGET="${ARTC_AIMD_TARGET_LATENCY_US-}"
if [[ "$PHASE2_START_AT" != ABLATION && "$PHASE2_START_AT" != OSCILLATION ]]; then
export ARTC_AIMD_TARGET_LATENCY_US="${ARTC_PHASE2_DEADLINE_TARGET_LATENCY_US:-75000}"
restart_service_b 50000
apply_netem a2 75ms
start_policy artc_adaptive_no_deadline
run_load deadline-off-warmup router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 10000 5000
run_load deadline-feasibility-off router:50050 constant \
  "${ARTC_PHASE2_DEADLINE_OVERLOAD_RPS:-8000}" 20000 "${ARTC_PHASE2_DEADLINE_CALLER_MS:-300}"
start_policy artc_adaptive
run_load deadline-on-warmup router:50050 constant "${ARTC_PHASE2_NORMAL_RPS:-200}" 10000 5000
run_load deadline-feasibility-on router:50050 constant \
  "${ARTC_PHASE2_DEADLINE_OVERLOAD_RPS:-8000}" 20000 "${ARTC_PHASE2_DEADLINE_CALLER_MS:-300}"
clear_faults
restart_service_b 0
if [[ "$HAD_AIMD_TARGET" == x ]]; then
  export ARTC_AIMD_TARGET_LATENCY_US="$PREVIOUS_AIMD_TARGET"
else
  unset ARTC_AIMD_TARGET_LATENCY_US
fi
fi

# Selector and concurrency ablations plus the four Phase 1 baselines.
if [[ "$PHASE2_START_AT" != OSCILLATION ]]; then
for policy in round_robin least_inflight ewma_latency p2c_latency_inflight \
              artc_selector_only adaptive_concurrency_only \
              artc_adaptive_no_deadline artc_adaptive; do
  start_policy "$policy"
  run_load "ablation-$policy" router:50050 constant \
    "${ARTC_PHASE2_ABLATION_RPS:-500}" 10000 5000
done

# Healthy path comparison: direct backend, Phase 1 proxy baselines, and Phase 2.
start_policy round_robin
run_load overhead-direct-a1 a1:50051 constant "${ARTC_PHASE2_OVERHEAD_RPS:-1000}" 15000 5000 0 true
run_load overhead-round-robin router:50050 constant "${ARTC_PHASE2_OVERHEAD_RPS:-1000}" 15000 5000
start_policy least_inflight
run_load overhead-phase1-least-inflight router:50050 constant \
  "${ARTC_PHASE2_OVERHEAD_RPS:-1000}" 15000 5000
start_policy artc_adaptive 0
run_load overhead-phase2-adaptive router:50050 constant \
  "${ARTC_PHASE2_OVERHEAD_RPS:-1000}" 15000 5000

# Representative Phase 3 policy under one isolated straggler. Earlier policy
# and ablation rows keep speculation disabled so their routing/control effects
# remain comparable.
export ARTC_EXECUTE_IDEMPOTENCY=idempotent
export ARTC_EXECUTE_HEDGING_ENABLED=true ARTC_EXECUTE_RETRY_ENABLED=true
export ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS=3 ARTC_EXECUTE_MAX_RETRIES=2
export ARTC_EXECUTE_HEDGE_DELAY_MIN_US=20000 ARTC_EXECUTE_HEDGE_DELAY_MAX_US=20000
export ARTC_HEDGE_BUDGET_CAPACITY=32 ARTC_HEDGE_BUDGET_REFILL_PER_SECOND=4
export ARTC_RETRY_BUDGET_CAPACITY=16 ARTC_RETRY_BUDGET_REFILL_PER_SECOND=2
start_policy artc_adaptive
apply_netem a2 150ms
run_load phase3-full-policy-straggler router:50050 constant \
  "${ARTC_PHASE2_NORMAL_RPS:-200}" 20000 5000
clear_faults
run_load phase3-full-policy-recovery router:50050 constant 100 5000 5000

build/release/artc_control_bench --output "$OUT_DIR/control-microbench" \
  --iterations "${ARTC_CONTROL_BENCH_ITERATIONS:-1000000}" \
  >"$OUT_DIR/control-microbench.stdout"
fi

if [[ "$PHASE2_START_AT" == OSCILLATION ]]; then
  start_policy artc_adaptive
  for wave in low-1 high-1 low-2 high-2 low-3 high-3; do
    case "$wave" in
      low-*) rate="${ARTC_PHASE2_STABILITY_NORMAL_RPS:-200}" ;;
      high-*) rate="${ARTC_PHASE2_OSCILLATION_HIGH_RPS:-1000}" ;;
    esac
    run_load "oscillation-$wave" router:50050 constant "$rate" 10000 5000
  done
fi

if [[ "$PHASE2_START_AT" == ABLATION ]]; then
  echo "phase2_ablation_complete=$OUT_DIR"
elif [[ "$PHASE2_START_AT" == OSCILLATION ]]; then
  echo "phase4_oscillation_complete=$OUT_DIR"
else
  python3 lab/analyze_phase2.py "$OUT_DIR" >"$OUT_DIR/analysis.stdout"
fi

echo "phase2_experiments_complete=$OUT_DIR"
