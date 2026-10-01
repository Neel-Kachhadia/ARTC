#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUN_ID="${ARTC_RUN_ID:-phase4_startup_$(date -u +%Y%m%d_%H%M%S)_$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
RESTART_CYCLES="${ARTC_STARTUP_RESTART_CYCLES:-20}"
if [[ ! "$RESTART_CYCLES" =~ ^[1-9][0-9]{0,2}$ ]] || ((RESTART_CYCLES > 100)); then
  echo "ARTC_STARTUP_RESTART_CYCLES must be in [1,100]" >&2
  exit 2
fi
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/startup-matrix"
COMPOSE_FILE="$ROOT/lab/compose.yaml"
mkdir -p "$(dirname "$OUT_DIR")"
if ! mkdir "$OUT_DIR"; then
  echo "refusing to overwrite existing startup matrix artifacts: $OUT_DIR" >&2
  exit 2
fi

export ARTC_RUN_ID="$RUN_ID"
export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  export ARTC_WORKTREE_DIRTY=true
else
  export ARTC_WORKTREE_DIRTY=false
fi
export ARTC_ROUTING_POLICY=artc_adaptive
export ARTC_DECISION_SAMPLE_EVERY=100
export ARTC_AIMD_MIN_LIMIT=1 ARTC_AIMD_MAX_LIMIT=64 ARTC_AIMD_INITIAL_LIMIT=64
export ARTC_AIMD_INTERVAL_MS=10 ARTC_AIMD_MIN_SAMPLES=4
export ARTC_AIMD_TARGET_LATENCY_US=50000 ARTC_AIMD_ALPHA=2 ARTC_AIMD_BETA=0.7
export ARTC_AIMD_OVERLOAD_ERROR_FRACTION=0.1
export ARTC_DEFAULT_DEADLINE_MS=5000 ARTC_DEADLINE_MARGIN_US=1000
export ARTC_HEALTH_MIN_SAMPLES=4 ARTC_HEALTH_FAILURES=3
export ARTC_HEALTH_RECOVERY_SUCCESSES=3 ARTC_HEALTH_RECOVERY_COOLDOWN_MS=500
export ARTC_HEALTH_DEGRADED_RATIO=2.0 ARTC_HEALTH_RECOVERED_RATIO=1.5
export ARTC_RECOVERY_PROBE_PERIOD=16
export ARTC_ATTEMPT_MAX_TOTAL=3 ARTC_ATTEMPT_MAX_ACTIVE=2
export ARTC_ATTEMPT_JITTER_SEED=20261001 ARTC_ATTEMPT_MINIMUM_BUDGET_US=1000
export ARTC_HEDGE_BUDGET_CAPACITY=10 ARTC_HEDGE_BUDGET_REFILL_PER_SECOND=1
export ARTC_RETRY_BUDGET_CAPACITY=10 ARTC_RETRY_BUDGET_REFILL_PER_SECOND=1
export ARTC_EXECUTE_IDEMPOTENCY=idempotent
export ARTC_EXECUTE_HEDGING_ENABLED=false ARTC_EXECUTE_RETRY_ENABLED=false
export ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY=false
export ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS=1 ARTC_EXECUTE_MAX_RETRIES=0
export ARTC_EXECUTE_HEDGE_DELAY_MIN_US=10000 ARTC_EXECUTE_HEDGE_DELAY_MAX_US=100000
export ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS=10 ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS=100
export ARTC_EXECUTE_RETRY_JITTER_MAX_MS=10 ARTC_EXECUTE_RETRYABLE_STATUSES=UNAVAILABLE
export ARTC_SERVICE_B_DELAY_US=0
export ARTC_SERVICE_A1_DELAY_US=0 ARTC_SERVICE_A2_DELAY_US=0 ARTC_SERVICE_A3_DELAY_US=0
export ARTC_SERVICE_A_UNAVAILABLE_FIRST_N=0
export ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=0 ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N=0
export ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N=0
export ARTC_SERVICE_A_HONOR_CANCELLATION=true
export ARTC_SERVICE_A1_HONOR_CANCELLATION=true ARTC_SERVICE_A2_HONOR_CANCELLATION=true
export ARTC_SERVICE_A3_HONOR_CANCELLATION=true

PROJECT_PREFIX="artc-phase4-startup-$RUN_ID"
CURRENT_PROJECT=""
CURRENT_SCENARIO=""
LAB_UP=false
FAILURE_REASON=""

compose() { docker compose -f "$COMPOSE_FILE" -p "$CURRENT_PROJECT" "$@"; }

verify_target() {
  local service="$1" container_id project_label service_label
  container_id="$(compose ps -q "$service")"
  [[ -n "$container_id" ]] || { echo "missing scoped service: $service" >&2; return 1; }
  project_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$container_id")"
  service_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$container_id")"
  [[ "$project_label" == "$CURRENT_PROJECT" && "$service_label" == "$service" ]] || {
    echo "refusing unexpected Docker target for $service" >&2
    return 1
  }
  printf '%s' "$container_id"
}

assert_project_removed() {
  local project="$1" remaining
  remaining="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$project")" || return 1
  if [[ -n "$remaining" ]]; then
    echo "Compose project containers remain after startup scenario: $project" >&2
    return 1
  fi
}

cleanup_current() {
  local project="$CURRENT_PROJECT" result=0 service container_id queues
  [[ -n "$project" ]] || return 0
  if [[ "$LAB_UP" == true ]]; then
    for service in service-b a1 a2 a3 router; do
      container_id="$(compose ps -q "$service")"
      [[ -n "$container_id" ]] || continue
      if [[ "$(docker inspect --format '{{.State.Paused}}' "$container_id")" == true ]]; then
        echo "paused target remains at cleanup: $service" >&2
        result=1
      fi
      if [[ "$service" != router ]]; then
        queues="$(compose exec -T "$service" tc qdisc show dev eth0)" || result=1
        if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
          echo "netem remains at cleanup on $service" >&2
          result=1
        fi
        if [[ "$service" != service-b ]] &&
            compose exec -T "$service" pgrep -x stress-ng >/dev/null 2>&1; then
          echo "stress-ng remains at cleanup on $service" >&2
          result=1
        fi
      fi
    done
    if docker ps --all --quiet --filter "label=com.docker.compose.project=$project" \
        --filter 'label=com.docker.compose.service=loadgen' | grep -q .; then
      echo "loadgen container remains at cleanup" >&2
      result=1
    fi
    compose down --remove-orphans || result=1
    LAB_UP=false
  fi
  assert_project_removed "$project" || result=1
  CURRENT_PROJECT=""
  return "$result"
}

on_exit() {
  local result=$?
  trap - EXIT INT TERM
  cleanup_current || result=1
  if (( result != 0 )); then
    FAILURE_REASON="${FAILURE_REASON:-startup matrix exited with status $result}"
    python3 - "$OUT_DIR/failure.json" "$CURRENT_SCENARIO" "$FAILURE_REASON" "$result" <<'PY' || true
import json
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(json.dumps({
    "scenario": sys.argv[2] or None,
    "reason": sys.argv[3],
    "exit_status": int(sys.argv[4]),
}, indent=2) + "\n", encoding="utf-8")
PY
  fi
  exit "$result"
}
trap on_exit EXIT
trap 'FAILURE_REASON="interrupted by SIGINT"; exit 130' INT
trap 'FAILURE_REASON="interrupted by SIGTERM"; exit 143' TERM

start_scenario() {
  CURRENT_SCENARIO="$1"
  CURRENT_PROJECT="$PROJECT_PREFIX-$CURRENT_SCENARIO"
  if [[ -n "$(docker ps --all --quiet --filter "label=com.docker.compose.project=$CURRENT_PROJECT")" ]]; then
    echo "refusing to reuse existing Compose project: $CURRENT_PROJECT" >&2
    return 1
  fi
  export COMPOSE_PROJECT_NAME="$CURRENT_PROJECT"
  compose config --quiet
  LAB_UP=true
}

wait_service() {
  local service="$1"
  compose up -d --no-deps --wait --wait-timeout 90 "$service"
  verify_target "$service" >/dev/null
}

health_router() {
  timeout 10s docker compose -f "$COMPOSE_FILE" -p "$CURRENT_PROJECT" \
    exec -T router /opt/artc/bin/artc_loadgen --health \
    --target 127.0.0.1:50050 >/dev/null
}

probe() {
  local label="$1" duration_ms="${2:-2000}" rate="${3:-100}"
  local run_dir="$ROOT/artifacts/runs/$RUN_ID/startup-matrix/$CURRENT_SCENARIO/$label"
  local console_dir="$ROOT/artifacts/runs/$RUN_ID/startup-matrix/console"
  mkdir -p "$run_dir" "$console_dir"
  ARTC_RUN_ID="$RUN_ID-$CURRENT_SCENARIO-$label" timeout 20s \
    docker compose -f "$COMPOSE_FILE" -p "$CURRENT_PROJECT" run --rm --no-deps loadgen \
    --target router:50050 --output "/artifacts/$RUN_ID/startup-matrix/$CURRENT_SCENARIO/$label" \
    --mode constant --duration-ms "$duration_ms" --rate-rps "$rate" \
    --max-inflight 512 --max-issue-lag-us 20000 --deadline-ms 5000 \
    --invoke-dependency --allow-errors \
    >"$console_dir/$CURRENT_SCENARIO-$label.loadgen.stdout" 2>&1
  python3 lab/validate_artifacts.py --allow-errors "$run_dir"
}

assert_healthy_probe() {
  local manifest="$1/manifest.json" require_a2="${2:-false}"
  python3 - "$manifest" "$require_a2" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1], encoding="utf-8"))
if manifest.get("completed", 0) <= 0 or manifest.get("errors", 0) != 0:
    raise SystemExit("startup recovery probe did not complete cleanly")
if sys.argv[2] == "true" and manifest.get("responses_by_replica", {}).get("A2", 0) <= 0:
    raise SystemExit("late A2 replica received no traffic after startup")
PY
}

wait_for_healthy_probe() {
  local base="$1" attempts="${2:-10}" start now attempt label manifest
  start="$(date +%s)"
  for ((attempt = 1; attempt <= attempts; ++attempt)); do
    label="$base-$attempt"
    probe "$label"
    manifest="$OUT_DIR/$CURRENT_SCENARIO/$label/manifest.json"
    if python3 - "$manifest" <<'PY'
import json
import sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
if (manifest.get("completed", 0) <= 0 or manifest.get("errors", 0) != 0 or
        manifest.get("successful", 0) != manifest.get("completed", 0)):
    raise SystemExit(1)
PY
    then
      now="$(date +%s)"
      RECOVERY_RESULT="pass_after_${attempt}_probes_$((now - start))s"
      return 0
    fi
    sleep 1
  done
  echo "startup recovery did not reach a clean probe within $attempts attempts" >&2
  return 1
}

write_result() {
  local scenario="$1" order="$2" recovery="$3" a2="$4"
  printf '%s,%s,%s,%s,verified\n' "$scenario" "$order" "$recovery" "$a2" \
    >>"$OUT_DIR/results.csv"
}

docker image inspect artc-phase1:local >/dev/null
docker info >/dev/null
compose_config_project="$PROJECT_PREFIX-config-check"
export COMPOSE_PROJECT_NAME="$compose_config_project"
docker compose -f "$COMPOSE_FILE" -p "$compose_config_project" config --quiet
printf 'scenario,startup_order,recovery,late_a2_traffic,cleanup\n' >"$OUT_DIR/results.csv"

start_scenario router_first
wait_service router
health_router
compose up -d --wait --wait-timeout 90
for service in service-b a1 a2 a3 router; do verify_target "$service" >/dev/null; done
health_router
probe recovery
assert_healthy_probe "$OUT_DIR/router_first/recovery"
cleanup_current
write_result router_first router_then_backends pass not_applicable

start_scenario backends_first
compose up -d --wait --wait-timeout 90 service-b a1 a2 a3
for service in service-b a1 a2 a3; do verify_target "$service" >/dev/null; done
wait_service router
health_router
probe recovery
assert_healthy_probe "$OUT_DIR/backends_first/recovery"
cleanup_current
write_result backends_first backends_then_router pass not_applicable

start_scenario service_b_late
compose up -d --no-deps --wait --wait-timeout 90 a1 a2 a3
wait_service router
health_router
probe before-service-b
wait_service service-b
health_router
wait_for_healthy_probe recovery
cleanup_current
write_result service_b_late router_and_replicas_then_b "$RECOVERY_RESULT" not_applicable

start_scenario a2_late
compose up -d --no-deps --wait --wait-timeout 90 service-b a1 a3
wait_service router
health_router
probe before-a2
wait_service a2
health_router
probe recovery
assert_healthy_probe "$OUT_DIR/a2_late/recovery" true
cleanup_current
write_result a2_late b_a1_a3_router_then_a2 pass pass

start_scenario simultaneous
compose up -d --wait --wait-timeout 90
for service in service-b a1 a2 a3 router; do verify_target "$service" >/dev/null; done
health_router
probe recovery
assert_healthy_probe "$OUT_DIR/simultaneous/recovery"
cleanup_current
write_result simultaneous compose_all pass not_applicable

for ((cycle = 1; cycle <= RESTART_CYCLES; ++cycle)); do
  cycle_label="$(printf 'restart-cycle-%02d' "$cycle")"
  start_scenario "$cycle_label"
  compose up -d --wait --wait-timeout 90
  for service in service-b a1 a2 a3 router; do verify_target "$service" >/dev/null; done
  health_router
  cleanup_current
  write_result "$cycle_label" all_services_start_stop pass not_applicable
done

printf 'phase4_startup_matrix=pass\ntelemetry_sink=not_deployed\nrepeated_start_stop_cycles=%s\n' \
  "$RESTART_CYCLES" \
  | tee "$OUT_DIR/result.txt"
echo "startup_matrix_artifacts=$OUT_DIR"
