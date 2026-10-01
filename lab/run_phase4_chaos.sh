#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUN_ID="${ARTC_RUN_ID:-phase4_chaos_$(date -u +%Y%m%d_%H%M%S)_$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
SEED="${ARTC_CHAOS_SEED:-20261001}"
EPISODES="${ARTC_CHAOS_EPISODES:-15}"
FIRST_EPISODE="${ARTC_CHAOS_FIRST_EPISODE:-1}"
HOLD_SEC="${ARTC_CHAOS_HOLD_SEC:-3}"
if [[ ! "$SEED" =~ ^[0-9]+$ ]] || (( ${#SEED} > 20 )); then
  echo "ARTC_CHAOS_SEED must be an unsigned 64-bit integer" >&2
  exit 2
fi
python3 - "$SEED" <<'PY'
import sys
if int(sys.argv[1]) > (1 << 64) - 1:
    raise SystemExit("ARTC_CHAOS_SEED exceeds uint64")
PY
if [[ ! "$EPISODES" =~ ^[0-9]+$ ]] || (( ${#EPISODES} > 2 )); then
  echo "ARTC_CHAOS_EPISODES must be in [1,15]" >&2
  exit 2
fi
if [[ ! "$HOLD_SEC" =~ ^[0-9]+$ ]] || (( ${#HOLD_SEC} > 2 )); then
  echo "ARTC_CHAOS_HOLD_SEC must be in [1,10]" >&2
  exit 2
fi
EPISODES=$((10#$EPISODES))
HOLD_SEC=$((10#$HOLD_SEC))
if (( EPISODES < 1 || EPISODES > 15 )); then
  echo "ARTC_CHAOS_EPISODES must be in [1,15]" >&2
  exit 2
fi
if [[ ! "$FIRST_EPISODE" =~ ^[0-9]+$ ]] || (( ${#FIRST_EPISODE} > 2 )); then
  echo "ARTC_CHAOS_FIRST_EPISODE must be in [1,ARTC_CHAOS_EPISODES]" >&2
  exit 2
fi
FIRST_EPISODE=$((10#$FIRST_EPISODE))
if (( FIRST_EPISODE < 1 || FIRST_EPISODE > EPISODES )); then
  echo "ARTC_CHAOS_FIRST_EPISODE must be in [1,ARTC_CHAOS_EPISODES]" >&2
  exit 2
fi
if (( HOLD_SEC < 1 || HOLD_SEC > 10 )); then
  echo "ARTC_CHAOS_HOLD_SEC must be in [1,10]" >&2
  exit 2
fi

PROJECT="artc-phase4-chaos-$RUN_ID"
COMPOSE_FILE="$ROOT/lab/compose.yaml"
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/chaos"
mkdir -p "$(dirname "$OUT_DIR")"
if ! mkdir "$OUT_DIR"; then
  echo "refusing to overwrite existing chaos artifacts: $OUT_DIR" >&2
  exit 2
fi
export COMPOSE_PROJECT_NAME="$PROJECT"
export ARTC_RUN_ID="$RUN_ID"
export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  export ARTC_WORKTREE_DIRTY=true
else
  export ARTC_WORKTREE_DIRTY=false
fi

LAB_UP=false
LOAD_PID=""
NETEM_ACTIVE=false
STRESS_ACTIVE=false
STRESS_PID=""
A2_PAUSED=false
A2_STOPPED=false
A3_STOPPED=false
A2_DELAY_ACTIVE=false
BACKENDS_UNAVAILABLE_ACTIVE=false
SERVICE_B_SLOW_ACTIVE=false
SERVICE_B_STOPPED=false
ALL_BACKENDS_SLOW_ACTIVE=false
ROUTER_STOPPED=false
FAILURE_REASON=""
CURRENT_EPISODE=""
CURRENT_EVENT=""
CURRENT_STARTED=""

compose() { docker compose -f "$COMPOSE_FILE" -p "$PROJECT" "$@"; }

service_id() { compose ps --all -q "$1"; }

verify_target() {
  local service="$1" container_id project_label service_label
  container_id="$(service_id "$service")"
  [[ -n "$container_id" ]] || { echo "missing scoped Compose service: $service" >&2; return 1; }
  project_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$container_id")"
  service_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$container_id")"
  if [[ "$project_label" != "$PROJECT" || "$service_label" != "$service" ]]; then
    echo "refusing unexpected Docker target for service $service" >&2
    return 1
  fi
  printf '%s' "$container_id"
}

assert_project_identity() {
  local service
  for service in service-b a1 a2 a3 router; do verify_target "$service" >/dev/null; done
}

assert_fault_state_clean() {
  local service container_id queues paused state
  for service in service-b a1 a2 a3 router; do
    container_id="$(verify_target "$service")"
    paused="$(docker inspect --format '{{.State.Paused}}' "$container_id")"
    state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
    if [[ "$paused" == true ]]; then
      echo "paused target remains: $service" >&2
      return 1
    fi
    if [[ "$state" != running ]]; then
      echo "target is not running after fault cleanup: $service ($state)" >&2
      return 1
    fi
    if [[ "$service" == service-b || "$service" == a1 || "$service" == a2 || "$service" == a3 ]]; then
      queues="$(compose exec -T "$service" tc qdisc show dev eth0)"
      if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
        echo "netem remains on scoped target: $service" >&2
        return 1
      fi
    fi
    if [[ "$service" == a1 || "$service" == a2 || "$service" == a3 ]] && \
        compose exec -T "$service" pgrep -x stress-ng >/dev/null 2>&1; then
      echo "stress-ng remains on scoped target: $service" >&2
      return 1
    fi
  done
}

assert_no_loadgen() {
  if docker ps --all --quiet --filter "label=com.docker.compose.project=$PROJECT" \
      --filter "label=com.docker.compose.service=loadgen" | grep -q .; then
    echo "stale load generator container remains in scoped project" >&2
    return 1
  fi
}

health_check() {
  local service port
  for service in service-b a1 a2 a3 router; do
    case "$service" in
      service-b) port=50052 ;;
      a1|a2|a3) port=50051 ;;
      router) port=50050 ;;
    esac
    timeout 10s docker compose -f "$COMPOSE_FILE" -p "$PROJECT" exec -T \
      "$service" /opt/artc/bin/artc_loadgen --health \
      --target "127.0.0.1:$port" >/dev/null
  done
}

clear_netem() {
  local service="$1" queues
  compose exec -T "$service" tc qdisc del dev eth0 root >/dev/null
  queues="$(compose exec -T "$service" tc qdisc show dev eth0)"
  if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
    echo "netem cleanup verification failed on $service" >&2
    return 1
  fi
  NETEM_ACTIVE=false
}

stop_stress() {
  if [[ "$STRESS_ACTIVE" != true ]]; then return 0; fi
  local container_id
  container_id="$(verify_target a2)"
  compose exec -T a2 sh -c 'kill -TERM "$1" 2>/dev/null || true' sh "$STRESS_PID" || true
  sleep 1
  compose exec -T a2 sh -c 'kill -KILL "$1" 2>/dev/null || true' sh "$STRESS_PID" || true
  if compose exec -T a2 pgrep -x stress-ng >/dev/null 2>&1; then
    echo "stress-ng survived targeted PID cleanup in $container_id" >&2
    return 1
  fi
  STRESS_PID=""
  STRESS_ACTIVE=false
}

restore_service_b() {
  if [[ "$SERVICE_B_SLOW_ACTIVE" != true ]]; then return 0; fi
  ARTC_SERVICE_B_DELAY_US=0 compose up -d --no-deps --force-recreate service-b
  compose up -d --no-deps --wait --wait-timeout 30 service-b
  SERVICE_B_SLOW_ACTIVE=false
  verify_target service-b >/dev/null
}

restore_service_b_running() {
  if [[ "$SERVICE_B_STOPPED" != true ]]; then return 0; fi
  local container_id state
  container_id="$(verify_target service-b)"
  state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
  if [[ "$state" != running ]]; then compose start service-b >/dev/null; fi
  compose up -d --no-deps --wait --wait-timeout 30 service-b
  SERVICE_B_STOPPED=false
}

restore_router_running() {
  if [[ "$ROUTER_STOPPED" != true ]]; then return 0; fi
  local container_id state
  container_id="$(verify_target router)"
  state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
  if [[ "$state" != running ]]; then compose start router >/dev/null; fi
  compose up -d --no-deps --wait --wait-timeout 30 router
  ROUTER_STOPPED=false
}

restore_backends() {
  if [[ "$BACKENDS_UNAVAILABLE_ACTIVE" != true ]]; then return 0; fi
  ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=0 \
  ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N=0 \
  ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N=0 \
    compose up -d --no-deps --force-recreate a1 a2 a3
  compose up -d --no-deps --wait --wait-timeout 30 a1 a2 a3
  assert_project_identity
  BACKENDS_UNAVAILABLE_ACTIVE=false
}

restore_slow_backends() {
  if [[ "$ALL_BACKENDS_SLOW_ACTIVE" != true ]]; then return 0; fi
  ARTC_SERVICE_A1_DELAY_US=0 ARTC_SERVICE_A2_DELAY_US=0 \
  ARTC_SERVICE_A3_DELAY_US=0 compose up -d --no-deps --force-recreate a1 a2 a3
  compose up -d --no-deps --wait --wait-timeout 30 a1 a2 a3
  assert_project_identity
  ALL_BACKENDS_SLOW_ACTIVE=false
}

restore_a2_delay() {
  if [[ "$A2_DELAY_ACTIVE" != true ]]; then return 0; fi
  ARTC_SERVICE_A2_DELAY_US=0 compose up -d --no-deps --force-recreate a2
  compose up -d --no-deps --wait --wait-timeout 30 a2
  assert_project_identity
  A2_DELAY_ACTIVE=false
}

restore_a2_running() {
  local container_id paused state
  container_id="$(verify_target a2)"
  paused="$(docker inspect --format '{{.State.Paused}}' "$container_id")"
  if [[ "$paused" == true ]]; then docker unpause "$container_id" >/dev/null; fi
  A2_PAUSED=false
  if [[ "$A2_STOPPED" == true ]]; then
    state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
    if [[ "$state" != running ]]; then compose start a2 >/dev/null; fi
    compose up -d --no-deps --wait --wait-timeout 30 a2
    A2_STOPPED=false
  fi
}

restore_a3_running() {
  if [[ "$A3_STOPPED" != true ]]; then return 0; fi
  local container_id state
  container_id="$(verify_target a3)"
  state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
  if [[ "$state" != running ]]; then compose start a3 >/dev/null; fi
  compose up -d --no-deps --wait --wait-timeout 30 a3
  A3_STOPPED=false
}

cleanup_faults() {
  local result=0
  stop_stress || result=1
  if [[ "$NETEM_ACTIVE" == true ]]; then clear_netem a2 || result=1; fi
  restore_a2_running || result=1
  restore_a2_delay || result=1
  restore_backends || result=1
  restore_slow_backends || result=1
  restore_a3_running || result=1
  restore_service_b_running || result=1
  restore_service_b || result=1
  restore_router_running || result=1
  assert_project_identity || result=1
  assert_fault_state_clean || result=1
  return "$result"
}

assert_project_removed() {
  local remaining remaining_networks
  remaining="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$PROJECT")" || return 1
  if [[ -n "$remaining" ]]; then
    echo "Compose project containers remain after cleanup: $PROJECT" >&2
    return 1
  fi
  remaining_networks="$(docker network ls --quiet --filter "label=com.docker.compose.project=$PROJECT")" || return 1
  if [[ -n "$remaining_networks" ]]; then
    echo "Compose project networks remain after cleanup: $PROJECT" >&2
    return 1
  fi
}

on_exit() {
  local result=$?
  local cleanup_result=0
  trap - EXIT INT TERM
  if [[ -n "$LOAD_PID" ]]; then
    kill "$LOAD_PID" >/dev/null 2>&1 || true
    wait "$LOAD_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$LAB_UP" == true ]]; then
    cleanup_faults || { result=1; cleanup_result=1; }
    compose down --remove-orphans || { result=1; cleanup_result=1; }
    LAB_UP=false
    assert_project_removed || { result=1; cleanup_result=1; }
  fi
  if (( result != 0 )); then
    FAILURE_REASON="${FAILURE_REASON:-campaign exited with status $result}"
    if [[ -n "$CURRENT_EPISODE" && -f "$OUT_DIR/events.csv" ]]; then
      python3 - "$OUT_DIR/events.csv" "$CURRENT_EPISODE" "$CURRENT_STARTED" \
        "$cleanup_result" <<'PY' || true
import csv
import sys

with open(sys.argv[1], newline="", encoding="utf-8") as source:
    rows = list(csv.DictReader(source))
for row in rows:
    if row["episode"] == sys.argv[2]:
        row["started_utc"] = sys.argv[3] or "unknown"
        row["recovered_utc"] = ""
        row["result"] = "FAIL"
        row["cleanup"] = "failed" if sys.argv[4] == "1" else "verified"
with open(sys.argv[1], "w", newline="", encoding="utf-8") as target:
    writer = csv.DictWriter(target, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
PY
    fi
    python3 - "$OUT_DIR/failure.json" "$CURRENT_EPISODE" "$CURRENT_EVENT" \
      "$CURRENT_STARTED" "$FAILURE_REASON" "$result" "$cleanup_result" <<'PY' || true
import json
import sys
from pathlib import Path

episode = int(sys.argv[2]) if sys.argv[2] else None
Path(sys.argv[1]).write_text(json.dumps({
    "episode": episode,
    "event": sys.argv[3] or None,
    "started_utc": sys.argv[4] or None,
    "reason": sys.argv[5],
    "exit_status": int(sys.argv[6]),
    "cleanup_failed": sys.argv[7] == "1",
}, indent=2) + "\n", encoding="utf-8")
PY
  fi
  if [[ -n "$FAILURE_REASON" ]]; then echo "$FAILURE_REASON" >&2; fi
  exit "$result"
}
trap on_exit EXIT
trap 'FAILURE_REASON="interrupted by SIGINT"; exit 130' INT
trap 'FAILURE_REASON="interrupted by SIGTERM"; exit 143' TERM

events=(latency cpu pause_resume process_restart packet_loss dependency_slowdown \
  load_step incident_load_cpu_dependency incident_loss_unavailable_retry \
  incident_straggler_hedge_deadline incident_replica_recovery_spike \
  incident_global_slow_burst dependency_restart replica_flapping router_restart)

python3 - "$SEED" "$EPISODES" "$HOLD_SEC" "$FIRST_EPISODE" "${events[@]}" >"$OUT_DIR/campaign.json" <<'PY'
import json
import sys

mask = (1 << 64) - 1
state = int(sys.argv[1])
count = int(sys.argv[2])
hold_seconds = int(sys.argv[3])
first_episode = int(sys.argv[4])
names = sys.argv[5:]

def next_u64():
    global state
    state = (state + 0x9E3779B97F4A7C15) & mask
    value = state
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
    return value ^ (value >> 31)

for index in range(len(names) - 1, 0, -1):
    other = next_u64() % (index + 1)
    names[index], names[other] = names[other], names[index]

episodes = []
fault_parameters = {
    "latency": {"target": "A2", "egress_delay_ms": 75},
    "cpu": {"target": "A2", "stress_workers": 1},
    "pause_resume": {"target": "A2"},
    "process_restart": {"target": "A2", "signal": "SIGKILL"},
    "packet_loss": {"target": "A2", "egress_loss_percent": 100, "deadline_ms": 10000},
    "dependency_slowdown": {"target": "Service B", "delay_us": 100000},
    "load_step": {"initial_rps": 50, "peak_rps": 500},
    "incident_load_cpu_dependency": {
        "initial_rps": 100, "peak_rps": 500,
        "A2_stress_workers": 1, "Service_B_delay_us": 100000},
    "incident_loss_unavailable_retry": {
        "initial_rps": 100, "peak_rps": 500,
        "A1_A2_A3_unavailable_first_n": 100000,
        "A2_egress_loss_percent": 100,
        "A3_process_restart_during_loss": True,
        "deadline_ms": 10000,
        "retry_budget_capacity": 8, "hedge_budget_capacity": 8},
    "incident_straggler_hedge_deadline": {
        "initial_rps": 100, "peak_rps": 1000,
        "A2_egress_delay_ms": 75, "deadline_ms": 500, "hedge_delay_us": 20000},
    "incident_replica_recovery_spike": {
        "initial_rps": 100, "peak_rps": 500, "target": "A2", "action": "kill_restart"},
    "incident_global_slow_burst": {
        "initial_rps": 100, "peak_rps": 500,
        "A1_A2_A3_delay_us": 100000},
    "dependency_restart": {"target": "Service B", "signal": "SIGKILL"},
    "replica_flapping": {
      "target": "A2", "sequence": ["slow", "healthy", "down", "healthy", "slow", "healthy"],
      "slow_delay_us": 120000},
    "router_restart": {"target": "ARTC router", "signal": "SIGKILL", "controller_state": "reinitialized"},
}
for index, name in enumerate(names[:count], 1):
    rate = (1000 if name == "incident_straggler_hedge_deadline" else 500
            if name.startswith("incident_") or name == "load_step"
            else [50, 100, 250][next_u64() % 3])
    deadline = (500 if name == "incident_straggler_hedge_deadline" else
                10000 if name in ("packet_loss", "incident_loss_unavailable_retry") else 5000)
    mode = "step" if name == "load_step" or name.startswith("incident_") else "constant"
    initial_rate = 100 if name.startswith("incident_") else 50 if mode == "step" else rate
    episodes.append({"episode": index, "event": name, "fault_parameters": fault_parameters[name],
                     "mode": mode,
                     "initial_rate_rps": initial_rate, "offered_rps": rate,
                     "deadline_ms": deadline, "hold_seconds": hold_seconds})
episodes = episodes[first_episode - 1:]
print(json.dumps({"schema": "artc-phase4-seeded-chaos-v1", "seed": int(sys.argv[1]),
                  "prng": "SplitMix64/Fisher-Yates", "allowed_events": names,
                  "first_episode": first_episode, "last_episode": count,
                  "episodes": episodes}, indent=2, sort_keys=True))
PY

python3 - "$OUT_DIR/campaign.json" "$OUT_DIR/events.csv" <<'PY'
import csv
import json
import sys

campaign = json.load(open(sys.argv[1], encoding="utf-8"))
with open(sys.argv[2], "w", newline="", encoding="utf-8") as target:
    writer = csv.writer(target)
    writer.writerow(["episode", "seed", "event", "fault_parameters", "mode", "initial_rate_rps",
                     "offered_rps", "deadline_ms",
                     "hold_seconds", "started_utc", "recovered_utc", "recovery_attempts",
                     "recovery_delay_sec", "result", "cleanup", "measurement_status",
                     "loadgen_exit_status", "attempt_metadata_gap"])
    for item in campaign["episodes"]:
        writer.writerow([item["episode"], campaign["seed"], item["event"],
                         json.dumps(item["fault_parameters"], sort_keys=True),
                         item["mode"], item["initial_rate_rps"], item["offered_rps"],
                         item["deadline_ms"],
                         item["hold_seconds"], "", "", "", "", "pending", "pending",
                         "not_run", "", ""])
PY

if ! docker image inspect artc-phase1:local >/dev/null 2>&1; then
  echo "missing artc-phase1:local; build with lab/run_phase1_gates.sh first" >&2
  exit 2
fi
docker info >/dev/null
if [[ -n "$(docker ps --all --quiet --filter "label=com.docker.compose.project=$PROJECT")" ]]; then
  echo "refusing to reuse an existing Compose project: $PROJECT" >&2
  exit 2
fi

export ARTC_ROUTING_POLICY=artc_adaptive
export ARTC_AIMD_MIN_LIMIT=1 ARTC_AIMD_MAX_LIMIT=64 ARTC_AIMD_INITIAL_LIMIT=64
export ARTC_AIMD_ALPHA=2 ARTC_AIMD_BETA=0.7 ARTC_AIMD_INTERVAL_MS=10
export ARTC_AIMD_MIN_SAMPLES=4 ARTC_AIMD_TARGET_LATENCY_US=50000
export ARTC_AIMD_OVERLOAD_ERROR_FRACTION=0.1
export ARTC_DEADLINE_MARGIN_US=1000 ARTC_DEFAULT_DEADLINE_MS=5000
export ARTC_DECISION_SAMPLE_EVERY=100
export ARTC_HEALTH_MIN_SAMPLES=4 ARTC_HEALTH_FAILURES=3
export ARTC_HEALTH_RECOVERY_SUCCESSES=3 ARTC_HEALTH_RECOVERY_COOLDOWN_MS=500
export ARTC_HEALTH_DEGRADED_RATIO=2.0 ARTC_HEALTH_RECOVERED_RATIO=1.5
export ARTC_RECOVERY_PROBE_PERIOD=16
export ARTC_ATTEMPT_MAX_TOTAL=3 ARTC_ATTEMPT_MAX_ACTIVE=2
export ARTC_ATTEMPT_JITTER_SEED="$SEED"
export ARTC_ATTEMPT_MINIMUM_BUDGET_US=1000
export ARTC_HEDGE_BUDGET_CAPACITY=8 ARTC_HEDGE_BUDGET_REFILL_PER_SECOND=0
export ARTC_RETRY_BUDGET_CAPACITY=8 ARTC_RETRY_BUDGET_REFILL_PER_SECOND=0
export ARTC_EXECUTE_IDEMPOTENCY=idempotent
export ARTC_EXECUTE_HEDGING_ENABLED=true ARTC_EXECUTE_RETRY_ENABLED=true
export ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY=false
export ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS=3 ARTC_EXECUTE_MAX_RETRIES=2
export ARTC_EXECUTE_HEDGE_DELAY_MIN_US=20000 ARTC_EXECUTE_HEDGE_DELAY_MAX_US=20000
export ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS=10 ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS=100
export ARTC_EXECUTE_RETRY_JITTER_MAX_MS=10 ARTC_EXECUTE_RETRYABLE_STATUSES=UNAVAILABLE
export ARTC_SERVICE_B_DELAY_US=0
export ARTC_SERVICE_A1_DELAY_US=0 ARTC_SERVICE_A2_DELAY_US=0 ARTC_SERVICE_A3_DELAY_US=0
export ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=0 ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N=0
export ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N=0
export ARTC_SERVICE_A1_HONOR_CANCELLATION=true ARTC_SERVICE_A2_HONOR_CANCELLATION=true
export ARTC_SERVICE_A3_HONOR_CANCELLATION=true
export ARTC_SERVICE_A_UNAVAILABLE_FIRST_N=0 ARTC_SERVICE_A_HONOR_CANCELLATION=true
compose config --quiet
LAB_UP=true
compose up -d --wait --wait-timeout 90
assert_project_identity
assert_fault_state_clean
health_check

mapfile -t plan < <(python3 - "$OUT_DIR/campaign.json" <<'PY'
import json
import sys
for row in json.load(open(sys.argv[1], encoding="utf-8"))["episodes"]:
    print(f'{row["episode"]},{row["event"]},{row["mode"]},'
          f'{row["initial_rate_rps"]},{row["offered_rps"]},{row["deadline_ms"]}')
PY
)
LIMITATION_COUNT=0

for row in "${plan[@]}"; do
  IFS=, read -r episode event mode initial_rate rate deadline <<<"$row"
  episode_dir="$OUT_DIR/episode-$episode"
  run_dir="$ROOT/artifacts/runs/$RUN_ID/chaos/episode-$episode"
  console_dir="$OUT_DIR/console"
  mkdir -p "$episode_dir" "$run_dir" "$console_dir"
  CURRENT_EPISODE="$episode"
  CURRENT_EVENT="$event"
  CURRENT_STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  EVENT_RESULT=PASS
  MEASUREMENT_STATUS=valid
  ATTEMPT_METADATA_GAP=0
  if [[ "$event" == incident_* ]]; then
    load_duration_ms=$(((HOLD_SEC + 5) * 2 * 1000))
  else
    load_duration_ms=$(((HOLD_SEC + 15) * 1000))
  fi
  ARTC_RUN_ID="$RUN_ID-episode-$episode" timeout "$((HOLD_SEC + 30))s" \
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT" run --rm --no-deps loadgen \
    --target router:50050 --output "/artifacts/$RUN_ID/chaos/episode-$episode" \
    --mode "$mode" --duration-ms "$load_duration_ms" --seed "$SEED" \
    --initial-rate-rps "$initial_rate" --rate-rps "$rate" \
    --max-inflight 4096 --max-issue-lag-us 20000 --deadline-ms "$deadline" \
    --invoke-dependency --allow-errors \
    >"$console_dir/episode-$episode.loadgen.stdout" 2>&1 &
  LOAD_PID=$!
  sleep 1
  started="$CURRENT_STARTED"
  if [[ "$event" == incident_* ]]; then
    sleep "$((HOLD_SEC + 5))"
  fi
  case "$event" in
    latency)
      NETEM_ACTIVE=true
      compose exec -T a2 tc qdisc add dev eth0 root netem delay 75ms
      ;;
    packet_loss)
      NETEM_ACTIVE=true
      compose exec -T a2 tc qdisc add dev eth0 root netem loss 100%
      ;;
    cpu)
      STRESS_ACTIVE=true
      STRESS_PID="$(compose exec -T a2 sh -c "stress-ng --cpu 1 --timeout $((HOLD_SEC + 5))s --quiet >/tmp/artc-phase4-stress.log 2>&1 & echo \$!" | tr -d '\r')"
      [[ "$STRESS_PID" =~ ^[0-9]+$ ]] || { echo "could not identify scoped stress-ng process" >&2; exit 1; }
      ;;
    pause_resume)
      A2_PAUSED=true
      docker pause "$(verify_target a2)" >/dev/null
      ;;
    process_restart)
      A2_STOPPED=true
      docker kill "$(verify_target a2)" >/dev/null
      ;;
    dependency_slowdown)
      SERVICE_B_SLOW_ACTIVE=true
      ARTC_SERVICE_B_DELAY_US=100000 compose up -d --no-deps --force-recreate service-b
      compose up -d --no-deps --wait --wait-timeout 30 service-b
      verify_target service-b >/dev/null
      ;;
    dependency_restart)
      SERVICE_B_STOPPED=true
      docker kill "$(verify_target service-b)" >/dev/null
      ;;
    load_step)
      ;;
    incident_load_cpu_dependency)
      SERVICE_B_SLOW_ACTIVE=true
      ARTC_SERVICE_B_DELAY_US=100000 compose up -d --no-deps --force-recreate service-b
      compose up -d --no-deps --wait --wait-timeout 30 service-b
      STRESS_ACTIVE=true
      STRESS_PID="$(compose exec -T a2 sh -c "stress-ng --cpu 1 --timeout $((HOLD_SEC + 5))s --quiet >/tmp/artc-phase4-stress.log 2>&1 & echo \$!" | tr -d '\r')"
      [[ "$STRESS_PID" =~ ^[0-9]+$ ]] || { echo "could not identify scoped stress-ng process" >&2; exit 1; }
      ;;
    incident_loss_unavailable_retry)
      BACKENDS_UNAVAILABLE_ACTIVE=true
      ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N=100000 \
      ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N=100000 \
      ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N=100000 \
        compose up -d --no-deps --force-recreate a1 a2 a3
      compose up -d --no-deps --wait --wait-timeout 30 a1 a2 a3
      NETEM_ACTIVE=true
      compose exec -T a2 tc qdisc add dev eth0 root netem loss 100%
      A3_STOPPED=true
      docker kill "$(verify_target a3)" >/dev/null
      sleep 1
      compose start a3 >/dev/null
      compose up -d --no-deps --wait --wait-timeout 30 a3
      A3_STOPPED=false
      ;;
    incident_straggler_hedge_deadline)
      NETEM_ACTIVE=true
      compose exec -T a2 tc qdisc add dev eth0 root netem delay 75ms
      ;;
    incident_replica_recovery_spike)
      A2_STOPPED=true
      docker kill "$(verify_target a2)" >/dev/null
      sleep 1
      compose start a2 >/dev/null
      compose up -d --no-deps --wait --wait-timeout 30 a2
      A2_STOPPED=false
      ;;
    incident_global_slow_burst)
      ALL_BACKENDS_SLOW_ACTIVE=true
      ARTC_SERVICE_A1_DELAY_US=100000 ARTC_SERVICE_A2_DELAY_US=100000 \
      ARTC_SERVICE_A3_DELAY_US=100000 compose up -d --no-deps --force-recreate a1 a2 a3
      compose up -d --no-deps --wait --wait-timeout 30 a1 a2 a3
      assert_project_identity
      ;;
    replica_flapping)
      A2_DELAY_ACTIVE=true
      ARTC_SERVICE_A2_DELAY_US=120000 compose up -d --no-deps --force-recreate a2
      compose up -d --no-deps --wait --wait-timeout 30 a2
      sleep 2
      restore_a2_delay
      A2_STOPPED=true
      docker kill "$(verify_target a2)" >/dev/null
      sleep 1
      compose start a2 >/dev/null
      compose up -d --no-deps --wait --wait-timeout 30 a2
      A2_STOPPED=false
      A2_DELAY_ACTIVE=true
      ARTC_SERVICE_A2_DELAY_US=120000 compose up -d --no-deps --force-recreate a2
      compose up -d --no-deps --wait --wait-timeout 30 a2
      sleep 2
      restore_a2_delay
      ;;
    router_restart)
      ROUTER_STOPPED=true
      docker kill "$(verify_target router)" >/dev/null
      ;;
    *)
      echo "internal error: unrecognized generated event" >&2
      exit 1
      ;;
  esac
  sleep "$HOLD_SEC"
  cleanup_faults
  health_check
  loadgen_exit_status=0
  if wait "$LOAD_PID"; then
    :
  else
    loadgen_exit_status=$?
  fi
  LOAD_PID=""
  assert_no_loadgen
  if ((loadgen_exit_status == 0)); then
    python3 lab/validate_artifacts.py --allow-errors "$run_dir"
  elif ((loadgen_exit_status == 2)) && [[ "$event" == router_restart ||
      "$event" == packet_loss || "$event" == incident_loss_unavailable_retry ]]; then
    MEASUREMENT_STATUS=invalid_expected_fault_metadata_gap
    EVENT_RESULT=PASS_WITH_LIMITATION
    ATTEMPT_METADATA_GAP="$(python3 - "$run_dir" <<'PY'
import csv
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))

def histogram_count(file_path):
    with file_path.open(newline="", encoding="utf-8") as source:
        rows = (line for line in source if not line.startswith("#"))
        return sum(int(row["count"]) for row in csv.DictReader(rows))

issued = manifest.get("issued", -1)
completed = manifest.get("completed", -1)
gap = issued - manifest.get("attempt_metadata_observed", -1)
if manifest.get("measurement_valid") is not False or manifest.get("valid") is not False:
    raise SystemExit("expected an explicitly invalid fault-window measurement")
if manifest.get("generator_saturated") is not False or manifest.get("invariant_violations") != 0:
    raise SystemExit("faulted requests exceeded generator or request-path bounds")
if issued != manifest.get("scheduled") or completed != issued:
    raise SystemExit("faulted load schedule did not finish all client calls")
if gap <= 0 or manifest.get("errors", 0) <= 0:
    raise SystemExit("faulted measurement has no expected missing-metadata errors")
if manifest.get("max_issue_lag_us", 0) > manifest.get("max_issue_lag_limit_us", 0):
    raise SystemExit("faulted load generator exceeded its issue-lag limit")
if manifest.get("latency_us", {}).get("sample_count") != completed or \
        histogram_count(path / "latency.hdr.csv") != completed:
    raise SystemExit("faulted latency histogram is incomplete")
if manifest.get("issue_lag_us", {}).get("sample_count") != issued or \
        histogram_count(path / "issue-lag.hdr.csv") != issued:
    raise SystemExit("faulted issue-lag histogram is incomplete")
if manifest.get("attempt_metadata_version") != 3:
    raise SystemExit("faulted run lacks Phase 3 attempt accounting")
if manifest["primary_attempts"] + manifest["hedge_attempts"] + manifest["retry_attempts"] != \
        manifest["backend_attempts"]:
    raise SystemExit("faulted attempt categories do not reconcile")
if manifest["backend_attempts"] > 3 * manifest["admitted"] or \
        manifest["max_admitted_attempts_per_request"] > 3:
    raise SystemExit("faulted attempts exceeded the Phase 3 per-request bound")
status_counts = manifest.get("status_code_counts", {})
if not (status_counts.get("4", 0) or status_counts.get("14", 0)):
    raise SystemExit("faulted errors do not match deadline or unavailable outcomes")
print(gap)
PY
)"
    ((LIMITATION_COUNT += 1))
    echo "fault-window measurement invalid by design; attempt metadata gap=$ATTEMPT_METADATA_GAP"
  else
    echo "loadgen failed outside an allowed fault-window limitation: event=$event status=$loadgen_exit_status" >&2
    exit "$loadgen_exit_status"
  fi
  recovery_started_epoch="$(date +%s)"
  recovery_attempts=0
  recovered=false
  for recovery_attempt in {1..10}; do
    recovery_attempts="$recovery_attempt"
    recovery_dir="$episode_dir/recovery-$recovery_attempt"
    recovery_run_dir="$run_dir/recovery-$recovery_attempt"
    mkdir -p "$recovery_dir" "$recovery_run_dir"
    ARTC_RUN_ID="$RUN_ID-episode-$episode-recovery-$recovery_attempt" timeout 20s \
      docker compose -f "$COMPOSE_FILE" -p "$PROJECT" run --rm --no-deps loadgen \
      --target router:50050 \
      --output "/artifacts/$RUN_ID/chaos/episode-$episode/recovery-$recovery_attempt" \
      --mode constant --duration-ms 2000 --rate-rps 100 --seed "$SEED" \
      --max-inflight 512 --max-issue-lag-us 20000 --deadline-ms 5000 \
      --invoke-dependency --allow-errors \
      >"$console_dir/episode-$episode-recovery-$recovery_attempt.loadgen.stdout" 2>&1
    assert_no_loadgen
    python3 lab/validate_artifacts.py --allow-errors "$recovery_run_dir"
    if python3 - "$recovery_run_dir/manifest.json" "$event" <<'PY'
import json
import sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
responses = manifest.get("responses_by_replica", {})
if (manifest.get("completed", 0) <= 0 or
        manifest.get("successful", 0) != manifest.get("completed", 0) or
        manifest.get("errors", 0) != 0 or manifest.get("generator_saturated")):
    raise SystemExit(1)
if not responses:
    raise SystemExit(1)
if sys.argv[2] in {"process_restart", "incident_replica_recovery_spike", "replica_flapping"}:
    if responses.get("A2", 0) <= 0:
        raise SystemExit(1)
PY
    then
      recovered=true
      break
    fi
    sleep 1
  done
  if [[ "$recovered" != true ]]; then
    echo "post-fault recovery did not reach a clean probe within 10 attempts" >&2
    exit 1
  fi
  recovery_finished_epoch="$(date +%s)"
  recovery_delay_sec=$((recovery_finished_epoch - recovery_started_epoch))
  recovered="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 - "$OUT_DIR/events.csv" "$episode" "$started" "$recovered" \
    "$recovery_attempts" "$recovery_delay_sec" "$EVENT_RESULT" \
    "$MEASUREMENT_STATUS" "$loadgen_exit_status" "$ATTEMPT_METADATA_GAP" <<'PY'
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
for row in rows:
    if row["episode"] == sys.argv[2]:
        row["started_utc"] = sys.argv[3]
        row["recovered_utc"] = sys.argv[4]
        row["recovery_attempts"] = sys.argv[5]
        row["recovery_delay_sec"] = sys.argv[6]
        row["result"] = sys.argv[7]
        row["cleanup"] = "verified"
        row["measurement_status"] = sys.argv[8]
        row["loadgen_exit_status"] = sys.argv[9]
        row["attempt_metadata_gap"] = sys.argv[10]
with path.open("w", newline="", encoding="utf-8") as target:
    writer = csv.DictWriter(target, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
PY
  assert_project_identity
  CURRENT_EPISODE=""
  CURRENT_EVENT=""
  CURRENT_STARTED=""
done

assert_fault_state_clean
health_check
compose logs --no-color --no-log-prefix >"$OUT_DIR/compose.log"
compose stop router >/dev/null
compose logs --no-color --no-log-prefix router >"$OUT_DIR/router-shutdown.log"
grep '^ARTC_ATTEMPT_SUMMARY ' "$OUT_DIR/router-shutdown.log" >"$OUT_DIR/router-summary.txt"
python3 - "$OUT_DIR/router-summary.txt" <<'PY'
import sys
line = open(sys.argv[1], encoding="utf-8").read().strip()
fields = dict(item.split("=", 1) for item in line.split()[1:])
if int(fields["logical_requests"]) != int(fields["logical_terminals"]):
    raise SystemExit("logical request/terminal accounting mismatch")
if int(fields["active_attempts"]) != 0 or int(fields["pending_backend_callbacks"]) != 0:
    raise SystemExit("attempts or callbacks remained after router shutdown")
if float(fields["attempt_amplification"]) > 3.0:
    raise SystemExit("observed attempt amplification exceeded Phase 3 bound")
print("chaos_shutdown_invariants=pass")
PY
compose down --remove-orphans
LAB_UP=false
assert_project_removed
CHAOS_STATUS=pass
if ((LIMITATION_COUNT > 0)); then CHAOS_STATUS=pass_with_limitations; fi
printf 'phase4_seeded_chaos=%s\nseed=%s\nepisodes=%s\nlimitations=%s\nepisode_range=%s-%s\n' \
  "$CHAOS_STATUS" "$SEED" "${#plan[@]}" "$LIMITATION_COUNT" "$FIRST_EPISODE" "$EPISODES" \
  | tee "$OUT_DIR/result.txt"
echo "chaos_artifacts=$OUT_DIR"
