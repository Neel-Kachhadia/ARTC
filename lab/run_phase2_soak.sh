#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUN_ID="${ARTC_RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
PROJECT="artc-phase4-soak-$RUN_ID"
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/phase2-soak"
SOAK_DURATION_MS="${ARTC_SOAK_DURATION_MS:-1800000}"
SOAK_RATE_RPS="${ARTC_SOAK_RATE_RPS:-500}"
SOAK_SEED="${ARTC_SOAK_SEED:-1}"
SOAK_FAULT_PROFILE="${ARTC_SOAK_FAULT_PROFILE:-none}"
SOAK_FAULT_AT_SEC="${ARTC_SOAK_FAULT_AT_SEC:-0}"
SOAK_FAULT_HOLD_SEC="${ARTC_SOAK_FAULT_HOLD_SEC:-0}"
SOAK_FAULT_DELAY_MS="${ARTC_SOAK_FAULT_DELAY_MS:-75}"
SOAK_FAULT_ACTIVE=false
SOAK_FAULT_FINALIZED=false
SOAK_FAULT_TARGET_ID=""
if [[ "$SOAK_FAULT_PROFILE" != none && "$SOAK_FAULT_PROFILE" != a2_egress_delay ]]; then
  echo "ARTC_SOAK_FAULT_PROFILE must be none or a2_egress_delay" >&2
  exit 2
fi
if [[ ! "$SOAK_DURATION_MS" =~ ^[1-9][0-9]{0,7}$ ||
      ! "$SOAK_RATE_RPS" =~ ^[1-9][0-9]{0,5}$ ||
      ! "$SOAK_SEED" =~ ^[0-9]{1,20}$ ]]; then
  echo "invalid soak duration, rate, or seed" >&2
  exit 2
fi
SOAK_DURATION_MS=$((10#$SOAK_DURATION_MS))
SOAK_RATE_RPS=$((10#$SOAK_RATE_RPS))
if [[ "$SOAK_FAULT_PROFILE" == a2_egress_delay ]]; then
  if [[ ! "$SOAK_FAULT_AT_SEC" =~ ^[1-9][0-9]{0,4}$ ||
        ! "$SOAK_FAULT_HOLD_SEC" =~ ^[1-9][0-9]{0,3}$ ||
        ! "$SOAK_FAULT_DELAY_MS" =~ ^[1-9][0-9]{0,3}$ ]]; then
    echo "invalid bounded a2_egress_delay schedule" >&2
    exit 2
  fi
  SOAK_FAULT_AT_SEC=$((10#$SOAK_FAULT_AT_SEC))
  SOAK_FAULT_HOLD_SEC=$((10#$SOAK_FAULT_HOLD_SEC))
  SOAK_FAULT_DELAY_MS=$((10#$SOAK_FAULT_DELAY_MS))
  if (( (SOAK_FAULT_AT_SEC + SOAK_FAULT_HOLD_SEC) * 1000 >= SOAK_DURATION_MS ||
        SOAK_FAULT_HOLD_SEC > 300 || SOAK_FAULT_DELAY_MS > 1000 )); then
    echo "invalid bounded a2_egress_delay schedule" >&2
    exit 2
  fi
fi
mkdir -p "$(dirname "$OUT_DIR")"
if ! mkdir "$OUT_DIR"; then
  echo "refusing to overwrite existing soak artifacts: $OUT_DIR" >&2
  exit 2
fi
export COMPOSE_PROJECT_NAME="$PROJECT"
export ARTC_RUN_ID="$RUN_ID-soak"
export ARTC_ROUTING_POLICY=artc_adaptive
export ARTC_DECISION_SAMPLE_EVERY="${ARTC_PHASE2_SAMPLE_EVERY:-100}"
export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  export ARTC_WORKTREE_DIRTY=true
else
  export ARTC_WORKTREE_DIRTY=false
fi

COMPOSE_FILE="lab/compose.yaml"
LAB_UP=false
LOAD_PID=""
MONITOR_PID=""
compose() { docker compose -f "$COMPOSE_FILE" -p "$PROJECT" "$@"; }

write_fault_event() {
  local status="$1" cleanup="$2"
  python3 - "$OUT_DIR/fault-event.json" "$RUN_ID" "$SOAK_SEED" \
    "$SOAK_FAULT_AT_SEC" "$SOAK_FAULT_HOLD_SEC" "$SOAK_FAULT_DELAY_MS" \
    "$status" "$cleanup" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path, run_id, seed, at_sec, hold_sec, delay_ms, status, cleanup = sys.argv[1:]
target = Path(path)
event = json.loads(target.read_text(encoding="utf-8")) if target.exists() else {
    "schema": "artc-phase4-soak-fault-v1",
    "run_id": run_id,
    "seed": int(seed),
    "event": "a2_egress_delay",
    "target": "A2 eth0 egress",
    "delay_ms": int(delay_ms),
    "scheduled_after_sec": int(at_sec),
    "hold_sec": int(hold_sec),
}
event["status"] = status
event["cleanup"] = cleanup
if status == "active":
    event["applied_utc"] = datetime.now(timezone.utc).isoformat()
if status in {"pass", "interrupted", "cleanup_failed"}:
    event["recovered_utc"] = datetime.now(timezone.utc).isoformat() if cleanup == "verified" else None
target.write_text(json.dumps(event, indent=2) + "\n", encoding="utf-8")
PY
}

clear_soak_fault() {
  [[ "$SOAK_FAULT_ACTIVE" == true ]] || return 0
  local project_label service_label queues
  project_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$SOAK_FAULT_TARGET_ID")"
  service_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$SOAK_FAULT_TARGET_ID")"
  [[ "$project_label" == "$PROJECT" && "$service_label" == a2 ]] || {
    echo "refusing fault cleanup for an unexpected target" >&2
    return 1
  }
  docker exec "$SOAK_FAULT_TARGET_ID" tc qdisc del dev eth0 root >/dev/null
  queues="$(docker exec "$SOAK_FAULT_TARGET_ID" tc qdisc show dev eth0)"
  if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
    echo "netem cleanup verification failed on A2" >&2
    return 1
  fi
  SOAK_FAULT_ACTIVE=false
}

assert_fault_cleanup() {
  local service container_id queues
  for service in service-b a1 a2 a3; do
    container_id="$(compose ps -q "$service")"
    [[ -n "$container_id" ]] || { echo "missing scoped service: $service" >&2; return 1; }
    if [[ "$(docker inspect --format '{{.State.Status}}' "$container_id")" != running ]]; then
      echo "service is not running after soak: $service" >&2
      return 1
    fi
    queues="$(compose exec -T "$service" tc qdisc show dev eth0)"
    if grep -Eq '(^|[[:space:]])netem([[:space:]]|$)' <<<"$queues"; then
      echo "netem remains after soak on scoped service: $service" >&2
      return 1
    fi
    if [[ "$service" != service-b ]] && compose exec -T "$service" pgrep -x stress-ng >/dev/null 2>&1; then
      echo "stress-ng remains after soak on scoped service: $service" >&2
      return 1
    fi
  done
}

assert_project_removed() {
  local remaining
  remaining="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$PROJECT")" || return 1
  if [[ -n "$remaining" ]]; then
    echo "Compose project containers remain after soak cleanup: $PROJECT" >&2
    return 1
  fi
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
  if [[ "$LAB_UP" == true ]]; then
    if [[ "$SOAK_FAULT_ACTIVE" == true ]]; then
      if clear_soak_fault; then
        write_fault_event interrupted verified || result=1
        SOAK_FAULT_FINALIZED=true
      else
        write_fault_event cleanup_failed failed || true
        result=1
        SOAK_FAULT_FINALIZED=true
      fi
    elif [[ "$SOAK_FAULT_PROFILE" == a2_egress_delay && "$SOAK_FAULT_FINALIZED" != true ]]; then
      write_fault_event interrupted not_applied || result=1
      SOAK_FAULT_FINALIZED=true
    fi
    compose down --remove-orphans || result=1
    LAB_UP=false
    assert_project_removed || result=1
  fi
  exit "$result"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! docker image inspect artc-phase1:local >/dev/null 2>&1; then
  echo "missing artc-phase1:local; run lab/run_phase1_gates.sh first" >&2
  exit 2
fi
docker info >/dev/null
if [[ -n "$(docker ps --all --quiet --filter "label=com.docker.compose.project=$PROJECT")" ]]; then
  echo "refusing to reuse an existing Compose project: $PROJECT" >&2
  exit 2
fi
compose config --quiet
LAB_UP=true
compose up -d --wait --wait-timeout 90
for service in service-b a1 a2 a3 router; do
  container_id="$(compose ps -q "$service")"
  [[ -n "$container_id" ]] || { echo "missing scoped service: $service" >&2; exit 1; }
  project_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$container_id")"
  service_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$container_id")"
  [[ "$project_label" == "$PROJECT" && "$service_label" == "$service" ]] || {
    echo "Compose target identity mismatch for $service" >&2
    exit 1
  }
done
compose run --rm --no-deps loadgen --health --target router:50050

container_id="$(compose ps -q router)"
run_dir="$OUT_DIR/run"
loadgen_log="$OUT_DIR/loadgen.stdout"
mkdir -p "$run_dir"
if [[ "$SOAK_FAULT_PROFILE" == a2_egress_delay ]]; then
  write_fault_event planned pending
fi

compose run --rm --no-deps loadgen \
  --target router:50050 \
  --output "/artifacts/$RUN_ID/phase2-soak/run" \
  --mode constant \
  --duration-ms "$SOAK_DURATION_MS" \
  --rate-rps "$SOAK_RATE_RPS" \
  --seed "$SOAK_SEED" \
  --max-inflight "${ARTC_LOADGEN_MAX_INFLIGHT:-8192}" \
  --max-issue-lag-us "${ARTC_PHASE2_MAX_ISSUE_LAG_US:-20000}" \
  --deadline-ms 5000 --invoke-dependency --allow-errors \
  >"$loadgen_log" 2>&1 &
LOAD_PID=$!

(
  echo "unix_s,fd_count,thread_count,socket_count,rss_kib,cpu_percent,docker_rss,pids"
  while kill -0 "$LOAD_PID" >/dev/null 2>&1; do
    compose exec -T router sh -c '
      pid="$(pgrep -xo artc_lab_node)"
      fd="$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 | wc -l)"
      threads="$(sed -n "s/^Threads:[[:space:]]*//p" "/proc/$pid/status")"
      sockets="$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 -lname "socket:*" | wc -l)"
      rss="$(sed -n "s/^VmRSS:[[:space:]]*//p" "/proc/$pid/status")"
      printf "%s,%s,%s,%s,%s\n" "$(date +%s)" "$fd" "$threads" "$sockets" "$rss"
    ' 2>/dev/null | while IFS=, read -r timestamp fd threads sockets rss; do
      cpu_memory="$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.PIDs}}' "$container_id" 2>/dev/null || true)"
      IFS=, read -r cpu memory pids <<<"$cpu_memory"
      printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$timestamp" "$fd" "$threads" "$sockets" "${rss%% *}" \
        "$cpu" "${memory%% / *}" "$pids"
    done
    sleep "${ARTC_SOAK_SAMPLE_INTERVAL_SEC:-10}"
  done
) >"$OUT_DIR/resources.csv" &
MONITOR_PID=$!

if [[ "$SOAK_FAULT_PROFILE" == a2_egress_delay ]]; then
  sleep "$SOAK_FAULT_AT_SEC"
  kill -0 "$LOAD_PID" || { echo "load generator exited before scheduled fault" >&2; exit 1; }
  SOAK_FAULT_TARGET_ID="$(compose ps -q a2)"
  [[ -n "$SOAK_FAULT_TARGET_ID" ]] || { echo "missing scoped A2 fault target" >&2; exit 1; }
  project_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$SOAK_FAULT_TARGET_ID")"
  service_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$SOAK_FAULT_TARGET_ID")"
  [[ "$project_label" == "$PROJECT" && "$service_label" == a2 ]] || {
    echo "refusing unexpected Docker target for A2 fault" >&2
    exit 1
  }
  SOAK_FAULT_ACTIVE=true
  compose exec -T a2 tc qdisc add dev eth0 root netem delay "${SOAK_FAULT_DELAY_MS}ms"
  write_fault_event active pending
  sleep "$SOAK_FAULT_HOLD_SEC"
  clear_soak_fault
  write_fault_event pass verified
  SOAK_FAULT_FINALIZED=true
fi

result=0
wait "$LOAD_PID" || result=$?
LOAD_PID=""
kill "$MONITOR_PID" >/dev/null 2>&1 || true
wait "$MONITOR_PID" >/dev/null 2>&1 || true
MONITOR_PID=""
if (( result != 0 )); then
  cat "$loadgen_log" >&2
  echo "soak loadgen failed status=$result" >&2
  exit "$result"
fi
assert_fault_cleanup

if [[ "$SOAK_FAULT_PROFILE" == a2_egress_delay ]]; then
  recovery_dir="$OUT_DIR/recovery"
  mkdir -p "$recovery_dir"
  compose run --rm --no-deps loadgen \
    --target router:50050 --output "/artifacts/$RUN_ID/phase2-soak/recovery" \
    --mode constant --duration-ms 2000 --rate-rps 100 --seed "$SOAK_SEED" \
    --max-inflight 512 --max-issue-lag-us 20000 --deadline-ms 5000 \
    --invoke-dependency --allow-errors >"$OUT_DIR/recovery.stdout" 2>&1
  python3 lab/validate_artifacts.py --allow-errors "$recovery_dir"
  python3 - "$recovery_dir/manifest.json" <<'PY'
import json
import sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
if manifest.get("completed", 0) <= 0 or manifest.get("errors", 0) != 0:
    raise SystemExit("post-fault soak recovery probe did not complete cleanly")
if manifest.get("responses_by_replica", {}).get("A2", 0) <= 0:
    raise SystemExit("A2 received no traffic after egress-fault recovery")
PY
fi

python3 lab/validate_artifacts.py --allow-errors "$run_dir"
python3 lab/analyze_phase2_resources.py "$OUT_DIR" >"$OUT_DIR/resource-analysis.stdout"
echo "phase2_soak_complete=$OUT_DIR"
