#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUN_ID="${ARTC_RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
PROJECT="${COMPOSE_PROJECT_NAME:-artc-phase2-soak-$RUN_ID}"
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/phase2-soak"
mkdir -p "$OUT_DIR"
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
compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

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
    compose down --remove-orphans || result=1
    LAB_UP=false
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
LAB_UP=true
compose up -d --wait --wait-timeout 90
compose run --rm --no-deps loadgen --health --target router:50050

duration_ms="${ARTC_SOAK_DURATION_MS:-1800000}"
rate_rps="${ARTC_SOAK_RATE_RPS:-500}"
container_id="$(compose ps -q router)"
run_dir="$OUT_DIR/run"
loadgen_log="$OUT_DIR/loadgen.stdout"
mkdir -p "$run_dir"

compose run --rm --no-deps loadgen \
  --target router:50050 \
  --output "/artifacts/$RUN_ID/phase2-soak/run" \
  --mode constant \
  --duration-ms "$duration_ms" \
  --rate-rps "$rate_rps" \
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

python3 lab/validate_artifacts.py --allow-errors "$run_dir"
python3 lab/analyze_phase2_resources.py "$OUT_DIR" >"$OUT_DIR/resource-analysis.stdout"
echo "phase2_soak_complete=$OUT_DIR"
