#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="full"
if [[ "${1:-}" == "--fault-hold-only" ]]; then MODE="fault-hold-only"; fi
RUN_ID="${ARTC_RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
OUT_DIR="${ARTC_OUT_DIR:-artifacts/runs/$RUN_ID}"
PROJECT="${COMPOSE_PROJECT_NAME:-artc-phase1-$RUN_ID}"
export ARTC_RUN_ID="$RUN_ID"
export COMPOSE_PROJECT_NAME="$PROJECT"
export ARTC_ROUTING_POLICY="${ARTC_ROUTING_POLICY:-round_robin}"
mkdir -p "$OUT_DIR"

COMPOSE_FILE="lab/compose.yaml"
LAB_UP=false
FAULT_ACTIVE=false
STRESS_ACTIVE=false
HOLD_PID=""

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

capture_qdiscs() {
  local destination="$1"
  mkdir -p "$destination"
  compose exec -T a1 tc qdisc show dev eth0 >"$destination/A1.txt"
  compose exec -T a2 tc qdisc show dev eth0 >"$destination/A2.txt"
  compose exec -T a3 tc qdisc show dev eth0 >"$destination/A3.txt"
}

assert_only_a2_faulted() {
  local proof="$1"
  capture_qdiscs "$proof/qdisc"
  grep -q netem "$proof/qdisc/A2.txt" || { echo "A2 has no netem fault" >&2; return 1; }
  if grep -q netem "$proof/qdisc/A1.txt" "$proof/qdisc/A3.txt"; then
    echo "a non-A2 replica has a netem fault" >&2
    return 1
  fi
  printf 'target=A2\nA1_netem=false\nA2_netem=true\nA3_netem=false\n' >"$proof/target-proof.txt"
}

cleanup_fault() {
  local proof="$1"
  compose exec -T a2 tc qdisc del dev eth0 root >/dev/null 2>&1 || true
  capture_qdiscs "$proof/qdisc"
  if grep -q netem "$proof/qdisc/A1.txt" "$proof/qdisc/A2.txt" "$proof/qdisc/A3.txt"; then
    printf 'cleanup_verified=false\n' >"$proof/cleanup-proof.txt"
    return 1
  fi
  printf 'cleanup_verified=true\nA1_netem=false\nA2_netem=false\nA3_netem=false\n' >"$proof/cleanup-proof.txt"
  FAULT_ACTIVE=false
}

stop_stress() {
  if [[ "$STRESS_ACTIVE" == true ]]; then
    compose exec -T a2 pkill -TERM -x stress-ng >/dev/null 2>&1 || true
    STRESS_ACTIVE=false
  fi
}

on_exit() {
  local result=$?
  trap - EXIT INT TERM
  if [[ -n "$HOLD_PID" ]]; then
    kill "$HOLD_PID" >/dev/null 2>&1 || true
    wait "$HOLD_PID" >/dev/null 2>&1 || true
  fi
  stop_stress || result=1
  if [[ "$FAULT_ACTIVE" == true && "$LAB_UP" == true ]]; then
    cleanup_fault "$OUT_DIR/fault-cleanup" || result=1
    cp "$OUT_DIR/fault-cleanup/cleanup-proof.txt" "$OUT_DIR/fault-cleanup-proof.txt" 2>/dev/null || true
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

check_health() {
  local service="$1" target="$2"
  compose run --rm --no-deps loadgen --health --target "$target" \
    | tee "$OUT_DIR/health-$service.log"
}

run_load() {
  local label="$1" target="$2"
  shift 2
  compose run --rm --no-deps loadgen \
    --target "$target" \
    --output "/artifacts/$RUN_ID/$label" \
    --mode constant --duration-ms "${ARTC_BENCH_DURATION_MS:-10000}" \
    --rate-rps "${ARTC_BENCH_RATE_RPS:-1000}" \
    --max-inflight 512 --max-issue-lag-us 5000 --deadline-ms 5000 \
    --invoke-dependency "$@" \
    | tee "$OUT_DIR/$label.stdout"
  python3 lab/validate_artifacts.py "$OUT_DIR/$label"
}

if [[ "$MODE" == "full" ]]; then
  cmake --preset release
  cmake --build --preset release -j"${ARTC_BUILD_JOBS:-2}"
  git rev-parse HEAD >"$OUT_DIR/git-revision.txt"
  ARTC_GIT_REVISION="$(<"$OUT_DIR/git-revision.txt")"
  git status --porcelain=v1 --untracked-files=all >"$OUT_DIR/git-status.txt"
  if [[ -s "$OUT_DIR/git-status.txt" ]]; then ARTC_WORKTREE_DIRTY=true; else ARTC_WORKTREE_DIRTY=false; fi
  export ARTC_GIT_REVISION ARTC_WORKTREE_DIRTY
  docker compose -f "$COMPOSE_FILE" build --pull
fi

if [[ "$MODE" == "fault-hold-only" ]]; then
  mkdir -p "$OUT_DIR/interrupted"
  LAB_UP=true
  compose up -d --wait --wait-timeout 60
  FAULT_ACTIVE=true
  compose exec -T a2 tc qdisc add dev eth0 root netem delay 150ms
  assert_only_a2_faulted "$OUT_DIR/interrupted"
  touch "$OUT_DIR/interrupted/fault-active.marker"
  sleep 120 &
  HOLD_PID=$!
  wait "$HOLD_PID"
  HOLD_PID=""
  exit 0
fi

LAB_UP=true
compose up -d --wait --wait-timeout 60
check_health service-b service-b:50052
check_health A1 a1:50051
check_health A2 a2:50051
check_health A3 a3:50051
check_health router router:50050

mkdir -p "$OUT_DIR/generator-validation"
compose run --rm --no-deps loadgen --validate-schedule --mode constant \
  --duration-ms 1000 --rate-rps 100 >"$OUT_DIR/generator-validation/schedule.txt"
run_load generator-open-loop router:50050 --rate-rps 100 --duration-ms 1000

set +e
compose run --rm --no-deps loadgen \
  --target a2:50051 \
  --output "/artifacts/$RUN_ID/self-saturation" \
  --mode constant --duration-ms 100 --rate-rps 10000 \
  --max-inflight 1 --max-issue-lag-us 1000 --deadline-ms 5000 \
  >"$OUT_DIR/self-saturation.stdout" 2>&1
saturation_status=$?
set -e
if [[ "$saturation_status" -ne 2 ]]; then
  echo "self-saturation probe returned $saturation_status, expected invalid-run status 2" >&2
  exit 1
fi
python3 lab/validate_artifacts.py --expect-saturated "$OUT_DIR/self-saturation"

for policy in round_robin least_inflight ewma_latency p2c_latency_inflight; do
  export ARTC_ROUTING_POLICY="$policy"
  compose up -d --force-recreate --wait --wait-timeout 30 router
  check_health router router:50050
  run_load "healthy-$policy" router:50050
done

export ARTC_ROUTING_POLICY=round_robin
compose up -d --force-recreate --wait --wait-timeout 30 router
check_health router router:50050

mkdir -p "$OUT_DIR/straggler"
FAULT_ACTIVE=true
compose exec -T a2 tc qdisc add dev eth0 root netem delay 150ms
assert_only_a2_faulted "$OUT_DIR/straggler"
run_load straggler-faulted router:50050
cleanup_fault "$OUT_DIR/straggler/cleanup"
cp "$OUT_DIR/straggler/cleanup/cleanup-proof.txt" "$OUT_DIR/straggler-cleanup-proof.txt"
check_health A2 a2:50051
check_health router router:50050
run_load straggler-recovery router:50050
python3 lab/validate_artifacts.py --compare \
  "$OUT_DIR/healthy-round_robin" "$OUT_DIR/straggler-faulted" "$OUT_DIR/straggler-recovery"

mkdir -p "$OUT_DIR/cpu-saturation"
mkdir -p "$OUT_DIR/cpu-saturation-proof"
compose exec -T -d a2 stress-ng --cpu 1 --cpu-load 90 --timeout 30s --metrics-brief
STRESS_ACTIVE=true
sleep 2
container_id="$(compose ps -q a2)"
docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "$container_id" \
  >"$OUT_DIR/cpu-saturation-proof/container-stats.csv"
python3 - "$OUT_DIR/cpu-saturation-proof/container-stats.csv" <<'PY'
import sys
from pathlib import Path
row = Path(sys.argv[1]).read_text(encoding="utf-8").strip().splitlines()[-1]
cpu = float(row.split(",")[1].rstrip("%"))
print(f"A2_cpu_percent={cpu}")
if cpu < 70:
    raise SystemExit("A2 did not reach the 70% CPU saturation threshold")
PY
run_load cpu-saturation router:50050 --duration-ms 3000
stop_stress

mkdir -p "$OUT_DIR/pause-resume"
compose pause a2
paused="$(docker inspect --format '{{.State.Paused}}' "$container_id")"
printf '%s\n' "$paused" >"$OUT_DIR/pause-resume/paused.txt"
[[ "$paused" == true ]]
compose unpause a2
resumed="$(docker inspect --format '{{.State.Paused}}' "$container_id")"
printf '%s\n' "$resumed" >"$OUT_DIR/pause-resume/resumed.txt"
[[ "$resumed" == false ]]
check_health A2 a2:50051

mkdir -p "$OUT_DIR/kill-restart"
compose kill -s SIGKILL a2
killed_status="$(docker inspect --format '{{.State.Status}}' "$container_id")"
printf 'after_kill=%s\n' "$killed_status" >"$OUT_DIR/kill-restart/state-proof.txt"
[[ "$killed_status" == exited ]]
compose up -d --wait --wait-timeout 30 a2
restarted_status="$(docker inspect --format '{{.State.Status}}' "$container_id")"
printf 'after_restart=%s\n' "$restarted_status" >>"$OUT_DIR/kill-restart/state-proof.txt"
[[ "$restarted_status" == running ]]
check_health A2 a2:50051
compose down --remove-orphans
LAB_UP=false

mkdir -p "$OUT_DIR/repeated-startup"
for cycle in 1 2 3; do
  export COMPOSE_PROJECT_NAME="${PROJECT}-repeat-${cycle}"
  compose up -d --wait --wait-timeout 60
  compose run --rm --no-deps loadgen --health --target router:50050 \
    >"$OUT_DIR/repeated-startup/cycle-${cycle}-health.txt"
  compose down --remove-orphans
  compose ps -aq >"$OUT_DIR/repeated-startup/cycle-${cycle}-remaining.txt"
  if [[ -s "$OUT_DIR/repeated-startup/cycle-${cycle}-remaining.txt" ]]; then
    echo "containers remained after repeated shutdown $cycle" >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR/interrupted"
export ARTC_RUN_ID="$RUN_ID"
export ARTC_OUT_DIR="$OUT_DIR"
export COMPOSE_PROJECT_NAME="${PROJECT}-interrupt"
bash "$ROOT/lab/run_phase1_gates.sh" --fault-hold-only \
  >"$OUT_DIR/interrupted/child.log" 2>&1 &
probe_pid=$!
for attempt in {1..60}; do
  [[ -f "$OUT_DIR/interrupted/fault-active.marker" ]] && break
  if ! kill -0 "$probe_pid" 2>/dev/null; then break; fi
  sleep 1
done
if [[ ! -f "$OUT_DIR/interrupted/fault-active.marker" ]]; then
  cat "$OUT_DIR/interrupted/child.log" >&2
  echo "interrupted-run probe did not reach its active-fault marker" >&2
  exit 1
fi
kill -TERM "$probe_pid"
set +e
wait "$probe_pid"
interrupted_status=$?
set -e
[[ "$interrupted_status" -eq 143 ]]
grep -q 'cleanup_verified=true' "$OUT_DIR/fault-cleanup-proof.txt"
compose ps -aq >"$OUT_DIR/interrupted/remaining-containers.txt"
[[ ! -s "$OUT_DIR/interrupted/remaining-containers.txt" ]]

printf 'phase1_gates=pass\nrun_id=%s\n' "$RUN_ID" | tee "$OUT_DIR/phase1-gates.txt"
