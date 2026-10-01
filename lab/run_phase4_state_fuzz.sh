#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${ARTC_RUN_ID:-phase4_fuzz_$(date -u +%Y%m%d_%H%M%S)_$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
TEST_BIN="${ARTC_TEST_BINARY:-build/debug/artc_integration_tests}"
SEED_LIST="${ARTC_FUZZ_SEEDS:-16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47}"
MAX_DURATION_SEC="${ARTC_FUZZ_MAX_DURATION_SEC:-300}"
PER_SEED_TIMEOUT_SEC="${ARTC_FUZZ_PER_SEED_TIMEOUT_SEC:-30}"
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/state-fuzz"
mkdir -p "$OUT_DIR"

if [[ ! -x "$TEST_BIN" ]]; then
  echo "missing executable state-machine test: $TEST_BIN" >&2
  exit 2
fi
if [[ ! "$MAX_DURATION_SEC" =~ ^[1-9][0-9]*$ ]] || ((MAX_DURATION_SEC > 1800)); then
  echo "ARTC_FUZZ_MAX_DURATION_SEC must be in [1,1800]" >&2
  exit 2
fi
if [[ ! "$PER_SEED_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] || ((PER_SEED_TIMEOUT_SEC > 120)); then
  echo "ARTC_FUZZ_PER_SEED_TIMEOUT_SEC must be in [1,120]" >&2
  exit 2
fi
read -r -a SEEDS <<<"$SEED_LIST"
if ((${#SEEDS[@]} == 0 || ${#SEEDS[@]} > 32)); then
  echo "ARTC_FUZZ_SEEDS must contain 1 to 32 seeds" >&2
  exit 2
fi
for seed in "${SEEDS[@]}"; do
  if [[ ! "$seed" =~ ^[0-9]+$ ]]; then
    echo "invalid fuzz seed: $seed" >&2
    exit 2
  fi
done

GIT_REVISION="$(git rev-parse HEAD)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
FAILING_SEED=""
FAILING_REASON=""
python3 - "$OUT_DIR/campaign.json" "$RUN_ID" "$GIT_REVISION" \
  "$STARTED_AT" "$MAX_DURATION_SEC" "${SEEDS[@]}" <<'PY'
import json
import sys
from pathlib import Path

path, run_id, revision, started, duration, *seeds = sys.argv[1:]
Path(path).write_text(json.dumps({
    "run_id": run_id,
    "git_revision": revision,
    "started_at_utc": started,
    "duration_limit_seconds": int(duration),
    "seeds": [int(seed) for seed in seeds],
    "test": "AttemptManagerIntegrationTest.SeededManagerEventSequencesCheckBoundsAfterEveryEvent",
    "status": "running",
    "completed_seeds": [],
}, indent=2) + "\n", encoding="utf-8")
PY
finalize_campaign() {
  local result=$?
  trap - EXIT
  python3 - "$OUT_DIR/campaign.json" "$result" "$FAILING_SEED" "$FAILING_REASON" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path, exit_status, failing_seed, reason = sys.argv[1:]
document = json.loads(Path(path).read_text(encoding="utf-8"))
if document["status"] == "running":
    document["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    document["exit_status"] = int(exit_status)
    document["status"] = "pass" if exit_status == "0" else "fail" if failing_seed else "incomplete"
    if failing_seed:
        document["failing_seed"] = int(failing_seed)
    if reason:
        document["failure_reason"] = reason
    Path(path).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PY
  exit "$result"
}
trap finalize_campaign EXIT
printf 'seed,status,elapsed_ms,log\n' >"$OUT_DIR/seeds.csv"

started_epoch="$(date +%s)"
deadline_epoch=$((started_epoch + MAX_DURATION_SEC))
for seed in "${SEEDS[@]}"; do
  now="$(date +%s)"
  if ((now >= deadline_epoch)); then
    FAILING_REASON="duration limit reached before seed=$seed"
    echo "state fuzz duration bound reached before seed=$seed" >&2
    exit 3
  fi
  log="$OUT_DIR/seed-$seed.log"
  seed_started="$(date +%s%3N)"
  if timeout "${PER_SEED_TIMEOUT_SEC}s" env ARTC_ATTEMPT_EVENT_SEED="$seed" \
      "$TEST_BIN" \
      --gtest_filter=AttemptManagerIntegrationTest.SeededManagerEventSequencesCheckBoundsAfterEveryEvent \
      >"$log" 2>&1; then
    status=pass
  else
    status=fail
  fi
  seed_finished="$(date +%s%3N)"
  elapsed_ms=$((seed_finished - seed_started))
  printf '%s,%s,%s,%s\n' "$seed" "$status" "$elapsed_ms" "seed-$seed.log" \
    >>"$OUT_DIR/seeds.csv"
  if [[ "$status" != pass ]]; then
    tail -80 "$log" >&2
    FAILING_SEED="$seed"
    FAILING_REASON="seed test exited unsuccessfully; see seed-$seed.log"
    echo "state fuzz failed; reproducer seed=$seed log=$log" >&2
    exit 1
  fi
  python3 - "$OUT_DIR/campaign.json" "$seed" <<'PY'
import json
import sys
from pathlib import Path

path, seed = sys.argv[1], int(sys.argv[2])
document = json.loads(Path(path).read_text(encoding="utf-8"))
document["completed_seeds"].append(seed)
Path(path).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PY
done

finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
python3 - "$OUT_DIR/campaign.json" "$finished_at" <<'PY'
import json
import sys
from pathlib import Path

path, finished = sys.argv[1:]
document = json.loads(Path(path).read_text(encoding="utf-8"))
document["finished_at_utc"] = finished
document["status"] = "pass"
Path(path).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PY
echo "state_fuzz_complete=$OUT_DIR seeds=${#SEEDS[@]} status=pass"
