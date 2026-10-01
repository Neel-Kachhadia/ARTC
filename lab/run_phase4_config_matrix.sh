#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
BIN="${ARTC_LAB_BINARY:-build/debug/artc_lab_node}"
RUN_ID="${ARTC_RUN_ID:-phase4_config_$(date -u +%Y%m%d_%H%M%S)_$$}"
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "invalid ARTC_RUN_ID" >&2
  exit 2
fi
if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN" >&2
  exit 2
fi
OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/config-matrix"
mkdir -p "$OUT_DIR"
BASE_ARGS=(router 127.0.0.1:0 artc_adaptive_no_deadline 1 0.2
  A1=127.0.0.1:50051 A2=127.0.0.1:50052 A3=127.0.0.1:50053)
REJECTS=0

expect_env_reject() {
  local label="$1" pattern="$2"
  shift 2
  local output="$OUT_DIR/$label.log" result=0
  timeout 3s env -i "PATH=$PATH" "$@" "$BIN" "${BASE_ARGS[@]}" \
    >"$output" 2>&1 || result=$?
  if ((result == 0 || result == 124)) || ! grep -Fq "$pattern" "$output"; then
    echo "configuration case failed to reject before serving: $label status=$result" >&2
    cat "$output" >&2
    return 1
  fi
  ((REJECTS += 1))
  printf '%s,%s,%s\n' "$label" "$result" "$pattern" >>"$OUT_DIR/rejections.csv"
}

expect_cli_reject() {
  local label="$1" pattern="$2"
  shift 2
  local output="$OUT_DIR/$label.log" result=0
  timeout 3s env -i "PATH=$PATH" "$BIN" "$@" >"$output" 2>&1 || result=$?
  if ((result == 0 || result == 124)) || ! grep -Fq "$pattern" "$output"; then
    echo "CLI case failed to reject before serving: $label status=$result" >&2
    cat "$output" >&2
    return 1
  fi
  ((REJECTS += 1))
  printf '%s,%s,%s\n' "$label" "$result" "$pattern" >>"$OUT_DIR/rejections.csv"
}

printf 'case,exit_code,expected_message\n' >"$OUT_DIR/rejections.csv"
expect_cli_reject unknown-routing-policy 'unknown routing policy' \
  router 127.0.0.1:0 unknown_policy 1 0.2 \
  A1=127.0.0.1:50051 A2=127.0.0.1:50052 A3=127.0.0.1:50053
expect_cli_reject malformed-seed 'invalid integer argument' \
  router 127.0.0.1:0 artc_adaptive_no_deadline bad-seed 0.2 \
  A1=127.0.0.1:50051 A2=127.0.0.1:50052 A3=127.0.0.1:50053
expect_cli_reject malformed-endpoint 'address must be valid' \
  router 127.0.0.1:0 artc_adaptive_no_deadline 1 0.2 \
  A1=backend-without-port A2=127.0.0.1:50052 A3=127.0.0.1:50053
expect_cli_reject duplicate-replica 'replica ids and addresses must be unique' \
  router 127.0.0.1:0 artc_adaptive_no_deadline 1 0.2 \
  A1=127.0.0.1:50051 A1=127.0.0.1:50052 A3=127.0.0.1:50053
expect_cli_reject duplicate-address 'replica ids and addresses must be unique' \
  router 127.0.0.1:0 artc_adaptive_no_deadline 1 0.2 \
  A1=127.0.0.1:50051 A2=127.0.0.1:50051 A3=127.0.0.1:50053
expect_cli_reject empty-replica-set 'usage:' \
  router 127.0.0.1:0 artc_adaptive_no_deadline 1 0.2

expect_env_reject unknown-field 'unknown ARTC router configuration variable' \
  ARTC_AIMD_BETTA=0.7
expect_env_reject negative-default-deadline 'invalid Phase 2 controller configuration' \
  ARTC_DEFAULT_DEADLINE_MS=-1
expect_env_reject zero-default-deadline 'invalid Phase 2 controller configuration' \
  ARTC_DEFAULT_DEADLINE_MS=0
expect_env_reject huge-default-deadline 'invalid Phase 2 controller configuration' \
  ARTC_DEFAULT_DEADLINE_MS=9223372036854775807
expect_env_reject negative-deadline-margin 'invalid Phase 2 controller configuration' \
  ARTC_DEADLINE_MARGIN_US=-1
expect_env_reject huge-deadline-margin 'invalid Phase 2 controller configuration' \
  ARTC_DEADLINE_MARGIN_US=9223372036854775807
expect_env_reject zero-control-interval 'invalid AIMD control window' \
  ARTC_AIMD_INTERVAL_MS=0
expect_env_reject huge-control-interval 'invalid AIMD control window' \
  ARTC_AIMD_INTERVAL_MS=9223372036854775807
expect_env_reject nan-aimd-beta 'invalid floating-point argument' \
  ARTC_AIMD_BETA=nan
expect_env_reject infinite-aimd-beta 'invalid floating-point argument' \
  ARTC_AIMD_BETA=inf
expect_env_reject invalid-aimd-beta 'invalid AIMD increase or decrease factor' \
  ARTC_AIMD_BETA=1.0
expect_env_reject min-greater-than-max 'invalid admission gate limits' \
  ARTC_AIMD_MIN_LIMIT=65 ARTC_AIMD_MAX_LIMIT=64 ARTC_AIMD_INITIAL_LIMIT=64
expect_env_reject huge-route-limit 'invalid or excessive AIMD concurrency limits' \
  ARTC_AIMD_MAX_LIMIT=65537 ARTC_AIMD_INITIAL_LIMIT=64
expect_env_reject invalid-bool 'must be exactly true or false' \
  ARTC_EXECUTE_HEDGING_ENABLED=yes
expect_env_reject malformed-attempt-seed 'invalid integer argument' \
  ARTC_ATTEMPT_JITTER_SEED=-1
expect_env_reject zero-window-size 'invalid AIMD control window' \
  ARTC_AIMD_MIN_SAMPLES=0
expect_env_reject active-attempts-exceed-total 'invalid Phase 3 attempt limits' \
  ARTC_ATTEMPT_MAX_TOTAL=1 ARTC_ATTEMPT_MAX_ACTIVE=2
expect_env_reject unknown-idempotency 'ARTC_EXECUTE_IDEMPOTENCY must be exactly' \
  ARTC_EXECUTE_IDEMPOTENCY=maybe
expect_env_reject retry-count-over-bound 'max_retries exceeds max_total_attempts' \
  ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS=2 ARTC_EXECUTE_MAX_RETRIES=2
expect_env_reject unknown-status-policy 'unknown gRPC status' \
  ARTC_EXECUTE_RETRYABLE_STATUSES=NOT_A_STATUS
expect_env_reject invalid-hedge-delay-range 'invalid hedge delay range' \
  ARTC_EXECUTE_HEDGE_DELAY_MIN_US=20000 ARTC_EXECUTE_HEDGE_DELAY_MAX_US=10000
expect_env_reject active-attempt-limit 'invalid Phase 3 attempt limits' \
  ARTC_ATTEMPT_MAX_ACTIVE=3
expect_env_reject negative-budget-capacity 'invalid integer argument' \
  ARTC_HEDGE_BUDGET_CAPACITY=-1
expect_env_reject excessive-budget-capacity 'attempt budget capacity exceeds hard limit' \
  ARTC_RETRY_BUDGET_CAPACITY=4294967295
expect_env_reject negative-budget-refill 'attempt budget refill must be finite and non-negative' \
  ARTC_HEDGE_BUDGET_REFILL_PER_SECOND=-1

# Missing optional environment values use validated defaults and start serving.
valid_log="$OUT_DIR/defaults-startup.log"
valid_result=0
timeout 1s env -i "PATH=$PATH" "$BIN" \
  router 127.0.0.1:0 artc_adaptive_no_deadline 1 0.2 \
  A1=localhost:50051 A2=localhost:50052 A3=localhost:50053 \
  >"$valid_log" 2>&1 || valid_result=$?
if ! grep -q '^ARTC_ATTEMPT_SUMMARY ' "$valid_log"; then
  echo "default configuration did not reach router shutdown summary (status=$valid_result)" >&2
  cat "$valid_log" >&2
  exit 1
fi

printf 'phase4_config_matrix=pass\nrejected_cases=%s\ndefault_startup=pass\n' \
  "$REJECTS" >"$OUT_DIR/result.txt"
echo "config_matrix_complete=$OUT_DIR rejected_cases=$REJECTS default_startup=pass"
