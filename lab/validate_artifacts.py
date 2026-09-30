#!/usr/bin/env python3
import argparse
import csv
import json
import math
from pathlib import Path


def raw_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as source:
        rows = (line for line in source if not line.startswith("#"))
        reader = csv.DictReader(rows)
        if reader.fieldnames != ["value_us", "count"]:
            raise ValueError(f"{path}: unexpected histogram columns")
        return sum(int(row["count"]) for row in reader)


def load_run(path: Path, expect_saturated: bool = False,
             allow_errors: bool = False,
             allow_uninstrumented: bool = False) -> dict:
    manifest_path = path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != "artc-run-v1":
        raise ValueError(f"{path}: unsupported artifact schema")
    for key in ("run_id", "git_revision", "worktree_dirty", "compose_project", "routing_policy"):
        if not manifest.get(key) or manifest[key] == "unknown":
            raise ValueError(f"{path}: missing reproducibility field {key}")
    if manifest.get("issued") != manifest.get("scheduled") or manifest.get("completed") != manifest.get("issued"):
        raise ValueError(f"{path}: scheduled, issued, and completed counts differ")
    if manifest.get("latency_us", {}).get("sample_count") != raw_count(path / "latency.hdr.csv"):
        raise ValueError(f"{path}: latency histogram count differs from manifest")
    if manifest.get("issue_lag_us", {}).get("sample_count") != raw_count(path / "issue-lag.hdr.csv"):
        raise ValueError(f"{path}: issue-lag histogram count differs from manifest")
    if manifest.get("completed") != raw_count(path / "latency.hdr.csv"):
        raise ValueError(f"{path}: completed count differs from latency samples")
    if manifest.get("issued") != raw_count(path / "issue-lag.hdr.csv"):
        raise ValueError(f"{path}: issued count differs from issue-lag samples")
    if expect_saturated:
        if manifest.get("generator_saturated") is not True or manifest.get("valid") is not False:
            raise ValueError(f"{path}: expected an explicitly invalid saturated run")
        if manifest.get("max_issue_lag_us", 0) <= manifest.get("max_issue_lag_limit_us", 0):
            raise ValueError(f"{path}: saturation threshold was not exceeded")
    elif manifest.get("valid") is not True or manifest.get("generator_saturated") is not False:
        raise ValueError(f"{path}: run is invalid or generator-saturated")
    if manifest.get("invariant_violations", 0) != 0:
        raise ValueError(f"{path}: request path invariant violation was observed")
    attempts = manifest.get("backend_attempts")
    issued = manifest.get("issued")
    phase3_fields = (
        "attempt_metadata_version",
        "phase3_attempt_metadata_observed",
        "phase3_admitted_metadata_observed",
        "admitted_without_attempts",
        "primary_attempts",
        "hedge_attempts",
        "retry_attempts",
        "cancelled_attempts",
        "min_admitted_attempts_per_request",
        "max_admitted_attempts_per_request",
        "attempt_amplification_per_admitted",
        "winning_attempt_kinds",
    )
    has_phase3_metadata = any(key in manifest for key in phase3_fields)
    if has_phase3_metadata:
        if manifest.get("attempt_metadata_version") != 3:
            raise ValueError(f"{path}: incomplete or unsupported Phase 3 attempt metadata")
        integer_fields = (
            "backend_attempts",
            "attempt_metadata_observed",
            "phase3_attempt_metadata_observed",
            "phase3_admitted_metadata_observed",
            "admitted_without_attempts",
            "primary_attempts",
            "hedge_attempts",
            "retry_attempts",
            "cancelled_attempts",
            "issued",
            "admitted",
            "rejected",
            "max_admitted_attempts_per_request",
        )
        for key in integer_fields:
            if type(manifest.get(key)) is not int or manifest[key] < 0:
                raise ValueError(f"{path}: Phase 3 field {key} must be a non-negative integer")
        if manifest["attempt_metadata_observed"] != issued:
            raise ValueError(f"{path}: one or more calls lack backend-attempt metadata")
        if manifest["phase3_attempt_metadata_observed"] != manifest["admitted"]:
            raise ValueError(f"{path}: Phase 3 kind metadata does not cover every admitted request")
        if manifest["phase3_admitted_metadata_observed"] != manifest["admitted"]:
            raise ValueError(f"{path}: Phase 3 metadata does not cover every admitted request")
        if manifest["admitted_without_attempts"] > manifest["admitted"]:
            raise ValueError(f"{path}: no-dispatch logical requests exceed admitted requests")
        status_counts = manifest.get("status_code_counts", {})
        if manifest["admitted_without_attempts"] > (
                status_counts.get("1", 0) + status_counts.get("4", 0)):
            raise ValueError(f"{path}: no-dispatch requests need cancellation or deadline status")
        classified_attempts = (manifest["primary_attempts"] +
                               manifest["hedge_attempts"] +
                               manifest["retry_attempts"])
        if classified_attempts != attempts:
            raise ValueError(f"{path}: attempt categories do not sum to backend attempts")
        if manifest["cancelled_attempts"] > attempts:
            raise ValueError(f"{path}: cancellations exceed backend attempts")
        attempt_limit = 3 * manifest["admitted"]
        if attempts > attempt_limit:
            raise ValueError(f"{path}: backend attempts exceed three per admitted request")
        minimum = manifest.get("min_admitted_attempts_per_request")
        maximum = manifest["max_admitted_attempts_per_request"]
        if manifest["admitted"] == 0:
            if minimum is not None or maximum != 0 or attempts != 0:
                raise ValueError(f"{path}: attempt bounds must be empty when no requests were admitted")
            if manifest.get("attempt_amplification_per_admitted") is not None:
                raise ValueError(f"{path}: amplification must be null when no requests were admitted")
        else:
            if type(minimum) is not int or not 0 <= minimum <= maximum <= 3:
                raise ValueError(f"{path}: per-request attempts must be between zero and three")
            if (minimum == 0) != (manifest["admitted_without_attempts"] > 0):
                raise ValueError(f"{path}: zero-attempt admissions disagree with per-request bounds")
            amplification = manifest.get("attempt_amplification_per_admitted")
            expected_amplification = attempts / manifest["admitted"]
            if type(amplification) not in (int, float) or not math.isfinite(amplification) or \
                    not math.isclose(amplification, expected_amplification,
                                     rel_tol=1e-7, abs_tol=1e-9):
                raise ValueError(f"{path}: admitted-request amplification is inconsistent")
        winners = manifest.get("winning_attempt_kinds")
        if not isinstance(winners, dict) or any(
                key not in ("primary", "hedge", "retry") or type(count) is not int or count < 0
                for key, count in winners.items()):
            raise ValueError(f"{path}: winning attempt kinds are malformed")
        if sum(winners.values()) > manifest["admitted"]:
            raise ValueError(f"{path}: winning attempt count exceeds admitted requests")
    elif attempts is not None and issued and attempts / issued > 1.0000001:
        # Preserve the Phase 2 one-primary limit when no Phase 3 metadata is present.
        raise ValueError(f"{path}: backend attempt amplification exceeds one")
    if not expect_saturated and not allow_uninstrumented:
        if manifest.get("attempt_metadata_observed") is not None and \
                manifest["attempt_metadata_observed"] != issued:
            raise ValueError(f"{path}: one or more calls lack backend-attempt metadata")
        if manifest.get("admitted") is not None and manifest.get("rejected") is not None and \
                manifest["admitted"] + manifest["rejected"] != issued:
            raise ValueError(f"{path}: admission accounting does not cover every issued request")
    if allow_errors:
        if manifest.get("measurement_valid", manifest.get("valid")) is not True:
            raise ValueError(f"{path}: measurement integrity failed")
    elif manifest.get("errors", 0) != 0:
        raise ValueError(f"{path}: RPC errors were observed")
    return manifest


def compare(paths: list[Path], min_increase_us: int, max_recovery_delta_us: int) -> None:
    healthy, faulted, recovered = [load_run(path) for path in paths]
    healthy_p99 = healthy["latency_us"].get("p99_us")
    faulted_p99 = faulted["latency_us"].get("p99_us")
    recovered_p99 = recovered["latency_us"].get("p99_us")
    if None in (healthy_p99, faulted_p99, recovered_p99):
        raise ValueError("all compared runs need enough samples for p99")
    increase = faulted_p99 - healthy_p99
    recovery_delta = abs(recovered_p99 - healthy_p99)
    if increase < min_increase_us:
        raise ValueError(f"fault p99 increase {increase}us is below {min_increase_us}us")
    if recovery_delta > max_recovery_delta_us:
        raise ValueError(f"recovery p99 delta {recovery_delta}us exceeds {max_recovery_delta_us}us")
    if recovered.get("responses_by_replica", {}).get("A2", 0) == 0:
        raise ValueError("A2 had no successful responses after recovery")
    print(f"p99_us healthy={healthy_p99} faulted={faulted_p99} recovered={recovered_p99}")
    print(f"fault_increase_us={increase} recovery_delta_us={recovery_delta} A2_reintegrated=true")


def compare_hedge(paths: list[Path]) -> None:
    unhedged, hedged = [load_run(path, allow_errors=True) for path in paths]
    for run in (unhedged, hedged):
        if run.get("attempt_metadata_version") != 3:
            raise ValueError("hedge comparison requires Phase 3 attempt metadata")
    before = unhedged["latency_us"]
    after = hedged["latency_us"]
    if before.get("p99_us") is None or after.get("p99_us") is None:
        raise ValueError("hedge comparison requires enough samples to estimate p99")
    admitted_before = unhedged["admitted"]
    admitted_after = hedged["admitted"]
    hedge_rate = (hedged["hedge_attempts"] / admitted_after
                  if admitted_after else 0.0)
    cancelled_loser_rate = (hedged["cancelled_attempts"] / admitted_after
                            if admitted_after else 0.0)
    summary = paths[1] / "router-summary.txt"
    wasted_time_us = None
    if summary.exists():
        for line in summary.read_text(encoding="utf-8").splitlines():
            if line.startswith("ARTC_ATTEMPT_SUMMARY "):
                values = dict(part.split("=", 1) for part in line.split()[1:]
                              if "=" in part)
                wasted_time_us = int(values["wasted_attempt_time_us"])
                break
    print(f"unhedged_p50_us={before.get('p50_us')} "
          f"unhedged_p95_us={before.get('p95_us')} "
          f"unhedged_p99_us={before['p99_us']} "
          f"unhedged_amplification={unhedged['attempt_amplification_per_admitted']}")
    print(f"hedged_p50_us={after.get('p50_us')} "
          f"hedged_p95_us={after.get('p95_us')} "
          f"hedged_p99_us={after['p99_us']} "
          f"hedge_rate={hedge_rate} "
          f"hedged_amplification={hedged['attempt_amplification_per_admitted']} "
          f"cancelled_loser_rate={cancelled_loser_rate} "
          f"wasted_attempt_time_us={wasted_time_us if wasted_time_us is not None else 'unavailable'} "
          f"process_cpu_percent={hedged.get('process_cpu_percent')} "
          f"p99_change_us={after['p99_us'] - before['p99_us']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect-saturated", action="store_true")
    parser.add_argument("--allow-errors", action="store_true",
                        help="allow measured RPC failures/rejections as experiment outcomes")
    parser.add_argument("--allow-uninstrumented", action="store_true",
                        help="allow a direct backend target without router admission metadata")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--compare-hedge", action="store_true")
    parser.add_argument("--min-p99-increase-us", type=int, default=50_000)
    parser.add_argument("--max-recovery-delta-us", type=int, default=20_000)
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()
    try:
        paths = [Path(value) for value in args.paths]
        if args.compare_hedge:
            if len(paths) != 2 or args.expect_saturated:
                raise ValueError("--compare-hedge requires unhedged and hedged runs")
            compare_hedge(paths)
        elif args.compare:
            if len(paths) != 3 or args.expect_saturated:
                raise ValueError("--compare requires healthy, faulted, and recovery paths")
            compare(paths, args.min_p99_increase_us, args.max_recovery_delta_us)
        else:
            if len(paths) != 1:
                raise ValueError("artifact validation requires one run path")
            manifest = load_run(paths[0], args.expect_saturated, args.allow_errors,
                                args.allow_uninstrumented)
            print(f"artifact_valid=true run={paths[0]} samples={manifest['completed']} "
                  f"admitted={manifest.get('admitted', 'n/a')} "
                  f"rejected={manifest.get('rejected', 'n/a')} "
                  f"deadline_goodput={manifest.get('deadline_goodput', 'n/a')} "
                  f"attempt_amplification={manifest.get('attempt_amplification', 'n/a')} "
                  f"generator_saturated={manifest['generator_saturated']}")
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        parser.exit(1, f"artifact validation failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
