#!/usr/bin/env python3
import argparse
import csv
import json
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
    if attempts is not None and issued and attempts / issued > 1.0000001:
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect-saturated", action="store_true")
    parser.add_argument("--allow-errors", action="store_true",
                        help="allow measured RPC failures/rejections as experiment outcomes")
    parser.add_argument("--allow-uninstrumented", action="store_true",
                        help="allow a direct backend target without router admission metadata")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--min-p99-increase-us", type=int, default=50_000)
    parser.add_argument("--max-recovery-delta-us", type=int, default=20_000)
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()
    try:
        paths = [Path(value) for value in args.paths]
        if args.compare:
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
