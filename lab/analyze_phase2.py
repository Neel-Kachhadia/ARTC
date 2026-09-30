#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
from pathlib import Path


def load_run(root: Path, name: str) -> tuple[dict, list[dict]]:
    directory = root / name
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema") != "artc-run-v1" or manifest.get("measurement_valid") is not True:
        raise ValueError(f"{name}: missing valid run manifest")
    decisions = []
    with (directory / "decision-samples.csv").open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            fields = {}
            for item in row["decision"].split(","):
                key, separator, value = item.partition("=")
                if separator:
                    fields[key] = value
            elapsed_us = int(row["elapsed_us"])
            fields["elapsed_us"] = elapsed_us
            fields["wall_ms"] = manifest["started_unix_ms"] + elapsed_us // 1000
            for key in ("limit", "inflight", "selected", "v"):
                if key in fields:
                    fields[key] = int(fields[key])
            if "candidates" in fields:
                candidates = {}
                for candidate in fields["candidates"].split(";"):
                    parts = candidate.split(":")
                    if len(parts) >= 5:
                        candidates[int(parts[0])] = {
                            "health": parts[1],
                            "inflight": int(parts[2]),
                            "latency_ewma_us": float(parts[3]),
                            "error_ewma": float(parts[4]),
                        }
                fields["replicas"] = candidates
            decisions.append(fields)
    decisions.sort(key=lambda sample: sample["wall_ms"])
    return manifest, decisions


def run_summary(manifest: dict, decisions: list[dict]) -> dict:
    latency = manifest.get("latency_us", {})
    return {
        "offered_rps": manifest.get("offered_rps"),
        "admitted_rps": manifest.get("admitted_rps"),
        "completed_rps": manifest.get("completed_rps"),
        "deadline_goodput_rps": manifest.get("deadline_goodput_rps"),
        "rejection_fraction": (
            manifest.get("rejected", 0) / manifest.get("issued", 1)
            if manifest.get("issued", 0) else 0.0),
        "admitted": manifest.get("admitted"),
        "rejected": manifest.get("rejected"),
        "admission_results": manifest.get("admission_results", {}),
        "status_code_counts": manifest.get("status_code_counts", {}),
        "deadline_misses": manifest.get("deadline_misses"),
        "errors": manifest.get("errors"),
        "backend_attempts": manifest.get("backend_attempts"),
        "attempt_metadata_observed": manifest.get("attempt_metadata_observed"),
        "attempt_amplification": (
            manifest.get("backend_attempts", 0) / manifest.get("admitted", 0)
            if manifest.get("admitted", 0) else None),
        "attempts_per_admitted_request": (
            manifest.get("backend_attempts", 0) / manifest.get("admitted", 0)
            if manifest.get("admitted", 0) else None),
        "backend_attempts_per_offered_request": (
            manifest.get("backend_attempts", 0) / manifest.get("issued", 0)
            if manifest.get("issued", 0) else None),
        "controller_parameters": manifest.get("controller_parameters", {}),
        "p50_us": latency.get("p50_us"),
        "p95_us": latency.get("p95_us"),
        "p99_us": latency.get("p99_us"),
        "route_limit_first": decisions[0].get("limit") if decisions else None,
        "route_limit_last": decisions[-1].get("limit") if decisions else None,
        "decision_samples": len(decisions),
    }


def read_resource_summary(path: Path) -> dict:
    cpus = []
    rss_mib = []
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.reader(source):
            if len(row) < 4:
                continue
            try:
                cpus.append(float(row[1].rstrip("%")))
            except ValueError:
                continue
            memory = row[2].split(" / ", 1)[0]
            amount, unit = memory[:-3], memory[-3:].lower()
            try:
                value = float(amount)
            except ValueError:
                continue
            scale = {"b": 1 / (1024 * 1024), "kib": 1 / 1024,
                     "mib": 1, "gib": 1024}.get(unit)
            if scale is not None:
                rss_mib.append(value * scale)
    return {
        "samples": len(cpus),
        "max_cpu_percent": max(cpus) if cpus else None,
        "max_rss_mib": max(rss_mib) if rss_mib else None,
    }


def median_limit(samples: list[dict]) -> float:
    values = [sample["limit"] for sample in samples if "limit" in sample]
    if not values:
        raise ValueError("controller limit samples are missing")
    return statistics.median(values)


def settle_time(samples: list[dict], target: float, start_ms: int,
                end_ms: int | None = None,
                tolerance_fraction: float = 0.10) -> float | None:
    values = [sample for sample in samples if "limit" in sample and
              sample["wall_ms"] >= start_ms and
              (end_ms is None or sample["wall_ms"] < end_ms)]
    lower = target * (1.0 - tolerance_fraction)
    upper = target * (1.0 + tolerance_fraction)
    for index, sample in enumerate(values):
        tail = values[index:]
        if tail and all(lower <= item["limit"] <= upper for item in tail):
            return max(0.0, (sample["wall_ms"] - start_ms) / 1000.0)
    return None


def direction_changes(samples: list[dict]) -> int:
    limits = []
    for sample in samples:
        if "limit" in sample and (not limits or limits[-1] != sample["limit"]):
            limits.append(sample["limit"])
    signs = []
    for previous, current in zip(limits, limits[1:]):
        sign = (current > previous) - (current < previous)
        if sign and (not signs or signs[-1] != sign):
            signs.append(sign)
    return max(0, len(signs) - 1)


def a2_selected_share(decisions: list[dict]) -> float | None:
    selected = [sample["selected"] for sample in decisions if "selected" in sample]
    return selected.count(1) / len(selected) if selected else None


def a2_reduction_time(decisions: list[dict], baseline_share: float) -> float | None:
    per_second = {}
    for sample in decisions:
        if "selected" not in sample:
            continue
        second = sample["elapsed_us"] // 1_000_000
        selected, total = per_second.get(second, (0, 0))
        per_second[second] = (selected + int(sample["selected"] == 1), total + 1)
    threshold = baseline_share / 2.0
    seconds = sorted(per_second)
    for index, second in enumerate(seconds[:-1]):
        shares = [per_second[value][0] / per_second[value][1]
                  for value in seconds[index:index + 2]]
        if len(shares) == 2 and all(share <= threshold for share in shares):
            return float(second)
    return None


def replica_health(decision: dict, index: int = 1) -> str | None:
    return decision.get("replicas", {}).get(index, {}).get("health")


def analyze(root: Path) -> dict:
    names = [
        "stability-00-30-normal", "stability-30-60-step-up",
        "stability-60-90-sustained", "stability-90-120-recovery",
        "recovery-00-30-healthy", "recovery-30-60-a2-unavailable",
        "recovery-60-120-a2-reintegrated", "deadline-feasibility-off",
        "deadline-feasibility-on", "overhead-direct-a1", "overhead-round-robin",
        "overhead-phase1-least-inflight", "overhead-phase2-adaptive",
    ]
    data = {name: load_run(root, name) for name in names}
    stability = []
    for name in names[:4]:
        manifest, decisions = data[name]
        stability.extend(decisions)
    steady_target = median_limit(data["stability-60-90-sustained"][1][-max(
        1, len(data["stability-60-90-sustained"][1]) // 3):])
    normal_target = median_limit(data["stability-00-30-normal"][1][-max(
        1, len(data["stability-00-30-normal"][1]) // 3):])
    high_start = data["stability-30-60-step-up"][0]["started_unix_ms"]
    normal_start = data["stability-90-120-recovery"][0]["started_unix_ms"]
    high_limits = [sample["limit"] for sample in stability
                   if "limit" in sample and high_start <= sample["wall_ms"] < normal_start]
    # This step raises load, so AIMD lowers its limit. Count undershoot below
    # the settled high-load limit; the pre-step limit is initial condition.
    undershoot = max(0.0, steady_target - min(high_limits)) if high_limits else None
    stability_result = {
        "normal_limit_median": normal_target,
        "high_steady_limit_median": steady_target,
        "initial_limit_gap": max(0.0, normal_target - steady_target),
        "step_undershoot_limit": undershoot,
        "step_undershoot_fraction": undershoot / steady_target if undershoot is not None and steady_target else None,
        "step_settling_time_s": settle_time(
            stability, steady_target, high_start, normal_start),
        "normal_recovery_time_s": settle_time(stability, normal_target, normal_start),
        "limit_direction_changes": direction_changes(stability),
        "limit_direction_changes_by_phase": {
            name: direction_changes(data[name][1]) for name in names[:4]
        },
        "phases": {name: run_summary(*data[name]) for name in names[:4]},
    }

    fault_manifest, fault_decisions = data["recovery-30-60-a2-unavailable"]
    recovered_manifest, recovered_decisions = data["recovery-60-120-a2-reintegrated"]
    fault_start = fault_manifest["started_unix_ms"]
    recovered_start = recovered_manifest["started_unix_ms"]
    degraded = next((sample for sample in fault_decisions
                     if replica_health(sample) in {"degraded", "unavailable"}), None)
    reintegrated = next((sample for sample in recovered_decisions
                         if replica_health(sample) == "healthy"), None)
    healthy_share = a2_selected_share(data["recovery-00-30-healthy"][1])
    recovery_result = {
        "healthy_a2_response_share": (
            data["recovery-00-30-healthy"][0].get("responses_by_replica", {}).get("A2", 0)
            / max(1, data["recovery-00-30-healthy"][0].get("successful", 0))),
        "fault_a2_sampled_route_share": a2_selected_share(fault_decisions),
        "time_to_reduce_a2_traffic_s": (
            a2_reduction_time(fault_decisions, healthy_share)
            if healthy_share is not None else None),
        "recovered_a2_response_share": (
            recovered_manifest.get("responses_by_replica", {}).get("A2", 0)
            / max(1, recovered_manifest.get("successful", 0))),
        "health_degradation_detection_s": (
            (degraded["wall_ms"] - fault_start) / 1000 if degraded else None),
        "healthy_reintegration_detection_s": (
            (reintegrated["wall_ms"] - recovered_start) / 1000 if reintegrated else None),
        "fault_run": run_summary(fault_manifest, fault_decisions),
        "recovery_run": run_summary(recovered_manifest, recovered_decisions),
    }

    deadline_off = data["deadline-feasibility-off"][0]
    deadline_on = data["deadline-feasibility-on"][0]
    deadline_result = {
        "off": run_summary(deadline_off, data["deadline-feasibility-off"][1]),
        "on": run_summary(deadline_on, data["deadline-feasibility-on"][1]),
        "deadline_infeasible_observed_on": int(
            deadline_on.get("admission_results", {}).get(
                "REJECT_DEADLINE_INFEASIBLE", 0)),
    }

    ablations = {}
    for policy in ("round_robin", "least_inflight", "ewma_latency", "p2c_latency_inflight",
                   "artc_selector_only", "adaptive_concurrency_only",
                   "artc_adaptive_no_deadline", "artc_adaptive"):
        run = load_run(root, f"ablation-{policy}")
        ablations[policy] = run_summary(*run)

    overhead = {}
    for name in names[9:]:
        manifest, decisions = data[name]
        overhead[name.removeprefix("overhead-")] = {
            **run_summary(manifest, decisions),
            "target_container_resources": read_resource_summary(
                root / f"{name}.router-resources.csv"),
        }
    microbench = json.loads((root / "control-microbench" / "manifest.json").read_text(encoding="utf-8"))
    return {
        "schema": "artc-phase2-analysis-v1",
        "stability": stability_result,
        "replica_recovery": recovery_result,
        "deadline_ablation": deadline_result,
        "ablation_subset": ablations,
        "healthy_overhead": overhead,
        "control_microbenchmark": microbench,
        "required_scenarios": {
            name: run_summary(*load_run(root, name)) for name in (
                "A-healthy", "B-a2-straggler", "C-a2-cpu-saturation",
                "D-a2-down", "E-a2-recovery", "F-downstream-b-slow",
                "G-one-to-five-burst", "H-global-overload",
                "I-all-replicas-slow", "J-load-normal-again")
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("experiment_root", type=Path)
    args = parser.parse_args()
    try:
        result = analyze(args.experiment_root)
        output = args.experiment_root / "analysis.json"
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2, sort_keys=True))
        print(f"analysis_artifact={output}")
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as error:
        parser.exit(1, f"phase2 analysis failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
