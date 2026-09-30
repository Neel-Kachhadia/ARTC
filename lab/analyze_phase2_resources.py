#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
from pathlib import Path


def to_float(value: str, suffix: str = "") -> float:
    return float(value.removesuffix(suffix))


def trend(values: list[float]) -> dict:
    if not values:
        return {"first_median": None, "last_median": None, "change": None}
    width = max(1, len(values) // 10)
    first = statistics.median(values[:width])
    last = statistics.median(values[-width:])
    return {"first_median": first, "last_median": last, "change": last - first}


def read_resources(path: Path) -> dict[str, list[float]]:
    columns = {name: [] for name in (
        "fd_count", "thread_count", "socket_count", "rss_kib",
        "cpu_percent", "docker_rss_mib", "docker_pids")}
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            for name in columns:
                source_name = {"docker_rss_mib": "docker_rss",
                               "docker_pids": "pids"}.get(name, name)
                value = row.get(source_name, "")
                if not value:
                    continue
                try:
                    if name == "cpu_percent":
                        columns[name].append(to_float(value, "%"))
                    elif name == "docker_rss_mib":
                        amount, unit = value[:-3], value[-3:].lower()
                        scale = {"kib": 1 / 1024, "mib": 1, "gib": 1024}.get(unit)
                        if scale is not None:
                            columns[name].append(float(amount) * scale)
                    else:
                        columns[name].append(float(value))
                except ValueError:
                    continue
    return columns


def analyze(root: Path) -> dict:
    samples = read_resources(root / "resources.csv")
    count = len(samples["fd_count"])
    if count < 3:
        raise ValueError(f"only {count} resource samples were captured")
    trends = {name: trend(values) for name, values in samples.items()}
    limits = []
    inflight = []
    drops = []
    replica_counts = set()
    decision_file = root / "run" / "decision-samples.csv"
    with decision_file.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            fields = {}
            for item in row["decision"].split(","):
                key, separator, value = item.partition("=")
                if separator:
                    fields[key] = value
            if "limit" in fields:
                limits.append(int(fields["limit"]))
            if "inflight" in fields:
                inflight.append(int(fields["inflight"]))
            if "observation_drops" in fields:
                drops.append(int(fields["observation_drops"]))
            if "candidates" in fields:
                replica_counts.add(len(fields["candidates"].split(";")))

    growth = {
        "fd_count": trends["fd_count"]["change"] > max(8, trends["fd_count"]["first_median"] * 0.15),
        "thread_count": trends["thread_count"]["change"] > 2,
        "socket_count": trends["socket_count"]["change"] > max(
            8, trends["socket_count"]["first_median"] * 0.15),
        "rss_kib": trends["rss_kib"]["change"] > max(
            131072, trends["rss_kib"]["first_median"] * 0.10),
        "docker_rss_mib": trends["docker_rss_mib"]["change"] > max(
            128, trends["docker_rss_mib"]["first_median"] * 0.10),
    }
    result = {
        "schema": "artc-phase2-resource-analysis-v1",
        "resource_samples": count,
        "trends": trends,
        "possible_monotonic_growth": growth,
        "controller": {
            "decision_samples": len(limits),
            "replica_counts_observed": sorted(replica_counts),
            "route_limit_min": min(limits) if limits else None,
            "route_limit_max": max(limits) if limits else None,
            "route_inflight_max": max(inflight) if inflight else None,
            "observation_drops_max": max(drops) if drops else None,
            "decision_sample_retention_bounded_at_10000": len(limits) <= 10_000,
        },
        "pass": not any(growth.values()) and (not replica_counts or replica_counts == {3}),
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("soak_root", type=Path)
    args = parser.parse_args()
    try:
        result = analyze(args.soak_root)
        output = args.soak_root / "resource-analysis.json"
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2, sort_keys=True))
        print(f"resource_analysis_artifact={output}")
        return 0 if result["pass"] else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"resource analysis failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
