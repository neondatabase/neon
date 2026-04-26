#!/usr/bin/env python3
"""Aggregate results.csv into summary.csv with mean/std and bottleneck notes.

Reads:
  benchmarks/results.csv         per-rep rows
  benchmarks/auth_*.csv          per-cell auth-Postgres watcher samples (optional)

Writes:
  benchmarks/summary.csv         one row per (config, workload, concurrency)
"""

from __future__ import annotations

import csv
import glob
import os
import statistics
import sys
from collections import defaultdict

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(BENCH_DIR, "results.csv")
SUMMARY = os.path.join(BENCH_DIR, "summary.csv")


def to_float(s: str) -> float | None:
    try:
        return float(s) if s else None
    except ValueError:
        return None


def load_results() -> dict[tuple[str, str, int], list[dict]]:
    grouped: dict[tuple[str, str, int], list[dict]] = defaultdict(list)
    with open(RESULTS) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["config"], row["workload"], int(row["concurrency"]))
            grouped[key].append(row)
    return grouped


def auth_note(cfg: str, wl: str, c: int) -> str:
    """If any auth-watcher sample saw active count >= half of concurrency,
    flag a possible auth-Postgres bottleneck."""
    if wl != "readonly_short" or cfg == "direct" or c < 25:
        return ""
    pattern = os.path.join(BENCH_DIR, f"auth_{cfg}_{wl}_c{c}_r*.csv")
    files = glob.glob(pattern)
    if not files:
        return ""
    max_active = 0
    for path in files:
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                a = to_float(row.get("active", ""))
                if a is not None:
                    max_active = max(max_active, int(a))
    if max_active >= max(2, c // 2):
        return f"auth-pg saw {max_active} active conns; likely bottleneck"
    if max_active >= 2:
        return f"auth-pg peak active={max_active}"
    return ""


def main() -> None:
    if not os.path.exists(RESULTS):
        sys.exit(f"missing {RESULTS}")

    grouped = load_results()

    out_rows = []
    for (cfg, wl, c), rows in sorted(grouped.items()):
        tps = [to_float(r["tps"]) for r in rows]
        tps = [x for x in tps if x is not None]
        p50 = [to_float(r["p50_ms"]) for r in rows]
        p95 = [to_float(r["p95_ms"]) for r in rows]
        p99 = [to_float(r["p99_ms"]) for r in rows]
        p50 = [x for x in p50 if x is not None]
        p95 = [x for x in p95 if x is not None]
        p99 = [x for x in p99 if x is not None]

        out_rows.append({
            "config": cfg,
            "workload": wl,
            "concurrency": c,
            "tps_mean": round(statistics.mean(tps), 2) if tps else "",
            "tps_std": round(statistics.pstdev(tps), 2) if len(tps) > 1 else 0.0,
            "p50_ms": round(statistics.mean(p50), 3) if p50 else "",
            "p95_ms": round(statistics.mean(p95), 3) if p95 else "",
            "p99_ms": round(statistics.mean(p99), 3) if p99 else "",
            "notes": auth_note(cfg, wl, c),
        })

    with open(SUMMARY, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "config",
                "workload",
                "concurrency",
                "tps_mean",
                "tps_std",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"wrote {SUMMARY} ({len(out_rows)} rows)")


if __name__ == "__main__":
    main()
