#!/usr/bin/env python3
"""Benchmark proxy TCP pooling configurations with pgbench.

The harness is intentionally deployment-agnostic: pass each PostgreSQL target
URL explicitly, and optionally pass local PIDs or Docker containers to sample
CPU/memory while pgbench runs.
"""

from __future__ import annotations

import argparse
import csv
import glob
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


WORKLOADS = {
    "readonly_steady": ["-S"],
    "readonly_connect": ["-S", "-C"],
    "tpcb_steady": [],
}

RUN_FIELDS = [
    "target",
    "workload",
    "concurrency",
    "rep",
    "duration_s",
    "exit_code",
    "transactions",
    "failed_transactions",
    "tps",
    "client_handshakes_per_sec",
    "latency_avg_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "initial_or_avg_connection_ms",
    "backend_before_total",
    "backend_during_max_total",
    "backend_after_final_total",
    "pgbench_stdout",
    "pgbench_stderr",
]

BACKEND_FIELDS = [
    "target",
    "workload",
    "concurrency",
    "rep",
    "phase",
    "ts",
    "total",
    "active",
    "idle",
    "idle_in_transaction",
    "other",
    "error",
]

RESOURCE_FIELDS = [
    "target",
    "workload",
    "concurrency",
    "rep",
    "resource",
    "kind",
    "ts",
    "cpu_pct",
    "rss_mib",
    "vsz_mib",
    "mem_mib",
    "raw",
    "error",
]


@dataclass(frozen=True)
class Target:
    name: str
    url: str


@dataclass(frozen=True)
class Resource:
    name: str
    kind: str
    ident: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_csv_ints(raw: str) -> list[int]:
    if raw.strip() == "":
        return []
    values = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            values.append(int(part))
    if not values:
        raise argparse.ArgumentTypeError("expected at least one integer")
    return values


def parse_target(raw: str) -> Target:
    name, sep, url = raw.partition("=")
    if not sep or not name or not url:
        raise argparse.ArgumentTypeError("--target must be NAME=POSTGRES_URL")
    return Target(name=name, url=url)


def parse_resource(raw: str) -> Resource:
    name, sep, spec = raw.partition("=")
    kind, sep2, ident = spec.partition(":")
    if not sep or not sep2 or not name or not kind or not ident:
        raise argparse.ArgumentTypeError("--resource must be NAME=pid:PID or NAME=docker:CONTAINER")
    if kind not in {"pid", "docker"}:
        raise argparse.ArgumentTypeError("resource kind must be pid or docker")
    return Resource(name=name, kind=kind, ident=ident)


def open_csv(path: Path, fields: list[str]):
    exists = path.exists() and path.stat().st_size > 0
    f = path.open("a", newline="")
    writer = csv.DictWriter(f, fieldnames=fields)
    if not exists:
        writer.writeheader()
        f.flush()
    return f, writer


def run_cmd(args: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, env=env)


def backend_counts(backend_url: str) -> dict[str, str]:
    sql = """
        select coalesce(state, 'other') as state, count(*)
        from pg_stat_activity
        where datname = current_database()
          and pid <> pg_backend_pid()
        group by 1
    """
    env = os.environ.copy()
    env["PGAPPNAME"] = "tcp_pool_bench_backend_sampler"
    proc = run_cmd(["psql", "-X", "-q", "-At", "-F", ",", "-c", sql, backend_url], env=env)
    row = {
        "total": "0",
        "active": "0",
        "idle": "0",
        "idle_in_transaction": "0",
        "other": "0",
        "error": "",
    }
    if proc.returncode != 0:
        row["error"] = (proc.stderr or proc.stdout).strip().replace("\n", " ")[:500]
        return row

    total = 0
    other = 0
    for line in proc.stdout.splitlines():
        state, _, count_raw = line.partition(",")
        try:
            count = int(count_raw)
        except ValueError:
            continue
        total += count
        key = state.strip().replace(" ", "_")
        if key in {"active", "idle", "idle_in_transaction"}:
            row[key] = str(int(row[key]) + count)
        else:
            other += count
    row["total"] = str(total)
    row["other"] = str(other)
    return row


def parse_mib(raw: str) -> float | None:
    match = re.match(r"\s*([0-9.]+)\s*([KMGT]?i?B)", raw)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    factor = {
        "B": 1 / 1024 / 1024,
        "KB": 1 / 1024,
        "KiB": 1 / 1024,
        "MB": 1,
        "MiB": 1,
        "GB": 1024,
        "GiB": 1024,
        "TB": 1024 * 1024,
        "TiB": 1024 * 1024,
    }.get(unit)
    return value * factor if factor is not None else None


def resource_sample(resource: Resource) -> dict[str, str]:
    row = {
        "cpu_pct": "",
        "rss_mib": "",
        "vsz_mib": "",
        "mem_mib": "",
        "raw": "",
        "error": "",
    }
    if resource.kind == "pid":
        proc = run_cmd(["ps", "-p", resource.ident, "-o", "pcpu=", "-o", "rss=", "-o", "vsz="])
        if proc.returncode != 0 or not proc.stdout.strip():
            row["error"] = (proc.stderr or proc.stdout).strip().replace("\n", " ")[:500]
            return row
        parts = proc.stdout.split()
        row["raw"] = " ".join(parts)
        if len(parts) >= 3:
            row["cpu_pct"] = parts[0]
            row["rss_mib"] = f"{int(parts[1]) / 1024:.3f}"
            row["vsz_mib"] = f"{int(parts[2]) / 1024:.3f}"
        return row

    proc = run_cmd([
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{.CPUPerc}},{{.MemUsage}}",
        resource.ident,
    ])
    if proc.returncode != 0:
        row["error"] = (proc.stderr or proc.stdout).strip().replace("\n", " ")[:500]
        return row
    raw = proc.stdout.strip()
    row["raw"] = raw
    cpu_raw, _, mem_raw = raw.partition(",")
    row["cpu_pct"] = cpu_raw.strip().rstrip("%")
    mem_mib = parse_mib(mem_raw.split("/")[0])
    if mem_mib is not None:
        row["mem_mib"] = f"{mem_mib:.3f}"
    return row


class Sampler:
    def __init__(
        self,
        *,
        target: str,
        workload: str,
        concurrency: int,
        rep: int,
        backend_url: str | None,
        resources: list[Resource],
        interval_s: float,
        backend_writer: csv.DictWriter,
        resource_writer: csv.DictWriter,
        files: list,
    ) -> None:
        self.target = target
        self.workload = workload
        self.concurrency = concurrency
        self.rep = rep
        self.backend_url = backend_url
        self.resources = resources
        self.interval_s = interval_s
        self.backend_writer = backend_writer
        self.resource_writer = resource_writer
        self.files = files
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.backend_samples: list[dict[str, str]] = []

    def write_backend(self, phase: str) -> dict[str, str] | None:
        if not self.backend_url:
            return None
        counts = backend_counts(self.backend_url)
        row = {
            "target": self.target,
            "workload": self.workload,
            "concurrency": self.concurrency,
            "rep": self.rep,
            "phase": phase,
            "ts": utc_now(),
            **counts,
        }
        self.backend_writer.writerow(row)
        self.backend_samples.append(row)
        for f in self.files:
            f.flush()
        return row

    def write_resources(self) -> None:
        for resource in self.resources:
            sample = resource_sample(resource)
            row = {
                "target": self.target,
                "workload": self.workload,
                "concurrency": self.concurrency,
                "rep": self.rep,
                "resource": resource.name,
                "kind": resource.kind,
                "ts": utc_now(),
                **sample,
            }
            self.resource_writer.writerow(row)
        for f in self.files:
            f.flush()

    def start(self) -> None:
        self.write_backend("before")

        def loop() -> None:
            while not self.stop_event.wait(self.interval_s):
                self.write_backend("during")
                self.write_resources()

        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join()

    def after_idle(self, waits: list[int]) -> None:
        elapsed = 0
        for wait_s in waits:
            sleep_s = max(0, wait_s - elapsed)
            if sleep_s:
                time.sleep(sleep_s)
            elapsed = wait_s
            self.write_backend(f"after_{wait_s}s")
            self.write_resources()

    def backend_before_total(self) -> str:
        for sample in self.backend_samples:
            if sample["phase"] == "before":
                return sample["total"]
        return ""

    def backend_during_max_total(self) -> str:
        values = [int(s["total"]) for s in self.backend_samples if s["phase"] == "during" and s["total"]]
        return str(max(values)) if values else ""

    def backend_after_final_total(self) -> str:
        after = [s for s in self.backend_samples if s["phase"].startswith("after_")]
        return after[-1]["total"] if after else ""


def parse_pgbench_stdout(stdout: str) -> dict[str, str]:
    patterns = {
        "transactions": r"number of transactions actually processed:\s+([0-9]+)",
        "failed_transactions": r"number of failed transactions:\s+([0-9]+)",
        "latency_avg_ms": r"latency average =\s+([0-9.]+)\s+ms",
        "initial_or_avg_connection_ms": (
            r"(?:initial connection time|average connection time) =\s+([0-9.]+)\s+ms"
        ),
        "tps": r"tps =\s+([0-9.]+)",
    }
    parsed = {key: "" for key in patterns}
    for key, pattern in patterns.items():
        match = re.search(pattern, stdout)
        if match:
            parsed[key] = match.group(1)
    return parsed


def percentile(values: list[int], pct: float) -> float:
    if not values:
        return math.nan
    values.sort()
    idx = max(0, min(len(values) - 1, math.ceil((pct / 100) * len(values)) - 1))
    return values[idx] / 1000


def parse_latency_logs(prefix: Path) -> dict[str, str]:
    lat_us: list[int] = []
    for path in glob.glob(str(prefix) + ".*"):
        with open(path) as f:
            for line in f:
                parts = line.split()
                if len(parts) < 3:
                    continue
                try:
                    lat_us.append(int(parts[2]))
                except ValueError:
                    continue
    if not lat_us:
        return {"p50_ms": "", "p95_ms": "", "p99_ms": "", "max_ms": ""}
    return {
        "p50_ms": f"{percentile(lat_us, 50):.3f}",
        "p95_ms": f"{percentile(lat_us, 95):.3f}",
        "p99_ms": f"{percentile(lat_us, 99):.3f}",
        "max_ms": f"{max(lat_us) / 1000:.3f}",
    }


def remove_latency_logs(prefix: Path) -> None:
    for path in glob.glob(str(prefix) + ".*"):
        try:
            os.remove(path)
        except OSError:
            pass


def run_pgbench(
    *,
    target: Target,
    workload: str,
    concurrency: int,
    rep: int,
    duration_s: int,
    max_jobs: int,
    out_dir: Path,
    keep_logs: bool,
) -> tuple[int, dict[str, str], str, str]:
    run_name = f"{target.name}_{workload}_c{concurrency}_r{rep}"
    prefix = out_dir / "pgbench_logs" / run_name
    prefix.parent.mkdir(parents=True, exist_ok=True)
    remove_latency_logs(prefix)

    flags = WORKLOADS[workload]
    args = [
        "pgbench",
        target.url,
        "-n",
        *flags,
        "-c",
        str(concurrency),
        "-j",
        str(max(1, min(concurrency, max_jobs))),
        "-T",
        str(duration_s),
        "-l",
        "--log-prefix",
        str(prefix),
    ]
    proc = run_cmd(args)
    parsed = parse_pgbench_stdout(proc.stdout)
    parsed.update(parse_latency_logs(prefix))
    if workload.endswith("_connect"):
        parsed["client_handshakes_per_sec"] = parsed.get("tps", "")
    else:
        parsed["client_handshakes_per_sec"] = ""

    stdout_path = out_dir / "pgbench_stdout" / f"{run_name}.out"
    stderr_path = out_dir / "pgbench_stdout" / f"{run_name}.err"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path.write_text(proc.stdout)
    stderr_path.write_text(proc.stderr)

    if not keep_logs:
        remove_latency_logs(prefix)

    return proc.returncode, parsed, str(stdout_path), str(stderr_path)


def to_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def mean_or_blank(values: list[float]) -> str:
    return f"{statistics.mean(values):.3f}" if values else ""


def pstdev_or_blank(values: list[float]) -> str:
    return f"{statistics.pstdev(values):.3f}" if len(values) > 1 else ("0.000" if values else "")


def write_run_summary(out_dir: Path) -> None:
    runs_path = out_dir / "runs.csv"
    if not runs_path.exists():
        return

    backend_max: dict[tuple[str, str, int, int], int] = defaultdict(int)
    backend_final: dict[tuple[str, str, int, int], int] = {}
    backend_path = out_dir / "backend_connections.csv"
    if backend_path.exists():
        with backend_path.open() as f:
            for row in csv.DictReader(f):
                key = (row["target"], row["workload"], int(row["concurrency"]), int(row["rep"]))
                total = int(row["total"] or 0)
                if row["phase"] == "during":
                    backend_max[key] = max(backend_max[key], total)
                if row["phase"].startswith("after_"):
                    backend_final[key] = total

    grouped: dict[tuple[str, str, int], list[dict[str, str]]] = defaultdict(list)
    with runs_path.open() as f:
        for row in csv.DictReader(f):
            grouped[(row["target"], row["workload"], int(row["concurrency"]))].append(row)

    fields = [
        "target",
        "workload",
        "concurrency",
        "runs",
        "failed_runs",
        "tps_mean",
        "tps_std",
        "client_handshakes_per_sec_mean",
        "latency_avg_ms_mean",
        "p50_ms_mean",
        "p95_ms_mean",
        "p99_ms_mean",
        "backend_during_max_total_mean",
        "backend_after_final_total_mean",
    ]
    with (out_dir / "summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for (target, workload, concurrency), rows in sorted(grouped.items()):
            key_prefix = (target, workload, concurrency)
            tps = [x for x in (to_float(r["tps"]) for r in rows) if x is not None]
            handshakes = [
                x for x in (to_float(r["client_handshakes_per_sec"]) for r in rows) if x is not None
            ]
            lat_avg = [x for x in (to_float(r["latency_avg_ms"]) for r in rows) if x is not None]
            p50 = [x for x in (to_float(r["p50_ms"]) for r in rows) if x is not None]
            p95 = [x for x in (to_float(r["p95_ms"]) for r in rows) if x is not None]
            p99 = [x for x in (to_float(r["p99_ms"]) for r in rows) if x is not None]
            bmax = [
                float(backend_max[(target, workload, concurrency, int(r["rep"]))])
                for r in rows
                if (target, workload, concurrency, int(r["rep"])) in backend_max
            ]
            bfinal = [
                float(backend_final[(target, workload, concurrency, int(r["rep"]))])
                for r in rows
                if (target, workload, concurrency, int(r["rep"])) in backend_final
            ]
            writer.writerow({
                "target": target,
                "workload": workload,
                "concurrency": concurrency,
                "runs": len(rows),
                "failed_runs": sum(1 for r in rows if r["exit_code"] != "0"),
                "tps_mean": mean_or_blank(tps),
                "tps_std": pstdev_or_blank(tps),
                "client_handshakes_per_sec_mean": mean_or_blank(handshakes),
                "latency_avg_ms_mean": mean_or_blank(lat_avg),
                "p50_ms_mean": mean_or_blank(p50),
                "p95_ms_mean": mean_or_blank(p95),
                "p99_ms_mean": mean_or_blank(p99),
                "backend_during_max_total_mean": mean_or_blank(bmax),
                "backend_after_final_total_mean": mean_or_blank(bfinal),
            })


def write_resource_summary(out_dir: Path) -> None:
    path = out_dir / "resources.csv"
    if not path.exists():
        return
    grouped: dict[tuple[str, str, int, str, str, int], list[dict[str, str]]] = defaultdict(list)
    with path.open() as f:
        for row in csv.DictReader(f):
            key = (
                row["target"],
                row["workload"],
                int(row["concurrency"]),
                row["resource"],
                row["kind"],
                int(row["rep"]),
            )
            grouped[key].append(row)

    per_rep = []
    for key, rows in grouped.items():
        cpus = [x for x in (to_float(r["cpu_pct"]) for r in rows) if x is not None]
        rss = [x for x in (to_float(r["rss_mib"]) for r in rows) if x is not None]
        mem = [x for x in (to_float(r["mem_mib"]) for r in rows) if x is not None]
        per_rep.append((*key, max(cpus) if cpus else None, max(rss) if rss else None, max(mem) if mem else None))

    final: dict[tuple[str, str, int, str, str], list[tuple[float | None, float | None, float | None]]] = defaultdict(list)
    for target, workload, concurrency, resource, kind, _rep, cpu, rss, mem in per_rep:
        final[(target, workload, concurrency, resource, kind)].append((cpu, rss, mem))

    fields = [
        "target",
        "workload",
        "concurrency",
        "resource",
        "kind",
        "cpu_pct_max_mean",
        "rss_mib_max_mean",
        "mem_mib_max_mean",
    ]
    with (out_dir / "resource_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for (target, workload, concurrency, resource, kind), rows in sorted(final.items()):
            cpus = [x[0] for x in rows if x[0] is not None]
            rss = [x[1] for x in rows if x[1] is not None]
            mem = [x[2] for x in rows if x[2] is not None]
            writer.writerow({
                "target": target,
                "workload": workload,
                "concurrency": concurrency,
                "resource": resource,
                "kind": kind,
                "cpu_pct_max_mean": mean_or_blank(cpus),
                "rss_mib_max_mean": mean_or_blank(rss),
                "mem_mib_max_mean": mean_or_blank(mem),
            })


def validate_tools(args: argparse.Namespace) -> None:
    missing = [tool for tool in ["pgbench"] if shutil.which(tool) is None]
    if args.backend_url and shutil.which("psql") is None:
        missing.append("psql")
    if any(r.kind == "docker" for r in args.resource) and shutil.which("docker") is None:
        missing.append("docker")
    if missing:
        sys.exit(f"missing required tool(s): {', '.join(sorted(set(missing)))}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        action="append",
        required=True,
        type=parse_target,
        help="Target to benchmark, as NAME=POSTGRES_URL. Repeat for proxy_pool, proxy_pgcat, proxy_direct.",
    )
    parser.add_argument(
        "--backend-url",
        help="Direct compute PostgreSQL URL used for pg_stat_activity backend connection sampling.",
    )
    parser.add_argument("--out-dir", default="benchmark-results/tcp-pool", type=Path)
    parser.add_argument("--duration", default=60, type=int, help="Seconds per pgbench run.")
    parser.add_argument("--concurrency", default=[10, 50, 100, 250], type=parse_csv_ints)
    parser.add_argument("--reps", default=3, type=int)
    parser.add_argument(
        "--workload",
        action="append",
        choices=sorted(WORKLOADS),
        help="Workload to run. Defaults to readonly_steady and readonly_connect.",
    )
    parser.add_argument("--max-jobs", default=64, type=int, help="Cap pgbench -j.")
    parser.add_argument("--sample-interval", default=1.0, type=float)
    parser.add_argument(
        "--idle-waits",
        default=[10, 30, 60],
        type=parse_csv_ints,
        help="Seconds after each run to sample backend idle cleanup.",
    )
    parser.add_argument(
        "--resource",
        action="append",
        default=[],
        type=parse_resource,
        help="CPU/memory resource to sample, NAME=pid:PID or NAME=docker:CONTAINER. Repeatable.",
    )
    parser.add_argument(
        "--keep-pgbench-logs",
        action="store_true",
        help="Keep raw pgbench latency logs. By default they are parsed then deleted.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workloads = args.workload or ["readonly_steady", "readonly_connect"]
    args.out_dir.mkdir(parents=True, exist_ok=True)
    validate_tools(args)

    runs_f, runs_writer = open_csv(args.out_dir / "runs.csv", RUN_FIELDS)
    backend_f, backend_writer = open_csv(args.out_dir / "backend_connections.csv", BACKEND_FIELDS)
    resource_f, resource_writer = open_csv(args.out_dir / "resources.csv", RESOURCE_FIELDS)
    files = [runs_f, backend_f, resource_f]

    try:
        for target in args.target:
            for workload in workloads:
                for concurrency in args.concurrency:
                    for rep in range(1, args.reps + 1):
                        print(
                            f"{utc_now()} target={target.name} workload={workload} "
                            f"c={concurrency} rep={rep}",
                            flush=True,
                        )
                        sampler = Sampler(
                            target=target.name,
                            workload=workload,
                            concurrency=concurrency,
                            rep=rep,
                            backend_url=args.backend_url,
                            resources=args.resource,
                            interval_s=args.sample_interval,
                            backend_writer=backend_writer,
                            resource_writer=resource_writer,
                            files=files,
                        )
                        sampler.start()
                        exit_code, parsed, stdout_path, stderr_path = run_pgbench(
                            target=target,
                            workload=workload,
                            concurrency=concurrency,
                            rep=rep,
                            duration_s=args.duration,
                            max_jobs=args.max_jobs,
                            out_dir=args.out_dir,
                            keep_logs=args.keep_pgbench_logs,
                        )
                        sampler.stop()
                        sampler.after_idle(args.idle_waits)

                        runs_writer.writerow({
                            "target": target.name,
                            "workload": workload,
                            "concurrency": concurrency,
                            "rep": rep,
                            "duration_s": args.duration,
                            "exit_code": exit_code,
                            **parsed,
                            "backend_before_total": sampler.backend_before_total(),
                            "backend_during_max_total": sampler.backend_during_max_total(),
                            "backend_after_final_total": sampler.backend_after_final_total(),
                            "pgbench_stdout": stdout_path,
                            "pgbench_stderr": stderr_path,
                        })
                        for f in files:
                            f.flush()

        write_run_summary(args.out_dir)
        write_resource_summary(args.out_dir)
    finally:
        for f in files:
            f.close()

    print(f"wrote {args.out_dir / 'summary.csv'}")
    if args.resource:
        print(f"wrote {args.out_dir / 'resource_summary.csv'}")


if __name__ == "__main__":
    main()
