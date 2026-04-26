#!/usr/bin/env python3
"""Read pgbench -l logs from stdin, print p50,p95,p99 latency in ms (CSV)."""

import sys


def main() -> None:
    xs: list[int] = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            xs.append(int(parts[2]))
        except ValueError:
            continue

    if not xs:
        print(",,")
        return

    xs.sort()

    def pct(q: float) -> str:
        idx = max(0, min(len(xs) - 1, int(round(len(xs) * q / 100)) - 1))
        return f"{xs[idx] / 1000:.3f}"

    print(f"{pct(50)},{pct(95)},{pct(99)}")


if __name__ == "__main__":
    main()
