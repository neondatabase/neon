#! /usr/bin/env python3

import argparse
import json
import logging
from typing import Dict

import psycopg2
import psycopg2.extras

"""
The script fetches the durations of benchmarks from the database and stores it in a file compatible with pytest-split plugin.
"""


BENCHMARKS_DURATION_QUERY = """
    SELECT
        DISTINCT parent_suite, suite, name,
        PERCENTILE_DISC(%s) WITHIN GROUP (ORDER BY duration) as percentile_ms
    FROM results
    WHERE
        started_at > CURRENT_DATE - INTERVAL '%s' day
        AND starts_with(parent_suite, 'test_runner.performance')
        AND status = 'passed'
    GROUP BY
        parent_suite, suite, name
    ;
"""

# For out benchmarks the default distibution for 4 worked produces pretty uneven chunks,
# the total duration varies from 8 to 40 minutes.
# We use some pre-collected durations as a fallback to have a better distribution.
FALLBACK_DURATION = {
    "test_runner/performance/test_branch_creation.py::test_branch_creation_heavy_write[20]": 62.144,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_many[1024]": 90.941,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_many_relations": 26.053,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_pgbench_perf": 25.67,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_read_perf": 14.497,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_write_perf": 18.852,
    "test_runner/performance/test_bulk_insert.py::test_bulk_insert[neon]": 26.572,
    "test_runner/performance/test_bulk_insert.py::test_bulk_insert[vanilla]": 6.259,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[10]": 21.206,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[1]": 3.474,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[5]": 11.262,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[100]": 94.225,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[10]": 68.159,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[50]": 76.719,
    "test_runner/performance/test_compaction.py::test_compaction": 110.222,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_ro_with_pgbench_select_only[neon-5-10-100]": 10.743,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_ro_with_pgbench_select_only[vanilla-5-10-100]": 16.541,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_rw_with_pgbench_default[neon-5-10-100]": 11.109,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_rw_with_pgbench_default[vanilla-5-10-100]": 18.121,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wal_with_pgbench_default[neon-5-10-100]": 11.3,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wal_with_pgbench_default[vanilla-5-10-100]": 16.086,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[neon-10-10]": 12.024,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[neon-10-1]": 11.14,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[vanilla-10-10]": 10.375,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[vanilla-10-1]": 10.075,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_pgbench_simple_update[neon-5-10-100]": 11.147,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_pgbench_simple_update[vanilla-5-10-100]": 16.321,
    "test_runner/performance/test_copy.py::test_copy[neon]": 16.579,
    "test_runner/performance/test_copy.py::test_copy[vanilla]": 10.094,
    "test_runner/performance/test_gc_feedback.py::test_gc_feedback": 590.157,
    "test_runner/performance/test_gist_build.py::test_gist_buffering_build[neon]": 14.102,
    "test_runner/performance/test_gist_build.py::test_gist_buffering_build[vanilla]": 8.677,
    "test_runner/performance/test_latency.py::test_measure_read_latency_heavy_write_workload[neon-1]": 31.079,
    "test_runner/performance/test_latency.py::test_measure_read_latency_heavy_write_workload[vanilla-1]": 38.119,
    "test_runner/performance/test_layer_map.py::test_layer_map": 24.784,
    "test_runner/performance/test_logical_replication.py::test_logical_replication": 117.707,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_different_tables[neon]": 21.194,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_different_tables[vanilla]": 59.068,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_same_table[neon]": 73.235,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_same_table[vanilla]": 82.586,
    "test_runner/performance/test_perf_pgbench.py::test_pgbench[neon-45-10]": 106.536,
    "test_runner/performance/test_perf_pgbench.py::test_pgbench[vanilla-45-10]": 98.753,
    "test_runner/performance/test_random_writes.py::test_random_writes[neon]": 6.975,
    "test_runner/performance/test_random_writes.py::test_random_writes[vanilla]": 3.69,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-100000-100-0]": 3.529,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-10000000-1-0]": 64.522,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-10000000-1-4]": 40.964,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-100000-100-0]": 0.55,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-10000000-1-0]": 12.189,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-10000000-1-4]": 13.899,
    "test_runner/performance/test_startup.py::test_startup_simple": 2.51,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[neon_off-10-5-5]": 527.245,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[neon_on-10-5-5]": 583.46,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[vanilla-10-5-5]": 113.653,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[neon_off-1000]": 233.728,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[neon_on-1000]": 419.093,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[vanilla-1000]": 982.461,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[neon_off-45-100]": 116.522,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[neon_on-45-100]": 115.583,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[vanilla-45-100]": 155.282,
    "test_runner/performance/test_write_amplification.py::test_write_amplification[neon]": 26.704,
    "test_runner/performance/test_write_amplification.py::test_write_amplification[vanilla]": 16.088,
}


def main(args: argparse.Namespace):
    connstr = args.connstr
    interval_days = args.days
    output = args.output
    percentile = args.percentile

    res: Dict[str, float] = {}

    try:
        logging.info("connecting to the database...")
        with psycopg2.connect(connstr, connect_timeout=30) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                logging.info("fetching benchmarks...")
                cur.execute(BENCHMARKS_DURATION_QUERY, (percentile, interval_days))
                rows = cur.fetchall()
    except psycopg2.OperationalError as exc:
        logging.error("cannot fetch benchmarks duration from the DB due to an error", exc)
        rows = []
        res = FALLBACK_DURATION

    for row in rows:
        pytest_name = f"{row['parent_suite'].replace('.', '/')}/{row['suite']}.py::{row['name']}"
        duration = row["percentile_ms"] / 1000
        logging.info(f"\t{pytest_name}: {duration}")
        res[pytest_name] = duration

    logging.info(f"saving results to {output.name}")
    json.dump(res, output, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get <percentile> of benchmarks duration for the last <N> days"
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        default=".test_durations",
        help="path to output json file (default: .test_durations)",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default="0.99",
        help="percentile (default: 0.99)",
    )
    parser.add_argument(
        "--days",
        required=False,
        default=10,
        type=int,
        help="how many days to look back for (default: 10)",
    )
    parser.add_argument(
        "connstr",
        help="connection string to the test results database",
    )
    args = parser.parse_args()

    level = logging.INFO
    logging.basicConfig(
        format="%(message)s",
        level=level,
    )

    main(args)
