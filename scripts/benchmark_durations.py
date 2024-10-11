#! /usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging

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
    "test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py::test_pageserver_max_throughput_getpage_at_latest_lsn[1-13-30]": 400.15,
    "test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py::test_pageserver_max_throughput_getpage_at_latest_lsn[1-6-30]": 372.521,
    "test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py::test_pageserver_max_throughput_getpage_at_latest_lsn[10-13-30]": 420.017,
    "test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py::test_pageserver_max_throughput_getpage_at_latest_lsn[10-6-30]": 373.769,
    "test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py::test_pageserver_max_throughput_getpage_at_latest_lsn[100-13-30]": 678.742,
    "test_runner/performance/pageserver/pagebench/test_pageserver_max_throughput_getpage_at_latest_lsn.py::test_pageserver_max_throughput_getpage_at_latest_lsn[100-6-30]": 512.135,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_heavy_write[20]": 58.036,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_many_relations": 22.104,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_many[1024]": 126.073,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_pgbench_perf": 25.759,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_read_perf": 6.885,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_write_perf": 8.758,
    "test_runner/performance/test_bulk_insert.py::test_bulk_insert[neon]": 18.275,
    "test_runner/performance/test_bulk_insert.py::test_bulk_insert[vanilla]": 9.533,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[1]": 12.09,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[10]": 35.145,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[5]": 22.28,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[10]": 66.353,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[100]": 75.487,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[50]": 54.142,
    "test_runner/performance/test_compaction.py::test_compaction": 110.715,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_ro_with_pgbench_select_only[neon-5-10-100]": 11.68,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_ro_with_pgbench_select_only[vanilla-5-10-100]": 16.384,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_rw_with_pgbench_default[neon-5-10-100]": 11.315,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_rw_with_pgbench_default[vanilla-5-10-100]": 18.783,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wal_with_pgbench_default[neon-5-10-100]": 11.647,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wal_with_pgbench_default[vanilla-5-10-100]": 17.04,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[neon-10-1]": 11.01,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[neon-10-10]": 11.902,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[vanilla-10-1]": 10.077,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[vanilla-10-10]": 10.4,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_pgbench_simple_update[neon-5-10-100]": 11.33,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_pgbench_simple_update[vanilla-5-10-100]": 16.434,
    "test_runner/performance/test_copy.py::test_copy[neon]": 13.817,
    "test_runner/performance/test_copy.py::test_copy[vanilla]": 11.736,
    "test_runner/performance/test_gc_feedback.py::test_gc_feedback": 575.735,
    "test_runner/performance/test_gc_feedback.py::test_gc_feedback_with_snapshots": 575.735,
    "test_runner/performance/test_gist_build.py::test_gist_buffering_build[neon]": 14.868,
    "test_runner/performance/test_gist_build.py::test_gist_buffering_build[vanilla]": 14.393,
    "test_runner/performance/test_latency.py::test_measure_read_latency_heavy_write_workload[neon-1]": 20.588,
    "test_runner/performance/test_latency.py::test_measure_read_latency_heavy_write_workload[vanilla-1]": 30.849,
    "test_runner/performance/test_layer_map.py::test_layer_map": 39.378,
    "test_runner/performance/test_lazy_startup.py::test_lazy_startup": 2848.938,
    "test_runner/performance/test_logical_replication.py::test_logical_replication": 120.952,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_different_tables[neon]": 35.552,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_different_tables[vanilla]": 66.762,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_same_table[neon]": 85.177,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_same_table[vanilla]": 92.12,
    "test_runner/performance/test_perf_pgbench.py::test_pgbench[neon-45-10]": 107.009,
    "test_runner/performance/test_perf_pgbench.py::test_pgbench[vanilla-45-10]": 99.582,
    "test_runner/performance/test_random_writes.py::test_random_writes[neon]": 4.737,
    "test_runner/performance/test_random_writes.py::test_random_writes[vanilla]": 2.686,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-100000-100-0]": 3.271,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-10000000-1-0]": 50.719,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-10000000-1-4]": 15.992,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-100000-100-0]": 0.566,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-10000000-1-0]": 13.542,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-10000000-1-4]": 13.35,
    "test_runner/performance/test_startup.py::test_startup_simple": 13.043,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[neon_off-10-5-5]": 194.841,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[neon_on-10-5-5]": 286.667,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[vanilla-10-5-5]": 85.577,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[neon_off-1000]": 297.626,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[neon_on-1000]": 646.187,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[vanilla-1000]": 989.776,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[neon_off-45-100]": 125.638,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[neon_on-45-100]": 123.554,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[vanilla-45-100]": 190.083,
    "test_runner/performance/test_write_amplification.py::test_write_amplification[neon]": 21.016,
    "test_runner/performance/test_write_amplification.py::test_write_amplification[vanilla]": 23.028,
}


def main(args: argparse.Namespace):
    connstr = args.connstr
    interval_days = args.days
    output = args.output
    percentile = args.percentile

    res: dict[str, float] = {}

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
