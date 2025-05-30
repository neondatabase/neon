import math  # Add this import
import os
import time
import traceback
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest
from fixtures.benchmark_fixture import NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI, connection_parameters_to_env
from fixtures.neon_fixtures import PgBin
from fixtures.pg_version import PgVersion

vacuum_times_sql = """
SELECT
    relname AS table_name,
    last_autovacuum,
    last_autoanalyze
FROM
    pg_stat_user_tables where relname = 'pgbench_accounts'
ORDER BY
    last_autovacuum DESC, last_autoanalyze DESC
"""


def insert_first_chunk_and_verify_autovacuum_is_not_running(
    cur, rows_to_insert, autovacuum_naptime
):
    cur.execute(f"""
    INSERT INTO pgbench_accounts (aid, bid, abalance, filler)
    SELECT
        aid,
        (random() * 10)::int + 1 AS bid,
        (random() * 10000)::int AS abalance,
        'filler text' AS filler
    FROM generate_series(6800001, {6800001 + rows_to_insert - 1}) AS aid;
    """)
    assert cur.rowcount == rows_to_insert
    for _ in range(5):
        time.sleep(0.5 * autovacuum_naptime)
        cur.execute(vacuum_times_sql)
        row = cur.fetchall()[0]
        log.info(f"last_autovacuum: {row[1]}, last_autoanalyze: {row[2]}")
        assert row[1] is None


def insert_second_chunk_and_verify_autovacuum_is_now_running(
    cur, rows_to_insert, autovacuum_naptime
):
    cur.execute(f"""
    INSERT INTO pgbench_accounts (aid, bid, abalance, filler)
    SELECT
        aid,
        (random() * 10)::int + 1 AS bid,
        (random() * 10000)::int AS abalance,
        'filler text' AS filler
    FROM generate_series({6800001 + rows_to_insert}, {6800001 + rows_to_insert * 2 - 1}) AS aid;
    """)
    assert cur.rowcount == rows_to_insert
    for _ in range(5):
        time.sleep(0.5 * autovacuum_naptime)
        cur.execute(vacuum_times_sql)
        row = cur.fetchall()[0]
        log.info(f"last_autovacuum: {row[1]}, last_autoanalyze: {row[2]}")
    assert row[1] is not None


@pytest.mark.remote_cluster
@pytest.mark.timeout(60 * 60)
def test_cumulative_statistics_persistence(
    pg_bin: PgBin,
    test_output_dir: Path,
    neon_api: NeonAPI,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    """
    Verifies that the cumulative statistics are correctly persisted across restarts.
    Cumulative statistics are important to persist across restarts because they are used
    when auto-vacuum an auto-analyze trigger conditions are met.
    The test performs the following steps:
    - Seed a new project using pgbench
    - insert tuples that by itself are not enough to trigger auto-vacuum
    - suspend the endpoint
    - resume the endpoint
    - insert additional tuples that by itself are not enough to trigger auto-vacuum but in combination with the previous tuples are
    - verify that autovacuum is triggered by the combination of tuples inserted before and after endpoint suspension
    """
    project = neon_api.create_project(
        pg_version,
        f"Test cumulative statistics persistence, GITHUB_RUN_ID={os.getenv('GITHUB_RUN_ID')}",
    )
    project_id = project["project"]["id"]
    neon_api.wait_for_operation_to_finish(project_id)
    endpoint_id = project["endpoints"][0]["id"]
    region_id = project["project"]["region_id"]
    log.info(f"Created project {project_id} with endpoint {endpoint_id} in region {region_id}")
    error_occurred = False
    try:
        connstr = project["connection_uris"][0]["connection_uri"]
        env = connection_parameters_to_env(project["connection_uris"][0]["connection_parameters"])
        # seed about 1 GiB of data into pgbench_accounts
        pg_bin.run_capture(["pgbench", "-i", "-s68"], env=env)

        # assert rows in pgbench_accounts is 6800000 rows
        conn = psycopg2.connect(connstr)
        conn.autocommit = True
        with conn.cursor() as cur:
            # assert rows in pgbench_accounts is 6800000 rows
            cur.execute("select count(*) from pgbench_accounts")
            row_count = cur.fetchall()[0][0]
            assert row_count == 6800000

            # verify n_tup_ins, n_live_tup, vacuum_count, analyze_count (manual vacuum and analyze)
            cur.execute(
                "select n_tup_ins, vacuum_count,analyze_count from pg_stat_user_tables where relname = 'pgbench_accounts'"
            )
            row = cur.fetchall()[0]
            assert row[0] == 6800000  # n_tup_ins
            assert row[1] == 1  # vacuum_count
            assert row[2] == 1  # analyze_count

            # retrieve some GUCs (postgres settings) relevant to autovacuum
            cur.execute(
                "SELECT setting::int AS autovacuum_naptime FROM pg_settings WHERE name = 'autovacuum_naptime'"
            )
            autovacuum_naptime = cur.fetchall()[0][0]
            assert autovacuum_naptime < 300 and autovacuum_naptime > 0
            cur.execute(
                "SELECT setting::float AS autovacuum_vacuum_insert_scale_factor FROM pg_settings WHERE name = 'autovacuum_vacuum_insert_scale_factor'"
            )
            autovacuum_vacuum_insert_scale_factor = cur.fetchall()[0][0]
            assert (
                autovacuum_vacuum_insert_scale_factor > 0.05
                and autovacuum_vacuum_insert_scale_factor < 1.0
            )
            cur.execute(
                "SELECT setting::int AS autovacuum_vacuum_insert_threshold FROM pg_settings WHERE name = 'autovacuum_vacuum_insert_threshold'"
            )
            autovacuum_vacuum_insert_threshold = cur.fetchall()[0][0]
            cur.execute(
                "SELECT setting::int AS pgstat_file_size_limit FROM pg_settings WHERE name = 'neon.pgstat_file_size_limit'"
            )
            pgstat_file_size_limit = cur.fetchall()[0][0]
            assert pgstat_file_size_limit > 10 * 1024  # at least 10 MB

            # insert rows that by itself are not enough to trigger auto-vacuum
            # vacuum insert threshold = vacuum base insert threshold + vacuum insert scale factor * number of tuples
            # https://www.postgresql.org/docs/17/routine-vacuuming.html
            rows_to_insert = int(
                math.ceil(
                    autovacuum_vacuum_insert_threshold / 2
                    + row_count * autovacuum_vacuum_insert_scale_factor * 0.6
                )
            )

            log.info(
                f"autovacuum_vacuum_insert_scale_factor: {autovacuum_vacuum_insert_scale_factor}, autovacuum_vacuum_insert_threshold: {autovacuum_vacuum_insert_threshold}, row_count: {row_count}"
            )
            log.info(
                f"Inserting {rows_to_insert} rows, which is below the 'vacuum insert threshold'"
            )

            insert_first_chunk_and_verify_autovacuum_is_not_running(
                cur, rows_to_insert, autovacuum_naptime
            )

        conn.close()

        # suspend the endpoint
        log.info(f"Suspending endpoint {endpoint_id}")
        neon_api.suspend_endpoint(project_id, endpoint_id)
        neon_api.wait_for_operation_to_finish(project_id)
        time.sleep(60)  # give some time in between suspend and resume

        # resume the endpoint
        log.info(f"Starting endpoint {endpoint_id}")
        neon_api.start_endpoint(project_id, endpoint_id)
        neon_api.wait_for_operation_to_finish(project_id)

        conn = psycopg2.connect(connstr)
        conn.autocommit = True
        with conn.cursor() as cur:
            # insert additional rows that by itself are not enough to trigger auto-vacuum, but in combination
            # with the previous rows inserted before the suspension are
            log.info(
                f"Inserting another {rows_to_insert} rows, which is below the 'vacuum insert threshold'"
            )
            insert_second_chunk_and_verify_autovacuum_is_now_running(
                cur, rows_to_insert, autovacuum_naptime
            )

            # verify estimatednumber of tuples in pgbench_accounts is within 6800000 + inserted rows +- 2 %
            cur.execute(
                "select reltuples::bigint from pg_class where relkind = 'r' and relname = 'pgbench_accounts'"
            )
            reltuples = cur.fetchall()[0][0]
            assert reltuples > 6800000 + rows_to_insert * 2 * 0.98
            assert reltuples < 6800000 + rows_to_insert * 2 * 1.02

            # verify exact number of pgbench_accounts rows (computed row_count)
            cur.execute("select count(*) from pgbench_accounts")
            row_count = cur.fetchall()[0][0]
            assert row_count == 6800000 + rows_to_insert * 2

            # verify n_tup_ins, n_live_tup, vacuum_count, analyze_count (manual vacuum and analyze)
            cur.execute(
                "select n_tup_ins, vacuum_count,analyze_count from pg_stat_user_tables where relname = 'pgbench_accounts'"
            )
            row = cur.fetchall()[0]
            assert row[0] == 6800000 + rows_to_insert * 2
            assert row[1] == 1
            assert row[2] == 1

        conn.close()

    except Exception as e:
        error_occurred = True
        log.error(f"Caught exception: {e}")
        log.error(traceback.format_exc())
    finally:
        assert not error_occurred  # Fail the test if an error occurred
        neon_api.delete_project(project_id)
