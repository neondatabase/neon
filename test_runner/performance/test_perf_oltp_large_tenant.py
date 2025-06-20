from __future__ import annotations

import os
import timeit
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fixtures.benchmark_fixture import PgBenchRunResult
from fixtures.log_helper import log

from performance.test_perf_pgbench import get_durations_matrix, utc_now_timestamp

if TYPE_CHECKING:
    from fixtures.compare_fixtures import PgCompare


def get_custom_scripts(
    default: str = "insert_webhooks.sql@2 select_any_webhook_with_skew.sql@4 select_recent_webhook.sql@4",
) -> list[str]:
    # We parametrize each run with the custom scripts to run and their weights.
    # The custom scripts and their weights are passed through TEST_PGBENCH_CUSTOM_SCRIPTS env variable.
    # Delimit the custom scripts for one run by spaces and for different runs by commas, for example:
    # "insert_webhooks.sql@2 select_any_webhook_with_skew.sql@4,insert_webhooks.sql@8 select_any_webhook_with_skew.sql@2"
    # Databases/branches  are pre-created and passed through BENCHMARK_CONNSTR env variable.
    scripts = os.getenv("TEST_PGBENCH_CUSTOM_SCRIPTS", default=str(default))
    rv = []
    for s in scripts.split(","):
        rv.append(s)
    return rv


def run_test_pgbench(
    env: PgCompare, custom_scripts: str, duration: int, clients: int = 500, jobs: int = 100
):
    password = env.pg.default_options.get("password", None)
    options = env.pg.default_options.get("options", "")
    # drop password from the connection string by passing password=None and set password separately
    connstr = env.pg.connstr(password=None, options=options)
    # if connstr does not contain pooler we can set statement_timeout to 0
    if "pooler" not in connstr:
        options = "-cstatement_timeout=0 " + env.pg.default_options.get("options", "")
        connstr = env.pg.connstr(password=None, options=options)

    script_args = [
        "pgbench",
        "-n",  # no explicit vacuum before the test - we want to rely on auto-vacuum
        "-M",
        "prepared",
        f"--client={clients}",
        f"--jobs={jobs}",
        f"-T{duration}",
        "-P60",  # progress every minute
        "--progress-timestamp",
    ]
    for script in custom_scripts.split():
        script_args.extend(["-f", f"test_runner/performance/large_synthetic_oltp/{script}"])
    script_args.append(connstr)

    run_pgbench(
        env,
        "custom-scripts",
        script_args,
        password=password,
    )


def run_pgbench(env: PgCompare, prefix: str, cmdline, password: None):
    environ: dict[str, str] = {}
    if password is not None:
        environ["PGPASSWORD"] = password

    run_start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    out = env.pg_bin.run_capture(cmdline, env=environ)
    run_duration = timeit.default_timer() - t0
    run_end_timestamp = utc_now_timestamp()
    env.flush()

    stdout = Path(f"{out}.stdout").read_text()

    res = PgBenchRunResult.parse_from_stdout(
        stdout=stdout,
        run_duration=run_duration,
        run_start_timestamp=run_start_timestamp,
        run_end_timestamp=run_end_timestamp,
    )
    env.zenbenchmark.record_pg_bench_result(prefix, res)


def run_database_maintenance(env: PgCompare):
    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            log.info("start vacuum analyze transaction.transaction")
            with env.zenbenchmark.record_duration("vacuum_analyze"):
                cur.execute("SET statement_timeout = 0;")
                cur.execute("SET max_parallel_maintenance_workers = 7;")
                cur.execute("SET maintenance_work_mem = '10GB';")
                cur.execute("vacuum analyze transaction.transaction;")
            log.info("finished vacuum analyze transaction.transaction")

            # recover previously failed or canceled re-indexing
            cur.execute(
                """
                DO $$
                DECLARE
                    invalid_index TEXT;
                BEGIN
                    FOR invalid_index IN
                        SELECT c.relname
                        FROM pg_class c
                        JOIN pg_index i ON i.indexrelid = c.oid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'transaction'
                        AND i.indisvalid = FALSE
                        AND c.relname LIKE '%_ccnew%'
                    LOOP
                        EXECUTE 'DROP INDEX IF EXISTS transaction.' || invalid_index;
                    END LOOP;
                END $$;
                """
            )
            # also recover failed or canceled re-indexing on toast part of table
            cur.execute(
                """
                DO $$
                DECLARE
                    invalid_index TEXT;
                BEGIN
                    FOR invalid_index IN
                        SELECT c.relname
                        FROM pg_class c
                        JOIN pg_index i ON i.indexrelid = c.oid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'pg_toast'
                        AND i.indisvalid = FALSE
                        AND c.relname LIKE '%_ccnew%'
                        AND i.indrelid = (
                            SELECT reltoastrelid FROM pg_class
                            WHERE relname = 'transaction'
                            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'transaction')
                        )
                    LOOP
                        EXECUTE 'DROP INDEX IF EXISTS pg_toast.' || invalid_index;
                    END LOOP;
                END $$;
                """
            )
            # in production a customer would likely use reindex concurrently
            # but for our test we don't care about the downtime
            # and it would just about double the time we report in the test
            # because we need one more table scan for each index
            log.info("start REINDEX TABLE transaction.transaction")
            with env.zenbenchmark.record_duration("reindex"):
                cur.execute("REINDEX TABLE transaction.transaction;")
            log.info("finished REINDEX TABLE transaction.transaction")


@pytest.mark.parametrize("custom_scripts", get_custom_scripts())
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_perf_oltp_large_tenant_pgbench(
    remote_compare: PgCompare, custom_scripts: str, duration: int
):
    run_test_pgbench(remote_compare, custom_scripts, duration)


@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_perf_oltp_large_tenant_growth(remote_compare: PgCompare, duration: int):
    run_test_pgbench(remote_compare, " ".join(get_custom_scripts()), duration, 35, 35)


@pytest.mark.remote_cluster
def test_perf_oltp_large_tenant_maintenance(remote_compare: PgCompare):
    # run analyze, vacuum, re-index after the test and measure and report its duration
    run_database_maintenance(remote_compare)
