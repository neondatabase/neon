from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.utils import USE_LFC, query_scalar

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_working_set_approximation(neon_simple_env: NeonEnv):
    env = neon_simple_env

    cache_dir = Path(env.repo_dir) / "file_cache"
    cache_dir.mkdir(exist_ok=True)

    log.info("Creating endpoint with 1MB shared_buffers and 64 MB LFC")
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "autovacuum=off",
            "bgwriter_lru_maxpages=0",
            "neon.max_file_cache_size='128MB'",
            "neon.file_cache_size_limit='64MB'",
        ],
    )

    cur = endpoint.connect().cursor()
    cur.execute("create extension neon")

    log.info(f"preparing some data in {endpoint.connstr()}")

    ddl = """
CREATE TABLE pgbench_accounts (
    aid bigint NOT NULL,
    bid integer,
    abalance integer,
    filler character(84),
    -- more web-app like columns
    text_column_plain TEXT  DEFAULT repeat('NeonIsCool', 5),
    jsonb_column_extended JSONB  DEFAULT ('{ "tell everyone": [' || repeat('{"Neon": "IsCool"},',9) || ' {"Neon": "IsCool"}]}')::jsonb
)
WITH (fillfactor='100');
"""

    cur.execute(ddl)
    # prepare index access below
    cur.execute(
        "ALTER TABLE ONLY pgbench_accounts ADD CONSTRAINT pgbench_accounts_pkey PRIMARY KEY (aid)"
    )
    cur.execute(
        "insert into pgbench_accounts(aid,bid,abalance,filler) select aid, (aid - 1) / 100000 + 1, 0, '' from generate_series(1, 100000) as aid;"
    )
    # ensure correct query plans and stats
    cur.execute("vacuum ANALYZE pgbench_accounts")
    # determine table size - working set should approximate table size after sequential scan
    pages = query_scalar(cur, "SELECT relpages FROM pg_class WHERE relname = 'pgbench_accounts'")
    log.info(f"pgbench_accounts has {pages} pages, resetting working set to zero")
    cur.execute("select approximate_working_set_size(true)")
    cur.execute(
        'SELECT count(*) FROM pgbench_accounts WHERE abalance > 0 or jsonb_column_extended @> \'{"tell everyone": [{"Neon": "IsCool"}]}\'::jsonb'
    )
    # verify working set size after sequential scan matches table size and reset working set for next test
    blocks = query_scalar(cur, "select approximate_working_set_size(true)")
    log.info(f"working set size after sequential scan on pgbench_accounts {blocks}")
    assert pages * 0.8 < blocks < pages * 1.2
    # run a few point queries with index lookup
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid =   4242")
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid =  54242")
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid = 104242")
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid = 204242")
    # verify working set size after some index access of a few select pages only
    blocks = query_scalar(cur, "select approximate_working_set_size(false)")
    log.info(f"working set size after some index access of a few select pages only {blocks}")
    assert blocks < 20

    # Also test the metrics from the /autoscaling_metrics endpoint
    autoscaling_metrics = endpoint.http_client().autoscaling_metrics()
    log.debug(f"Raw metrics: {autoscaling_metrics}")
    m = parse_metrics(autoscaling_metrics)

    http_estimate = m.query_one(
        "lfc_approximate_working_set_size_windows",
        {
            "duration_seconds": "60",
        },
    ).value
    log.info(f"http estimate: {http_estimate}, blocks: {blocks}")
    assert http_estimate > 0 and http_estimate < 20


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_sliding_working_set_approximation(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create_start(
        branch_name="main",
        config_lines=[
            "autovacuum = off",
            "bgwriter_lru_maxpages=0",
            "shared_buffers=1MB",
            "neon.max_file_cache_size=256MB",
            "neon.file_cache_size_limit=245MB",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("create extension neon")
    cur.execute(
        "create table t(pk integer primary key, count integer default 0, payload text default repeat('?', 1000)) with (fillfactor=10)"
    )
    cur.execute("insert into t (pk) values (generate_series(1,100000))")
    time.sleep(2)
    before_10k = time.monotonic()
    cur.execute("select sum(count) from t where pk between 10000 and 20000")
    time.sleep(2)
    before_1k = time.monotonic()
    cur.execute("select sum(count) from t where pk between 1000 and 2000")
    after = time.monotonic()

    cur.execute(f"select approximate_working_set_size_seconds({int(after - before_1k + 1)})")
    estimation_1k = cur.fetchall()[0][0]
    log.info(f"Working set size for selecting 1k records {estimation_1k}")

    cur.execute(f"select approximate_working_set_size_seconds({int(after - before_10k + 1)})")
    estimation_10k = cur.fetchall()[0][0]
    log.info(f"Working set size for selecting 10k records {estimation_10k}")

    cur.execute("select pg_table_size('t')")
    size = cur.fetchall()[0][0] // 8192
    log.info(f"Table size {size} blocks")

    assert estimation_1k >= 900 and estimation_1k <= 2000
    assert estimation_10k >= 9000 and estimation_10k <= 20000
