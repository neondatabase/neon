from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_explain_with_lfc_stats(neon_simple_env: NeonEnv):
    env = neon_simple_env

    cache_dir = Path(env.repo_dir) / "file_cache"
    cache_dir.mkdir(exist_ok=True)

    log.info("Creating endpoint with 1MB shared_buffers and 64 MB LFC")
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size='128MB'",
            "neon.file_cache_size_limit='64MB'",
        ],
    )

    cur = endpoint.connect().cursor()

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
    cur.execute(
        "insert into pgbench_accounts(aid,bid,abalance,filler) select aid, (aid - 1) / 100000 + 1, 0, '' from generate_series(1, 100000) as aid;"
    )

    log.info(f"warming up caches with sequential scan in {endpoint.connstr()}")
    cur.execute("SELECT * FROM pgbench_accounts WHERE abalance > 0")

    log.info("running explain analyze without LFC values to verify they do not show up in the plan")
    cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM pgbench_accounts WHERE abalance > 0")
    rows = cur.fetchall()
    plan = "\n".join(r[0] for r in rows)
    log.debug(plan)
    assert "Seq Scan on pgbench_accounts" in plan
    assert "Buffers: shared hit" in plan
    assert "File cache: hits=" not in plan
    log.info("running explain analyze WITH LFC values to verify they do now show up")
    cur.execute(
        "EXPLAIN (ANALYZE, BUFFERS,FILECACHE) SELECT * FROM pgbench_accounts WHERE abalance > 0"
    )
    rows = cur.fetchall()
    plan = "\n".join(r[0] for r in rows)
    log.debug(plan)
    assert "Seq Scan on pgbench_accounts" in plan
    assert "Buffers: shared hit" in plan
    assert "File cache: hits=" in plan
    log.info("running explain analyze WITH LFC values to verify json output")
    cur.execute(
        "EXPLAIN (ANALYZE, BUFFERS,FILECACHE, FORMAT JSON) SELECT * FROM pgbench_accounts WHERE abalance > 0"
    )
    jsonplan = cur.fetchall()[0][0]
    log.debug(jsonplan)
    # Directly access the 'Plan' part of the first element of the JSON array
    plan_details = jsonplan[0]["Plan"]

    # Extract "File Cache Hits" and "File Cache Misses"
    file_cache_hits = plan_details.get("File Cache Hits")
    file_cache_misses = plan_details.get("File Cache Misses")

    # Now you can assert the values
    assert file_cache_hits >= 5000, f"Expected File Cache Hits to be > 5000, got {file_cache_hits}"
    assert file_cache_misses == 0, f"Expected File Cache Misses to be 0, got {file_cache_misses}"
