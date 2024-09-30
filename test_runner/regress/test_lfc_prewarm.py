import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv


def test_lfc_prewarm(neon_simple_env: NeonEnv):
    env = neon_simple_env
    n_records = 1000000

    endpoint = env.endpoints.create_start(
        branch_name="main",
        config_lines=[
            "autovacuum = off",
            "shared_buffers=1MB",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "neon.file_cache_prewarm_limit=1000",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("create extension neon")
    cur.execute("create table t(pk integer primary key, payload text default repeat('?', 128))")
    cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")

    endpoint.stop()
    endpoint.start()

    conn = endpoint.connect()
    cur = conn.cursor()

    for _ in range(20):
        time.sleep(1)  # give prewarm BGW some time to proceed
        cur.execute("select file_cache_used from neon_stat_file_cache")
        lfc_used = cur.fetchall()[0][0]
        if lfc_used > 100:
            break

    log.info(f"Used LFC size: {lfc_used}")
    assert lfc_used > 100

    cur.execute("select sum(pk) from t")
    assert cur.fetchall()[0][0] == n_records * (n_records + 1) / 2
