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
    cur.execute("create extension neon version '1.6'")
    cur.execute("create table t(pk integer primary key, payload text default repeat('?', 128))")
    cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")

    endpoint.stop()
    endpoint.start()

    conn = endpoint.connect()
    cur = conn.cursor()

    for _ in range(20):
        time.sleep(1)  # give prewarm BGW some time to proceed
        cur.execute("select lfc_value from neon_lfc_stats where lfc_key='file_cache_used_pages'")
        lfc_used_pages = cur.fetchall()[0][0]
        log.info(f"Used LFC size: {lfc_used_pages}")
        cur.execute("select * from get_prewarm_info()")
        prewarm_info = cur.fetchall()[0]
        log.info(f"Prewrm info: {prewarm_info}")
        if prewarm_info[0] > 0:
            log.info(f"Prewarm progress: {prewarm_info[1]*100//prewarm_info[0]}%")
            if prewarm_info[0] == prewarm_info[1]:
                break

    assert lfc_used_pages > 10000
    assert prewarm_info[0] > 0 and prewarm_info[0] == prewarm_info[1]:

    cur.execute("select sum(pk) from t")
    assert cur.fetchall()[0][0] == n_records * (n_records + 1) / 2

    assert prewarm_info[1] > 0
