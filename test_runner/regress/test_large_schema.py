from __future__ import annotations

import os
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


# This test creates large number of tables which cause large catalog.
# Right now Neon serialize directory as single key-value storage entry and so
# it leads to layer filled mostly by one key.
# Originally Neon implementation of checkpoint and compaction is not able to split key which leads
# to large (several gigabytes) layer files (both ephemeral and delta layers).
# It may cause problems with uploading to S3 and also degrade performance because ephemeral file swapping.
#
def test_large_schema(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    tables = 2  # 10 is too much for debug build
    partitions = 1000
    for i in range(1, tables + 1):
        print(f"iteration {i} / {tables}")

        # Restart compute. Restart is actually not strictly needed.
        # It is done mostly because this test originally tries to model the problem reported by Ketteq.
        endpoint.stop()
        # Kill and restart the pageserver.
        # env.pageserver.stop(immediate=True)
        # env.pageserver.start()
        endpoint.start()

        retry_sleep = 0.5
        max_retries = 200
        retries = 0
        while True:
            try:
                conn = endpoint.connect()
                cur = conn.cursor()
                cur.execute(f"CREATE TABLE if not exists t_{i}(pk integer) partition by range (pk)")
                for j in range(1, partitions + 1):
                    cur.execute(
                        f"create table if not exists p_{i}_{j} partition of t_{i} for values from ({j}) to ({j + 1})"
                    )
                cur.execute(f"insert into t_{i} values (generate_series(1,{partitions}))")
                cur.execute("vacuum full")
                conn.close()

            except Exception as error:
                # It's normal that it takes some time for the pageserver to
                # restart, and for the connection to fail until it does. It
                # should eventually recover, so retry until it succeeds.
                print(f"failed: {error}")
                if retries < max_retries:
                    retries += 1
                    print(f"retry {retries} / {max_retries}")
                    time.sleep(retry_sleep)
                    continue
                else:
                    raise
            break

    conn = endpoint.connect()
    cur = conn.cursor()

    for i in range(1, tables + 1):
        cur.execute(f"SELECT count(*) FROM t_{i}")
        assert cur.fetchone() == (partitions,)

    cur.execute("set enable_sort=off")
    cur.execute("select * from pg_depend order by refclassid, refobjid, refobjsubid")

    # Check layer file sizes
    timeline_path = (
        f"{env.pageserver.workdir}/tenants/{env.initial_tenant}/timelines/{env.initial_timeline}/"
    )
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            log.info(f"layer {filename} size is {os.path.getsize(timeline_path + filename)}")
            assert os.path.getsize(timeline_path + filename) < 512_000_000
