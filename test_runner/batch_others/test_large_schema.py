import time
import os
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.log_helper import log


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_large_schema(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    pg = env.postgres.create_start('main')

    conn = pg.connect()
    cur = conn.cursor()

    tables = 10
    partitions = 1000
    for i in range(1, tables + 1):
        print(f'iteration {i} / {tables}')

        # Kill and restart the pageserver.
        pg.stop()
        # env.pageserver.stop(immediate=True)
        # env.pageserver.start()
        pg.start()

        retry_sleep = 0.5
        max_retries = 200
        retries = 0
        while True:
            try:
                conn = pg.connect()
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
                print(f'failed: {error}')
                if retries < max_retries:
                    retries += 1
                    print(f'retry {retries} / {max_retries}')
                    time.sleep(retry_sleep)
                    continue
                else:
                    raise
            break

    conn = pg.connect()
    cur = conn.cursor()

    for i in range(1, tables + 1):
        cur.execute(f"SELECT count(*) FROM t_{i}")
        assert cur.fetchone() == (partitions, )

    cur.execute("set enable_sort=off")
    cur.execute("select * from pg_depend order by refclassid, refobjid, refobjsubid")

    # Check layer file sizes
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]
    timeline_path = "{}/tenants/{}/timelines/{}/".format(env.repo_dir, tenant_id, timeline_id)
    for filename in os.listdir(timeline_path):
        if filename.startswith('00000'):
            log.info(f'layer {filename} size is {os.path.getsize(timeline_path + filename)}')
            assert os.path.getsize(timeline_path + filename) < 512_000_000
