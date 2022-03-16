from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker

def _get_page():
    # u8 tag: 2, big endian
    # u8 latest
    # u64 lsn
    # reltag:
    #   u32 spcnode
    #   u32 dbnode
    #   u32 relnode
    #   u8 forknum
    # u32 blkno
    pass


def test_get_page(zenith_simple_env: ZenithEnv, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli.create_branch("test_pageserver", "empty")
    pg = env.postgres.create_start('test_pageserver')
    tenant_hex = env.initial_tenant.hex
    timeline = pg.safe_psql("SHOW zenith.zenith_timeline")[0][0]

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('create table t (i integer);')
            cur.execute('insert into t values (generate_series(1,3));')

            cur.execute("select * from t;")
            res = cur.fetchall()

            cur.execute("select pg_relation_filepath('t');")
            res = cur.fetchall()
            print(res)

    env.run_psbench(timeline)
    return

    import os
    ps_log_filename = os.path.join(env.repo_dir, "pageserver.log")
    with open(ps_log_filename) as log_file:
        log = log_file.readlines()

    ps_connstr = env.pageserver.connstr()



    latest_write = None
    for line in log:
        if line.startswith("wal-at-lsn-modified-page "):
            tokens = line.split()
            lsn_hex = tokens[1]
            page_hex = tokens[2]
            latest_write = (lsn_hex, page_hex)

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as cur:
            cur.execute(f"pagestream {tenant_hex} {timeline}")
        with psconn.cursor() as cur:
            cur.execute(f"select 1;")

            # res = cur.fetchall()
            # print(res)
    # TODO send query to pageserver, see what is logged

    # TODO send queries on these pages
    # 1. Craft binary message
    # 2. Send as postgres query

    # TODO maybe make rust program for this side of the protocol?
