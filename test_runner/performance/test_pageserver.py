from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker


def test_get_page(zenith_simple_env: ZenithEnv, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
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
