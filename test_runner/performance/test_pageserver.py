from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv, PgBin
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker


def test_get_page(zenith_simple_env: ZenithEnv,
                  zenbenchmark: ZenithBenchmarker,
                  pg_bin: PgBin):
    env = zenith_simple_env
    env.zenith_cli.create_branch("test_pageserver", "empty")
    pg = env.postgres.create_start('test_pageserver')
    tenant_hex = env.initial_tenant.hex
    timeline = pg.safe_psql("SHOW zenith.zenith_timeline")[0][0]

    # Long-lived cursor, useful for flushing
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            workload = "pgbench"
            if workload == "hot page":
                cur.execute('create table t (i integer);')
                cur.execute('insert into t values (0);')
                for i in range(100000):
                    cur.execute(f'update t set i = {i};')
            elif workload == "pgbench":
                pg_bin.run_capture(['pgbench', '-s5', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t5000', pg.connstr()])

            pscur.execute(f"checkpoint {env.initial_tenant.hex} {timeline} 0")

    env.run_psbench(timeline)
