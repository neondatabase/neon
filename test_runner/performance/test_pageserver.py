from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnv, PgBin, ZenithEnvBuilder, DEFAULT_BRANCH_NAME, PsbenchBin
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker


# TODO split this into separate tests maybe
@pytest.mark.parametrize(
    "workload",
    [
        pytest.param("hot-page", marks=pytest.mark.slow),
        pytest.param("pgbench"),
        pytest.param("pgbench-big", marks=pytest.mark.slow),
        pytest.param("pgbench-long", marks=pytest.mark.slow),
    ])
def test_get_page(zenith_env_builder: ZenithEnvBuilder,
                  zenbenchmark: ZenithBenchmarker,
                  pg_bin: PgBin,
                  psbench_bin: PsbenchBin,
                  workload: str):
    zenith_env_builder.pageserver_config_override = "emit_wal_metadata=true"
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch("test_pageserver", DEFAULT_BRANCH_NAME)
    pg = env.postgres.create_start('test_pageserver')
    tenant_hex = env.initial_tenant.hex
    timeline = pg.safe_psql("SHOW zenith.zenith_timeline")[0][0]

    # Long-lived cursor, useful for flushing
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            if workload == "hot-page":
                cur.execute('create table t (i integer);')
                cur.execute('insert into t values (0);')
                for i in range(100000):
                    cur.execute(f'update t set i = {i};')
            elif workload == "pgbench":
                pg_bin.run_capture(['pgbench', '-s5', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t5000', pg.connstr()])
            elif workload == "pgbench-big":
                pg_bin.run_capture(['pgbench', '-s100', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t100000', pg.connstr()])
            elif workload == "pgbench-long":
                pg_bin.run_capture(['pgbench', '-s100', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t1000000', pg.connstr()])

            pscur.execute(f"checkpoint {env.initial_tenant.hex} {timeline} 0")

    output = psbench_bin.test_latest_pages(env.initial_tenant.hex, timeline)
    zenbenchmark.record_psbench_result(output)
