from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv, PgBin, ZenithEnvBuilder
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker


def test_get_page(zenith_env_builder: ZenithEnvBuilder,
                  zenbenchmark: ZenithBenchmarker,
                  pg_bin: PgBin):
    zenith_env_builder.pageserver_config_override = "emit_wal_metadata=true"
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch("test_pageserver", "main")
    pg = env.postgres.create_start('test_pageserver')
    tenant_hex = env.initial_tenant.hex
    timeline = pg.safe_psql("SHOW zenith.zenith_timeline")[0][0]

    # Long-lived cursor, useful for flushing
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            workload = "pgbench"

            print(f"Running workload {workload}")
            if workload == "hot page":
                cur.execute('create table t (i integer);')
                cur.execute('insert into t values (0);')
                for i in range(100000):
                    cur.execute(f'update t set i = {i};')
            elif workload == "pgbench":
                pg_bin.run_capture(['pgbench', '-s5', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t5000', pg.connstr()])
            elif workload == "pgbench big":
                pg_bin.run_capture(['pgbench', '-s100', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t100000', pg.connstr()])
            elif workload == "pgbench long":
                pg_bin.run_capture(['pgbench', '-s100', '-i', pg.connstr()])
                pg_bin.run_capture(['pgbench', '-c1', '-t1000000', pg.connstr()])

            pscur.execute(f"checkpoint {env.initial_tenant.hex} {timeline} 0")

    output = env.run_psbench(timeline)
    for line in output.split("\n"):
        tokens = line.split(" ")
        report = tokens[0]
        name = tokens[1]
        value = tokens[2]
        unit = tokens[3] if len(tokens) > 3 else ""
        zenbenchmark.record(name, value, unit, report=report)
