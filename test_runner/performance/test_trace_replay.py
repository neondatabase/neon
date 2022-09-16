from contextlib import closing

from fixtures.benchmark_fixture import NeonBenchmarker
from fixtures.neon_fixtures import NeonEnvBuilder, ReplayBin


# This test is a demonstration of how to do trace playback. With the current
# workload it uses it's not testing anything meaningful.
def test_trace_replay(
    neon_env_builder: NeonEnvBuilder, replay_bin: ReplayBin, zenbenchmark: NeonBenchmarker
):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "trace_read_requests": "true",
        }
    )
    # TODO This doesn't work because I haven't updated tenant_config_handler
    # env.neon_cli.config_tenant(tenant, conf={
    #     "trace_read_requests": "true",
    # })
    env.neon_cli.create_timeline("test_trace_replay", tenant_id=tenant)
    pg = env.postgres.create_start("test_trace_replay", "main", tenant)

    with zenbenchmark.record_duration("run"):
        pg.safe_psql("select 1;")
        pg.safe_psql("select 1;")
        pg.safe_psql("select 1;")
        pg.safe_psql("select 1;")

        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("create table t (i integer);")
                cur.execute(f"insert into t values (generate_series(1,{10000}));")
                cur.execute("select count(*) from t;")

    # Stop pg so we drop the connection and flush the traces
    pg.stop()

    # TODO This doesn't work because I haven't updated tenant_config_handler
    # env.neon_cli.config_tenant(tenant, conf={
    #     "trace_read_requests": "false",
    # })

    # trace_path = env.repo_dir / "traces" / str(tenant) / str(timeline) / str(timeline)
    # assert trace_path.exists()

    print("replaying")
    ps_connstr = env.pageserver.connstr()

    # ps_connstr = "host=localhost port=15004 dbname=postgres user=neon_admin"
    with zenbenchmark.record_duration("replay"):
        output = replay_bin.replay_all(ps_connstr)
    print(output)
