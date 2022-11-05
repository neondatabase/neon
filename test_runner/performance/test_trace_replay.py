from contextlib import closing

from fixtures.benchmark_fixture import NeonBenchmarker
from fixtures.neon_fixtures import NeonEnvBuilder


# This test demonstrates how to collect a read trace
def test_read_request_tracing(neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker):
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

    timeline = env.neon_cli.create_timeline("test_trace_replay", tenant_id=tenant)
    pg = env.postgres.create_start("test_trace_replay", "main", tenant)

    with zenbenchmark.record_duration("run"):
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("create table t (i integer);")
                cur.execute(f"insert into t values (generate_series(1,{10000}));")
                cur.execute("select count(*) from t;")

    # Stop pg so we drop the connection and flush the traces
    pg.stop()

    trace_path = env.repo_dir / "traces" / str(tenant) / str(timeline)
    assert trace_path.exists()
