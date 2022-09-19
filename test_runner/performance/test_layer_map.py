import time

from fixtures.neon_fixtures import NeonEnvBuilder


#
# Benchmark searching the layer map, when there are a lot of small layer files.
#
def test_layer_map(neon_env_builder: NeonEnvBuilder, zenbenchmark):

    env = neon_env_builder.init_start()
    n_iters = 10
    n_records = 100000

    # We want to have a lot of lot of layer files to exercise the layer map. Make
    # gc_horizon and checkpoint_distance very small, so that we get a lot of small layer files.
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "gc_period": "100 m",
            "gc_horizon": "1048576",
            "checkpoint_distance": "8192",
            "compaction_period": "1 s",
            "compaction_threshold": "1",
            "compaction_target_size": "8192",
        }
    )

    env.neon_cli.create_timeline("test_layer_map", tenant_id=tenant)
    pg = env.postgres.create_start("test_layer_map", tenant_id=tenant)
    cur = pg.connect().cursor()
    cur.execute("create table t(x integer)")
    for i in range(n_iters):
        cur.execute(f"insert into t values (generate_series(1,{n_records}))")
        time.sleep(1)

    cur.execute("vacuum t")
    with zenbenchmark.record_duration("test_query"):
        cur.execute("SELECT count(*) from t")
        assert cur.fetchone() == (n_iters * n_records,)
