from __future__ import annotations

import time

from fixtures.neon_fixtures import NeonEnvBuilder


#
# Test truncation of FSM and VM forks of a relation
#
def test_truncate(neon_env_builder: NeonEnvBuilder, zenbenchmark):
    env = neon_env_builder.init_start()
    n_records = 10000
    n_iter = 10

    # Problems with FSM/VM forks truncation are most frequently detected during page reconstruction triggered
    # by image layer generation. So adjust default parameters to make it happen more frequently.
    tenant, _ = env.create_tenant(
        conf={
            # disable automatic GC
            "gc_period": "0s",
            # Compact and create images aggressively
            "checkpoint_distance": "1000000",
            "compaction_period": "1 s",
            "compaction_threshold": "3",
            "image_creation_threshold": "1",
            "compaction_target_size": "1000000",
        }
    )

    endpoint = env.endpoints.create_start("main", tenant_id=tenant)
    cur = endpoint.connect().cursor()
    cur.execute("create table t1(x integer)")
    cur.execute(f"insert into t1 values (generate_series(1,{n_records}))")
    cur.execute("vacuum t1")
    for _ in range(n_iter):
        cur.execute(f"delete from t1 where x>{n_records//2}")
        cur.execute("vacuum t1")
        time.sleep(1)  # let pageserver a chance to create image layers
        cur.execute(f"insert into t1 values (generate_series({n_records//2+1}, {n_records}))")
        cur.execute("vacuum t1")
        time.sleep(1)  # let pageserver a chance to create image layers

    cur.execute("select count(*) from t1")
    res = cur.fetchone()
    assert res is not None
    assert res[0] == n_records
