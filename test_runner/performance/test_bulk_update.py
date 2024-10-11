from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn


#
# Benchmark effect of prefetch on bulk update operations
#
# A sequential scan that's part of a bulk update is the same as any other sequential scan,
# but dirtying the pages as you go affects the last-written LSN tracking. We used to have
# an issue with the last-written LSN cache where rapidly evicting dirty pages always
# invalidated the prefetched responses, which showed up in bad performance in this test.
#
@pytest.mark.timeout(10000)
@pytest.mark.parametrize("fillfactor", [10, 50, 100])
def test_bulk_update(neon_env_builder: NeonEnvBuilder, zenbenchmark, fillfactor):
    env = neon_env_builder.init_start()
    n_records = 1000000

    timeline_id = env.create_branch("test_bulk_update")
    tenant_id = env.initial_tenant
    endpoint = env.endpoints.create_start("test_bulk_update")
    cur = endpoint.connect().cursor()
    cur.execute("set statement_timeout=0")

    cur.execute(f"create table t(x integer) WITH (fillfactor={fillfactor})")

    with zenbenchmark.record_duration("insert-1"):
        cur.execute(f"insert into t values (generate_series(1,{n_records}))")

    cur.execute("vacuum t")
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    with zenbenchmark.record_duration("update-no-prefetch"):
        cur.execute("update t set x=x+1")

    cur.execute("vacuum t")
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    with zenbenchmark.record_duration("delete-no-prefetch"):
        cur.execute("delete from t")

    cur.execute("drop table t")
    cur.execute("set enable_seqscan_prefetch=on")
    cur.execute("set effective_io_concurrency=32")
    cur.execute("set maintenance_io_concurrency=32")

    cur.execute(f"create table t2(x integer) WITH (fillfactor={fillfactor})")

    with zenbenchmark.record_duration("insert-2"):
        cur.execute(f"insert into t2 values (generate_series(1,{n_records}))")

    cur.execute("vacuum t2")
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    with zenbenchmark.record_duration("update-with-prefetch"):
        cur.execute("update t2 set x=x+1")

    cur.execute("vacuum t2")
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    with zenbenchmark.record_duration("delete-with-prefetch"):
        cur.execute("delete from t2")
