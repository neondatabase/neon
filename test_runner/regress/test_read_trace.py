from contextlib import closing

from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_record_lsn
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar


# This test demonstrates how to collect a read trace. It's useful until
# it gets replaced by a test that actually does stuff with the trace.
#
# Additionally, tests that pageserver is able to create tenants with custom configs.
def test_read_request_tracing(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "trace_read_requests": "true",
        }
    )

    timeline = env.neon_cli.create_timeline("test_trace_replay", tenant_id=tenant)
    pg = env.postgres.create_start("test_trace_replay", "main", tenant)

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t (i integer);")
            cur.execute(f"insert into t values (generate_series(1,{10000}));")
            cur.execute("select count(*) from t;")
            tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
            timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    # wait until pageserver receives that data
    pageserver_http = env.pageserver.http_client()
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)

    # Stop pg so we drop the connection and flush the traces
    pg.stop()

    trace_path = env.repo_dir / "traces" / str(tenant) / str(timeline)
    assert trace_path.exists()
