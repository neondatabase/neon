from contextlib import closing

from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.pageserver.utils import wait_for_last_record_lsn
from fixtures.types import Lsn
from fixtures.utils import query_scalar


# This test demonstrates how to collect a read trace. It's useful until
# it gets replaced by a test that actually does stuff with the trace.
#
# Additionally, tests that pageserver is able to create tenants with custom configs.
def test_read_request_tracing(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "trace_read_requests": "true",
        }
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    endpoint = env.endpoints.create_start("main")

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t (i integer);")
            cur.execute(f"insert into t values (generate_series(1,{10000}));")
            cur.execute("select count(*) from t;")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    # wait until pageserver receives that data
    pageserver_http = env.pageserver.http_client()
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)

    # Stop postgres so we drop the connection and flush the traces
    endpoint.stop()

    trace_path = env.pageserver.workdir / "traces" / str(tenant_id) / str(timeline_id)
    assert trace_path.exists()
