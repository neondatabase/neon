from contextlib import closing

import pytest
from fixtures.neon_fixtures import NeonEnv
from psycopg2.errors import QueryCanceled

"""
Test that we can handle broken pageservers correctly
"""


def test_pageserver_breaks_while_running(neon_simple_env: NeonEnv):
    env = neon_simple_env
    ps = env.pageserver
    ps_http = ps.http_client()
    ps_http.is_testing_enabled_or_skip()

    (tid, tlid) = env.neon_cli.create_tenant()
    env.neon_cli.create_branch("test_config", tenant_id=tid)

    # We don't want to have any racy behaviour with autovacuum IOs
    ep = env.endpoints.create_start(
        "test_config",
        config_lines=[
            "autovacuum = off",
            "shared_buffers = 128MB",
        ],
    )

    # tenant is still attached, no errors from PS
    with closing(ep.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE test1 AS
                    SELECT id, sha256(id::text::bytea) payload
                    FROM generate_series(1, 1024::bigint) p(id);
                """
            )

    ps_http.tenant_detach(tid)

    # create a new connection to PS, this will cause errors.
    with closing(ep.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SET query_timeout = 1s;
                """
            )
            with pytest.raises(QueryCanceled):
                # definitely uncached relation
                cur.execute(
                    """
                    SELECT count(*) FROM pg_rewrite;
                    """
                )

    ep.stop()
    ep.log_contains("""could not complete handshake: PageServer returned error: """)
