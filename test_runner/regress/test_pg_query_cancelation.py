import random
from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonPageserver
from fixtures.pageserver.http import PageserverHttpClient
from psycopg2.errors import QueryCanceled

CRITICAL_PG_PS_WAIT_FAILPOINTS = [
    "ps::connection-start::pre-login",
    "ps::connection-start::startup-packet",
    "ps::connection-start::process-query",
    "ps::handle-pagerequest-message",
]


# @pytest.mark.parametrize('failpoint', CRITICAL_PG_PS_WAIT_FAILPOINTS)
def test_cancelations(neon_simple_env: NeonEnv):
    env = neon_simple_env
    ps = env.pageserver
    ps_http = ps.http_client()
    ps_http.is_testing_enabled_or_skip()

    env.neon_cli.create_branch("test_config", "empty")

    # We don't want to have any racy behaviour with autovacuum IOs
    ep = env.endpoints.create_start(
        "test_config",
        config_lines=[
            "autovacuum = off",
            "shared_buffers = 128MB",
        ],
    )

    with closing(ep.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE test1 AS
                    SELECT id, sha256(id::text::bytea) payload
                    FROM generate_series(1, 1024::bigint) p(id);
                """
            )
            cur.execute(
                """
                CREATE TABLE test2 AS
                    SELECT id, sha256(id::text::bytea) payload
                    FROM generate_series(1025, 2048::bigint) p(id);
                """
            )
            cur.execute(
                """
                VACUUM (ANALYZE, FREEZE) test1, test2;
                """
            )
            cur.execute(
                """
                CREATE EXTENSION pg_buffercache;
                """
            )
            cur.execute(
                """
                CREATE EXTENSION pg_prewarm;
                """
            )

    # data preparation is now complete, with 2 disjoint tables that aren't
    # preloaded into any caches.

    ep.stop()
    random.shuffle(CRITICAL_PG_PS_WAIT_FAILPOINTS)

    for failpoint in CRITICAL_PG_PS_WAIT_FAILPOINTS:
        connect_works_correctly(failpoint, ep, ps, ps_http)


def connect_works_correctly(
    failpoint: str, ep: Endpoint, ps: NeonPageserver, ps_http: PageserverHttpClient
):
    log.debug("Starting work on %s", failpoint)
    # All queries we use should finish (incl. IO) within 500ms,
    # including all their IO.
    # This allows us to use `SET statement_timeout` to let the query
    # timeout system cancel queries, rather than us having to go
    # through the most annoying effort of manual query cancellation
    # in psycopg2.
    options = "-cstatement_timeout=500ms -ceffective_io_concurrency=1"

    ep.start()

    def fp_enable():
        ps_http.configure_failpoints(
            [
                (failpoint, "pause"),
            ]
        )
        log.debug('Enabled failpoint "%s"', failpoint, stacklevel=2)

    def fp_disable():
        ps_http.configure_failpoints(
            [
                (failpoint, "off"),
            ]
        )
        log.debug('Disabled failpoint "%s"', failpoint, stacklevel=2)

    def check_buffers(cur):
        cur.execute(
            """
            SELECT n.nspname AS nspname
                 , c.relname AS relname
                 , count(*)  AS count
            FROM pg_buffercache b
            JOIN pg_class c
              ON b.relfilenode = pg_relation_filenode(c.oid) AND
                 b.reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database())
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid IN ('test1'::regclass::oid, 'test2'::regclass::oid)
            GROUP BY n.nspname, c.relname
            ORDER BY 3 DESC
            LIMIT 10
            """
        )
        return cur.fetchone()

    fp_disable()

    # Warm caches required for new connections, without reading any caches.
    with closing(ep.connect(options=options)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1;
                """
            )
            assert cur.fetchone() == (1,)
            cur.execute("SHOW statement_timeout;")
            assert cur.fetchone() == ("500ms",)

            assert check_buffers(cur) is None
            # Ensure all caches required for connection start are correctly
            # filled, so that we don't have any "accidents" in this test run
            # caused by changes in connection startup plans that require
            # requests to the PageServer.
            cur.execute(
                """
                select array_agg(distinct (pg_prewarm(c.oid::regclass, 'buffer') >= 0))
                from pg_class c
                where c.reltablespace = (
                    select oid from pg_tablespace where spcname = 'pg_global'
                );
                """
            )
            assert cur.fetchone() == ([True],)

    # Enable failpoint
    fp_enable()

    with closing(ep.connect(options=options)) as conn:
        with conn.cursor() as cur:
            assert check_buffers(cur) is None

            with pytest.raises(QueryCanceled):
                cur.execute(
                    """
                    SELECT min(id) FROM test1;
                    """
                )
                assert cur.fetchone() == (1,)

    fp_disable()

    with closing(ep.connect(options=options)) as conn:
        with conn.cursor() as cur:
            # Do a select on the data, putting some buffers into the prefetch
            # queue.
            cur.execute(
                """
                SELECT count(id) FROM (select * from test1 LIMIT 256) a;
                """
            )
            assert cur.fetchone() == (256,)

            ps.stop()
            ps.start()
            fp_enable()

            with pytest.raises(QueryCanceled):
                cur.execute(
                    """
                    SELECT COUNT(id) FROM test1;
                    """
                )
                assert cur.fetchone() == (1024,)

    with closing(ep.connect(options=options)) as conn:
        with conn.cursor() as cur:
            with pytest.raises(QueryCanceled):
                cur.execute(
                    """
                    SELECT COUNT(id) FROM test2;
                    """
                )
                cur.fetchone()
                assert cur.fetchone() == (1024,)

        fp_disable()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(id), min(id), max(id) FROM test2;
                """
            )

            assert cur.fetchone() == (1024, 1025, 2048)

            cur.execute(
                """
                SELECT count(id), min(id), max(id) FROM test1;
                """
            )

            assert cur.fetchone() == (1024, 1, 1024)

    ep.stop()
