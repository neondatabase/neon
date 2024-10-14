from __future__ import annotations

from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonPageserver
from fixtures.pageserver.http import PageserverHttpClient
from psycopg2.errors import QueryCanceled

CRITICAL_PG_PS_WAIT_FAILPOINTS: set[str] = {
    "ps::connection-start::pre-login",
    "ps::connection-start::startup-packet",
    "ps::connection-start::process-query",
    "ps::handle-pagerequest-message::exists",
    "ps::handle-pagerequest-message::nblocks",
    "ps::handle-pagerequest-message::getpage",
    "ps::handle-pagerequest-message::dbsize",
    # We don't yet have a good way to on-demand guarantee the download of an
    # SLRU segment, so that's disabled for now.
    # "ps::handle-pagerequest-message::slrusegment",
}

PG_PS_START_FAILPOINTS = {
    "ps::connection-start::pre-login",
    "ps::connection-start::startup-packet",
    "ps::connection-start::process-query",
}
SMGR_EXISTS = "ps::handle-pagerequest-message::exists"
SMGR_NBLOCKS = "ps::handle-pagerequest-message::nblocks"
SMGR_GETPAGE = "ps::handle-pagerequest-message::getpage"
SMGR_DBSIZE = "ps::handle-pagerequest-message::dbsize"

"""
Test that we can handle connection delays and cancellations at various
unfortunate connection startup and request states.
"""


def test_cancellations(neon_simple_env: NeonEnv):
    env = neon_simple_env
    ps = env.pageserver
    ps_http = ps.http_client()
    ps_http.is_testing_enabled_or_skip()

    # We don't want to have any racy behaviour with autovacuum IOs
    ep = env.endpoints.create_start(
        "main",
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

    for failpoint in CRITICAL_PG_PS_WAIT_FAILPOINTS:
        connect_works_correctly(failpoint, ep, ps, ps_http)


ENABLED_FAILPOINTS: set[str] = set()


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
        global ENABLED_FAILPOINTS
        ps_http.configure_failpoints(
            [
                (failpoint, "pause"),
            ]
        )
        ENABLED_FAILPOINTS = ENABLED_FAILPOINTS | {failpoint}
        log.info(
            'Enabled failpoint "%s", current_active=%s', failpoint, ENABLED_FAILPOINTS, stacklevel=2
        )

    def fp_disable():
        global ENABLED_FAILPOINTS
        ps_http.configure_failpoints(
            [
                (failpoint, "off"),
            ]
        )
        ENABLED_FAILPOINTS = ENABLED_FAILPOINTS - {failpoint}
        log.info(
            'Disabled failpoint "%s", current_active=%s',
            failpoint,
            ENABLED_FAILPOINTS,
            stacklevel=2,
        )

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

    def exec_may_cancel(query, cursor, result, cancels):
        if cancels:
            with pytest.raises(QueryCanceled):
                cursor.execute(query)
                assert cursor.fetchone() == result
        else:
            cursor.execute(query)
            assert cursor.fetchone() == result

    fp_disable()

    # Warm caches required for new connections, so that they can run without
    # requiring catalog reads.
    with closing(ep.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1;
                """
            )
            assert cur.fetchone() == (1,)

            assert check_buffers(cur) is None
            # Ensure all caches required for connection start are correctly
            # filled, so that we don't have any "accidents" in this test run
            # caused by changes in connection startup plans that require
            # requests to the PageServer.
            cur.execute(
                """
                select array_agg(distinct (pg_prewarm(c.oid::regclass, 'buffer') >= 0))
                from pg_class c
                where c.oid < 16384 AND c.relkind IN ('i', 'r');
                """
            )
            assert cur.fetchone() == ([True],)

    # Enable failpoint
    fp_enable()

    with closing(ep.connect(options=options, autocommit=True)) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW statement_timeout;")
            assert cur.fetchone() == ("500ms",)
            assert check_buffers(cur) is None
            exec_may_cancel(
                """
                SELECT min(id) FROM test1;
                """,
                cur,
                (1,),
                failpoint in (CRITICAL_PG_PS_WAIT_FAILPOINTS - {SMGR_EXISTS, SMGR_DBSIZE}),
            )

    fp_disable()

    with closing(ep.connect(options=options, autocommit=True)) as conn:
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

            exec_may_cancel(
                """
                SELECT COUNT(id) FROM test1;
                """,
                cur,
                (1024,),
                failpoint
                in (CRITICAL_PG_PS_WAIT_FAILPOINTS - {SMGR_EXISTS, SMGR_NBLOCKS, SMGR_DBSIZE}),
            )

    with closing(ep.connect(options=options, autocommit=True)) as conn:
        with conn.cursor() as cur:
            exec_may_cancel(
                """
                SELECT COUNT(id) FROM test2;
                """,
                cur,
                (1024,),
                failpoint in (CRITICAL_PG_PS_WAIT_FAILPOINTS - {SMGR_EXISTS, SMGR_DBSIZE}),
            )

            fp_disable()
            fp_enable()

            exec_may_cancel(
                """
                SELECT 0 < pg_database_size(CURRENT_DATABASE());
                """,
                cur,
                (True,),
                failpoint
                in (CRITICAL_PG_PS_WAIT_FAILPOINTS - {SMGR_EXISTS, SMGR_GETPAGE, SMGR_NBLOCKS}),
            )

            fp_disable()

            cur.execute(
                """
                SELECT count(id), count(distinct payload), min(id), max(id), sum(id) FROM test2;
                """
            )

            assert cur.fetchone() == (1024, 1024, 1025, 2048, 1573376)

            cur.execute(
                """
                SELECT count(id), count(distinct payload), min(id), max(id), sum(id) FROM test1;
                """
            )

            assert cur.fetchone() == (1024, 1024, 1, 1024, 524800)

    ep.stop()
