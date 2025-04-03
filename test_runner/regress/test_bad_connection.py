from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

import psycopg2.errors
import pytest
from fixtures.log_helper import log
from fixtures.utils import USE_LFC

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.timeout(600)
def test_compute_pageserver_connection_stress(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.append(".*simulated connection error.*")  # this is never hit

    # the real reason (Simulated Connection Error) is on the next line, and we cannot filter this out.
    env.pageserver.allowed_errors.append(
        ".*ERROR error in page_service connection task: Postgres query error"
    )

    # Enable failpoint before starting everything else up so that we exercise the retry
    # on fetching basebackup
    pageserver_http = env.pageserver.http_client()
    pageserver_http.configure_failpoints(("simulated-bad-compute-connection", "50%return(15)"))

    env.create_branch("test_compute_pageserver_connection_stress")
    endpoint = env.endpoints.create_start("test_compute_pageserver_connection_stress")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    def execute_retry_on_timeout(query):
        while True:
            try:
                cur.execute(query)
                return
            except psycopg2.errors.QueryCanceled:
                log.info(f"Query '{query}' timed out - retrying")

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    execute_retry_on_timeout("CREATE TABLE foo (t text)")
    execute_retry_on_timeout(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
        """
    )

    # Verify that the table is larger than shared_buffers
    execute_retry_on_timeout(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
        """
    )
    row = cur.fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    execute_retry_on_timeout("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    end_time = time.time() + 30
    times_executed = 0
    while time.time() < end_time:
        if random.random() < 0.5:
            execute_retry_on_timeout("INSERT INTO foo VALUES ('stas'), ('heikki')")
        else:
            execute_retry_on_timeout("SELECT t FROM foo ORDER BY RANDOM() LIMIT 10")
            cur.fetchall()
        times_executed += 1
    log.info(f"Workload executed {times_executed} times")

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()


def test_compute_pageserver_hung_connections(neon_env_builder: NeonEnvBuilder):
    """
    Test timeouts in waiting for response to pageserver request
    """
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.append(".*slow GetPage.*")
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start(
        "main",
        tenant_id=env.initial_tenant,
        config_lines=["autovacuum = off"],
    )
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
        """
    )

    # Verify that the table is larger than shared_buffers
    cur.execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
        """
    )
    row = cur.fetchone()
    assert row is not None
    log.debug(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    # Print the backend PID so that it can be compared with the logs easily
    cur.execute("SELECT pg_backend_pid()")
    row = cur.fetchone()
    assert row is not None
    log.info(f"running test workload in backend PID {row[0]}")

    def run_workload(duration: float):
        end_time = time.time() + duration
        times_executed = 0
        while time.time() < end_time:
            if random.random() < 0.5:
                cur.execute("INSERT INTO foo VALUES ('stas'), ('heikki')")
            else:
                cur.execute("SELECT t FROM foo ORDER BY RANDOM() LIMIT 10")
                cur.fetchall()
                times_executed += 1
        log.info(f"Workload executed {times_executed} times")
        assert times_executed > 0

    ## Test short connection hiccups
    ##
    ## This is to exercise the logging timeout.
    log.info("running workload with log timeout")
    cur.execute("SET neon.pageserver_response_log_timeout = '500ms'")
    pageserver_http.configure_failpoints(("before-pagestream-msg-flush", "10%3*return(3000)"))
    run_workload(20)

    # check that the message was logged
    assert endpoint.log_contains("no response received from pageserver for .* s, still waiting")
    assert endpoint.log_contains("received response from pageserver after .* s")

    ## Test connections that are hung for longer
    ##
    ## This exercises the disconnect timeout. We'll disconnect and
    ## reconnect after 500 ms.
    log.info("running workload with disconnect timeout")
    cur.execute("SET neon.pageserver_response_log_timeout = '250ms'")
    cur.execute("SET neon.pageserver_response_disconnect_timeout = '500ms'")
    pageserver_http.configure_failpoints(("before-pagestream-msg-flush", "10%3*return(3000)"))
    run_workload(15)

    assert endpoint.log_contains("no response from pageserver for .* s, disconnecting")

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()


def test_compute_pageserver_statement_timeout(neon_env_builder: NeonEnvBuilder):
    """
    Test statement_timeout while waiting for response to pageserver request
    """
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.append(".*slow GetPage.*")
    pageserver_http = env.pageserver.http_client()

    # Make sure the shared_buffers and LFC are tiny, to ensure the queries
    # hit the storage. Disable autovacuum to make the test more deterministic.
    config_lines = [
        "shared_buffers='512kB'",
        "autovacuum = off",
    ]
    if USE_LFC:
        config_lines = ["neon.max_file_cache_size = 1MB", "neon.file_cache_size_limit = 1MB"]
    endpoint = env.endpoints.create_start(
        "main",
        tenant_id=env.initial_tenant,
        config_lines=config_lines,
    )
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Disable parallel query. Parallel workers open their own pageserver connections,
    # which messes up the test logic.
    cur.execute("SET max_parallel_workers_per_gather=0")
    cur.execute("SET effective_io_concurrency=0")

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
        """
    )

    # Verify that the table is larger than shared_buffers
    cur.execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
        """
    )
    row = cur.fetchone()
    assert row is not None
    log.debug(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    ## Run a query until the compute->pageserver connection hits the failpoint and
    ## get stuck. This tests that the statement_timeout is obeyed while waiting on a
    ## GetPage request.
    log.info("running workload with statement_timeout")
    cur.execute("SET neon.pageserver_response_log_timeout = '2000ms'")
    cur.execute("SET neon.pageserver_response_disconnect_timeout = '30000ms'")
    cur.execute("SET statement_timeout='10s'")
    pageserver_http.configure_failpoints(("before-pagestream-msg-flush", "10%return(60000)"))

    start_time = time.time()
    with pytest.raises(psycopg2.errors.QueryCanceled):
        cur.execute("SELECT count(*) FROM foo")
        cur.fetchall()
    log.info("Statement timeout reached")
    end_time = time.time()
    # Verify that the statement_timeout canceled the query before
    # neon.pageserver_response_disconnect_timeout expired
    assert end_time - start_time < 40
    times_canceled = 1

    # Should not have disconnected yet
    assert not endpoint.log_contains("no response from pageserver for .* s, disconnecting")

    # Clear the failpoint. This doesn't affect the connection that already hit it. It
    # will keep waiting. But subsequent connections will work normally.
    pageserver_http.configure_failpoints(("before-pagestream-msg-flush", "off"))

    # If we keep retrying, we should eventually succeed. (This tests that the
    # neon.pageserver_response_disconnect_timeout is not reset on query
    # cancellation.)
    while times_canceled < 10:
        try:
            cur.execute("SELECT count(*) FROM foo")
            cur.fetchall()
            log.info("Statement succeeded")
            break
        except psycopg2.errors.QueryCanceled:
            log.info("Statement timed out, retrying")
            times_canceled += 1
    assert times_canceled > 1 and times_canceled < 10

    assert endpoint.log_contains("no response from pageserver for .* s, disconnecting")

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()
