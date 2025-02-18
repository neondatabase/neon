from __future__ import annotations

import random
import time

import psycopg2.errors
import pytest
from fixtures.log_helper import log
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


@pytest.mark.timeout(600)
def test_compute_pageserver_hung_connections(neon_env_builder: NeonEnvBuilder):
    """
    Test timeouts in waiting for response to pageserver request
    """
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main", tenant_id=env.initial_tenant)
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

    ## Test short connections hiccups
    ##
    ## This is to exercise the logging timeout.
    log.info("running workload with log timeout")
    cur.execute("SET neon.pageserver_response_log_timeout = '500ms'")
    pageserver_http.configure_failpoints(("before-pagestream-msg-flush", "10%3*return(1500)"))
    run_workload(15)

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
    pageserver_http.configure_failpoints(("before-pagestream-msg-flush", "10%3*return(1500)"))
    run_workload(15)

    assert endpoint.log_contains("no response from pageserver for .* s, disconnecting")

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()
