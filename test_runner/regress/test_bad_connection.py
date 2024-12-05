from __future__ import annotations

import asyncio
import random
import time

import psycopg2.errors
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.pageserver_mitm import BreakConnectionException, PageserverProxy
from fixtures.port_distributor import PortDistributor


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
def test_compute_pageserver_connection_stress2(
        neon_env_builder: NeonEnvBuilder, port_distributor: PortDistributor,
        pg_bin: PgBin
):
    env = neon_env_builder.init_start()

    # Set up the MITM proxy

    error_fraction = 0

    async def response_cb(conn_id):
        global error_fraction
        if random.random() < error_fraction:
            raise BreakConnectionException("unlucky")

    mitm_listen_port = port_distributor.get_port()
    mitm = PageserverProxy(mitm_listen_port, env.pageserver.service_port.pg, response_cb)

    def main():
        global error_fraction
        endpoint = env.endpoints.create(
            "main",
            config_lines=[
                "max_connections=1000",
            ])
        endpoint.start()

        with open(endpoint.pg_data_dir_path() / "postgresql.conf", "a") as conf:
            conf.write(
                f"neon.pageserver_connstring='postgres://localhost:{mitm_listen_port}'  # MITM proxy\n"
            )

        pg_conn = endpoint.connect()
        cur = pg_conn.cursor()

        cur.execute("select pg_reload_conf()")

        scale = 5
        connstr = endpoint.connstr()
        log.info(f"Start a pgbench workload on pg {connstr}")

        error_fraction=0.001

        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", f"-s{scale}", connstr])
        error_fraction=0.01
        pg_bin.run_capture(["pgbench", "-S", "-c80", "-j4", "-P1", "-T60", connstr])

        mitm.shutdown()

    async def mm():
        await asyncio.gather(
            asyncio.to_thread(main),
            mitm.run_server()
        )

    asyncio.run(mm())

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()
