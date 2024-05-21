import os
import re
import threading
import time
from functools import partial

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    log_replica_lag,
    tenant_get_shards,
    wait_replica_caughtup,
)
from fixtures.utils import wait_until


# Check for corrupted WAL messages which might otherwise go unnoticed if
# reconnection fixes this.
def scan_standby_log_for_errors(secondary):
    log_path = secondary.endpoint_path() / "compute.log"
    with log_path.open("r") as f:
        markers = re.compile(
            r"incorrect resource manager data|record with incorrect|invalid magic number|unexpected pageaddr"
        )
        for line in f:
            if markers.search(line):
                log.info(f"bad error in standby log: {line}")
                raise AssertionError()


def test_hot_standby(neon_simple_env: NeonEnv):
    env = neon_simple_env

    # We've had a bug caused by WAL records split across multiple XLogData
    # messages resulting in corrupted WAL complains on standby. It reproduced
    # only when sending from safekeeper is slow enough to grab full
    # MAX_SEND_SIZE messages. So insert sleep through failpoints, but only in
    # one conf to decrease test time.
    slow_down_send = "[debug-pg16]" in os.environ.get("PYTEST_CURRENT_TEST", "")
    if slow_down_send:
        sk_http = env.safekeepers[0].http_client()
        sk_http.configure_failpoints([("sk-send-wal-replica-sleep", "return(100)")])

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        time.sleep(1)
        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            queries = [
                "SHOW neon.timeline_id",
                "SHOW neon.tenant_id",
                "SELECT relname FROM pg_class WHERE relnamespace = current_schema()::regnamespace::oid",
                "SELECT COUNT(*), SUM(i) FROM test",
            ]
            responses = dict()

            with primary.connect() as p_con:
                with p_con.cursor() as p_cur:
                    p_cur.execute("CREATE TABLE test AS SELECT generate_series(1, 100) AS i")

                for query in queries:
                    with p_con.cursor() as p_cur:
                        p_cur.execute(query)
                        res = p_cur.fetchone()
                        assert res is not None
                        response = res
                        responses[query] = response

                # insert more data to make safekeeper send MAX_SEND_SIZE messages
                if slow_down_send:
                    primary.safe_psql("create table t(key int, value text)")
                    primary.safe_psql("insert into t select generate_series(1, 100000), 'payload'")

            wait_replica_caughtup(primary, secondary)

            with secondary.connect() as s_con:
                with s_con.cursor() as s_cur:
                    s_cur.execute("SELECT 1 WHERE pg_is_in_recovery()")
                    res = s_cur.fetchone()
                    assert res is not None

                for query in queries:
                    with s_con.cursor() as secondary_cursor:
                        secondary_cursor.execute(query)
                        response = secondary_cursor.fetchone()
                        assert response is not None
                        assert response == responses[query]

            scan_standby_log_for_errors(secondary)

    # clean up
    if slow_down_send:
        sk_http.configure_failpoints(("sk-send-wal-replica-sleep", "off"))


def test_2_replicas_start(neon_simple_env: NeonEnv):
    env = neon_simple_env

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        time.sleep(1)
        with env.endpoints.new_replica_start(
            origin=primary, endpoint_id="secondary1"
        ) as secondary1:
            with env.endpoints.new_replica_start(
                origin=primary, endpoint_id="secondary2"
            ) as secondary2:
                wait_replica_caughtup(primary, secondary1)
                wait_replica_caughtup(primary, secondary2)


# Test two different scenarios related to gc of data needed by hot standby.
#
# When pause_apply is False, standby is mostly caught up with the primary.
# However, in compute <-> pageserver protocol version 1 only one LSN had been
# sent to the pageserver in page request, and to avoid waits in the pageserver
# it was last-written LSN cache value. If page hasn't been updated for a long
# time that resulted in an error from the pageserver: "Bad request: tried to
# request a page version that was garbage collected". For primary this wasn't a
# problem because pageserver always bumped LSN to the newest one; for standy
# that would be incorrect since we might get page fresher then apply LSN. Hence,
# in protocol version v2 two LSNs were introduced: main request_lsn (apply LSN
# in case of standby) and not_modified_since which could be used as an
# optimization to avoid waiting.
#
# https://github.com/neondatabase/neon/issues/6211
#
# When pause_apply is True we model standby lagging behind primary (e.g. due to
# high max_standby_streaming_delay). To prevent pageserver from removing data
# still needed by the standby apply LSN is propagated in standby -> safekeepers
# -> broker -> pageserver flow so that pageserver could hold off gc for it.
@pytest.mark.parametrize("pause_apply", [False, True])
def test_hot_standby_gc(neon_env_builder: NeonEnvBuilder, pause_apply: bool):
    tenant_conf = {
        # set PITR interval to be small, so we can do GC
        "pitr_interval": "0 s",
    }
    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)
    timeline_id = env.initial_timeline
    tenant_id = env.initial_tenant

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with env.endpoints.new_replica_start(
            origin=primary,
            endpoint_id="secondary",
            # Protocol version 2 was introduced to fix the issue
            # that this test exercises. With protocol version 1 it
            # fails.
            config_lines=["neon.protocol_version=2"],
        ) as secondary:
            p_cur = primary.connect().cursor()
            p_cur.execute("CREATE EXTENSION neon_test_utils")
            p_cur.execute("CREATE TABLE test (id int primary key) WITH (autovacuum_enabled=false)")
            p_cur.execute("INSERT INTO test SELECT generate_series(1, 10000) AS g")

            wait_replica_caughtup(primary, secondary)

            s_cur = secondary.connect().cursor()

            s_cur.execute("SELECT 1 WHERE pg_is_in_recovery()")
            res = s_cur.fetchone()
            assert res is not None

            s_cur.execute("SELECT COUNT(*) FROM test")
            res = s_cur.fetchone()
            assert res[0] == 10000

            # Clear the cache in the standby, so that when we
            # re-execute the query, it will make GetPage
            # requests. This does not clear the last-written LSN cache
            # so we still remember the LSNs of the pages.
            s_cur.execute("SELECT clear_buffer_cache()")

            if pause_apply:
                s_cur.execute("SELECT pg_wal_replay_pause()")

            # Do other stuff on the primary, to advance the WAL
            p_cur.execute("CREATE TABLE test2 AS SELECT generate_series(1, 1000000) AS g")

            # Run GC. The PITR interval is very small, so this advances the GC cutoff LSN
            # very close to the primary's current insert LSN.
            shards = tenant_get_shards(env, tenant_id, None)
            for tenant_shard_id, pageserver in shards:
                client = pageserver.http_client()
                client.timeline_checkpoint(tenant_shard_id, timeline_id)
                client.timeline_compact(tenant_shard_id, timeline_id)
                client.timeline_gc(tenant_shard_id, timeline_id, 0)

            # Re-execute the query. The GetPage requests that this
            # generates use old not_modified_since LSNs, older than
            # the GC cutoff, but new request LSNs. (In protocol
            # version 1 there was only one LSN, and this failed.)
            log_replica_lag(primary, secondary)
            s_cur.execute("SELECT COUNT(*) FROM test")
            log_replica_lag(primary, secondary)
            res = s_cur.fetchone()
            assert res[0] == 10000
