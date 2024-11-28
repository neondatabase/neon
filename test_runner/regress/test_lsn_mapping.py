from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.pageserver.http import PageserverApiException
from fixtures.utils import query_scalar, wait_until
from requests.exceptions import ReadTimeout


def assert_lsn_lease_granted(result, with_lease: bool):
    """
    Asserts an LSN lease is granted when `with_lease` flag is turned on.
    Always asserts no LSN lease is granted when `with_lease` flag is off.
    """
    if with_lease:
        assert result.get("valid_until")
    else:
        assert result.get("valid_until") is None


@pytest.mark.parametrize("with_lease", [True, False])
def test_lsn_mapping(neon_env_builder: NeonEnvBuilder, with_lease: bool):
    """
    Test pageserver get_lsn_by_timestamp API.

    :param with_lease: Whether to get a lease associated with returned LSN.
    """
    env = neon_env_builder.init_start()

    tenant_id, _ = env.create_tenant(
        conf={
            # disable default GC and compaction
            "gc_period": "1000 m",
            "compaction_period": "0 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
        }
    )

    timeline_id = env.create_branch("test_lsn_mapping", tenant_id=tenant_id)
    endpoint_main = env.endpoints.create_start("test_lsn_mapping", tenant_id=tenant_id)
    timeline_id = endpoint_main.safe_psql("show neon.timeline_id")[0][0]

    cur = endpoint_main.connect().cursor()

    # Obtain an lsn before all write operations on this branch
    start_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_lsn()"))

    # Create table, and insert rows, each in a separate transaction
    # Disable synchronous_commit to make this initialization go faster.
    # Disable `synchronous_commit` to make this initialization go faster.
    # XXX: on my laptop this test takes 7s, and setting `synchronous_commit=off`
    #      doesn't change anything.
    #
    # Each row contains current insert LSN and the current timestamp, when
    # the row was inserted.
    cur.execute("SET synchronous_commit=off")
    cur.execute("CREATE TABLE foo (x integer)")
    tbl = []
    for i in range(1000):
        cur.execute("INSERT INTO foo VALUES(%s)", (i,))
        # Get the timestamp at UTC
        after_timestamp = query_scalar(cur, "SELECT clock_timestamp()").replace(tzinfo=None)
        tbl.append([i, after_timestamp])

    # Execute one more transaction with synchronous_commit enabled, to flush
    # all the previous transactions
    cur.execute("SET synchronous_commit=on")
    cur.execute("INSERT INTO foo VALUES (-1)")

    # Wait until WAL is received by pageserver
    last_flush_lsn = wait_for_last_flush_lsn(env, endpoint_main, tenant_id, timeline_id)

    with env.pageserver.http_client() as client:
        # Check edge cases
        # Timestamp is in the future
        probe_timestamp = tbl[-1][1] + timedelta(hours=1)
        result = client.timeline_get_lsn_by_timestamp(
            tenant_id, timeline_id, probe_timestamp, with_lease=with_lease
        )
        assert result["kind"] == "future"
        assert_lsn_lease_granted(result, with_lease)
        # make sure that we return a well advanced lsn here
        assert Lsn(result["lsn"]) > start_lsn

        # Timestamp is in the unreachable past
        probe_timestamp = tbl[0][1] - timedelta(hours=10)
        result = client.timeline_get_lsn_by_timestamp(
            tenant_id, timeline_id, probe_timestamp, with_lease=with_lease
        )
        assert result["kind"] == "past"
        assert_lsn_lease_granted(result, with_lease)

        # make sure that we return the minimum lsn here at the start of the range
        assert Lsn(result["lsn"]) < start_lsn

        # Probe a bunch of timestamps in the valid range
        for i in range(1, len(tbl), 100):
            probe_timestamp = tbl[i][1]
            result = client.timeline_get_lsn_by_timestamp(
                tenant_id, timeline_id, probe_timestamp, with_lease=with_lease
            )
            assert result["kind"] not in ["past", "nodata"]
            assert_lsn_lease_granted(result, with_lease)
            lsn = result["lsn"]
            # Call get_lsn_by_timestamp to get the LSN
            # Launch a new read-only node at that LSN, and check that only the rows
            # that were supposed to be committed at that point in time are visible.
            endpoint_here = env.endpoints.create_start(
                branch_name="test_lsn_mapping",
                endpoint_id="ep-lsn_mapping_read",
                lsn=lsn,
                tenant_id=tenant_id,
            )
            assert endpoint_here.safe_psql("SELECT max(x) FROM foo")[0][0] == i

            endpoint_here.stop_and_destroy()

        # Do the "past" check again at a new branch to ensure that we don't return something before the branch cutoff
        timeline_id_child = env.create_branch(
            "test_lsn_mapping_child", ancestor_branch_name="test_lsn_mapping", tenant_id=tenant_id
        )

        # Timestamp is in the unreachable past
        probe_timestamp = tbl[0][1] - timedelta(hours=10)
        result = client.timeline_get_lsn_by_timestamp(
            tenant_id, timeline_id_child, probe_timestamp, with_lease=with_lease
        )
        assert result["kind"] == "past"
        assert_lsn_lease_granted(result, with_lease)
        # make sure that we return the minimum lsn here at the start of the range
        assert Lsn(result["lsn"]) >= last_flush_lsn


def test_get_lsn_by_timestamp_cancelled(neon_env_builder: NeonEnvBuilder):
    """
    Test if cancelled pageserver get_lsn_by_timestamp request is correctly handled.
    Added as an effort to improve error handling and avoid full anyhow backtrace.
    """

    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(
        [
            ".*request was dropped before completing.*",
            ".*Cancelled request finished with an error: Cancelled",
        ]
    )

    client = env.pageserver.http_client()
    failpoint = "find-lsn-for-timestamp-pausable"
    client.configure_failpoints((failpoint, "pause"))

    with ThreadPoolExecutor(max_workers=1) as exec:
        # Request get_lsn_by_timestamp, hit the pausable failpoint
        failing = exec.submit(
            client.timeline_get_lsn_by_timestamp,
            env.initial_tenant,
            env.initial_timeline,
            datetime.now(),
            timeout=2,
        )

        _, offset = wait_until(
            20, 0.5, lambda: env.pageserver.assert_log_contains(f"at failpoint {failpoint}")
        )

        with pytest.raises(ReadTimeout):
            failing.result()

        client.configure_failpoints((failpoint, "off"))

        _, offset = wait_until(
            20,
            0.5,
            lambda: env.pageserver.assert_log_contains(
                "Cancelled request finished with an error: Cancelled$", offset
            ),
        )


# Test pageserver get_timestamp_of_lsn API
def test_ts_of_lsn_api(neon_env_builder: NeonEnvBuilder):
    key_not_found_error = r".*could not find data for key.*"

    env = neon_env_builder.init_start()

    new_timeline_id = env.create_branch("test_ts_of_lsn_api")
    endpoint_main = env.endpoints.create_start("test_ts_of_lsn_api")

    cur = endpoint_main.connect().cursor()
    # Create table, and insert rows, each in a separate transaction
    # Enable synchronous commit as we are timing sensitive
    #
    # Each row contains current insert LSN and the current timestamp, when
    # the row was inserted.
    cur.execute("SET synchronous_commit=on")
    cur.execute("CREATE TABLE foo (x integer)")
    tbl = []
    for i in range(1000):
        cur.execute("INSERT INTO foo VALUES(%s)", (i,))
        # Get the timestamp at UTC
        after_timestamp = query_scalar(cur, "SELECT clock_timestamp()").replace(tzinfo=UTC)
        after_lsn = query_scalar(cur, "SELECT pg_current_wal_lsn()")
        tbl.append([i, after_timestamp, after_lsn])
        time.sleep(0.02)

    # Execute one more transaction with synchronous_commit enabled, to flush
    # all the previous transactions
    cur.execute("SET synchronous_commit=on")
    cur.execute("INSERT INTO foo VALUES (-1)")

    # Wait until WAL is received by pageserver
    last_flush_lsn = wait_for_last_flush_lsn(
        env, endpoint_main, env.initial_tenant, new_timeline_id
    )

    with env.pageserver.http_client() as client:
        # Check edge cases: lsn larger than the last flush lsn
        probe_lsn = Lsn(int(last_flush_lsn) * 20 + 80_000)
        result = client.timeline_get_timestamp_of_lsn(
            env.initial_tenant,
            new_timeline_id,
            probe_lsn,
        )

        # lsn of zero
        try:
            probe_lsn = Lsn(0)
            result = client.timeline_get_timestamp_of_lsn(
                env.initial_tenant,
                new_timeline_id,
                probe_lsn,
            )
            # There should always be an error here.
            raise RuntimeError("there should have been an 'Invalid LSN' error")
        except PageserverApiException as error:
            assert error.status_code == 500
            assert str(error) == "Invalid LSN"
            env.pageserver.allowed_errors.append(".*Invalid LSN.*")

        # small lsn before initdb_lsn
        try:
            probe_lsn = Lsn(64)
            result = client.timeline_get_timestamp_of_lsn(
                env.initial_tenant,
                new_timeline_id,
                probe_lsn,
            )
            # There should always be an error here.
            raise RuntimeError("there should have been an 'could not find data for key' error")
        except PageserverApiException as error:
            assert error.status_code == 500
            assert re.match(key_not_found_error, str(error))
            env.pageserver.allowed_errors.append(key_not_found_error)

        # Probe a bunch of timestamps in the valid range
        step_size = 100
        for i in range(step_size, len(tbl), step_size):
            after_timestamp = tbl[i][1]
            after_lsn = tbl[i][2]
            result = client.timeline_get_timestamp_of_lsn(
                env.initial_tenant,
                new_timeline_id,
                after_lsn,
            )
            log.info("result: %s, after_ts: %s", result, after_timestamp)

            timestamp = datetime.fromisoformat(result).replace(tzinfo=UTC)
            assert timestamp < after_timestamp, "after_timestamp after timestamp"
            if i > 1:
                before_timestamp = tbl[i - step_size][1]
                assert timestamp >= before_timestamp, "before_timestamp before timestamp"
