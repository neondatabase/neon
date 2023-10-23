import time
from datetime import datetime, timedelta, timezone

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.pageserver.http import PageserverApiException
from fixtures.types import Lsn
from fixtures.utils import query_scalar


#
# Test pageserver get_lsn_by_timestamp API
#
def test_lsn_mapping(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch("test_lsn_mapping")
    endpoint_main = env.endpoints.create_start("test_lsn_mapping")
    log.info("postgres is running on 'test_lsn_mapping' branch")

    cur = endpoint_main.connect().cursor()
    # Create table, and insert rows, each in a separate transaction
    # Disable synchronous_commit to make this initialization go faster.
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
    wait_for_last_flush_lsn(env, endpoint_main, env.initial_tenant, new_timeline_id)

    with env.pageserver.http_client() as client:
        # Check edge cases: timestamp in the future
        probe_timestamp = tbl[-1][1] + timedelta(hours=1)
        result = client.timeline_get_lsn_by_timestamp(
            env.initial_tenant, new_timeline_id, f"{probe_timestamp.isoformat()}Z"
        )
        assert result == "future"

        # timestamp too the far history
        probe_timestamp = tbl[0][1] - timedelta(hours=10)
        result = client.timeline_get_lsn_by_timestamp(
            env.initial_tenant, new_timeline_id, f"{probe_timestamp.isoformat()}Z"
        )
        assert result == "past"

        # Probe a bunch of timestamps in the valid range
        for i in range(1, len(tbl), 100):
            probe_timestamp = tbl[i][1]
            lsn = client.timeline_get_lsn_by_timestamp(
                env.initial_tenant, new_timeline_id, f"{probe_timestamp.isoformat()}Z"
            )
            # Call get_lsn_by_timestamp to get the LSN
            # Launch a new read-only node at that LSN, and check that only the rows
            # that were supposed to be committed at that point in time are visible.
            endpoint_here = env.endpoints.create_start(
                branch_name="test_lsn_mapping", endpoint_id="ep-lsn_mapping_read", lsn=lsn
            )
            assert endpoint_here.safe_psql("SELECT max(x) FROM foo")[0][0] == i

            endpoint_here.stop_and_destroy()


# Test pageserver get_timestamp_of_lsn API
def test_ts_of_lsn_api(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch("test_ts_of_lsn_api")
    endpoint_main = env.endpoints.create_start("test_ts_of_lsn_api")
    log.info("postgres is running on 'test_ts_of_lsn_api' branch")

    cur = endpoint_main.connect().cursor()
    # Create table, and insert rows, each in a separate transaction
    # Disable synchronous_commit to make this initialization go faster.
    #
    # Each row contains current insert LSN and the current timestamp, when
    # the row was inserted.
    cur.execute("SET synchronous_commit=off")
    cur.execute("CREATE TABLE foo (x integer)")
    tbl = []
    for i in range(1000):
        cur.execute("INSERT INTO foo VALUES(%s)", (i,))
        # Get the timestamp at UTC
        after_timestamp = query_scalar(cur, "SELECT clock_timestamp()").replace(tzinfo=timezone.utc)
        after_lsn = query_scalar(cur, "SELECT pg_current_wal_lsn()")
        tbl.append([i, after_timestamp, after_lsn])
        time.sleep(0.005)

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
            assert str(error).startswith("could not find data for key")
            env.pageserver.allowed_errors.append(".*could not find data for key.*")

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

            # TODO use fromisoformat once we have Python 3.11+
            # which has https://github.com/python/cpython/pull/92177
            timestamp = datetime.strptime(result, "%Y-%m-%dT%H:%M:%S.%f000Z").replace(
                tzinfo=timezone.utc
            )
            assert timestamp < after_timestamp, "after_timestamp after timestamp"
            if i > 1:
                before_timestamp = tbl[i - step_size][1]
                assert timestamp >= before_timestamp, "before_timestamp before timestamp"
