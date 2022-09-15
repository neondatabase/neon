from datetime import timedelta

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.utils import query_scalar


#
# Test pageserver get_lsn_by_timestamp API
#
def test_lsn_mapping(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch("test_lsn_mapping")
    pgmain = env.postgres.create_start("test_lsn_mapping")
    log.info("postgres is running on 'test_lsn_mapping' branch")

    ps_cur = env.pageserver.connect().cursor()
    cur = pgmain.connect().cursor()
    # Create table, and insert rows, each in a separate transaction
    # Disable synchronous_commit to make this initialization go faster.
    #
    # Each row contains current insert LSN and the current timestamp, when
    # the row was inserted.
    cur.execute("SET synchronous_commit=off")
    cur.execute("CREATE TABLE foo (x integer)")
    tbl = []
    for i in range(1000):
        cur.execute(f"INSERT INTO foo VALUES({i})")
        # Get the timestamp at UTC
        after_timestamp = query_scalar(cur, "SELECT clock_timestamp()").replace(tzinfo=None)
        tbl.append([i, after_timestamp])

    # Execute one more transaction with synchronous_commit enabled, to flush
    # all the previous transactions
    cur.execute("INSERT INTO foo VALUES (-1)")

    # Wait until WAL is received by pageserver
    wait_for_last_flush_lsn(env, pgmain, env.initial_tenant, new_timeline_id)

    # Check edge cases: timestamp in the future
    probe_timestamp = tbl[-1][1] + timedelta(hours=1)
    result = query_scalar(
        ps_cur,
        f"get_lsn_by_timestamp {env.initial_tenant} {new_timeline_id} '{probe_timestamp.isoformat()}Z'",
    )
    assert result == "future"

    # timestamp too the far history
    probe_timestamp = tbl[0][1] - timedelta(hours=10)
    result = query_scalar(
        ps_cur,
        f"get_lsn_by_timestamp {env.initial_tenant} {new_timeline_id} '{probe_timestamp.isoformat()}Z'",
    )
    assert result == "past"

    # Probe a bunch of timestamps in the valid range
    for i in range(1, len(tbl), 100):
        probe_timestamp = tbl[i][1]

        # Call get_lsn_by_timestamp to get the LSN
        lsn = query_scalar(
            ps_cur,
            f"get_lsn_by_timestamp {env.initial_tenant} {new_timeline_id} '{probe_timestamp.isoformat()}Z'",
        )

        # Launch a new read-only node at that LSN, and check that only the rows
        # that were supposed to be committed at that point in time are visible.
        pg_here = env.postgres.create_start(
            branch_name="test_lsn_mapping", node_name="test_lsn_mapping_read", lsn=lsn
        )
        assert pg_here.safe_psql("SELECT max(x) FROM foo")[0][0] == i

        pg_here.stop_and_destroy()
