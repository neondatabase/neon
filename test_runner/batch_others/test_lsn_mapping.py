from contextlib import closing
from datetime import timedelta, timezone, tzinfo
import math
from uuid import UUID
import psycopg2.extras
import psycopg2.errors
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, Postgres
from fixtures.log_helper import log
import time


#
# Test pageserver get_lsn_by_timestamp API
#
def test_lsn_mapping(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch('test_lsn_mapping')
    pgmain = env.postgres.create_start("test_lsn_mapping")
    log.info("postgres is running on 'test_lsn_mapping' branch")

    ps_conn = env.pageserver.connect()
    ps_cur = ps_conn.cursor()
    conn = pgmain.connect()
    cur = conn.cursor()

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
        cur.execute(f'SELECT clock_timestamp()')
        # Get the timestamp at UTC
        after_timestamp = cur.fetchone()[0].replace(tzinfo=None)
        tbl.append([i, after_timestamp])

    # Execute one more transaction with synchronous_commit enabled, to flush
    # all the previous transactions
    cur.execute("SET synchronous_commit=on")
    cur.execute("INSERT INTO foo VALUES (-1)")

    # Check edge cases: timestamp in the future
    probe_timestamp = tbl[-1][1] + timedelta(hours=1)
    ps_cur.execute(
        f"get_lsn_by_timestamp {env.initial_tenant.hex} {new_timeline_id.hex} '{probe_timestamp.isoformat()}Z'"
    )
    result = ps_cur.fetchone()[0]
    assert result == 'future'

    # timestamp too the far history
    probe_timestamp = tbl[0][1] - timedelta(hours=10)
    ps_cur.execute(
        f"get_lsn_by_timestamp {env.initial_tenant.hex} {new_timeline_id.hex} '{probe_timestamp.isoformat()}Z'"
    )
    result = ps_cur.fetchone()[0]
    assert result == 'past'

    # Probe a bunch of timestamps in the valid range
    for i in range(1, len(tbl), 100):
        probe_timestamp = tbl[i][1]

        # Call get_lsn_by_timestamp to get the LSN
        ps_cur.execute(
            f"get_lsn_by_timestamp {env.initial_tenant.hex} {new_timeline_id.hex} '{probe_timestamp.isoformat()}Z'"
        )
        lsn = ps_cur.fetchone()[0]

        # Launch a new read-only node at that LSN, and check that only the rows
        # that were supposed to be committed at that point in time are visible.
        pg_here = env.postgres.create_start(branch_name='test_lsn_mapping',
                                            node_name='test_lsn_mapping_read',
                                            lsn=lsn)
        with closing(pg_here.connect()) as conn_here:
            with conn_here.cursor() as cur_here:
                cur_here.execute("SELECT max(x) FROM foo")
                assert cur_here.fetchone()[0] == i

        pg_here.stop_and_destroy()
