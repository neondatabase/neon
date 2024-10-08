from __future__ import annotations

from fixtures.neon_fixtures import NeonEnv, check_restored_datadir_content
from fixtures.utils import query_scalar


#
# Test multixact state after branching
# Now this test is very minimalistic -
# it only checks next_multixact_id field in restored pg_control,
# since we don't have functions to check multixact internals.
# We do check that the datadir contents exported from the
# pageserver match what the running PostgreSQL produced. This
# is enough to verify that the WAL records are handled correctly
# in the pageserver.
#
def test_multixact(neon_simple_env: NeonEnv, test_output_dir):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    cur = endpoint.connect().cursor()
    cur.execute(
        """
        CREATE TABLE t1(i int primary key, n_updated int);
        INSERT INTO t1 select g, 0 from generate_series(1, 50) g;
    """
    )

    next_multixact_id_old = query_scalar(
        cur, "SELECT next_multixact_id FROM pg_control_checkpoint()"
    )

    # Lock entries using parallel connections in a round-robin fashion.
    nclients = 20
    update_every = 97
    connections = []
    for _ in range(nclients):
        # Do not turn on autocommit. We want to hold the key-share locks.
        conn = endpoint.connect(autocommit=False)
        connections.append(conn)

    # On each iteration, we commit the previous transaction on a connection,
    # and issue another select. Each SELECT generates a new multixact that
    # includes the new XID, and the XIDs of all the other parallel transactions.
    # This generates enough traffic on both multixact offsets and members SLRUs
    # to cross page boundaries.
    for i in range(20000):
        conn = connections[i % nclients]
        conn.commit()

        # Perform some non-key UPDATEs too, to exercise different multixact
        # member statuses.
        if i % update_every == 0:
            conn.cursor().execute(f"update t1 set n_updated = n_updated + 1 where i = {i % 50}")
        else:
            conn.cursor().execute("select * from t1 for key share")

    # We have multixacts now. We can close the connections.
    for c in connections:
        c.close()

    # force wal flush
    cur.execute("checkpoint")

    cur.execute(
        "SELECT next_multixact_id, pg_current_wal_insert_lsn() FROM pg_control_checkpoint()"
    )
    res = cur.fetchone()
    assert res is not None
    next_multixact_id = res[0]
    lsn = res[1]

    # Ensure that we did lock some tuples
    assert int(next_multixact_id) > int(next_multixact_id_old)

    # Branch at this point
    env.create_branch("test_multixact_new", ancestor_branch_name="main", ancestor_start_lsn=lsn)
    endpoint_new = env.endpoints.create_start("test_multixact_new")

    next_multixact_id_new = endpoint_new.safe_psql(
        "SELECT next_multixact_id FROM pg_control_checkpoint()"
    )[0][0]

    # Check that we restored pg_controlfile correctly
    assert next_multixact_id_new == next_multixact_id

    # Check that we can restore the content of the datadir correctly
    check_restored_datadir_content(test_output_dir, env, endpoint)
