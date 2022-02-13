from fixtures.zenith_fixtures import ZenithEnv, check_restored_datadir_content
from fixtures.log_helper import log


#
# Test multixact state after branching
# Now this test is very minimalistic -
# it only checks next_multixact_id field in restored pg_control,
# since we don't have functions to check multixact internals.
#
def test_multixact(zenith_simple_env: ZenithEnv, test_output_dir):
    env = zenith_simple_env
    test_multixact_timeline_id = env.zenith_cli.branch_timeline()
    pg = env.postgres.create_start('test_multixact', timeline_id=test_multixact_timeline_id)

    log.info("postgres is running on 'test_multixact' branch")
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute('''
        CREATE TABLE t1(i int primary key);
        INSERT INTO t1 select * from generate_series(1, 100);
    ''')

    cur.execute('SELECT next_multixact_id FROM pg_control_checkpoint()')
    next_multixact_id_old = cur.fetchone()[0]

    # Lock entries using parallel connections in a round-robin fashion.
    nclients = 20
    connections = []
    for i in range(nclients):
        # Do not turn on autocommit. We want to hold the key-share locks.
        conn = pg.connect(autocommit=False)
        connections.append(conn)

    # On each iteration, we commit the previous transaction on a connection,
    # and issue antoher select. Each SELECT generates a new multixact that
    # includes the new XID, and the XIDs of all the other parallel transactions.
    # This generates enough traffic on both multixact offsets and members SLRUs
    # to cross page boundaries.
    for i in range(5000):
        conn = connections[i % nclients]
        conn.commit()
        conn.cursor().execute('select * from t1 for key share')

    # We have multixacts now. We can close the connections.
    for c in connections:
        c.close()

    # force wal flush
    cur.execute('checkpoint')

    cur.execute(
        'SELECT next_multixact_id, pg_current_wal_insert_lsn() FROM pg_control_checkpoint()')
    res = cur.fetchone()
    next_multixact_id = res[0]
    lsn = res[1]

    # Ensure that we did lock some tuples
    assert int(next_multixact_id) > int(next_multixact_id_old)

    # Branch at this point
    test_multixact_new_timeline_id = env.zenith_cli.branch_timeline(
        ancestor_timeline_id=test_multixact_timeline_id, ancestor_start_lsn=lsn)
    pg_new = env.postgres.create_start('test_multixact_new',
                                       timeline_id=test_multixact_new_timeline_id)

    log.info("postgres is running on 'test_multixact_new' branch")
    pg_new_conn = pg_new.connect()
    cur_new = pg_new_conn.cursor()

    cur_new.execute('SELECT next_multixact_id FROM pg_control_checkpoint()')
    next_multixact_id_new = cur_new.fetchone()[0]

    # Check that we restored pg_controlfile correctly
    assert next_multixact_id_new == next_multixact_id

    # Check that we can restore the content of the datadir correctly
    check_restored_datadir_content(test_output_dir, env, pg)
