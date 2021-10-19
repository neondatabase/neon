from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver, check_restored_datadir_content
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test multixact state after branching
# Now this test is very minimalistic -
# it only checks next_multixact_id field in restored pg_control,
# since we don't have functions to check multixact internals.
#
def test_multixact(pageserver: ZenithPageserver,
                   postgres: PostgresFactory,
                   pg_bin,
                   zenith_cli,
                   base_dir,
                   test_output_dir):
    # Create a branch for us
    zenith_cli.run(["branch", "test_multixact", "empty"])
    pg = postgres.create_start('test_multixact')

    log.info("postgres is running on 'test_multixact' branch")
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute('''
        CREATE TABLE t1(i int primary key);
        INSERT INTO t1 select * from generate_series(1, 100);
    ''')

    cur.execute('SELECT next_multixact_id FROM pg_control_checkpoint()')
    next_multixact_id_old = cur.fetchone()[0]

    # Lock entries in parallel connections to set multixact
    nclients = 3
    connections = []
    for i in range(nclients):
        # Do not turn on autocommit. We want to hold the key-share locks.
        conn = pg.connect(autocommit=False)
        conn.cursor().execute('select * from t1 for key share')
        connections.append(conn)

    # We should have a multixact now. We can close the connections.
    for c in connections:
        c.close()

    # force wal flush
    cur.execute('checkpoint')

    cur.execute('SELECT next_multixact_id, pg_current_wal_flush_lsn() FROM pg_control_checkpoint()')
    res = cur.fetchone()
    next_multixact_id = res[0]
    lsn = res[1]

    # Ensure that we did lock some tuples
    assert int(next_multixact_id) > int(next_multixact_id_old)

    # Branch at this point
    zenith_cli.run(["branch", "test_multixact_new", "test_multixact@" + lsn])
    pg_new = postgres.create_start('test_multixact_new')

    log.info("postgres is running on 'test_multixact_new' branch")
    pg_new_conn = pg_new.connect()
    cur_new = pg_new_conn.cursor()

    cur_new.execute('SELECT next_multixact_id FROM pg_control_checkpoint()')
    next_multixact_id_new = cur_new.fetchone()[0]

    # Check that we restored pg_controlfile correctly
    assert next_multixact_id_new == next_multixact_id

    # Check that we restore the content of the datadir correctly
    check_restored_datadir_content(zenith_cli, test_output_dir, pg_new, pageserver.service_port.pg)
