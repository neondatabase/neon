from fixtures.zenith_fixtures import ZenithEnv, check_restored_datadir_content
from fixtures.log_helper import log


# Test subtransactions
#
# The pg_subxact SLRU is not preserved on restarts, and doesn't need to be
# maintained in the pageserver, so subtransactions are not very exciting for
# Zenith. They are included in the commit record though and updated in the
# CLOG.
def test_subxacts(zenith_simple_env: ZenithEnv, test_output_dir):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_subxacts", "empty"])
    pg = env.postgres.create_start('test_subxacts')

    log.info("postgres is running on 'test_subxacts' branch")
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute('''
        CREATE TABLE t1(i int, j int);
    ''')

    cur.execute('select pg_switch_wal();')

    # Issue 100 transactions, with 1000 subtransactions in each.
    for i in range(100):
        cur.execute('begin')
        for j in range(1000):
            cur.execute(f'savepoint sp{j}')
            cur.execute(f'insert into t1 values ({i}, {j})')
        cur.execute('commit')

    # force wal flush
    cur.execute('checkpoint')

    # Check that we can restore the content of the datadir correctly
    check_restored_datadir_content(test_output_dir, env, pg)
