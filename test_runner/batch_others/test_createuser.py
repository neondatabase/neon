from contextlib import closing

from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli.create_branch('test_createuser', 'empty')
    pg = env.postgres.create_start('test_createuser')
    log.info("postgres is running on 'test_createuser' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # Cause a 'relmapper' change in the original branch
            cur.execute('CREATE USER testuser with password %s', ('testpwd', ))

            cur.execute('CHECKPOINT')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]

    # Create a branch
    env.zenith_cli.create_branch('test_createuser2', 'test_createuser', ancestor_start_lsn=lsn)
    pg2 = env.postgres.create_start('test_createuser2')

    # Test that you can connect to new branch as a new user
    assert pg2.safe_psql('select current_user', user='testuser') == [('testuser', )]
