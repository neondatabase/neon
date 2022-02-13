from contextlib import closing

from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    test_createuser_timeline_id = env.zenith_cli.branch_timeline()
    pg = env.postgres.create_start('test_createuser', timeline_id=test_createuser_timeline_id)
    log.info("postgres is running on 'test_createuser' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # Cause a 'relmapper' change in the original branch
            cur.execute('CREATE USER testuser with password %s', ('testpwd', ))

            cur.execute('CHECKPOINT')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]

    # Create a branch
    test_createuser2_timeline_id = env.zenith_cli.branch_timeline(
        ancestor_timeline_id=test_createuser_timeline_id, ancestor_start_lsn=lsn)
    pg2 = env.postgres.create_start('test_createuser2', timeline_id=test_createuser2_timeline_id)

    # Test that you can connect to new branch as a new user
    assert pg2.safe_psql('select current_user', username='testuser') == [('testuser', )]
