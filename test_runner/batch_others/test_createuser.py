from contextlib import closing

from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver

import logging
import fixtures.log_helper  # configures loggers
log = logging.getLogger('root')

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, pg_bin):
    zenith_cli.run(["branch", "test_createuser", "empty"])

    pg = postgres.create_start('test_createuser')
    log.info("postgres is running on 'test_createuser' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # Cause a 'relmapper' change in the original branch
            cur.execute('CREATE USER testuser with password %s', ('testpwd', ))

            cur.execute('CHECKPOINT')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]

    # Create a branch
    zenith_cli.run(["branch", "test_createuser2", "test_createuser@" + lsn])

    pg2 = postgres.create_start('test_createuser2')

    # Test that you can connect to new branch as a new user
    assert pg2.safe_psql('select current_user', username='testuser') == [('testuser', )]
