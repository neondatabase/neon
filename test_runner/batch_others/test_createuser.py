import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_createuser", "empty"])

    pg = postgres.create_start('test_createuser')
    print("postgres is running on 'test_createuser' branch")

    with psycopg2.connect(pg.connstr()) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

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
    conn2 = psycopg2.connect(pg2.connstr(username='testuser'))
    with conn2.cursor() as cur:
        cur.execute('select current_user;')
        assert cur.fetchone() == ('testuser', )
