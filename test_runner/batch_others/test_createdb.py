import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test CREATE DATABASE when there have been relmapper changes
#
def test_createdb(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_createdb", "empty"])

    pg = postgres.create_start('test_createdb')
    print("postgres is running on 'test_createdb' branch")

    with psycopg2.connect(pg.connstr()) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cur:
            # Cause a 'relmapper' change in the original branch
            cur.execute('VACUUM FULL pg_class')

            cur.execute('CREATE DATABASE foodb')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]

    # Create a branch
    zenith_cli.run(["branch", "test_createdb2", "test_createdb@" + lsn])

    pg2 = postgres.create_start('test_createdb2')

    # Test that you can connect to the new database on both branches
    for db in (pg, pg2):
        psycopg2.connect(db.connstr('foodb')).close()
