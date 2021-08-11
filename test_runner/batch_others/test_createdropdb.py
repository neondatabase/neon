import os
import pathlib

from contextlib import closing
from fixtures.zenith_fixtures import ZenithPageserver, PostgresFactory, ZenithCli

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test CREATE DATABASE when there have been relmapper changes
#
def test_createdb(
    zenith_cli: ZenithCli,
    pageserver: ZenithPageserver,
    postgres: PostgresFactory,
    pg_bin,
):
    zenith_cli.run(["branch", "test_createdb", "empty"])

    pg = postgres.create_start('test_createdb')
    print("postgres is running on 'test_createdb' branch")

    with closing(pg.connect()) as conn:
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
        db.connect(dbname='foodb').close()

#
# Test DROP DATABASE
#
def test_dropdb(
    zenith_cli: ZenithCli,
    pageserver: ZenithPageserver,
    postgres: PostgresFactory,
    pg_bin,
):
    zenith_cli.run(["branch", "test_dropdb", "empty"])

    pg = postgres.create_start('test_dropdb')
    print("postgres is running on 'test_dropdb' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE DATABASE foodb')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn_before_drop = cur.fetchone()[0]

            cur.execute("SELECT oid FROM pg_database WHERE datname='foodb';")
            dboid = cur.fetchone()[0]


    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP DATABASE foodb')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn_after_drop = cur.fetchone()[0]


    # Create two branches before and after database drop.
    zenith_cli.run(["branch", "test_before_dropdb", "test_dropdb@" + lsn_before_drop])
    pg_before = postgres.create_start('test_before_dropdb')

    zenith_cli.run(["branch", "test_after_dropdb", "test_dropdb@" + lsn_after_drop])
    pg_after = postgres.create_start('test_after_dropdb')

    # Test that database exists on the branch before drop
    pg_before.connect(dbname='foodb').close()

    # Test that database subdir exists on the branch before drop
    dbpath = pathlib.Path(pg_before.pgdata_dir) / 'base' / str(dboid)
    print(dbpath)

    assert os.path.isdir(dbpath) == True

    # Test that database subdir doesn't exist on the branch after drop
    dbpath = pathlib.Path(pg_after.pgdata_dir) / 'base' / str(dboid)
    print(dbpath)

    assert os.path.isdir(dbpath) == False
