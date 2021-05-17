import pytest
import getpass
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Create a couple of branches off the main branch, at a historical point in time.
#
def test_branch_behind(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run_init()
    pageserver.start()
    print('pageserver is running')

    pgmain = postgres.create_start()
    print('postgres is running on main branch')

    main_pg_conn = pgmain.connect();
    main_pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    main_cur = main_pg_conn.cursor()

    # Create table, and insert the first 100 rows
    main_cur.execute('CREATE TABLE foo (t text)');
    main_cur.execute("INSERT INTO foo SELECT 'long string to consume some space' || g FROM generate_series(1, 100) g");
    main_cur.execute('SELECT pg_current_wal_insert_lsn()');
    lsn_a = main_cur.fetchone()[0]
    print('LSN after 100 rows: ' + lsn_a)

    # Insert some more rows. (This generates enough WAL to fill a few segments.)
    main_cur.execute("INSERT INTO foo SELECT 'long string to consume some space' || g FROM generate_series(1, 100000) g");
    main_cur.execute('SELECT pg_current_wal_insert_lsn()');
    lsn_b = main_cur.fetchone()[0]
    print('LSN after 100100 rows: ' + lsn_b)

    # Branch at the point where only 100 rows were inserted
    zenith_cli.run(["branch", "hundred", "main@"+lsn_a]);

    # Insert many more rows. This generates enough WAL to fill a few segments.
    main_cur.execute("INSERT INTO foo SELECT 'long string to consume some space' || g FROM generate_series(1, 100000) g");
    main_cur.execute('SELECT pg_current_wal_insert_lsn()');

    main_cur.execute('SELECT pg_current_wal_insert_lsn()');
    lsn_c = main_cur.fetchone()[0]
    print('LSN after 200100 rows: ' + lsn_c)

    # Branch at the point where only 200 rows were inserted
    zenith_cli.run(["branch", "more", "main@"+lsn_b]);

    pg_hundred = postgres.create_start("hundred")
    pg_more = postgres.create_start("more")

    # On the 'hundred' branch, we should see only 100 rows
    hundred_pg_conn = pg_hundred.connect()
    hundred_pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    hundred_cur = hundred_pg_conn.cursor()
    hundred_cur.execute('SELECT count(*) FROM foo');
    assert(hundred_cur.fetchone()[0] == 100);

    # On the 'more' branch, we should see 100200 rows
    more_pg_conn = pg_more.connect()
    more_pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    more_cur = more_pg_conn.cursor()
    more_cur.execute('SELECT count(*) FROM foo');
    assert(more_cur.fetchone()[0] == 100100);

    # All the rows are visible on the main branch
    main_cur.execute('SELECT count(*) FROM foo');
    assert(main_cur.fetchone()[0] == 200100);
