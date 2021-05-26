import pytest
import os
import psycopg2
import multiprocessing

pytest_plugins = ("fixtures.zenith_fixtures")


def test_multixact(pageserver, postgres, pg_bin, zenith_cli, base_dir):

    # Create a branch for us
    zenith_cli.run(["branch", "test_multixact", "empty"])
    pg = postgres.create_start('test_multixact')

    print("postgres is running on 'test_multixact' branch")
    pg_conn = psycopg2.connect(pg.connstr())
    cur = pg_conn.cursor()

    cur.execute('CREATE TABLE t1(i int primary key);'
    'INSERT INTO t1 select * from generate_series(1,10);')
    pg_conn.commit()

    cur.execute('SELECT next_multixact_id FROM pg_control_checkpoint();')
    old_multixact_id = cur.fetchone()[0]

    # Lock entries in parallel connections to set multixact
    cur.execute('select * from t1 for key share') 

    pg_conn2 = psycopg2.connect(pg.connstr())
    cur2 = pg_conn2.cursor()
    cur2.execute('select * from t1 for key share') 

    pg_conn.commit()
    pg_conn2.commit()

    # force wal flush
    cur.execute('checkpoint')
    pg_conn.commit()

    cur.execute('SELECT next_multixact_id, next_multi_offset,'
                'pg_current_wal_flush_lsn() FROM pg_control_checkpoint();')
    res = cur.fetchone()
    next_multixact_id = res[0]
    next_multi_offset = res[1]
    lsn = res[2]

    # Ensure that we did lock some tuples
    assert(int(next_multixact_id) > int(old_multixact_id))

    # Branch at this point
    zenith_cli.run(["branch", "test_multixact_new", "test_multixact@"+lsn]);
    pg_new = postgres.create_start('test_multixact_new')

    print("postgres is running on 'test_multixact_new' branch")
    pg_new_conn = psycopg2.connect(pg_new.connstr())
    pg_new_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur_new = pg_new_conn.cursor()

    cur_new.execute('SELECT next_multixact_id, next_multi_offset  FROM pg_control_checkpoint();')
    res = cur_new.fetchone()
    next_multixact_id_new = res[0]
    next_multi_offset_new = res[1]

    # Check that we restored pg_controlfile field correctly
    assert(next_multixact_id_new == next_multixact_id)
    assert(next_multi_offset_new == next_multi_offset)
