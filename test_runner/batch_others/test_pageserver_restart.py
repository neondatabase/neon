import pytest
import random
import time

from contextlib import closing
from multiprocessing import Process, Value
from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_pageserver_restart(zenith_env_builder: ZenithEnvBuilder):
    # One safekeeper is enough for this test.
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_pageserver_restart", "main"])
    pg = env.postgres.create_start('test_pageserver_restart')

    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute('CREATE TABLE foo (t text)')
    cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    ''')

    # Verify that the table is larger than shared_buffers
    cur.execute('''
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_ize
        from pg_settings where name = 'shared_buffers'
    ''')
    row = cur.fetchone()
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    # Stop and restart pageserver. This is a more or less graceful shutdown, although
    # the page server doesn't currently have a shutdown routine so there's no difference
    # between stopping and crashing.
    env.pageserver.stop()
    env.pageserver.start()

    # Stopping the pageserver breaks the connection from the postgres backend to
    # the page server, and causes the next query on the connection to fail. Start a new
    # postgres connection too, to avoid that error. (Ideally, the compute node would
    # handle that and retry internally, without propagating the error to the user, but
    # currently it doesn't...)
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000, )

    # Stop the page server by force, and restart it
    env.pageserver.stop()
    env.pageserver.start()
