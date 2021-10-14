import pytest
import random
import time

from contextlib import closing
from multiprocessing import Process, Value
from fixtures.zenith_fixtures import WalAcceptorFactory, ZenithPageserver, PostgresFactory
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")

# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_pageserver_restart(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, wa_factory: WalAcceptorFactory):

    # One safekeeper is enough for this test.
    wa_factory.start_n_new(1)

    zenith_cli.run(["branch", "test_pageserver_restart", "empty"])
    pg = postgres.create_start('test_pageserver_restart',
                               wal_acceptors=wa_factory.get_connstrs())

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
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}");
    assert int(row[0]) < int(row[1])

    # Stop and restart pageserver. This is a more or less graceful shutdown, although
    # the page server doesn't currently have a shutdown routine so there's no difference
    # between stopping and crashing.
    pageserver.stop();
    pageserver.start();

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
    pageserver.stop();
    pageserver.start();

