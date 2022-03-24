from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log


#
# Test where Postgres generates a lot of WAL, and it's garbage collected away, but
# no pages are evicted so that Postgres uses an old LSN in a GetPage request.
# We had a bug where the page server failed to find the page version because it
# thought it was already garbage collected away, because the LSN in the GetPage
# request was very old and the WAL from that time was indeed already removed.
# In reality, the LSN on a GetPage request coming from a primary server is
# just a hint that the page hasn't been modified since that LSN, and the page
# server should return the latest page version regardless of the LSN.
#
def test_old_request_lsn(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli.create_branch("test_old_request_lsn", "empty")
    pg = env.postgres.create_start('test_old_request_lsn')
    log.info('postgres is running on test_old_request_lsn branch')

    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    cur.execute("SHOW zenith.zenith_timeline")
    timeline = cur.fetchone()[0]

    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers.
    cur.execute('CREATE TABLE foo (id int4 PRIMARY KEY, val int, t text)')
    cur.execute('''
        INSERT INTO foo
            SELECT g, 1, 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    ''')

    # Verify that the table is larger than shared_buffers, so that the SELECT below
    # will cause GetPage requests.
    cur.execute('''
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_ize
        from pg_settings where name = 'shared_buffers'
    ''')
    row = cur.fetchone()
    log.info(f'shared_buffers is {row[0]}, table size {row[1]}')
    assert int(row[0]) < int(row[1])

    cur.execute('VACUUM foo')

    # Make a lot of updates on a single row, generating a lot of WAL. Trigger
    # garbage collections so that the page server will remove old page versions.
    for i in range(10):
        pscur.execute(f"do_gc {env.initial_tenant.hex} {timeline} 0")
        for j in range(100):
            cur.execute('UPDATE foo SET val = val + 1 WHERE id = 1;')

    # All (or at least most of) the updates should've been on the same page, so
    # that we haven't had to evict any dirty pages for a long time. Now run
    # a query that sends GetPage@LSN requests with the old LSN.
    cur.execute("SELECT COUNT(*), SUM(val) FROM foo")
    assert cur.fetchone() == (100000, 101000)
