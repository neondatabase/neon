import pytest

from contextlib import closing
from fixtures.zenith_fixtures import ZenithPageserver, PostgresFactory
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test restarting and recreating a postgres instance
#
@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_restart_compute(
        zenith_cli,
        pageserver: ZenithPageserver,
        postgres: PostgresFactory,
        pg_bin,
        wa_factory,
        with_wal_acceptors: bool,
    ):
    wal_acceptor_connstrs = None
    zenith_cli.run(["branch", "test_restart_compute", "empty"])

    if with_wal_acceptors:
        wa_factory.start_n_new(3)
        wal_acceptor_connstrs = wa_factory.get_connstrs()

    pg = postgres.create_start('test_restart_compute',
                               wal_acceptors=wal_acceptor_connstrs)
    log.info("postgres is running on 'test_restart_compute' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            r = cur.fetchone()
            assert r == (5000050000, )
            log.info(f"res = {r}")

    # Remove data directory and restart
    pg.stop_and_destroy().create_start('test_restart_compute',
                                       wal_acceptors=wal_acceptor_connstrs)


    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # We can still see the row
            cur.execute('SELECT sum(key) FROM t')
            r = cur.fetchone()
            assert r == (5000050000, )
            log.info(f"res = {r}")

            # Insert another row
            cur.execute("INSERT INTO t VALUES (100001, 'payload2')")
            cur.execute('SELECT count(*) FROM t')

            r = cur.fetchone()
            assert r == (100001, )
            log.info(f"res = {r}")

    # Again remove data directory and restart
    pg.stop_and_destroy().create_start('test_restart_compute',
                                       wal_acceptors=wal_acceptor_connstrs)

    # That select causes lots of FPI's and increases probability of wakeepers
    # lagging behind after query completion
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # We can still see the rows
            cur.execute('SELECT count(*) FROM t')

            r = cur.fetchone()
            assert r == (100001, )
            log.info(f"res = {r}")

    # And again remove data directory and restart
    pg.stop_and_destroy().create_start('test_restart_compute',
                                       wal_acceptors=wal_acceptor_connstrs)

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # We can still see the rows
            cur.execute('SELECT count(*) FROM t')

            r = cur.fetchone()
            assert r == (100001, )
            log.info(f"res = {r}")
