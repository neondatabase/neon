import pytest

from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log


#
# Test restarting and recreating a postgres instance
#
@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_restart_compute(zenith_env_builder: ZenithEnvBuilder, with_wal_acceptors: bool):
    zenith_env_builder.pageserver_auth_enabled = True
    if with_wal_acceptors:
        zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_restart_compute')
    pg = env.postgres.create_start('test_restart_compute')
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
    pg.stop_and_destroy().create_start('test_restart_compute')

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
    pg.stop_and_destroy().create_start('test_restart_compute')

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
    pg.stop_and_destroy().create_start('test_restart_compute')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # We can still see the rows
            cur.execute('SELECT count(*) FROM t')

            r = cur.fetchone()
            assert r == (100001, )
            log.info(f"res = {r}")
