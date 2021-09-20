import pytest
import random
import time

from contextlib import closing
from multiprocessing import Process, Value
from fixtures.zenith_fixtures import WalAcceptorFactory, ZenithPageserver, PostgresFactory

pytest_plugins = ("fixtures.zenith_fixtures")


# basic test, write something in setup with wal acceptors, ensure that commits
# succeed and data is written
def test_normal_work(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, wa_factory):
    zenith_cli.run(["branch", "test_wal_acceptors_normal_work", "empty"])
    wa_factory.start_n_new(3)
    pg = postgres.create_start('test_wal_acceptors_normal_work',
                               wal_acceptors=wa_factory.get_connstrs())

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            assert cur.fetchone() == (5000050000, )


# Run page server and multiple acceptors, and multiple compute nodes running
# against different timelines.
def test_many_timelines(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, wa_factory):
    n_timelines = 2

    wa_factory.start_n_new(3)

    branches = ["test_wal_acceptors_many_timelines_{}".format(tlin) for tlin in range(n_timelines)]

    # start postgres on each timeline
    pgs = []
    for branch in branches:
        zenith_cli.run(["branch", branch, "empty"])
        pgs.append(postgres.create_start(branch, wal_acceptors=wa_factory.get_connstrs()))

    # Do everything in different loops to have actions on different timelines
    # interleaved.
    # create schema
    for pg in pgs:
        pg.safe_psql("CREATE TABLE t(key int primary key, value text)")

    # Populate data
    for pg in pgs:
        pg.safe_psql("INSERT INTO t SELECT generate_series(1,100000), 'payload'")

    # Check data
    for pg in pgs:
        res = pg.safe_psql("SELECT sum(key) FROM t")
        assert res[0] == (5000050000, )


# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_restarts(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, wa_factory: WalAcceptorFactory):
    fault_probability = 0.01
    n_inserts = 1000
    n_acceptors = 3

    wa_factory.start_n_new(n_acceptors)

    zenith_cli.run(["branch", "test_wal_acceptors_restarts", "empty"])
    pg = postgres.create_start('test_wal_acceptors_restarts',
                               wal_acceptors=wa_factory.get_connstrs())

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    failed_node = None
    cur.execute('CREATE TABLE t(key int primary key, value text)')
    for i in range(n_inserts):
        cur.execute("INSERT INTO t values (%s, 'payload');", (i + 1, ))

        if random.random() <= fault_probability:
            if failed_node is None:
                failed_node = wa_factory.instances[random.randrange(0, n_acceptors)]
                failed_node.stop()
            else:
                failed_node.start()
                failed_node = None
    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (500500, )


start_delay_sec = 2


def delayed_wal_acceptor_start(wa):
    time.sleep(start_delay_sec)
    wa.start()


# When majority of acceptors is offline, commits are expected to be frozen
def test_unavailability(zenith_cli, postgres: PostgresFactory, wa_factory):
    wa_factory.start_n_new(2)

    zenith_cli.run(["branch", "test_wal_acceptors_unavailability", "empty"])
    pg = postgres.create_start('test_wal_acceptors_unavailability',
                               wal_acceptors=wa_factory.get_connstrs())

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # check basic work with table
    cur.execute('CREATE TABLE t(key int primary key, value text)')
    cur.execute("INSERT INTO t values (1, 'payload')")

    # shutdown one of two acceptors, that is, majority
    wa_factory.instances[0].stop()

    proc = Process(target=delayed_wal_acceptor_start, args=(wa_factory.instances[0], ))
    proc.start()

    start = time.time()
    cur.execute("INSERT INTO t values (2, 'payload')")
    # ensure that the query above was hanging while acceptor was down
    assert (time.time() - start) >= start_delay_sec
    proc.join()

    # for the world's balance, do the same with second acceptor
    wa_factory.instances[1].stop()

    proc = Process(target=delayed_wal_acceptor_start, args=(wa_factory.instances[1], ))
    proc.start()

    start = time.time()
    cur.execute("INSERT INTO t values (3, 'payload')")
    # ensure that the query above was hanging while acceptor was down
    assert (time.time() - start) >= start_delay_sec
    proc.join()

    cur.execute("INSERT INTO t values (4, 'payload')")

    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (10, )


# shut down random subset of acceptors, sleep, wake them up, rinse, repeat
def xmas_garland(acceptors, stop):
    while not bool(stop.value):
        victims = []
        for wa in acceptors:
            if random.random() >= 0.5:
                victims.append(wa)
        for v in victims:
            v.stop()
        time.sleep(1)
        for v in victims:
            v.start()
        time.sleep(1)


# value which gets unset on exit
@pytest.fixture
def stop_value():
    stop = Value('i', 0)
    yield stop
    stop.value = 1


# do inserts while concurrently getting up/down subsets of acceptors
def test_race_conditions(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, wa_factory, stop_value):

    wa_factory.start_n_new(3)

    zenith_cli.run(["branch", "test_wal_acceptors_race_conditions", "empty"])
    pg = postgres.create_start('test_wal_acceptors_race_conditions',
                               wal_acceptors=wa_factory.get_connstrs())

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute('CREATE TABLE t(key int primary key, value text)')

    proc = Process(target=xmas_garland, args=(wa_factory.instances, stop_value))
    proc.start()

    for i in range(1000):
        cur.execute("INSERT INTO t values (%s, 'payload');", (i + 1, ))

    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (500500, )

    stop_value.value = 1
    proc.join()

# insert wal in all safekeepers and run sync on proposer
def test_json_ctrl_sync(zenith_cli, postgres: PostgresFactory, wa_factory: WalAcceptorFactory):
    wa_factory.start_n_new(3)

    zenith_cli.run(["branch", "test_json_ctrl_sync", "empty"])
    pg = postgres.create_start('test_json_ctrl_sync',
                               wal_acceptors=wa_factory.get_connstrs())

    tenant_id = postgres.initial_tenant

    # fetch timeline_id for safekeeper connection
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline_id = cur.fetchone()[0]

    # stop postgres to modify WAL on safekeepers
    pg.stop()
    
    wa_factory.instances[0].append_logical_message(tenant_id, timeline_id, {
        'prefix': 'prefix',
        'message': 'message',
    })


def test_do_not_commit(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, wa_factory):
    wa_factory.start_n_new(3)

    zenith_cli.run(["branch", "test_do_not_commit", "empty"])
    pg = postgres.create_start('test_do_not_commit',
                               wal_acceptors=wa_factory.get_connstrs())

    print(pg.connstr(dbname='postgres'))

    time.sleep(5 * 60)
