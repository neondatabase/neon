import pytest
import random
import time
import os
import subprocess
import uuid

from contextlib import closing
from multiprocessing import Process, Value
from fixtures.zenith_fixtures import WalAcceptorFactory, ZenithPageserver, PostgresFactory, PgBin
from fixtures.utils import lsn_to_hex, mkdir_if_needed
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


# basic test, write something in setup with wal acceptors, ensure that commits
# succeed and data is written
def test_normal_work(zenith_cli,
                     pageserver: ZenithPageserver,
                     postgres: PostgresFactory,
                     wa_factory):
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
def test_many_timelines(zenith_cli,
                        pageserver: ZenithPageserver,
                        postgres: PostgresFactory,
                        wa_factory):
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
def test_restarts(zenith_cli,
                  pageserver: ZenithPageserver,
                  postgres: PostgresFactory,
                  wa_factory: WalAcceptorFactory):
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
def test_race_conditions(zenith_cli,
                         pageserver: ZenithPageserver,
                         postgres: PostgresFactory,
                         wa_factory,
                         stop_value):

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


class ProposerPostgres:
    """Object for running safekeepers sync with walproposer"""
    def __init__(self, pgdata_dir: str, pg_bin: PgBin, timeline_id: str, tenant_id: str):
        self.pgdata_dir: str = pgdata_dir
        self.pg_bin: PgBin = pg_bin
        self.timeline_id: str = timeline_id
        self.tenant_id: str = tenant_id

    def pg_data_dir_path(self) -> str:
        """ Path to data directory """
        return self.pgdata_dir

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        return os.path.join(self.pgdata_dir, 'postgresql.conf')

    def create_dir_config(self, wal_acceptors: str):
        """ Create dir and config for running --sync-safekeepers """

        mkdir_if_needed(self.pg_data_dir_path())
        with open(self.config_file_path(), "w") as f:
            f.writelines([
                "synchronous_standby_names = 'walproposer'\n",
                f"zenith.zenith_timeline = '{self.timeline_id}'\n",
                f"zenith.zenith_tenant = '{self.tenant_id}'\n",
                f"wal_acceptors = '{wal_acceptors}'\n",
            ])

    def sync_safekeepers(self) -> str:
        """
        Run 'postgres --sync-safekeepers'.
        Returns execution result, which is commit_lsn after sync.
        """

        command = ["postgres", "--sync-safekeepers"]
        env = {
            "PGDATA": self.pg_data_dir_path(),
        }

        basepath = self.pg_bin.run_capture(command, env)
        stdout_filename = basepath + '.stdout'

        with open(stdout_filename, 'r') as stdout_f:
            stdout = stdout_f.read()
            return stdout.strip("\n ")


# insert wal in all safekeepers and run sync on proposer
def test_sync_safekeepers(repo_dir: str, pg_bin: PgBin, wa_factory: WalAcceptorFactory):
    wa_factory.start_n_new(3)

    timeline_id = uuid.uuid4().hex
    tenant_id = uuid.uuid4().hex

    # write config for proposer
    pgdata_dir = os.path.join(repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(pgdata_dir, pg_bin, timeline_id, tenant_id)
    pg.create_dir_config(wa_factory.get_connstrs())

    # valid lsn, which is not in the segment start, nor in zero segment
    epoch_start_lsn = 0x16B9188  # 0/16B9188
    begin_lsn = epoch_start_lsn

    # append and commit WAL
    lsn_after_append = []
    for i in range(3):
        res = wa_factory.instances[i].append_logical_message(
            tenant_id,
            timeline_id,
            {
                "lm_prefix": "prefix",
                "lm_message": "message",
                "set_commit_lsn": True,
                "term": 2,
                "begin_lsn": begin_lsn,
                "epoch_start_lsn": epoch_start_lsn,
                "truncate_lsn": epoch_start_lsn,
            },
        )
        lsn_hex = lsn_to_hex(res["inserted_wal"]["end_lsn"])
        lsn_after_append.append(lsn_hex)
        log.info(f"safekeeper[{i}] lsn after append: {lsn_hex}")

    # run sync safekeepers
    lsn_after_sync = pg.sync_safekeepers()
    log.info(f"lsn after sync = {lsn_after_sync}")

    assert all(lsn_after_sync == lsn for lsn in lsn_after_append)


def test_timeline_status(zenith_cli, pageserver, postgres, wa_factory: WalAcceptorFactory):
    wa_factory.start_n_new(1)

    zenith_cli.run(["branch", "test_timeline_status", "empty"])
    pg = postgres.create_start('test_timeline_status', wal_acceptors=wa_factory.get_connstrs())

    wa = wa_factory.instances[0]
    wa_http_cli = wa.http_client()
    wa_http_cli.check_status()

    # learn zenith timeline from compute
    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    # fetch something sensible from status
    epoch = wa_http_cli.timeline_status(tenant_id, timeline_id).acceptor_epoch

    pg.safe_psql("create table t(i int)")

    # ensure epoch goes up after reboot
    pg.stop().start()
    pg.safe_psql("insert into t values(10)")

    epoch_after_reboot = wa_http_cli.timeline_status(tenant_id, timeline_id).acceptor_epoch
    assert epoch_after_reboot > epoch
