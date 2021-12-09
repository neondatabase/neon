import pytest
import random
import time
import os
import subprocess
import sys
import threading
import uuid

from contextlib import closing
from dataclasses import dataclass, field
from multiprocessing import Process, Value
from fixtures.zenith_fixtures import PgBin, ZenithEnv, ZenithEnvBuilder
from fixtures.utils import lsn_to_hex, mkdir_if_needed
from fixtures.log_helper import log
from typing import List, Optional

pytest_plugins = ("fixtures.zenith_fixtures")


# basic test, write something in setup with wal acceptors, ensure that commits
# succeed and data is written
def test_normal_work(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_wal_acceptors_normal_work", "main"])

    pg = env.postgres.create_start('test_wal_acceptors_normal_work')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            assert cur.fetchone() == (5000050000, )


@dataclass
class BranchMetrics:
    name: str
    latest_valid_lsn: int
    # One entry per each Safekeeper, order is the same
    flush_lsns: List[int] = field(default_factory=list)
    commit_lsns: List[int] = field(default_factory=list)


# Run page server and multiple acceptors, and multiple compute nodes running
# against different timelines.
def test_many_timelines(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    n_timelines = 3

    branches = ["test_wal_acceptors_many_timelines_{}".format(tlin) for tlin in range(n_timelines)]

    # start postgres on each timeline
    pgs = []
    for branch in branches:
        env.zenith_cli(["branch", branch, "main"])
        pgs.append(env.postgres.create_start(branch))

    tenant_id = uuid.UUID(env.initial_tenant)

    def collect_metrics(message: str) -> List[BranchMetrics]:
        with env.pageserver.http_client() as pageserver_http:
            branch_details = [
                pageserver_http.branch_detail(tenant_id=tenant_id, name=branch)
                for branch in branches
            ]
        # All changes visible to pageserver (latest_valid_lsn) should be
        # confirmed by safekeepers first. As we cannot atomically get
        # state of both pageserver and safekeepers, we should start with
        # pageserver. Looking at outdated data from pageserver is ok.
        # Asking safekeepers first is not ok because new commits may arrive
        # to both safekeepers and pageserver after we've already obtained
        # safekeepers' state, it will look contradictory.
        sk_metrics = [sk.http_client().get_metrics() for sk in env.safekeepers]

        branch_metrics = []
        with env.pageserver.http_client() as pageserver_http:
            for branch_detail in branch_details:
                timeline_id: str = branch_detail["timeline_id"]

                m = BranchMetrics(
                    name=branch_detail["name"],
                    latest_valid_lsn=branch_detail["latest_valid_lsn"],
                )
                for sk_m in sk_metrics:
                    m.flush_lsns.append(sk_m.flush_lsn_inexact[timeline_id])
                    m.commit_lsns.append(sk_m.commit_lsn_inexact[timeline_id])

                for flush_lsn, commit_lsn in zip(m.flush_lsns, m.commit_lsns):
                    # Invariant. May be < when transaction is in progress.
                    assert commit_lsn <= flush_lsn
                # We only call collect_metrics() after a transaction is confirmed by
                # the compute node, which only happens after a consensus of safekeepers
                # has confirmed the transaction. We assume majority consensus here.
                assert (2 * sum(m.latest_valid_lsn <= lsn
                                for lsn in m.flush_lsns) > zenith_env_builder.num_safekeepers)
                assert (2 * sum(m.latest_valid_lsn <= lsn
                                for lsn in m.commit_lsns) > zenith_env_builder.num_safekeepers)
                branch_metrics.append(m)
        log.info(f"{message}: {branch_metrics}")
        return branch_metrics

    # TODO: https://github.com/zenithdb/zenith/issues/809
    # collect_metrics("before CREATE TABLE")

    # Do everything in different loops to have actions on different timelines
    # interleaved.
    # create schema
    for pg in pgs:
        pg.safe_psql("CREATE TABLE t(key int primary key, value text)")
    init_m = collect_metrics("after CREATE TABLE")

    # Populate data for 2/3 branches
    class MetricsChecker(threading.Thread):
        def __init__(self) -> None:
            super().__init__(daemon=True)
            self.should_stop = threading.Event()
            self.exception: Optional[BaseException] = None

        def run(self) -> None:
            try:
                while not self.should_stop.is_set():
                    collect_metrics("during INSERT INTO")
                    time.sleep(1)
            except:
                log.error("MetricsChecker's thread failed, the test will be failed on .stop() call",
                          exc_info=True)
                # We want to preserve traceback as well as the exception
                exc_type, exc_value, exc_tb = sys.exc_info()
                assert exc_type
                e = exc_type(exc_value)
                e.__traceback__ = exc_tb
                self.exception = e

        def stop(self) -> None:
            self.should_stop.set()
            self.join()
            if self.exception:
                raise self.exception

    metrics_checker = MetricsChecker()
    metrics_checker.start()

    for pg in pgs[:-1]:
        pg.safe_psql("INSERT INTO t SELECT generate_series(1,100000), 'payload'")

    metrics_checker.stop()

    collect_metrics("after INSERT INTO")

    # Check data for 2/3 branches
    for pg in pgs[:-1]:
        res = pg.safe_psql("SELECT sum(key) FROM t")
        assert res[0] == (5000050000, )

    final_m = collect_metrics("after SELECT")
    # Assume that LSNs (a) behave similarly in all branches; and (b) INSERT INTO alters LSN significantly.
    # Also assume that safekeepers will not be significantly out of sync in this test.
    middle_lsn = (init_m[0].latest_valid_lsn + final_m[0].latest_valid_lsn) // 2
    assert max(init_m[0].flush_lsns) < middle_lsn < min(final_m[0].flush_lsns)
    assert max(init_m[0].commit_lsns) < middle_lsn < min(final_m[0].commit_lsns)
    assert max(init_m[1].flush_lsns) < middle_lsn < min(final_m[1].flush_lsns)
    assert max(init_m[1].commit_lsns) < middle_lsn < min(final_m[1].commit_lsns)
    assert max(init_m[2].flush_lsns) <= min(final_m[2].flush_lsns) < middle_lsn
    assert max(init_m[2].commit_lsns) <= min(final_m[2].commit_lsns) < middle_lsn


# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_restarts(zenith_env_builder: ZenithEnvBuilder):
    fault_probability = 0.01
    n_inserts = 1000
    n_acceptors = 3

    zenith_env_builder.num_safekeepers = n_acceptors
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_wal_acceptors_restarts", "main"])
    pg = env.postgres.create_start('test_wal_acceptors_restarts')

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
                failed_node = env.safekeepers[random.randrange(0, n_acceptors)]
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
def test_unavailability(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 2
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_wal_acceptors_unavailability", "main"])
    pg = env.postgres.create_start('test_wal_acceptors_unavailability')

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # check basic work with table
    cur.execute('CREATE TABLE t(key int primary key, value text)')
    cur.execute("INSERT INTO t values (1, 'payload')")

    # shutdown one of two acceptors, that is, majority
    env.safekeepers[0].stop()

    proc = Process(target=delayed_wal_acceptor_start, args=(env.safekeepers[0], ))
    proc.start()

    start = time.time()
    cur.execute("INSERT INTO t values (2, 'payload')")
    # ensure that the query above was hanging while acceptor was down
    assert (time.time() - start) >= start_delay_sec
    proc.join()

    # for the world's balance, do the same with second acceptor
    env.safekeepers[1].stop()

    proc = Process(target=delayed_wal_acceptor_start, args=(env.safekeepers[1], ))
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
def test_race_conditions(zenith_env_builder: ZenithEnvBuilder, stop_value):

    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_wal_acceptors_race_conditions", "main"])
    pg = env.postgres.create_start('test_wal_acceptors_race_conditions')

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute('CREATE TABLE t(key int primary key, value text)')

    proc = Process(target=xmas_garland, args=(env.safekeepers, stop_value))
    proc.start()

    for i in range(1000):
        cur.execute("INSERT INTO t values (%s, 'payload');", (i + 1, ))

    cur.execute('SELECT sum(key) FROM t')
    assert cur.fetchone() == (500500, )

    stop_value.value = 1
    proc.join()


class ProposerPostgres:
    """Object for running safekeepers sync with walproposer"""
    def __init__(self, env: ZenithEnv, pgdata_dir: str, pg_bin, timeline_id: str, tenant_id: str):
        self.env = env
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
                f"zenith.page_server_connstring = ''\n",
                f"wal_acceptors = '{wal_acceptors}'\n",
            ])

    def sync_safekeepers(self, timeout=60) -> str:
        """
        Run 'postgres --sync-safekeepers'.
        Returns execution result, which is commit_lsn after sync.
        """

        command = ["postgres", "--sync-safekeepers"]
        env = {
            "PGDATA": self.pg_data_dir_path(),
        }

        basepath = self.pg_bin.run_capture(command, env, timeout=timeout)
        stdout_filename = basepath + '.stdout'

        with open(stdout_filename, 'r') as stdout_f:
            stdout = stdout_f.read()
            return stdout.strip("\n ")


# insert wal in all safekeepers and run sync on proposer
def test_sync_safekeepers(zenith_env_builder: ZenithEnvBuilder, pg_bin: PgBin):

    # We don't really need the full environment for this test, just the
    # safekeepers would be enough.
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    timeline_id = uuid.uuid4().hex
    tenant_id = uuid.uuid4().hex

    # write config for proposer
    pgdata_dir = os.path.join(env.repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(env, pgdata_dir, pg_bin, timeline_id, tenant_id)
    pg.create_dir_config(env.get_safekeeper_connstrs())

    # valid lsn, which is not in the segment start, nor in zero segment
    epoch_start_lsn = 0x16B9188  # 0/16B9188
    begin_lsn = epoch_start_lsn

    # append and commit WAL
    lsn_after_append = []
    for i in range(3):
        res = env.safekeepers[i].append_logical_message(
            tenant_id,
            timeline_id,
            {
                "lm_prefix": "prefix",
                "lm_message": "message",
                "set_commit_lsn": True,
                "send_proposer_elected": True,
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


def test_timeline_status(zenith_env_builder: ZenithEnvBuilder):

    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_timeline_status", "main"])
    pg = env.postgres.create_start('test_timeline_status')

    wa = env.safekeepers[0]
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


class WalAppender:
    """Helper for appending WAL to safekeepers, keeps track of last inserted lsn."""

    # 0/16B9188 is good lsn to start with, it's valid and not in the segment start, nor in zero segment
    def __init__(self,
                 acceptors,
                 tenant_id,
                 timeline_id,
                 epoch_start_lsn=0x16B9188,
                 begin_lsn=0x16B9188):
        self.acceptors = acceptors
        self.epoch_start_lsn = epoch_start_lsn
        self.begin_lsn = begin_lsn
        self.tenant_id = tenant_id
        self.timeline_id = timeline_id
        self.flush_lsns = dict()

    def append(self, i, term, lm_message="message", lm_prefix="", set_commit_lsn=False):
        """Append new logical message to i'th safekeeper."""
        lsn = self.flush_lsns.get(i, self.begin_lsn)
        req = {
            "lm_prefix": lm_prefix,
            "lm_message": lm_message,
            "set_commit_lsn": set_commit_lsn,
            "term": term,
            "begin_lsn": lsn,
            "epoch_start_lsn": self.epoch_start_lsn,
            "truncate_lsn": self.begin_lsn,
        }

        res = self.acceptors[i].append_logical_message(
            self.tenant_id,
            self.timeline_id,
            req,
        )

        end_lsn = res["inserted_wal"]["end_lsn"]
        self.flush_lsns[i] = end_lsn

        return res

    def debug_print(self):
        """Print lsn for each acceptor."""
        for i, lsn in self.flush_lsns.items():
            print(f'end_lsn for acceptors[{i}] = {lsn_to_hex(lsn)}')


# one safekeeper with old term and a lot of non-commited wal
def test_sync_safekeepers_old_term_ahead(repo_dir: str,
                                         pg_bin: PgBin,
                                         wa_factory: WalAcceptorFactory):
    wa_factory.start_n_new(3)

    timeline_id = uuid.uuid4().hex
    tenant_id = uuid.uuid4().hex

    # write config for proposer
    pgdata_dir = os.path.join(repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(pgdata_dir, pg_bin, timeline_id, tenant_id)
    pg.create_dir_config(wa_factory.get_connstrs())

    # append wal to safekeepers
    appender = WalAppender(wa_factory.instances, tenant_id, timeline_id)

    appender.append(0, term=1, lm_message="msg1")
    appender.append(1, term=1, lm_message="msg1")
    appender.append(2, term=1, lm_message="msg1")

    appender.append(2, term=1, lm_message="msg1")
    appender.append(2, term=1, lm_message="msg1")
    appender.append(2, term=1, lm_message="msg1")
    appender.append(2, term=1, lm_message="msg1")

    new_epoch = appender.flush_lsns[0]
    appender.epoch_start_lsn = new_epoch

    appender.append(0, term=2, lm_message="msg2")
    appender.append(1, term=2, lm_message="msg2")

    # run sync safekeepers
    # FIXME: fails with timeout
    lsn_after_sync = pg.sync_safekeepers()
    print(f"lsn after sync = {lsn_after_sync}")

    appender.debug_print()
