import pytest
import random
import time
import os
import shutil
import signal
import subprocess
import sys
import threading
import uuid

from contextlib import closing
from dataclasses import dataclass, field
from multiprocessing import Process, Value
from pathlib import Path
from fixtures.neon_fixtures import PgBin, Etcd, Postgres, RemoteStorageUsers, Safekeeper, NeonEnv, NeonEnvBuilder, PortDistributor, SafekeeperPort, neon_binpath, PgProtocol
from fixtures.utils import get_dir_size, lsn_to_hex, mkdir_if_needed, lsn_from_hex
from fixtures.log_helper import log
from typing import List, Optional, Any
from uuid import uuid4


@dataclass
class TimelineMetrics:
    timeline_id: str
    last_record_lsn: int
    # One entry per each Safekeeper, order is the same
    flush_lsns: List[int] = field(default_factory=list)
    commit_lsns: List[int] = field(default_factory=list)


# Run page server and multiple acceptors, and multiple compute nodes running
# against different timelines.
def test_many_timelines(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    n_timelines = 3

    branch_names = [
        "test_safekeepers_many_timelines_{}".format(tlin) for tlin in range(n_timelines)
    ]
    # pageserver, safekeeper operate timelines via their ids (can be represented in hex as 'ad50847381e248feaac9876cc71ae418')
    # that's not really human readable, so the branch names are introduced in Neon CLI.
    # Neon CLI stores its branch <-> timeline mapping in its internals,
    # but we need this to collect metrics from other servers, related to the timeline.
    branch_names_to_timeline_ids = {}

    # start postgres on each timeline
    pgs = []
    for branch_name in branch_names:
        new_timeline_id = env.neon_cli.create_branch(branch_name)
        pgs.append(env.postgres.create_start(branch_name))
        branch_names_to_timeline_ids[branch_name] = new_timeline_id

    tenant_id = env.initial_tenant

    def collect_metrics(message: str) -> List[TimelineMetrics]:
        with env.pageserver.http_client() as pageserver_http:
            timeline_details = [
                pageserver_http.timeline_detail(
                    tenant_id=tenant_id, timeline_id=branch_names_to_timeline_ids[branch_name])
                for branch_name in branch_names
            ]
        # All changes visible to pageserver (last_record_lsn) should be
        # confirmed by safekeepers first. As we cannot atomically get
        # state of both pageserver and safekeepers, we should start with
        # pageserver. Looking at outdated data from pageserver is ok.
        # Asking safekeepers first is not ok because new commits may arrive
        # to both safekeepers and pageserver after we've already obtained
        # safekeepers' state, it will look contradictory.
        sk_metrics = [sk.http_client().get_metrics() for sk in env.safekeepers]

        timeline_metrics = []
        for timeline_detail in timeline_details:
            timeline_id: str = timeline_detail["timeline_id"]

            local_timeline_detail = timeline_detail.get('local')
            if local_timeline_detail is None:
                log.debug(f"Timeline {timeline_id} is not present locally, skipping")
                continue

            m = TimelineMetrics(
                timeline_id=timeline_id,
                last_record_lsn=lsn_from_hex(local_timeline_detail['last_record_lsn']),
            )
            for sk_m in sk_metrics:
                m.flush_lsns.append(sk_m.flush_lsn_inexact[(tenant_id.hex, timeline_id)])
                m.commit_lsns.append(sk_m.commit_lsn_inexact[(tenant_id.hex, timeline_id)])

            for flush_lsn, commit_lsn in zip(m.flush_lsns, m.commit_lsns):
                # Invariant. May be < when transaction is in progress.
                assert commit_lsn <= flush_lsn, f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            # We only call collect_metrics() after a transaction is confirmed by
            # the compute node, which only happens after a consensus of safekeepers
            # has confirmed the transaction. We assume majority consensus here.
            assert (2 * sum(m.last_record_lsn <= lsn
                            for lsn in m.flush_lsns) > neon_env_builder.num_safekeepers), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            assert (2 * sum(m.last_record_lsn <= lsn
                            for lsn in m.commit_lsns) > neon_env_builder.num_safekeepers), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            timeline_metrics.append(m)
        log.info(f"{message}: {timeline_metrics}")
        return timeline_metrics

    # TODO: https://github.com/neondatabase/neon/issues/809
    # collect_metrics("before CREATE TABLE")

    # Do everything in different loops to have actions on different timelines
    # interleaved.
    # create schema
    for pg in pgs:
        pg.safe_psql("CREATE TABLE t(key int primary key, value text)")
    init_m = collect_metrics("after CREATE TABLE")

    # Populate data for 2/3 timelines
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

    # Check data for 2/3 timelines
    for pg in pgs[:-1]:
        res = pg.safe_psql("SELECT sum(key) FROM t")
        assert res[0] == (5000050000, )

    final_m = collect_metrics("after SELECT")
    # Assume that LSNs (a) behave similarly in all timelines; and (b) INSERT INTO alters LSN significantly.
    # Also assume that safekeepers will not be significantly out of sync in this test.
    middle_lsn = (init_m[0].last_record_lsn + final_m[0].last_record_lsn) // 2
    assert max(init_m[0].flush_lsns) < middle_lsn < min(final_m[0].flush_lsns)
    assert max(init_m[0].commit_lsns) < middle_lsn < min(final_m[0].commit_lsns)
    assert max(init_m[1].flush_lsns) < middle_lsn < min(final_m[1].flush_lsns)
    assert max(init_m[1].commit_lsns) < middle_lsn < min(final_m[1].commit_lsns)
    assert max(init_m[2].flush_lsns) <= min(final_m[2].flush_lsns) < middle_lsn
    assert max(init_m[2].commit_lsns) <= min(final_m[2].commit_lsns) < middle_lsn


# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_restarts(neon_env_builder: NeonEnvBuilder):
    fault_probability = 0.01
    n_inserts = 1000
    n_acceptors = 3

    neon_env_builder.num_safekeepers = n_acceptors
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_safekeepers_restarts')
    pg = env.postgres.create_start('test_safekeepers_restarts')

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


def delayed_safekeeper_start(wa):
    time.sleep(start_delay_sec)
    wa.start()


# When majority of acceptors is offline, commits are expected to be frozen
def test_unavailability(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 2
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_safekeepers_unavailability')
    pg = env.postgres.create_start('test_safekeepers_unavailability')

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # check basic work with table
    cur.execute('CREATE TABLE t(key int primary key, value text)')
    cur.execute("INSERT INTO t values (1, 'payload')")

    # shutdown one of two acceptors, that is, majority
    env.safekeepers[0].stop()

    proc = Process(target=delayed_safekeeper_start, args=(env.safekeepers[0], ))
    proc.start()

    start = time.time()
    cur.execute("INSERT INTO t values (2, 'payload')")
    # ensure that the query above was hanging while acceptor was down
    assert (time.time() - start) >= start_delay_sec
    proc.join()

    # for the world's balance, do the same with second acceptor
    env.safekeepers[1].stop()

    proc = Process(target=delayed_safekeeper_start, args=(env.safekeepers[1], ))
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
def test_race_conditions(neon_env_builder: NeonEnvBuilder, stop_value):

    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_safekeepers_race_conditions')
    pg = env.postgres.create_start('test_safekeepers_race_conditions')

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


# Test that safekeepers push their info to the broker and learn peer status from it
def test_broker(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_local_fs_remote_storage()
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_broker", "main")
    pg = env.postgres.create_start('test_broker')
    pg.safe_psql("CREATE TABLE t(key int primary key, value text)")

    # learn neon timeline from compute
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    # wait until remote_consistent_lsn gets advanced on all safekeepers
    clients = [sk.http_client() for sk in env.safekeepers]
    stat_before = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
    log.info(f"statuses is {stat_before}")

    pg.safe_psql("INSERT INTO t SELECT generate_series(1,100), 'payload'")
    # force checkpoint to advance remote_consistent_lsn
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"checkpoint {tenant_id} {timeline_id}")
    # and wait till remote_consistent_lsn propagates to all safekeepers
    started_at = time.time()
    while True:
        stat_after = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
        if all(
                lsn_from_hex(s_after.remote_consistent_lsn) > lsn_from_hex(
                    s_before.remote_consistent_lsn) for s_after,
                s_before in zip(stat_after, stat_before)):
            break
        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(
                f"timed out waiting {elapsed:.0f}s for remote_consistent_lsn propagation: status before {stat_before}, status current {stat_after}"
            )
        time.sleep(0.5)


# Test that old WAL consumed by peers and pageserver is removed from safekeepers.
@pytest.mark.parametrize('auth_enabled', [False, True])
def test_wal_removal(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.num_safekeepers = 2
    # to advance remote_consistent_lsn
    neon_env_builder.enable_local_fs_remote_storage()
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_safekeepers_wal_removal')
    pg = env.postgres.create_start('test_safekeepers_wal_removal')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    # force checkpoint to advance remote_consistent_lsn
    pageserver_conn_options = {}
    if auth_enabled:
        pageserver_conn_options['password'] = env.auth_keys.generate_tenant_token(tenant_id)
    with closing(env.pageserver.connect(**pageserver_conn_options)) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"checkpoint {tenant_id} {timeline_id}")

    # We will wait for first segment removal. Make sure they exist for starter.
    first_segments = [
        os.path.join(sk.data_dir(), tenant_id, timeline_id, '000000010000000000000001')
        for sk in env.safekeepers
    ]
    assert all(os.path.exists(p) for p in first_segments)

    if not auth_enabled:
        http_cli = env.safekeepers[0].http_client()
    else:
        http_cli = env.safekeepers[0].http_client(
            auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        http_cli_other = env.safekeepers[0].http_client(
            auth_token=env.auth_keys.generate_tenant_token(uuid4().hex))
        http_cli_noauth = env.safekeepers[0].http_client()

    # Pretend WAL is offloaded to s3.
    if auth_enabled:
        old_backup_lsn = http_cli.timeline_status(tenant_id=tenant_id,
                                                  timeline_id=timeline_id).backup_lsn
        assert 'FFFFFFFF/FEFFFFFF' != old_backup_lsn
        for cli in [http_cli_other, http_cli_noauth]:
            with pytest.raises(cli.HTTPError, match='Forbidden|Unauthorized'):
                cli.record_safekeeper_info(tenant_id,
                                           timeline_id, {'backup_lsn': 'FFFFFFFF/FEFFFFFF'})
        assert old_backup_lsn == http_cli.timeline_status(tenant_id=tenant_id,
                                                          timeline_id=timeline_id).backup_lsn
    http_cli.record_safekeeper_info(tenant_id, timeline_id, {'backup_lsn': 'FFFFFFFF/FEFFFFFF'})
    assert 'FFFFFFFF/FEFFFFFF' == http_cli.timeline_status(tenant_id=tenant_id,
                                                           timeline_id=timeline_id).backup_lsn

    # wait till first segment is removed on all safekeepers
    started_at = time.time()
    while True:
        if all(not os.path.exists(p) for p in first_segments):
            break
        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(f"timed out waiting {elapsed:.0f}s for first segment get removed")
        time.sleep(0.5)


def wait_segment_offload(tenant_id, timeline_id, live_sk, seg_end):
    started_at = time.time()
    http_cli = live_sk.http_client()
    while True:
        tli_status = http_cli.timeline_status(tenant_id, timeline_id)
        log.info(f"live sk status is {tli_status}")

        if lsn_from_hex(tli_status.backup_lsn) >= lsn_from_hex(seg_end):
            break
        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(
                f"timed out waiting {elapsed:.0f}s for segment ending at {seg_end} get offloaded")
        time.sleep(0.5)

def wait_wal_trim(tenant_id, timeline_id, sk, target_size):
    started_at = time.time()
    http_cli = sk.http_client()
    while True:
        tli_status = http_cli.timeline_status(tenant_id, timeline_id)
        sk_wal_size = get_dir_size(os.path.join(sk.data_dir(), tenant_id, timeline_id)) / 1024 / 1024
        log.info(f"Safekeeper id={sk.id} wal_size={sk_wal_size:.2f}MB status={tli_status}")

        if sk_wal_size <= target_size:
            break

        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(
                f"timed out waiting {elapsed:.0f}s for sk_id={sk.id} to trim WAL to {target_size:.2f}MB, current size is {sk_wal_size:.2f}MB")
        time.sleep(0.5)


@pytest.mark.parametrize('storage_type', ['mock_s3', 'local_fs'])
def test_wal_backup(neon_env_builder: NeonEnvBuilder, storage_type: str):
    neon_env_builder.num_safekeepers = 3
    if storage_type == 'local_fs':
        neon_env_builder.enable_local_fs_remote_storage()
    elif storage_type == 'mock_s3':
        neon_env_builder.enable_s3_mock_remote_storage('test_safekeepers_wal_backup')
    else:
        raise RuntimeError(f'Unknown storage type: {storage_type}')
    neon_env_builder.remote_storage_users = RemoteStorageUsers.SAFEKEEPER

    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_safekeepers_wal_backup')
    pg = env.postgres.create_start('test_safekeepers_wal_backup')

    # learn neon timeline from compute
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    pg_conn = pg.connect()
    cur = pg_conn.cursor()
    cur.execute('create table t(key int, value text)')

    # Shut down subsequently each of safekeepers and fill a segment while sk is
    # down; ensure segment gets offloaded by others.
    offloaded_seg_end = ['0/2000000', '0/3000000', '0/4000000']
    for victim, seg_end in zip(env.safekeepers, offloaded_seg_end):
        victim.stop()
        # roughly fills one segment
        cur.execute("insert into t select generate_series(1,250000), 'payload'")
        live_sk = [sk for sk in env.safekeepers if sk != victim][0]

        wait_segment_offload(tenant_id, timeline_id, live_sk, seg_end)

        victim.start()

    # put one of safekeepers down again
    env.safekeepers[0].stop()
    # restart postgres
    pg.stop_and_destroy().create_start('test_safekeepers_wal_backup')
    # and ensure offloading still works
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("insert into t select generate_series(1,250000), 'payload'")
    wait_segment_offload(tenant_id, timeline_id, env.safekeepers[1], '0/5000000')

@pytest.mark.parametrize('storage_type', ['local_fs'])
def test_s3_wal_replay(neon_env_builder: NeonEnvBuilder, storage_type: str):
    neon_env_builder.num_safekeepers = 3
    if storage_type == 'local_fs':
        neon_env_builder.enable_local_fs_remote_storage()
    elif storage_type == 'mock_s3':
        neon_env_builder.enable_s3_mock_remote_storage('test_s3_wal_replay')
    else:
        raise RuntimeError(f'Unknown storage type: {storage_type}')
    neon_env_builder.remote_storage_users = RemoteStorageUsers.SAFEKEEPER

    env = neon_env_builder.init_start()
    env.neon_cli.create_branch('test_s3_wal_replay')

    env.pageserver.stop()
    pageserver_tenants_dir = os.path.join(env.repo_dir, 'tenants')
    pageserver_fresh_copy = os.path.join(env.repo_dir, 'tenants_fresh')
    log.info(f"Creating a copy of pageserver in a fresh state at {pageserver_fresh_copy}")
    shutil.copytree(pageserver_tenants_dir, pageserver_fresh_copy)
    env.pageserver.start()

    pg = env.postgres.create_start('test_s3_wal_replay')

    # learn neon timeline from compute
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    expected_sum = 0

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t(key int, value text)")
            cur.execute("insert into t values (1, 'payload')")
            expected_sum += 1

            # TODO: implement s3 download in safekeepers

            offloaded_seg_end = ['0/3000000', '0/5000000', '0/7000000']
            for seg_end in offloaded_seg_end:
                # roughly fills two segments
                cur.execute("insert into t select generate_series(1,500000), 'payload'")
                expected_sum += 500000 * 500001 / 2

                cur.execute("select sum(key) from t")
                assert cur.fetchone()[0] == expected_sum

                for sk in env.safekeepers:
                    wait_segment_offload(tenant_id, timeline_id, sk, seg_end)

            # advance remote_consistent_lsn to trigger WAL trimming
            env.safekeepers[0].http_client().record_safekeeper_info(tenant_id, timeline_id, {'remote_consistent_lsn': 'FFFFFFFF/FEFFFFFF'})

            for sk in env.safekeepers:
                # require WAL to be trimmed, so no more than 2 segments are left on disk
                wait_wal_trim(tenant_id, timeline_id, sk, 16 * 2.5)

    # replace pageserver with a fresh copy
    pg.stop_and_destroy()
    env.pageserver.stop()

    log.info(f'Removing current pageserver state at {pageserver_tenants_dir}')
    shutil.rmtree(pageserver_tenants_dir)
    log.info(f'Copying fresh pageserver state from {pageserver_fresh_copy}')
    shutil.move(pageserver_fresh_copy, pageserver_tenants_dir)

    # start everything, verify data
    env.pageserver.start()
    pg.create_start('test_s3_wal_replay')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("select sum(key) from t")
            assert cur.fetchone()[0] == expected_sum

    # # and ensure offloading still works
    # with closing(pg.connect()) as conn:
    #     with conn.cursor() as cur:
    #         cur.execute("insert into t select generate_series(1,250000), 'payload'")
    # wait_segment_offload(tenant_id, timeline_id, env.safekeepers[1], '0/5000000')


class ProposerPostgres(PgProtocol):
    """Object for running postgres without NeonEnv"""
    def __init__(self,
                 pgdata_dir: str,
                 pg_bin,
                 timeline_id: uuid.UUID,
                 tenant_id: uuid.UUID,
                 listen_addr: str,
                 port: int):
        super().__init__(host=listen_addr, port=port, user='cloud_admin', dbname='postgres')

        self.pgdata_dir: str = pgdata_dir
        self.pg_bin: PgBin = pg_bin
        self.timeline_id: uuid.UUID = timeline_id
        self.tenant_id: uuid.UUID = tenant_id
        self.listen_addr: str = listen_addr
        self.port: int = port

    def pg_data_dir_path(self) -> str:
        """ Path to data directory """
        return self.pgdata_dir

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        return os.path.join(self.pgdata_dir, 'postgresql.conf')

    def create_dir_config(self, safekeepers: str):
        """ Create dir and config for running --sync-safekeepers """

        mkdir_if_needed(self.pg_data_dir_path())
        with open(self.config_file_path(), "w") as f:
            cfg = [
                "synchronous_standby_names = 'walproposer'\n",
                "shared_preload_libraries = 'neon'\n",
                f"neon.timeline_id = '{self.timeline_id.hex}'\n",
                f"neon.tenant_id = '{self.tenant_id.hex}'\n",
                f"neon.pageserver_connstring = ''\n",
                f"safekeepers = '{safekeepers}'\n",
                f"listen_addresses = '{self.listen_addr}'\n",
                f"port = '{self.port}'\n",
            ]

            f.writelines(cfg)

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

    def initdb(self):
        """ Run initdb """

        args = ["initdb", "-U", "cloud_admin", "-D", self.pg_data_dir_path()]
        self.pg_bin.run(args)

    def start(self):
        """ Start postgres with pg_ctl """

        log_path = os.path.join(self.pg_data_dir_path(), "pg.log")
        args = ["pg_ctl", "-D", self.pg_data_dir_path(), "-l", log_path, "-w", "start"]
        self.pg_bin.run(args)

    def stop(self):
        """ Stop postgres with pg_ctl """

        args = ["pg_ctl", "-D", self.pg_data_dir_path(), "-m", "immediate", "-w", "stop"]
        self.pg_bin.run(args)


# insert wal in all safekeepers and run sync on proposer
def test_sync_safekeepers(neon_env_builder: NeonEnvBuilder,
                          pg_bin: PgBin,
                          port_distributor: PortDistributor):

    # We don't really need the full environment for this test, just the
    # safekeepers would be enough.
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    timeline_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    # write config for proposer
    pgdata_dir = os.path.join(env.repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(pgdata_dir,
                          pg_bin,
                          timeline_id,
                          tenant_id,
                          '127.0.0.1',
                          port_distributor.get_port())
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


@pytest.mark.parametrize('auth_enabled', [False, True])
def test_timeline_status(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_timeline_status')
    pg = env.postgres.create_start('test_timeline_status')

    wa = env.safekeepers[0]

    # learn neon timeline from compute
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    if not auth_enabled:
        wa_http_cli = wa.http_client()
        wa_http_cli.check_status()
    else:
        wa_http_cli = wa.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        wa_http_cli.check_status()
        wa_http_cli_bad = wa.http_client(
            auth_token=env.auth_keys.generate_tenant_token(uuid4().hex))
        wa_http_cli_bad.check_status()
        wa_http_cli_noauth = wa.http_client()
        wa_http_cli_noauth.check_status()

    # fetch something sensible from status
    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    epoch = tli_status.acceptor_epoch
    timeline_start_lsn = tli_status.timeline_start_lsn

    if auth_enabled:
        for cli in [wa_http_cli_bad, wa_http_cli_noauth]:
            with pytest.raises(cli.HTTPError, match='Forbidden|Unauthorized'):
                cli.timeline_status(tenant_id, timeline_id)

    pg.safe_psql("create table t(i int)")

    # ensure epoch goes up after reboot
    pg.stop().start()
    pg.safe_psql("insert into t values(10)")

    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    epoch_after_reboot = tli_status.acceptor_epoch
    assert epoch_after_reboot > epoch

    # and timeline_start_lsn stays the same
    assert tli_status.timeline_start_lsn == timeline_start_lsn


class SafekeeperEnv:
    def __init__(self,
                 repo_dir: Path,
                 port_distributor: PortDistributor,
                 pg_bin: PgBin,
                 num_safekeepers: int = 1):
        self.repo_dir = repo_dir
        self.port_distributor = port_distributor
        self.broker = Etcd(datadir=os.path.join(self.repo_dir, "etcd"),
                           port=self.port_distributor.get_port(),
                           peer_port=self.port_distributor.get_port())
        self.pg_bin = pg_bin
        self.num_safekeepers = num_safekeepers
        self.bin_safekeeper = os.path.join(str(neon_binpath), 'safekeeper')
        self.safekeepers: Optional[List[subprocess.CompletedProcess[Any]]] = None
        self.postgres: Optional[ProposerPostgres] = None
        self.tenant_id: Optional[uuid.UUID] = None
        self.timeline_id: Optional[uuid.UUID] = None

    def init(self) -> "SafekeeperEnv":
        assert self.postgres is None, "postgres is already initialized"
        assert self.safekeepers is None, "safekeepers are already initialized"

        self.timeline_id = uuid.uuid4()
        self.tenant_id = uuid.uuid4()
        mkdir_if_needed(str(self.repo_dir))

        # Create config and a Safekeeper object for each safekeeper
        self.safekeepers = []
        for i in range(1, self.num_safekeepers + 1):
            self.safekeepers.append(self.start_safekeeper(i))

        # Create and start postgres
        self.postgres = self.create_postgres()
        self.postgres.start()

        return self

    def start_safekeeper(self, i):
        port = SafekeeperPort(
            pg=self.port_distributor.get_port(),
            http=self.port_distributor.get_port(),
        )

        safekeeper_dir = os.path.join(self.repo_dir, f"sk{i}")
        mkdir_if_needed(safekeeper_dir)

        args = [
            self.bin_safekeeper,
            "-l",
            f"127.0.0.1:{port.pg}",
            "--listen-http",
            f"127.0.0.1:{port.http}",
            "-D",
            safekeeper_dir,
            "--id",
            str(i),
            "--broker-endpoints",
            self.broker.client_url(),
            "--daemonize"
        ]

        log.info(f'Running command "{" ".join(args)}"')
        return subprocess.run(args, check=True)

    def get_safekeeper_connstrs(self):
        return ','.join([sk_proc.args[2] for sk_proc in self.safekeepers])

    def create_postgres(self):
        pgdata_dir = os.path.join(self.repo_dir, "proposer_pgdata")
        pg = ProposerPostgres(pgdata_dir,
                              self.pg_bin,
                              self.timeline_id,
                              self.tenant_id,
                              "127.0.0.1",
                              self.port_distributor.get_port())
        pg.initdb()
        pg.create_dir_config(self.get_safekeeper_connstrs())
        return pg

    def kill_safekeeper(self, sk_dir):
        """Read pid file and kill process"""
        pid_file = os.path.join(sk_dir, "safekeeper.pid")
        with open(pid_file, "r") as f:
            pid = int(f.read())
            log.info(f"Killing safekeeper with pid {pid}")
            os.kill(pid, signal.SIGKILL)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        log.info('Cleaning up all safekeeper and compute nodes')

        # Stop all the nodes
        if self.postgres is not None:
            self.postgres.stop()
        if self.safekeepers is not None:
            for sk_proc in self.safekeepers:
                self.kill_safekeeper(sk_proc.args[6])


def test_safekeeper_without_pageserver(test_output_dir: str,
                                       port_distributor: PortDistributor,
                                       pg_bin: PgBin):
    # Create the environment in the test-specific output dir
    repo_dir = Path(os.path.join(test_output_dir, "repo"))

    env = SafekeeperEnv(
        repo_dir,
        port_distributor,
        pg_bin,
    )

    with env:
        env.init()
        assert env.postgres is not None

        env.postgres.safe_psql("create table t(i int)")
        env.postgres.safe_psql("insert into t select generate_series(1, 100)")
        res = env.postgres.safe_psql("select sum(i) from t")[0][0]
        assert res == 5050


def test_replace_safekeeper(neon_env_builder: NeonEnvBuilder):
    def safekeepers_guc(env: NeonEnv, sk_names: List[int]) -> str:
        return ','.join([f'localhost:{sk.port.pg}' for sk in env.safekeepers if sk.id in sk_names])

    def execute_payload(pg: Postgres):
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute('CREATE TABLE IF NOT EXISTS t(key int, value text)')
                cur.execute("INSERT INTO t VALUES (0, 'something')")
                cur.execute('SELECT SUM(key) FROM t')
                sum_before = cur.fetchone()[0]

                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute('SELECT SUM(key) FROM t')
                sum_after = cur.fetchone()[0]
                assert sum_after == sum_before + 5000050000

    def show_statuses(safekeepers: List[Safekeeper], tenant_id: str, timeline_id: str):
        for sk in safekeepers:
            http_cli = sk.http_client()
            try:
                status = http_cli.timeline_status(tenant_id, timeline_id)
                log.info(f"Safekeeper {sk.id} status: {status}")
            except Exception as e:
                log.info(f"Safekeeper {sk.id} status error: {e}")

    neon_env_builder.num_safekeepers = 4
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch('test_replace_safekeeper')

    log.info("Use only first 3 safekeepers")
    env.safekeepers[3].stop()
    active_safekeepers = [1, 2, 3]
    pg = env.postgres.create('test_replace_safekeeper')
    pg.adjust_for_safekeepers(safekeepers_guc(env, active_safekeepers))
    pg.start()

    # learn neon timeline from compute
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Restart all safekeepers to flush everything")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(pg)
    env.safekeepers[0].start()
    env.safekeepers[1].stop(immediate=True)
    execute_payload(pg)
    env.safekeepers[1].start()
    env.safekeepers[2].stop(immediate=True)
    execute_payload(pg)
    env.safekeepers[2].start()

    env.safekeepers[0].stop(immediate=True)
    env.safekeepers[1].stop(immediate=True)
    env.safekeepers[2].stop(immediate=True)
    env.safekeepers[0].start()
    env.safekeepers[1].start()
    env.safekeepers[2].start()

    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk1 (simulate failure) and use only quorum of sk2 and sk3")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Recreate postgres to replace failed sk1 with new sk4")
    pg.stop_and_destroy().create('test_replace_safekeeper')
    active_safekeepers = [2, 3, 4]
    env.safekeepers[3].start()
    pg.adjust_for_safekeepers(safekeepers_guc(env, active_safekeepers))
    pg.start()

    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk2 to require quorum of sk3 and sk4 for normal work")
    env.safekeepers[1].stop(immediate=True)
    execute_payload(pg)
    show_statuses(env.safekeepers, tenant_id, timeline_id)


# We have `wal_keep_size=0`, so postgres should trim WAL once it's broadcasted
# to all safekeepers. This test checks that compute WAL can fit into small number
# of WAL segments.
def test_wal_deleted_after_broadcast(neon_env_builder: NeonEnvBuilder):
    # used to calculate delta in collect_stats
    last_lsn = .0

    # returns LSN and pg_wal size, all in MB
    def collect_stats(pg: Postgres, cur, enable_logs=True):
        nonlocal last_lsn
        assert pg.pgdata_dir is not None

        log.info('executing INSERT to generate WAL')
        cur.execute("select pg_current_wal_lsn()")
        current_lsn = lsn_from_hex(cur.fetchone()[0]) / 1024 / 1024
        pg_wal_size = get_dir_size(os.path.join(pg.pgdata_dir, 'pg_wal')) / 1024 / 1024
        if enable_logs:
            log.info(f"LSN delta: {current_lsn - last_lsn} MB, current WAL size: {pg_wal_size} MB")
        last_lsn = current_lsn
        return current_lsn, pg_wal_size

    # generates about ~20MB of WAL, to create at least one new segment
    def generate_wal(cur):
        cur.execute("INSERT INTO t SELECT generate_series(1,300000), 'payload'")

    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_wal_deleted_after_broadcast')
    # Adjust checkpoint config to prevent keeping old WAL segments
    pg = env.postgres.create_start(
        'test_wal_deleted_after_broadcast',
        config_lines=['min_wal_size=32MB', 'max_wal_size=32MB', 'log_checkpoints=on'])

    pg_conn = pg.connect()
    cur = pg_conn.cursor()
    cur.execute('CREATE TABLE t(key int, value text)')

    collect_stats(pg, cur)

    # generate WAL to simulate normal workload
    for i in range(5):
        generate_wal(cur)
        collect_stats(pg, cur)

    log.info('executing checkpoint')
    cur.execute('CHECKPOINT')
    wal_size_after_checkpoint = collect_stats(pg, cur)[1]

    # there shouldn't be more than 2 WAL segments (but dir may have archive_status files)
    assert wal_size_after_checkpoint < 16 * 2.5


@pytest.mark.parametrize('auth_enabled', [False, True])
def test_delete_force(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.num_safekeepers = 1
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    # Create two tenants: one will be deleted, other should be preserved.
    tenant_id = env.initial_tenant.hex
    timeline_id_1 = env.neon_cli.create_branch('br1').hex  # Active, delete explicitly
    timeline_id_2 = env.neon_cli.create_branch('br2').hex  # Inactive, delete explicitly
    timeline_id_3 = env.neon_cli.create_branch('br3').hex  # Active, delete with the tenant
    timeline_id_4 = env.neon_cli.create_branch('br4').hex  # Inactive, delete with the tenant

    tenant_id_other_uuid, timeline_id_other_uuid = env.neon_cli.create_tenant()
    tenant_id_other = tenant_id_other_uuid.hex
    timeline_id_other = timeline_id_other_uuid.hex

    # Populate branches
    pg_1 = env.postgres.create_start('br1')
    pg_2 = env.postgres.create_start('br2')
    pg_3 = env.postgres.create_start('br3')
    pg_4 = env.postgres.create_start('br4')
    pg_other = env.postgres.create_start('main', tenant_id=uuid.UUID(hex=tenant_id_other))
    for pg in [pg_1, pg_2, pg_3, pg_4, pg_other]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE t(key int primary key)')
    sk = env.safekeepers[0]
    sk_data_dir = Path(sk.data_dir())
    if not auth_enabled:
        sk_http = sk.http_client()
        sk_http_other = sk_http
    else:
        sk_http = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        sk_http_other = sk.http_client(
            auth_token=env.auth_keys.generate_tenant_token(tenant_id_other))
        sk_http_noauth = sk.http_client()
    assert (sk_data_dir / tenant_id / timeline_id_1).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_2).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Stop branches which should be inactive and restart Safekeeper to drop its in-memory state.
    pg_2.stop_and_destroy()
    pg_4.stop_and_destroy()
    sk.stop()
    sk.start()

    # Ensure connections to Safekeeper are established
    for pg in [pg_1, pg_3, pg_other]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO t (key) VALUES (1)')

    # Remove initial tenant's br1 (active)
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_1) == {
        "dir_existed": True,
        "was_active": True,
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert (sk_data_dir / tenant_id / timeline_id_2).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Ensure repeated deletion succeeds
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_1) == {
        "dir_existed": False, "was_active": False
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert (sk_data_dir / tenant_id / timeline_id_2).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    if auth_enabled:
        # Ensure we cannot delete the other tenant
        for sk_h in [sk_http, sk_http_noauth]:
            with pytest.raises(sk_h.HTTPError, match='Forbidden|Unauthorized'):
                assert sk_h.timeline_delete_force(tenant_id_other, timeline_id_other)
            with pytest.raises(sk_h.HTTPError, match='Forbidden|Unauthorized'):
                assert sk_h.tenant_delete_force(tenant_id_other)
        assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove initial tenant's br2 (inactive)
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_2) == {
        "dir_existed": True,
        "was_active": False,
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert not (sk_data_dir / tenant_id / timeline_id_2).exists()
    assert (sk_data_dir / tenant_id / timeline_id_3).is_dir()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove non-existing branch, should succeed
    assert sk_http.timeline_delete_force(tenant_id, '00' * 16) == {
        "dir_existed": False,
        "was_active": False,
    }
    assert not (sk_data_dir / tenant_id / timeline_id_1).exists()
    assert not (sk_data_dir / tenant_id / timeline_id_2).exists()
    assert (sk_data_dir / tenant_id / timeline_id_3).exists()
    assert (sk_data_dir / tenant_id / timeline_id_4).is_dir()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove initial tenant fully (two branches are active)
    response = sk_http.tenant_delete_force(tenant_id)
    assert response == {
        timeline_id_3: {
            "dir_existed": True,
            "was_active": True,
        }
    }
    assert not (sk_data_dir / tenant_id).exists()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Remove initial tenant again.
    response = sk_http.tenant_delete_force(tenant_id)
    assert response == {}
    assert not (sk_data_dir / tenant_id).exists()
    assert (sk_data_dir / tenant_id_other / timeline_id_other).is_dir()

    # Ensure the other tenant still works
    sk_http_other.timeline_status(tenant_id_other, timeline_id_other)
    with closing(pg_other.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO t (key) VALUES (123)')
