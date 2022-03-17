from contextlib import closing, contextmanager
import os
import pathlib
import subprocess
import threading
from typing import Dict
from uuid import UUID
from fixtures.log_helper import log
import time
import signal
import pytest

from fixtures.zenith_fixtures import PgProtocol, PortDistributor, Postgres, ZenithEnvBuilder, ZenithPageserverHttpClient, assert_local, wait_for, wait_for_last_record_lsn, wait_for_upload, zenith_binpath, pg_distrib_dir


def assert_abs_margin_ratio(a: float, b: float, margin_ratio: float):
    print("!" * 100, abs(a - b) / a)
    assert abs(a - b) / a < margin_ratio, abs(a - b) / a


@contextmanager
def new_pageserver_helper(new_pageserver_dir: pathlib.Path,
                          pageserver_bin: pathlib.Path,
                          remote_storage_mock_path: pathlib.Path,
                          pg_port: int,
                          http_port: int):
    """
    cannot use ZenithPageserver yet because it depends on zenith cli
    which currently lacks support for multiple pageservers
    """
    cmd = [
        str(pageserver_bin),
        '--init',
        '--workdir',
        str(new_pageserver_dir),
        f"-c listen_pg_addr='localhost:{pg_port}'",
        f"-c listen_http_addr='localhost:{http_port}'",
        f"-c pg_distrib_dir='{pg_distrib_dir}'",
        f"-c id=2",
        f"-c remote_storage={{local_path='{remote_storage_mock_path}'}}",
    ]

    subprocess.check_output(cmd, text=True)

    # actually run new pageserver
    cmd = [
        str(pageserver_bin),
        '--workdir',
        str(new_pageserver_dir),
        '--daemonize',
    ]
    log.info("starting new pageserver %s", cmd)
    out = subprocess.check_output(cmd, text=True)
    log.info("started new pageserver %s", out)
    try:
        yield
    finally:
        log.info("stopping new pageserver")
        pid = int((new_pageserver_dir / 'pageserver.pid').read_text())
        os.kill(pid, signal.SIGQUIT)


@contextmanager
def pg_cur(pg):
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            yield cur


def load(pg: Postgres, stop_event: threading.Event, load_ok_event: threading.Event):
    log.info("load started")

    inserted_ctr = 0
    failed = False
    while not stop_event.is_set():
        try:
            with pg_cur(pg) as cur:
                cur.execute("INSERT INTO load VALUES ('some payload')")
                inserted_ctr += 1
        except:
            if not failed:
                log.info("load failed")
            failed = True
            load_ok_event.clear()
        else:
            if failed:
                with pg_cur(pg) as cur:
                    # if we recovered after failure verify that we have correct number of rows
                    log.info("recovering at %s", inserted_ctr)
                    cur.execute("SELECT count(*) FROM load")
                    # it seems that sometimes transaction gets commited before we can acknowledge
                    # the result, so sometimes selected value is larger by one than we expect
                    assert cur.fetchone()[0] - inserted_ctr <= 1
                    log.info("successfully recovered %s", inserted_ctr)
                    failed = False
                    load_ok_event.set()
    log.info('load thread stopped')


@pytest.mark.parametrize('with_load', ['with_load', 'without_load'])
def test_tenant_relocation(zenith_env_builder: ZenithEnvBuilder,
                           port_distributor: PortDistributor,
                           with_load: str):
    zenith_env_builder.num_safekeepers = 1
    zenith_env_builder.enable_local_fs_remote_storage()

    env = zenith_env_builder.init_start()

    # create folder for remote storage mock
    remote_storage_mock_path = env.repo_dir / 'local_fs_remote_storage'

    tenant = env.zenith_cli.create_tenant(UUID("74ee8b079a0e437eb0afea7d26a07209"))
    log.info("tenant to relocate %s", tenant)
    env.zenith_cli.create_root_branch('main', tenant_id=tenant)
    env.zenith_cli.create_branch('test_tenant_relocation', tenant_id=tenant)

    tenant_pg = env.postgres.create_start(branch_name='main',
                                          node_name='test_tenant_relocation',
                                          tenant_id=tenant)

    # insert some data
    with closing(tenant_pg.connect()) as conn:
        with conn.cursor() as cur:
            # save timeline for later gc call
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = UUID(cur.fetchone()[0])
            log.info("timeline to relocate %s", timeline.hex)

            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,1000), 'some payload'")
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (500500, )
            cur.execute("SELECT pg_current_wal_flush_lsn()")

            current_lsn = int(cur.fetchone()[0].split('/')[1], base=16)

    pageserver_http = env.pageserver.http_client()

    # wait until pageserver receives that data
    wait_for_last_record_lsn(pageserver_http, tenant, timeline, current_lsn)
    timeline_detail = pageserver_http.timeline_detail_v2(tenant, timeline)

    if with_load == 'with_load':
        # create load table
        with pg_cur(tenant_pg) as cur:
            cur.execute("CREATE TABLE load(value text)")

        load_stop_event = threading.Event()
        load_ok_event = threading.Event()
        load_thread = threading.Thread(target=load,
                                       args=(tenant_pg, load_stop_event, load_ok_event))
        load_thread.start()

    # run checkpoint manually to be sure that data landed in remote storage
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"checkpoint {tenant.hex} {timeline.hex}")

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(pageserver_http, tenant, timeline, current_lsn)

    log.info("inititalizing new pageserver")
    # bootstrap second pageserver
    new_pageserver_dir = env.repo_dir / 'new_pageserver'
    new_pageserver_dir.mkdir()

    new_pageserver_pg_port = port_distributor.get_port()
    new_pageserver_http_port = port_distributor.get_port()
    log.info("new pageserver ports pg %s http %s", new_pageserver_pg_port, new_pageserver_http_port)
    pageserver_bin = pathlib.Path(zenith_binpath) / 'pageserver'

    new_pageserver_http = ZenithPageserverHttpClient(port=new_pageserver_http_port, auth_token=None)

    with new_pageserver_helper(new_pageserver_dir,
                               pageserver_bin,
                               remote_storage_mock_path,
                               new_pageserver_pg_port,
                               new_pageserver_http_port):

        # call to attach timeline to new pageserver
        new_pageserver_http.timeline_attach(tenant, timeline)
        # new pageserver should be in sync (modulo wal tail or vacuum activity) with the old one because there was no new writes since checkpoint
        new_timeline_detail = wait_for(
            number_of_iterations=5,
            interval=1,
            func=lambda: assert_local(new_pageserver_http, tenant, timeline))

        # when load is active these checks can break because lsns are not static
        # so lets check with some margin
        assert_abs_margin_ratio(new_timeline_detail['local']['disk_consistent_lsn'],
                                timeline_detail['local']['disk_consistent_lsn'],
                                0.03)

        # callmemaybe to start replication from safekeeper to the new pageserver
        # when there is no load there is a clean checkpoint and no wal delta
        # needs to be streamed to the new pageserver
        # TODO (rodionov) use attach to start replication
        with pg_cur(PgProtocol(host='localhost', port=new_pageserver_pg_port)) as cur:
            # "callmemaybe {} {} host={} port={} options='-c ztimelineid={} ztenantid={}'"
            safekeeper_connstring = f"host=localhost port={env.safekeepers[0].port.pg} options='-c ztimelineid={timeline} ztenantid={tenant} pageserver_connstr=postgresql://no_user:@localhost:{new_pageserver_pg_port}'"
            cur.execute("callmemaybe {} {} {}".format(tenant.hex,
                                                      timeline.hex,
                                                      safekeeper_connstring))

        tenant_pg.stop()

        # rewrite zenith cli config to use new pageserver for basebackup to start new compute
        cli_config_lines = (env.repo_dir / 'config').read_text().splitlines()
        cli_config_lines[-2] = f"listen_http_addr = 'localhost:{new_pageserver_http_port}'"
        cli_config_lines[-1] = f"listen_pg_addr = 'localhost:{new_pageserver_pg_port}'"
        (env.repo_dir / 'config').write_text('\n'.join(cli_config_lines))

        tenant_pg_config_file_path = pathlib.Path(tenant_pg.config_file_path())
        tenant_pg_config_file_path.open('a').write(
            f"\nzenith.page_server_connstring = 'postgresql://no_user:@localhost:{new_pageserver_pg_port}'"
        )

        tenant_pg.start()

        # detach tenant from old pageserver before we check
        # that all the data is there to be sure that old pageserver
        # is no longer involved, and if it is, we will see the errors
        pageserver_http.timeline_detach(tenant, timeline)

        with pg_cur(tenant_pg) as cur:
            # check that data is still there
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (500500, )
            # check that we can write new data
            cur.execute("INSERT INTO t SELECT generate_series(1001,2000), 'some payload'")
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (2001000, )

        if with_load == 'with_load':
            assert load_ok_event.wait(1)
            log.info('stopping load thread')
            load_stop_event.set()
            load_thread.join()
            log.info('load thread stopped')

        # bring old pageserver back for clean shutdown via zenith cli
        # new pageserver will be shut down by the context manager
        cli_config_lines = (env.repo_dir / 'config').read_text().splitlines()
        cli_config_lines[-2] = f"listen_http_addr = 'localhost:{env.pageserver.service_port.http}'"
        cli_config_lines[-1] = f"listen_pg_addr = 'localhost:{env.pageserver.service_port.pg}'"
        (env.repo_dir / 'config').write_text('\n'.join(cli_config_lines))
