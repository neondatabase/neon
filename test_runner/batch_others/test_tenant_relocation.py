from contextlib import closing, contextmanager
import os
import pathlib
import subprocess
import threading
import typing
from uuid import UUID
from fixtures.log_helper import log
from typing import Optional
import signal
import pytest

from fixtures.neon_fixtures import PgProtocol, PortDistributor, Postgres, NeonEnvBuilder, Etcd, NeonPageserverHttpClient, assert_local, wait_until, wait_for_last_record_lsn, wait_for_upload, neon_binpath, pg_distrib_dir
from fixtures.utils import lsn_from_hex


def assert_abs_margin_ratio(a: float, b: float, margin_ratio: float):
    assert abs(a - b) / a < margin_ratio, abs(a - b) / a


@contextmanager
def new_pageserver_helper(new_pageserver_dir: pathlib.Path,
                          pageserver_bin: pathlib.Path,
                          remote_storage_mock_path: pathlib.Path,
                          pg_port: int,
                          http_port: int,
                          broker: Optional[Etcd]):
    """
    cannot use NeonPageserver yet because it depends on neon cli
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

    if broker is not None:
        cmd.append(f"-c broker_endpoints=['{broker.client_url()}']", )

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
                    # it seems that sometimes transaction gets committed before we can acknowledge
                    # the result, so sometimes selected value is larger by one than we expect
                    assert cur.fetchone()[0] - inserted_ctr <= 1
                    log.info("successfully recovered %s", inserted_ctr)
                    failed = False
                    load_ok_event.set()
    log.info('load thread stopped')


@pytest.mark.parametrize('with_load', ['with_load', 'without_load'])
def test_tenant_relocation(neon_env_builder: NeonEnvBuilder,
                           port_distributor: PortDistributor,
                           with_load: str):
    neon_env_builder.enable_local_fs_remote_storage()

    env = neon_env_builder.init_start()

    # create folder for remote storage mock
    remote_storage_mock_path = env.repo_dir / 'local_fs_remote_storage'

    tenant, _ = env.neon_cli.create_tenant(UUID("74ee8b079a0e437eb0afea7d26a07209"))
    log.info("tenant to relocate %s", tenant)

    # attach does not download ancestor branches (should it?), just use root branch for now
    env.neon_cli.create_root_branch('test_tenant_relocation', tenant_id=tenant)

    tenant_pg = env.postgres.create_start(branch_name='test_tenant_relocation',
                                          node_name='test_tenant_relocation',
                                          tenant_id=tenant)

    # insert some data
    with closing(tenant_pg.connect()) as conn:
        with conn.cursor() as cur:
            # save timeline for later gc call
            cur.execute("SHOW neon.timeline_id")
            timeline = UUID(cur.fetchone()[0])
            log.info("timeline to relocate %s", timeline.hex)

            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,1000), 'some payload'")
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (500500, )
            cur.execute("SELECT pg_current_wal_flush_lsn()")

            current_lsn = lsn_from_hex(cur.fetchone()[0])

    pageserver_http = env.pageserver.http_client()

    # wait until pageserver receives that data
    wait_for_last_record_lsn(pageserver_http, tenant, timeline, current_lsn)
    timeline_detail = assert_local(pageserver_http, tenant, timeline)

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
    pageserver_bin = pathlib.Path(neon_binpath) / 'pageserver'

    new_pageserver_http = NeonPageserverHttpClient(port=new_pageserver_http_port, auth_token=None)

    with new_pageserver_helper(new_pageserver_dir,
                               pageserver_bin,
                               remote_storage_mock_path,
                               new_pageserver_pg_port,
                               new_pageserver_http_port,
                               neon_env_builder.broker):

        # call to attach timeline to new pageserver
        new_pageserver_http.timeline_attach(tenant, timeline)
        # new pageserver should be in sync (modulo wal tail or vacuum activity) with the old one because there was no new writes since checkpoint
        new_timeline_detail = wait_until(
            number_of_iterations=5,
            interval=1,
            func=lambda: assert_local(new_pageserver_http, tenant, timeline))

        # when load is active these checks can break because lsns are not static
        # so lets check with some margin
        assert_abs_margin_ratio(lsn_from_hex(new_timeline_detail['local']['disk_consistent_lsn']),
                                lsn_from_hex(timeline_detail['local']['disk_consistent_lsn']),
                                0.03)

        tenant_pg.stop()

        # rewrite neon cli config to use new pageserver for basebackup to start new compute
        cli_config_lines = (env.repo_dir / 'config').read_text().splitlines()
        cli_config_lines[-2] = f"listen_http_addr = 'localhost:{new_pageserver_http_port}'"
        cli_config_lines[-1] = f"listen_pg_addr = 'localhost:{new_pageserver_pg_port}'"
        (env.repo_dir / 'config').write_text('\n'.join(cli_config_lines))

        tenant_pg_config_file_path = pathlib.Path(tenant_pg.config_file_path())
        tenant_pg_config_file_path.open('a').write(
            f"\nneon.pageserver_connstring = 'postgresql://no_user:@localhost:{new_pageserver_pg_port}'"
        )

        tenant_pg.start()

        timeline_to_detach_local_path = env.repo_dir / 'tenants' / tenant.hex / 'timelines' / timeline.hex
        files_before_detach = os.listdir(timeline_to_detach_local_path)
        assert 'metadata' in files_before_detach, f'Regular timeline {timeline_to_detach_local_path} should have the metadata file,\
             but got: {files_before_detach}'
        assert len(files_before_detach) > 2, f'Regular timeline {timeline_to_detach_local_path} should have at least one layer file,\
             but got {files_before_detach}'

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
            assert load_ok_event.wait(3)
            log.info('stopping load thread')
            load_stop_event.set()
            load_thread.join(timeout=10)
            log.info('load thread stopped')

        assert not os.path.exists(timeline_to_detach_local_path), f'After detach, local timeline dir {timeline_to_detach_local_path} should be removed'

        # bring old pageserver back for clean shutdown via neon cli
        # new pageserver will be shut down by the context manager
        cli_config_lines = (env.repo_dir / 'config').read_text().splitlines()
        cli_config_lines[-2] = f"listen_http_addr = 'localhost:{env.pageserver.service_port.http}'"
        cli_config_lines[-1] = f"listen_pg_addr = 'localhost:{env.pageserver.service_port.pg}'"
        (env.repo_dir / 'config').write_text('\n'.join(cli_config_lines))
