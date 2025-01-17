from __future__ import annotations

import time

import psutil
import pytest
from fixtures.common_types import TenantId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.pageserver.http import PageserverApiException


def assert_child_processes(pageserver_pid, wal_redo_present=False, defunct_present=False):
    children = psutil.Process(pageserver_pid).children()
    for child in children:
        if not wal_redo_present:
            assert "--wal-redo" not in child.cmdline()
        if not defunct_present:
            assert child.status() != psutil.STATUS_ZOMBIE


# Check that the pageserver doesn't leave behind WAL redo processes
# when a tenant is detached. We had an issue previously where we failed
# to wait and consume the exit code of the WAL redo process, leaving it behind
# as a zombie process.
def test_walredo_not_left_behind_on_detach(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    # We intentionally test for a non-existent tenant.
    env.pageserver.allowed_errors.append(".*NotFound: tenant.*")
    pageserver_http = env.pageserver.http_client()

    pagserver_pid = int((env.pageserver.workdir / "pageserver.pid").read_text())

    assert_child_processes(pagserver_pid, wal_redo_present=False, defunct_present=False)

    # first check for non existing tenant
    tenant_id = TenantId.generate()
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"NotFound: tenant {tenant_id}",
    ):
        pageserver_http.tenant_status(tenant_id)

    # create new nenant
    tenant_id, _ = env.create_tenant()

    # assert tenant exists on disk
    assert (env.pageserver.tenant_dir(tenant_id)).exists()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """
    )

    # Verify that the table is larger than shared_buffers
    cur.execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
    """
    )
    row = cur.fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    # After filling the table and doing the SELECT, it is guaranteed that we did some WAL redo.
    # So, assert that the WAL redo process is present.
    # XXX this is quite brittle as the lifecycle of the WAL redo process is an implementation detail
    assert_child_processes(pagserver_pid, wal_redo_present=True, defunct_present=False)

    # Stop the compute before detaching, to avoid errors in the log.
    endpoint.stop()

    last_error = None
    for i in range(3):
        try:
            pageserver_http.tenant_detach(tenant_id)
        except Exception as e:
            last_error = e
            log.error(f"try {i} error detaching tenant: {e}")
            continue
        else:
            break
    # else is called if the loop finished without reaching "break"
    else:
        pytest.fail(f"could not detach tenant: {last_error}")

    # check that nothing is left on disk for deleted tenant
    assert not env.pageserver.tenant_dir(tenant_id).exists()

    # Pageserver schedules kill+wait of the WAL redo process to the background runtime,
    # asynchronously to tenant detach. Cut it some slack to complete kill+wait before
    # checking.
    time.sleep(1.0)
    assert_child_processes(pagserver_pid, wal_redo_present=False, defunct_present=False)
