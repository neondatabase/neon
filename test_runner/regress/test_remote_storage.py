# It's possible to run any regular test with the local fs remote storage via
# env NEON_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/neon_zzz/'}" poetry ......

import os
import shutil
import time
from pathlib import Path
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonCli,
    NeonEnvBuilder,
    NeonPageserverHttpClient,
    Postgres,
    PostgresFactory,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_record_lsn,
    wait_until,
)
from fixtures.remote_storage import (
    assert_no_in_progress_downloads_for_tenant,
    read_local_fs_index_part,
    wait_for_upload,
    write_local_fs_index_part,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar

FILLER_STRING = "long string to consume some space"
CHECKPOINT_ROWS = 100


def populate_checkpoints(
    pg: Postgres,
    ps: NeonPageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    checkpoint_ids: List[int],
):
    with pg.cursor() as cur:
        cur.execute("CREATE TABLE foo (c int, t text)")

        for checkpoint_id in checkpoint_ids:
            cur.execute(
                f"""
                INSERT INTO foo
                    SELECT {checkpoint_id}, '{FILLER_STRING}'
                    FROM generate_series(1, {CHECKPOINT_ROWS}) g
            """
            )

            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

            # wait until pageserver receives that data
            wait_for_last_record_lsn(ps, tenant_id, timeline_id, current_lsn)

            # run checkpoint manually to be sure that data landed in remote storage
            ps.timeline_checkpoint(tenant_id, timeline_id)

            log.info(f"waiting for checkpoint {checkpoint_id} upload")
            # wait until pageserver successfully uploaded a checkpoint to remote storage
            wait_for_upload(ps, tenant_id, timeline_id, current_lsn)
            log.info(f"upload of checkpoint {checkpoint_id} is done")

    return current_lsn


#
# Tests that a piece of data is backed up and restored correctly:
#
# 1. Initial pageserver
#   * starts a pageserver with remote storage, stores specific data in its tables
#   * triggers a checkpoint (which produces a local data scheduled for backup), gets the corresponding timeline id
#   * polls the timeline status to ensure it's copied remotely
#   * inserts more data in the pageserver and repeats the process, to check multiple checkpoints case
#   * stops the pageserver, clears all local directories
#
# 2. Second pageserver
#   * starts another pageserver, connected to the same remote storage
#   * timeline_attach is called for the same timeline id
#   * timeline status is polled until it's downloaded
#   * queries the specific data, ensuring that it matches the one stored before
#
# The tests are done for all types of remote storage pageserver supports.
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_remote_storage_backup_and_restore(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    # Use this test to check more realistic SK ids: some etcd key parsing bugs were related,
    # and this test needs SK to write data to pageserver, so it will be visible
    neon_env_builder.safekeepers_id_start = 12

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_backup_and_restore",
    )

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    checkpoint_ids = list(range(1, 3))

    current_lsn = populate_checkpoints(
        pg=pg,
        ps=ps_http,
        tenant_id=tenant_id,
        timeline_id=timeline_id,
        checkpoint_ids=checkpoint_ids,
    )

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    # Introduce failpoint in download
    ps_http.configure_failpoints(("remote-storage-download-pre-rename", "return"))

    ps_http.tenant_attach(tenant_id)

    # is there a better way to assert that failpoint triggered?
    time.sleep(10)

    # assert cannot attach timeline that is scheduled for download
    with pytest.raises(Exception, match="Conflict: Tenant download is already in progress"):
        ps_http.tenant_attach(tenant_id)

    detail = ps_http.timeline_detail(tenant_id, timeline_id)
    log.info("Timeline detail with active failpoint: %s", detail)
    assert detail["local"] is None
    assert detail["remote"]["awaits_download"]

    # trigger temporary download files removal
    env.pageserver.stop()
    env.pageserver.start()

    ps_http.tenant_attach(tenant_id)

    log.info("waiting for timeline redownload")
    wait_until(
        number_of_iterations=20,
        interval=1,
        func=lambda: assert_no_in_progress_downloads_for_tenant(ps_http, tenant_id),
    )

    detail = ps_http.timeline_detail(tenant_id, timeline_id)
    assert detail["local"] is not None
    log.info("Timeline detail after attach completed: %s", detail)
    assert (
        Lsn(detail["local"]["last_record_lsn"]) >= current_lsn
    ), "current db Lsn should should not be less than the one stored on remote storage"
    assert not detail["remote"]["awaits_download"]

    pg = env.postgres.create_start("main")
    with pg.cursor() as cur:
        for checkpoint_id in checkpoint_ids:
            assert (
                query_scalar(
                    cur,
                    f"SELECT COUNT(*) FROM foo WHERE c = {checkpoint_id} AND t = '{FILLER_STRING}'",
                )
                == CHECKPOINT_ROWS
            )


def populate_tenant(
    ps_http: NeonPageserverHttpClient,
    cli: NeonCli,
    postgres_factory: PostgresFactory,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    checkpoint_ids: List[int],
):
    cli.create_tenant(tenant_id=tenant_id, timeline_id=timeline_id)

    # should get the same timeline id because it is the only one
    pg = postgres_factory.create_start("main", tenant_id=tenant_id)

    populate_checkpoints(
        pg=pg,
        ps=ps_http,
        tenant_id=tenant_id,
        timeline_id=timeline_id,
        checkpoint_ids=checkpoint_ids,
    )
    return pg


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_remote_storage_corrupt_index_part(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    """
    create two tenants
    corrupt metadata in the remote index part json on one of them
    check that pageserver loaded, and non corrupted timeline works as expected
    """

    # populate data
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_corrupt_index_part",
    )

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    tenant_id1 = TenantId.generate()
    timeline_id1 = TimelineId.generate()

    checkpoint_ids = list(range(1, 3))

    pg1 = populate_tenant(
        ps_http=ps_http,
        cli=env.neon_cli,
        postgres_factory=env.postgres,
        tenant_id=tenant_id1,
        timeline_id=timeline_id1,
        checkpoint_ids=checkpoint_ids,
    )
    pg1.stop()

    tenant_id2 = TenantId.generate()
    timeline_id2 = TimelineId.generate()

    pg2 = populate_tenant(
        ps_http=ps_http,
        cli=env.neon_cli,
        postgres_factory=env.postgres,
        tenant_id=tenant_id2,
        timeline_id=timeline_id2,
        checkpoint_ids=checkpoint_ids,
    )
    pg2.stop()

    # stop pageserver, corrupt metadata
    env.pageserver.stop()

    index_part = read_local_fs_index_part(env, tenant_id1, timeline_id1)
    # corrupt metadata body, so it is not header but real body

    # XXX: should we check for header corruptions too?
    index_part["metadata_bytes"][16] += 1

    write_local_fs_index_part(env, tenant_id1, timeline_id1, index_part)

    env.pageserver.start()

    # I would imagine that tenant with corrupted remote metadata wont be registered as healthy one
    # but in fact because error occurs only during remote index build error does not get propagated to the
    # registration code, so pageserver starts this timeline with all the background threads active

    # check that tenant2 data
    pg2.start()
    with pg2.cursor() as cur:
        for checkpoint_id in checkpoint_ids:
            assert (
                query_scalar(
                    cur,
                    f"SELECT COUNT(*) FROM foo WHERE c = {checkpoint_id} AND t = '{FILLER_STRING}'",
                )
                == CHECKPOINT_ROWS
            )

    # TODO check list of tenants, broken one should be either missing or broken
    #   check timeline detail
    #   pg1 should fail to start
