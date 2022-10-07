#
# Little stress test for the checkpointing and remote storage code.
#
# The test creates several tenants, and runs a simple workload on
# each tenant, in parallel. The test uses remote storage, and a tiny
# checkpoint_distance setting so that a lot of layer files are created.
#

import asyncio
import os
from pathlib import Path
from typing import List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserverHttpClient,
    Postgres,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar


async def tenant_workload(env: NeonEnv, pg: Postgres):
    await env.pageserver.connect_async()

    pg_conn = await pg.connect_async()

    await pg_conn.execute("CREATE TABLE t(key int primary key, value text)")
    for i in range(1, 100):
        await pg_conn.execute(
            f"INSERT INTO t SELECT {i}*1000 + g, 'payload' from generate_series(1,1000) g"
        )

        # we rely upon autocommit after each statement
        # as waiting for acceptors happens there
        res = await pg_conn.fetchval("SELECT count(*) FROM t")
        assert res == i * 1000


async def all_tenants_workload(env: NeonEnv, tenants_pgs):
    workers = []
    for _, pg in tenants_pgs:
        worker = tenant_workload(env, pg)
        workers.append(asyncio.create_task(worker))

    # await all workers
    await asyncio.gather(*workers)


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_tenants_many(neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_tenants_many",
    )

    env = neon_env_builder.init_start()

    tenants_pgs: List[Tuple[TenantId, Postgres]] = []

    for _ in range(1, 5):
        # Use a tiny checkpoint distance, to create a lot of layers quickly
        tenant, _ = env.neon_cli.create_tenant(
            conf={
                "checkpoint_distance": "5000000",
            }
        )
        env.neon_cli.create_timeline("test_tenants_many", tenant_id=tenant)

        pg = env.postgres.create_start(
            "test_tenants_many",
            tenant_id=tenant,
        )
        tenants_pgs.append((tenant, pg))

    asyncio.run(all_tenants_workload(env, tenants_pgs))

    # Wait for the remote storage uploads to finish
    pageserver_http = env.pageserver.http_client()
    for tenant, pg in tenants_pgs:
        res = pg.safe_psql_many(
            ["SHOW neon.tenant_id", "SHOW neon.timeline_id", "SELECT pg_current_wal_flush_lsn()"]
        )
        tenant_id = TenantId(res[0][0][0])
        timeline_id = TimelineId(res[1][0][0])
        current_lsn = Lsn(res[2][0][0])

        # wait until pageserver receives all the data
        wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)

        # run final checkpoint manually to flush all the data to remote storage
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_tenants_attached_after_download(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="remote_storage_kind",
    )

    data_id = 1
    data_secret = "very secret secret"

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    for checkpoint_number in range(1, 3):
        with pg.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE t{checkpoint_number}(id int primary key, secret text);
                INSERT INTO t{checkpoint_number} VALUES ({data_id}, '{data_secret}|{checkpoint_number}');
            """
            )
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        log.info(f"waiting for checkpoint {checkpoint_number} upload")
        # wait until pageserver successfully uploaded a checkpoint to remote storage
        wait_for_upload(client, tenant_id, timeline_id, current_lsn)
        log.info(f"upload of checkpoint {checkpoint_number} is done")

    ##### Stop the pageserver, erase its layer file to force it being downloaded from S3
    env.postgres.stop_all()
    env.pageserver.stop()

    timeline_dir = Path(env.repo_dir) / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    local_layer_deleted = False
    for path in Path.iterdir(timeline_dir):
        if path.name.startswith("00000"):
            # Looks like a layer file. Remove it
            os.remove(path)
            local_layer_deleted = True
            break
    assert local_layer_deleted, f"Found no local layer files to delete in directory {timeline_dir}"

    ##### Start the pageserver, forcing it to download the layer file and load the timeline into memory
    env.pageserver.start()
    client = env.pageserver.http_client()

    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: expect_tenant_to_download_timeline(client, tenant_id),
    )

    restored_timelines = client.timeline_list(tenant_id)
    assert (
        len(restored_timelines) == 1
    ), f"Tenant {tenant_id} should have its timeline reattached after its layer is downloaded from the remote storage"
    retored_timeline = restored_timelines[0]
    assert retored_timeline["timeline_id"] == str(
        timeline_id
    ), f"Tenant {tenant_id} should have its old timeline {timeline_id} restored from the remote storage"


def expect_tenant_to_download_timeline(
    client: NeonPageserverHttpClient,
    tenant_id: TenantId,
):
    for tenant in client.tenant_list():
        if tenant["id"] == str(tenant_id):
            assert not tenant.get(
                "has_in_progress_downloads", True
            ), f"Tenant {tenant_id} should have no downloads in progress"
            return
    assert False, f"Tenant {tenant_id} is missing on pageserver"
