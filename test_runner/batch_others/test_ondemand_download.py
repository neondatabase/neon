# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/neon_zzz/'}" poetry ......

import os
import shutil
import time
from pathlib import Path
from uuid import UUID

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until,
)
from fixtures.utils import lsn_from_hex, lsn_to_hex, query_scalar


#
# If you have a large relation, check that the pageserver downloads parts of it as
# require by queries.
#
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_ondemand_download_large_rel(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ondemand_download_large_rel",
    )

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # Override defaults, to create more layers
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            # disable background GC
            "gc_period": "10 m",
            "gc_horizon": f"{10 * 1024 ** 3}",  # 10 GB
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{10 * 1024 ** 2}",  # 10 MB
            "compaction_threshold": "2",
            "compaction_target_size": f"{10 * 1024 ** 2}",  # 10 MB
        }
    )
    env.initial_tenant = tenant

    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    lsns = []

    # We want to make sure that the data is large enough that the keyspace is partitioned.
    num_rows = 1000000

    with pg.cursor() as cur:
        # data loading may take a while, so increase statement timeout
        cur.execute("SET statement_timeout='300s'")
        cur.execute(
            f"""CREATE TABLE tbl AS SELECT g as id, 'long string to consume some space' || g
        from generate_series(1,{num_rows}) g"""
        )
        cur.execute("CREATE INDEX ON tbl (id)")
        cur.execute("VACUUM tbl")

        current_lsn = lsn_from_hex(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # wait until pageserver receives that data
    wait_for_last_record_lsn(client, UUID(tenant_id), UUID(timeline_id), current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    env.pageserver.safe_psql(f"checkpoint {tenant_id} {timeline_id}")

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, UUID(tenant_id), UUID(timeline_id), current_lsn)
    log.info(f"uploads have finished")

    time.sleep(10)

    ##### Stop the first pageserver instance, erase all its data
    pg.stop()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    client.tenant_attach(UUID(tenant_id))

    pg.start()
    before_downloads = int(
        client.get_metric_value(
            "pageserver_layers_downloaded_total", UUID(tenant_id), UUID(timeline_id)
        )
    )

    # Probe in the middle of the table. There's a high chance that the beginning
    # and end of the table was stored together in the same layer files with data
    # from other tables, and with the entry that stores the size of the
    # relation, so they are likely already downloaded. But the middle of the
    # table should not have been needed by anything yet.
    with pg.cursor() as cur:
        assert query_scalar(cur, "select count(*) from tbl where id = 500000") == 1

    after_downloads = int(
        client.get_metric_value(
            "pageserver_layers_downloaded_total", UUID(tenant_id), UUID(timeline_id)
        )
    )
    log.info(f"layers downloaded before {before_downloads} and after {after_downloads}")
    assert after_downloads > before_downloads


#
# If you have a relation with a long history of updates,the pageserver downloads the layer
# files containing the history as needed by timetravel queries.
#
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_ondemand_download_timetravel(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ondemand_download_timetravel",
    )

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # FIXME: The initial tenant isn't uploaded correctly at bootstrapping.
    # Create a tenant after bootstrapping and use that instead.
    # See https://github.com/neondatabase/neon/pull/2272
    tenant, _ = env.neon_cli.create_tenant()
    env.initial_tenant = tenant

    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    lsns = []

    with pg.cursor() as cur:
        cur.execute(
            f"""
        CREATE TABLE testtab(id serial primary key, checkpoint_number int, data text);
        INSERT INTO testtab (checkpoint_number, data) SELECT 0, 'data' FROM generate_series(1, 100000);
        """
        )
        current_lsn = lsn_from_hex(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    lsns.append((0, current_lsn))

    for checkpoint_number in range(1, 5):
        with pg.cursor() as cur:
            cur.execute(f"UPDATE testtab SET checkpoint_number = {checkpoint_number}")
            current_lsn = lsn_from_hex(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        lsns.append((checkpoint_number, current_lsn))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, UUID(tenant_id), UUID(timeline_id), current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        env.pageserver.safe_psql(f"checkpoint {tenant_id} {timeline_id}")

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, UUID(tenant_id), UUID(timeline_id), current_lsn)
    log.info(f"uploads have finished")

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    client.tenant_attach(UUID(tenant_id))

    # FIXME: work around bug https://github.com/neondatabase/neon/issues/2305.
    # Once that's fixed, this reverse() call can be removed.
    lsns.reverse()

    for (checkpoint_number, lsn) in lsns:
        pg_old = env.postgres.create_start(
            branch_name="main", node_name=f"test_old_lsn_{checkpoint_number}", lsn=lsn_to_hex(lsn)
        )
        before_downloads = int(
            client.get_metric_value(
                "pageserver_layers_downloaded_total", UUID(tenant_id), UUID(timeline_id)
            )
        )
        with pg_old.cursor() as cur:
            # assert query_scalar(cur, f"select count(*) from testtab where checkpoint_number={checkpoint_number}") == 100000
            assert (
                query_scalar(
                    cur,
                    f"select count(*) from testtab where checkpoint_number<>{checkpoint_number}",
                )
                == 0
            )
        after_downloads = int(
            client.get_metric_value(
                "pageserver_layers_downloaded_total", UUID(tenant_id), UUID(timeline_id)
            )
        )
        # FIXME: the idea here was to check that on each query, we need to download one more
        # layer file. However, calculating the initial timeline size currently downloads all the
        # layers. That's unfortunate.
        # assert after_downloads > before_downloads
        log.info(f"layers downloaded before {before_downloads} and after {after_downloads}")
