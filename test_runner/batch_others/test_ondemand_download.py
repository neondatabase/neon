# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/neon_zzz/'}" poetry ......

import shutil, os
from pathlib import Path
import time
from uuid import UUID
from fixtures.neon_fixtures import NeonEnvBuilder, RemoteStorageKind, available_remote_storages, wait_until, wait_for_last_record_lsn, wait_for_upload
from fixtures.log_helper import log
from fixtures.utils import lsn_from_hex, lsn_to_hex, query_scalar
import pytest

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
@pytest.mark.parametrize('remote_storage_kind', available_remote_storages())
def test_ondemand_download(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name='test_remote_storage_backup_and_restore',
    )

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # FIXME: The initial tenant isn't uploaded correctly at bootstrapping.
    # Create a tenant after bootstrapping and use that instead.
    # See https://github.com/neondatabase/neon/pull/2272
    tenant, _ = env.neon_cli.create_tenant()
    env.initial_tenant = tenant

    pg = env.postgres.create_start('main')

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    lsns = []

    with pg.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE testtab(id serial primary key, checkpoint_number int, data text);
        INSERT INTO testtab (checkpoint_number, data) SELECT 0, 'data' FROM generate_series(1, 100000);
        """)
        current_lsn = lsn_from_hex(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    lsns.append((0, current_lsn))

    for checkpoint_number in range(1, 5):
        with pg.cursor() as cur:
            cur.execute(f'UPDATE testtab SET checkpoint_number = {checkpoint_number}')
            current_lsn = lsn_from_hex(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        lsns.append((checkpoint_number, current_lsn))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, UUID(tenant_id), UUID(timeline_id), current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        env.pageserver.safe_psql(f"checkpoint {tenant_id} {timeline_id}")

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, UUID(tenant_id), UUID(timeline_id), current_lsn)
    log.info(f'uploads have finished')

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / 'tenants'
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    client.tenant_attach(UUID(tenant_id))
    
    # FIXME: work around bug https://github.com/neondatabase/neon/issues/2305.
    # Once that's fixed, this reverse() call can be removed.
    lsns.reverse()

    for (checkpoint_number, lsn) in lsns:
        pg_old = env.postgres.create_start(branch_name='main',
                                           node_name=f'test_old_lsn_{checkpoint_number}',
                                           lsn=lsn_to_hex(lsn))
        before_downloads = int(client.get_metric_value("pageserver_layers_downloaded_total", UUID(tenant_id), UUID(timeline_id)))
        with pg_old.cursor() as cur:
            #assert query_scalar(cur, f"select count(*) from testtab where checkpoint_number={checkpoint_number}") == 100000
            assert query_scalar(cur, f"select count(*) from testtab where checkpoint_number<>{checkpoint_number}") == 0
        after_downloads = int(client.get_metric_value("pageserver_layers_downloaded_total", UUID(tenant_id), UUID(timeline_id)))
        # FIXME: the idea here was to check that on each query, we need to download one more
        # layer file. However, calculating the initial timeline size currently downloads all the
        # layers. That's unfortunate.
        # assert after_downloads > before_downloads
        log.info(f"layers downloaded before {before_downloads} and after {after_downloads}")
