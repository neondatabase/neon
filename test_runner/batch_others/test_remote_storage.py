# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/zenith_zzz/'}" poetry ......

import shutil, os
from contextlib import closing
from pathlib import Path
import time
from uuid import UUID
from fixtures.zenith_fixtures import ZenithEnvBuilder, assert_local, wait_until, wait_for_last_record_lsn, wait_for_upload
from fixtures.log_helper import log
from fixtures.utils import lsn_from_hex, lsn_to_hex
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
@pytest.mark.parametrize('storage_type', ['local_fs', 'mock_s3'])
def test_remote_storage_backup_and_restore(zenith_env_builder: ZenithEnvBuilder, storage_type: str):
    # zenith_env_builder.rust_log_override = 'debug'
    if storage_type == 'local_fs':
        zenith_env_builder.enable_local_fs_remote_storage()
    elif storage_type == 'mock_s3':
        zenith_env_builder.enable_s3_mock_remote_storage('test_remote_storage_backup_and_restore')
    else:
        raise RuntimeError(f'Unknown storage type: {storage_type}')

    data_id = 1
    data_secret = 'very secret secret'

    ##### First start, insert secret data and upload it to the remote storage
    env = zenith_env_builder.init_start()
    pg = env.postgres.create_start('main')

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    checkpoint_numbers = range(1, 3)

    for checkpoint_number in checkpoint_numbers:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute(f'''
                    CREATE TABLE t{checkpoint_number}(id int primary key, secret text);
                    INSERT INTO t{checkpoint_number} VALUES ({data_id}, '{data_secret}|{checkpoint_number}');
                ''')
                cur.execute("SELECT pg_current_wal_flush_lsn()")
                current_lsn = lsn_from_hex(cur.fetchone()[0])

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, UUID(tenant_id), UUID(timeline_id), current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        env.pageserver.safe_psql(f"checkpoint {tenant_id} {timeline_id}")

        log.info(f'waiting for checkpoint {checkpoint_number} upload')
        # wait until pageserver successfully uploaded a checkpoint to remote storage
        wait_for_upload(client, UUID(tenant_id), UUID(timeline_id), current_lsn)
        log.info(f'upload of checkpoint {checkpoint_number} is done')

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / 'tenants'
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    # Introduce failpoint in download
    env.pageserver.safe_psql(f"failpoints remote-storage-download-pre-rename=return")

    client.timeline_attach(UUID(tenant_id), UUID(timeline_id))

    # is there a better way to assert that fafilpoint triggered?
    time.sleep(10)

    # assert cannot attach timeline that is scheduled for download
    with pytest.raises(Exception, match="Timeline download is already in progress"):
        client.timeline_attach(UUID(tenant_id), UUID(timeline_id))

    detail = client.timeline_detail(UUID(tenant_id), UUID(timeline_id))
    log.info("Timeline detail with active failpoint: %s", detail)
    assert detail['local'] is None
    assert detail['remote']['awaits_download']

    # trigger temporary download files removal
    env.pageserver.stop()
    env.pageserver.start()

    client.timeline_attach(UUID(tenant_id), UUID(timeline_id))

    log.info("waiting for timeline redownload")
    wait_until(number_of_iterations=10,
               interval=1,
               func=lambda: assert_local(client, UUID(tenant_id), UUID(timeline_id)))

    detail = client.timeline_detail(UUID(tenant_id), UUID(timeline_id))
    assert detail['local'] is not None
    log.info("Timeline detail after attach completed: %s", detail)
    assert lsn_from_hex(detail['local']['last_record_lsn']) >= current_lsn, 'current db Lsn should should not be less than the one stored on remote storage'
    assert not detail['remote']['awaits_download']

    pg = env.postgres.create_start('main')
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            for checkpoint_number in checkpoint_numbers:
                cur.execute(f'SELECT secret FROM t{checkpoint_number} WHERE id = {data_id};')
                assert cur.fetchone() == (f'{data_secret}|{checkpoint_number}', )
