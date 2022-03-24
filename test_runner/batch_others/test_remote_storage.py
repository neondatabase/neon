# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/zenith_zzz/'}" poetry ......

import shutil, os
from contextlib import closing
from pathlib import Path
from uuid import UUID
from fixtures.zenith_fixtures import ZenithEnvBuilder, assert_local, wait_for, wait_for_last_record_lsn, wait_for_upload
from fixtures.log_helper import log
from fixtures.utils import lsn_from_hex
import pytest


#
# Tests that a piece of data is backed up and restored correctly:
#
# 1. Initial pageserver
#   * starts a pageserver with remote storage, stores specific data in its tables
#   * triggers a checkpoint (which produces a local data scheduled for backup), gets the corresponding timeline id
#   * polls the timeline status to ensure it's copied remotely
#   * stops the pageserver, clears all local directories
#
# 2. Second pageserver
#   * starts another pageserver, connected to the same remote storage
#   * same timeline id is queried for status, triggering timeline's download
#   * timeline status is polled until it's downloaded
#   * queries the specific data, ensuring that it matches the one stored before
#
# The tests are done for all types of remote storage pageserver supports.
@pytest.mark.parametrize('storage_type', ['local_fs', 'mock_s3'])
def test_remote_storage_backup_and_restore(zenith_env_builder: ZenithEnvBuilder, storage_type: str):
    zenith_env_builder.rust_log_override = 'debug'
    zenith_env_builder.num_safekeepers = 1
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

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                CREATE TABLE t1(id int primary key, secret text);
                INSERT INTO t1 VALUES ({data_id}, '{data_secret}');
            ''')
            cur.execute("SELECT pg_current_wal_flush_lsn()")
            current_lsn = lsn_from_hex(cur.fetchone()[0])

    # wait until pageserver receives that data
    wait_for_last_record_lsn(client, UUID(tenant_id), UUID(timeline_id), current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"checkpoint {tenant_id} {timeline_id}")

    log.info("waiting for upload")
    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, UUID(tenant_id), UUID(timeline_id), current_lsn)
    log.info("upload is done")

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / 'tenants'
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    client.timeline_attach(UUID(tenant_id), UUID(timeline_id))

    log.info("waiting for timeline redownload")
    wait_for(number_of_iterations=10,
             interval=1,
             func=lambda: assert_local(client, UUID(tenant_id), UUID(timeline_id)))

    pg = env.postgres.create_start('main')
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT secret FROM t1 WHERE id = {data_id};')
            assert cur.fetchone() == (data_secret, )
