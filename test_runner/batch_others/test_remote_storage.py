# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/zenith_zzz/'}" poetry ......

import time, shutil, os
from contextlib import closing
from pathlib import Path
from uuid import UUID
from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log
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
@pytest.mark.skip(reason="will be fixed with https://github.com/zenithdb/zenith/issues/1193")
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

    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    timeline_id = pg.safe_psql("show zenith.zenith_timeline")[0][0]

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                CREATE TABLE t1(id int primary key, secret text);
                INSERT INTO t1 VALUES ({data_id}, '{data_secret}');
            ''')

    # run checkpoint manually to be sure that data landed in remote storage
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"do_gc {tenant_id} {timeline_id}")
    log.info("waiting for upload")  # TODO api to check if upload is done
    time.sleep(2)

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / 'tenants'
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    client = env.pageserver.http_client()
    client.timeline_attach(UUID(tenant_id), UUID(timeline_id))
    # FIXME cannot handle duplicate download requests (which might be caused by repeated timeline detail calls)
    #   subject to fix in https://github.com/zenithdb/zenith/issues/997
    time.sleep(5)

    log.info("waiting for timeline redownload")
    attempts = 0
    while True:
        timeline_details = client.timeline_detail(UUID(tenant_id), UUID(timeline_id))
        assert timeline_details['timeline_id'] == timeline_id
        assert timeline_details['tenant_id'] == tenant_id
        if timeline_details['type'] == 'Local':
            log.info("timeline downloaded, checking its data")
            break
        attempts += 1
        if attempts > 10:
            raise Exception("timeline redownload failed")
        log.debug("still waiting")
        time.sleep(1)

    pg = env.postgres.create_start('main')
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT secret FROM t1 WHERE id = {data_id};')
            assert cur.fetchone() == (data_secret, )
