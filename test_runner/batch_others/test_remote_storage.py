# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/zenith_zzz/'}" pipenv ......

import tempfile, time, shutil, os
from contextlib import closing
from pathlib import Path
from fixtures.zenith_fixtures import ZenithEnvBuilder, LocalFsStorage, check_restored_datadir_content
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


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
def test_remote_storage_backup_and_restore(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.rust_log_override = 'debug'
    zenith_env_builder.num_safekeepers = 1
    zenith_env_builder.enable_local_fs_remote_storage()

    data_id = 1
    data_secret = 'very secret secret'

    ##### First start, insert secret data and upload it to the remote storage
    env = zenith_env_builder.init()
    pg = env.postgres.create_start()

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

    log.info("waiting for timeline redownload")
    client = env.pageserver.http_client()
    attempts = 0
    while True:
        timeline_details = client.timeline_details(tenant_id, timeline_id)
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

    pg = env.postgres.create_start()
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT secret FROM t1 WHERE id = {data_id};')
            assert cur.fetchone() == (data_secret, )
