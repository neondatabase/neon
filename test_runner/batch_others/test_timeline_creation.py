from typing import List
from fixtures.zenith_fixtures import ZenithEnv, zenith_binpath
import concurrent.futures
import os
from uuid import UUID
import subprocess
from fixtures.log_helper import log
from contextlib import closing


def test_create_multiple_timelines_parallel(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env

    tenant_id, _ = env.zenith_cli.create_tenant()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(env.zenith_cli.create_timeline, f"test-timeline-{i}", tenant_id)
            for i in range(4)
        ]
        for future in futures:
            future.result()


def update_timeline_metadata(env: ZenithEnv,
                             tenant_id: UUID,
                             timeline_id: UUID,
                             args: List[str] = []):
    bin_update_metadata = os.path.join(str(zenith_binpath), 'update_metadata')

    metadata_path = f"{env.repo_dir}/tenants/{tenant_id.hex}/timelines/{timeline_id.hex}/metadata"

    args = [bin_update_metadata, metadata_path] + args

    # Run the `update_metadata` command
    # The codes below are copied from `zenith_fixtures.ZenithCli.raw_cli` function.
    log.info('Running command "{}"'.format(' '.join(args)))
    log.info(f'Running in "{env.repo_dir}"')

    try:
        res = subprocess.run(args,
                             check=True,
                             universal_newlines=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        log.info(f"Run success: {res.stdout}")
    except subprocess.CalledProcessError as exc:
        # this way command output will be in recorded and shown in CI in failure message
        msg = f"""\
        Run failed: {exc}
            stdout: {exc.stdout}
            stderr: {exc.stderr}
        """
        log.info(msg)

        raise Exception(msg) from exc


def test_find_and_fix_unintialized_timelines(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env

    tenant_id, _ = env.zenith_cli.create_tenant()

    timeline_id = env.zenith_cli.create_timeline("test", tenant_id)

    # Try to simulate an uninintialized timeline by
    # - removing all layer files
    # - update the `prev_record_lsn` and `disk_lsn` of the timeline's metadata
    timeline_path = f"{env.repo_dir}/tenants/{tenant_id.hex}/timelines/{timeline_id.hex}"
    for file in os.listdir(timeline_path):
        print(file)
        if file != "metadata":
            os.remove(f"{timeline_path}/{file}")
    update_timeline_metadata(env, tenant_id, timeline_id, ["--disk_lsn", "0/0", "--prev_lsn", ""])

    env.zenith_cli.pageserver_stop(immediate=True)
    env.zenith_cli.pageserver_start()

    # Ensure that the tenant state is not broken and executing transactions
    # on the new timeline should succeed without any errors.
    pg = env.postgres.create_start("test", tenant_id=tenant_id)
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100), 'payload'")
            cur.execute("SELECT COUNT(*) FROM t")
            assert cur.fetchone()[0] == 100
