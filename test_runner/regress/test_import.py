import json
import os
import re
import shutil
import tarfile
from contextlib import closing
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    Postgres,
    pg_distrib_dir,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import subprocess_capture


@pytest.mark.timeout(600)
def test_import_from_vanilla(test_output_dir, pg_bin, vanilla_pg, neon_env_builder):
    # Put data in vanilla pg
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    vanilla_pg.safe_psql(
        """create table t as select 'long string to consume some space' || g
     from generate_series(1,300000) g"""
    )
    assert vanilla_pg.safe_psql("select count(*) from t") == [(300000,)]

    # Take basebackup
    basebackup_dir = os.path.join(test_output_dir, "basebackup")
    base_tar = os.path.join(basebackup_dir, "base.tar")
    wal_tar = os.path.join(basebackup_dir, "pg_wal.tar")
    os.mkdir(basebackup_dir)
    vanilla_pg.safe_psql("CHECKPOINT")
    pg_bin.run(
        [
            "pg_basebackup",
            "-F",
            "tar",
            "-d",
            vanilla_pg.connstr(),
            "-D",
            basebackup_dir,
        ]
    )

    # Make corrupt base tar with missing pg_control
    unpacked_base = os.path.join(basebackup_dir, "unpacked-base")
    corrupt_base_tar = os.path.join(unpacked_base, "corrupt-base.tar")
    os.mkdir(unpacked_base, 0o750)
    subprocess_capture(str(test_output_dir), ["tar", "-xf", base_tar, "-C", unpacked_base])
    os.remove(os.path.join(unpacked_base, "global/pg_control"))
    subprocess_capture(
        str(test_output_dir),
        ["tar", "-cf", "corrupt-base.tar"] + os.listdir(unpacked_base),
        cwd=unpacked_base,
    )

    # Get start_lsn and end_lsn
    with open(os.path.join(basebackup_dir, "backup_manifest")) as f:
        manifest = json.load(f)
        start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
        end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    node_name = "import_from_vanilla"
    tenant = TenantId.generate()
    timeline = TimelineId.generate()

    # Set up pageserver for import
    neon_env_builder.enable_local_fs_remote_storage()
    env = neon_env_builder.init_start()
    env.pageserver.http_client().tenant_create(tenant)

    def import_tar(base, wal):
        env.neon_cli.raw_cli(
            [
                "timeline",
                "import",
                "--tenant-id",
                str(tenant),
                "--timeline-id",
                str(timeline),
                "--node-name",
                node_name,
                "--base-lsn",
                start_lsn,
                "--base-tarfile",
                base,
                "--end-lsn",
                end_lsn,
                "--wal-tarfile",
                wal,
                "--pg-version",
                env.pg_version,
            ]
        )

    # Importing corrupt backup fails
    with pytest.raises(Exception):
        import_tar(corrupt_base_tar, wal_tar)

    # Clean up
    # TODO it should clean itself
    client = env.pageserver.http_client()
    client.timeline_delete(tenant, timeline)

    # Importing correct backup works
    import_tar(base_tar, wal_tar)

    # Wait for data to land in s3
    wait_for_last_record_lsn(client, tenant, timeline, Lsn(end_lsn))
    wait_for_upload(client, tenant, timeline, Lsn(end_lsn))

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql("select count(*) from t") == [(300000,)]


@pytest.mark.timeout(600)
def test_import_from_pageserver_small(pg_bin: PgBin, neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_local_fs_remote_storage()
    env = neon_env_builder.init_start()

    timeline = env.neon_cli.create_branch("test_import_from_pageserver_small")
    pg = env.postgres.create_start("test_import_from_pageserver_small")

    num_rows = 3000
    lsn = _generate_data(num_rows, pg)
    _import(num_rows, lsn, env, pg_bin, timeline)


@pytest.mark.timeout(1800)
# TODO: temporarily disable `test_import_from_pageserver_multisegment` test, enable
# the test back after finding the failure cause.
# @pytest.mark.skipif(os.environ.get('BUILD_TYPE') == "debug", reason="only run with release build")
@pytest.mark.skip("See https://github.com/neondatabase/neon/issues/2255")
def test_import_from_pageserver_multisegment(pg_bin: PgBin, neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_local_fs_remote_storage()
    env = neon_env_builder.init_start()

    timeline = env.neon_cli.create_branch("test_import_from_pageserver_multisegment")
    pg = env.postgres.create_start("test_import_from_pageserver_multisegment")

    # For `test_import_from_pageserver_multisegment`, we want to make sure that the data
    # is large enough to create multi-segment files. Typically, a segment file's size is
    # at most 1GB. A large number of inserted rows (`30000000`) is used to increase the
    # DB size to above 1GB. Related: https://github.com/neondatabase/neon/issues/2097.
    num_rows = 30000000
    lsn = _generate_data(num_rows, pg)

    logical_size = env.pageserver.http_client().timeline_detail(env.initial_tenant, timeline)[
        "local"
    ]["current_logical_size"]
    log.info(f"timeline logical size = {logical_size / (1024 ** 2)}MB")
    assert logical_size > 1024**3  # = 1GB

    tar_output_file = _import(num_rows, lsn, env, pg_bin, timeline)

    # Check if the backup data contains multiple segment files
    cnt_seg_files = 0
    segfile_re = re.compile("[0-9]+\\.[0-9]+")
    with tarfile.open(tar_output_file, "r") as tar_f:
        for f in tar_f.getnames():
            if segfile_re.search(f) is not None:
                cnt_seg_files += 1
                log.info(f"Found a segment file: {f} in the backup archive file")
    assert cnt_seg_files > 0


def _generate_data(num_rows: int, pg: Postgres) -> Lsn:
    """Generate a table with `num_rows` rows.

    Returns:
    the latest insert WAL's LSN"""
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # data loading may take a while, so increase statement timeout
            cur.execute("SET statement_timeout='300s'")
            cur.execute(
                f"""CREATE TABLE tbl AS SELECT 'long string to consume some space' || g
                        from generate_series(1,{num_rows}) g"""
            )
            cur.execute("CHECKPOINT")

            cur.execute("SELECT pg_current_wal_insert_lsn()")
            res = cur.fetchone()
            assert res is not None and isinstance(res[0], str)
            return Lsn(res[0])


def _import(
    expected_num_rows: int, lsn: Lsn, env: NeonEnv, pg_bin: PgBin, timeline: TimelineId
) -> str:
    """Test importing backup data to the pageserver.

    Args:
    expected_num_rows: the expected number of rows of the test table in the backup data
    lsn: the backup's base LSN

    Returns:
    path to the backup archive file"""
    log.info(f"start_backup_lsn = {lsn}")

    # Set LD_LIBRARY_PATH in the env properly, otherwise we may use the wrong libpq.
    # PgBin sets it automatically, but here we need to pipe psql output to the tar command.
    psql_env = {"LD_LIBRARY_PATH": os.path.join(str(pg_distrib_dir), "lib")}

    # Get a fullbackup from pageserver
    query = f"fullbackup { env.initial_tenant} {timeline} {lsn}"
    cmd = ["psql", "--no-psqlrc", env.pageserver.connstr(), "-c", query]
    result_basepath = pg_bin.run_capture(cmd, env=psql_env)
    tar_output_file = result_basepath + ".stdout"

    # Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    # start the pageserver again
    env.pageserver.start()

    # Import using another tenant_id, because we use the same pageserver.
    # TODO Create another pageserver to make test more realistic.
    tenant = TenantId.generate()

    # Import to pageserver
    node_name = "import_from_pageserver"
    client = env.pageserver.http_client()
    client.tenant_create(tenant)
    env.neon_cli.raw_cli(
        [
            "timeline",
            "import",
            "--tenant-id",
            str(tenant),
            "--timeline-id",
            str(timeline),
            "--node-name",
            node_name,
            "--base-lsn",
            str(lsn),
            "--base-tarfile",
            os.path.join(tar_output_file),
            "--pg-version",
            env.pg_version,
        ]
    )

    # Wait for data to land in s3
    wait_for_last_record_lsn(client, tenant, timeline, lsn)
    wait_for_upload(client, tenant, timeline, lsn)

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql("select count(*) from tbl") == [(expected_num_rows,)]

    # Take another fullbackup
    query = f"fullbackup { tenant} {timeline} {lsn}"
    cmd = ["psql", "--no-psqlrc", env.pageserver.connstr(), "-c", query]
    result_basepath = pg_bin.run_capture(cmd, env=psql_env)
    new_tar_output_file = result_basepath + ".stdout"

    # Check it's the same as the first fullbackup
    # TODO pageserver should be checking checksum
    assert os.path.getsize(tar_output_file) == os.path.getsize(new_tar_output_file)

    # Check that gc works
    pageserver_http = env.pageserver.http_client()
    pageserver_http.timeline_gc(tenant, timeline, 0)

    return tar_output_file
