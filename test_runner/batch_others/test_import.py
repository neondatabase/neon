import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_upload, wait_for_last_record_lsn
from fixtures.utils import lsn_from_hex, lsn_to_hex
from uuid import UUID, uuid4
import tarfile
import os
import shutil
from pathlib import Path
import json
from fixtures.utils import subprocess_capture
from fixtures.log_helper import log
from contextlib import closing
from fixtures.neon_fixtures import pg_distrib_dir


@pytest.mark.timeout(600)
def test_import_from_vanilla(test_output_dir, pg_bin, vanilla_pg, neon_env_builder):
    # Put data in vanilla pg
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    vanilla_pg.safe_psql('''create table t as select 'long string to consume some space' || g
     from generate_series(1,300000) g''')
    assert vanilla_pg.safe_psql('select count(*) from t') == [(300000, )]

    # Take basebackup
    basebackup_dir = os.path.join(test_output_dir, "basebackup")
    base_tar = os.path.join(basebackup_dir, "base.tar")
    wal_tar = os.path.join(basebackup_dir, "pg_wal.tar")
    os.mkdir(basebackup_dir)
    vanilla_pg.safe_psql("CHECKPOINT")
    pg_bin.run([
        "pg_basebackup",
        "-F",
        "tar",
        "-d",
        vanilla_pg.connstr(),
        "-D",
        basebackup_dir,
    ])

    # Make corrupt base tar with missing pg_control
    unpacked_base = os.path.join(basebackup_dir, "unpacked-base")
    corrupt_base_tar = os.path.join(unpacked_base, "corrupt-base.tar")
    os.mkdir(unpacked_base, 0o750)
    subprocess_capture(str(test_output_dir), ["tar", "-xf", base_tar, "-C", unpacked_base])
    os.remove(os.path.join(unpacked_base, "global/pg_control"))
    subprocess_capture(str(test_output_dir),
                       ["tar", "-cf", "corrupt-base.tar"] + os.listdir(unpacked_base),
                       cwd=unpacked_base)

    # Get start_lsn and end_lsn
    with open(os.path.join(basebackup_dir, "backup_manifest")) as f:
        manifest = json.load(f)
        start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
        end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    node_name = "import_from_vanilla"
    tenant = uuid4()
    timeline = uuid4()

    # Set up pageserver for import
    neon_env_builder.enable_local_fs_remote_storage()
    env = neon_env_builder.init_start()
    env.pageserver.http_client().tenant_create(tenant)

    def import_tar(base, wal):
        env.neon_cli.raw_cli([
            "timeline",
            "import",
            "--tenant-id",
            tenant.hex,
            "--timeline-id",
            timeline.hex,
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
        ])

    # Importing empty file fails
    empty_file = os.path.join(test_output_dir, "empty_file")
    with open(empty_file, 'w') as _:
        with pytest.raises(Exception):
            import_tar(empty_file, empty_file)

    # Importing corrupt backup fails
    with pytest.raises(Exception):
        import_tar(corrupt_base_tar, wal_tar)

    # Clean up
    # TODO it should clean itself
    client = env.pageserver.http_client()
    client.timeline_detach(tenant, timeline)

    # Importing correct backup works
    import_tar(base_tar, wal_tar)

    # Wait for data to land in s3
    wait_for_last_record_lsn(client, tenant, timeline, lsn_from_hex(end_lsn))
    wait_for_upload(client, tenant, timeline, lsn_from_hex(end_lsn))

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql('select count(*) from t') == [(300000, )]


@pytest.mark.timeout(600)
def test_import_from_pageserver(test_output_dir, pg_bin, vanilla_pg, neon_env_builder):

    num_rows = 3000
    neon_env_builder.num_safekeepers = 1
    neon_env_builder.enable_local_fs_remote_storage()
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_import_from_pageserver')
    pgmain = env.postgres.create_start('test_import_from_pageserver')
    log.info("postgres is running on 'test_import_from_pageserver' branch")

    timeline = pgmain.safe_psql("SHOW neon.timeline_id")[0][0]

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            # data loading may take a while, so increase statement timeout
            cur.execute("SET statement_timeout='300s'")
            cur.execute(f'''CREATE TABLE tbl AS SELECT 'long string to consume some space' || g
                        from generate_series(1,{num_rows}) g''')
            cur.execute("CHECKPOINT")

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]
            log.info(f"start_backup_lsn = {lsn}")

    # Set LD_LIBRARY_PATH in the env properly, otherwise we may use the wrong libpq.
    # PgBin sets it automatically, but here we need to pipe psql output to the tar command.
    psql_env = {'LD_LIBRARY_PATH': os.path.join(str(pg_distrib_dir), 'lib')}

    # Get a fullbackup from pageserver
    query = f"fullbackup { env.initial_tenant.hex} {timeline} {lsn}"
    cmd = ["psql", "--no-psqlrc", env.pageserver.connstr(), "-c", query]
    result_basepath = pg_bin.run_capture(cmd, env=psql_env)
    tar_output_file = result_basepath + ".stdout"

    # Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / 'tenants'
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    #start the pageserver again
    env.pageserver.start()

    # Import using another tenantid, because we use the same pageserver.
    # TODO Create another pageserver to maeke test more realistic.
    tenant = uuid4()

    # Import to pageserver
    node_name = "import_from_pageserver"
    client = env.pageserver.http_client()
    client.tenant_create(tenant)
    env.neon_cli.raw_cli([
        "timeline",
        "import",
        "--tenant-id",
        tenant.hex,
        "--timeline-id",
        timeline,
        "--node-name",
        node_name,
        "--base-lsn",
        lsn,
        "--base-tarfile",
        os.path.join(tar_output_file),
    ])

    # Wait for data to land in s3
    wait_for_last_record_lsn(client, tenant, UUID(timeline), lsn_from_hex(lsn))
    wait_for_upload(client, tenant, UUID(timeline), lsn_from_hex(lsn))

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql('select count(*) from tbl') == [(num_rows, )]

    # Take another fullbackup
    query = f"fullbackup { tenant.hex} {timeline} {lsn}"
    cmd = ["psql", "--no-psqlrc", env.pageserver.connstr(), "-c", query]
    result_basepath = pg_bin.run_capture(cmd, env=psql_env)
    new_tar_output_file = result_basepath + ".stdout"

    # Check it's the same as the first fullbackup
    # TODO pageserver should be checking checksum
    assert os.path.getsize(tar_output_file) == os.path.getsize(new_tar_output_file)
