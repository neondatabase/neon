import pytest
from fixtures.neon_fixtures import NeonEnvBuilder
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
     from generate_series(1,30000000) g''')
    assert vanilla_pg.safe_psql('select count(*) from t') == [(30000000, )]
    # ensure that relation is larger than 1GB to test multisegment restore
    assert vanilla_pg.safe_psql("select pg_relation_size('t')")[0][0] > 1024 * 1024 * 1024

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

    # Make corrupt base tar with missing files
    # TODO try deleting less obvious files. This deletes pg_control,
    #      which is essential and we explicitly check for its existence.
    corrupt_base_tar = os.path.join(basebackup_dir, "corrupt-base.tar")
    cmd = ["tar", "-cvf", corrupt_base_tar, "--exclude", "base/.*", base_tar]
    subprocess_capture(str(test_output_dir), cmd)

    # Get start_lsn and end_lsn
    with open(os.path.join(basebackup_dir, "backup_manifest")) as f:
        manifest = json.load(f)
        start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
        end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    node_name = "import_from_vanilla"
    tenant = uuid4()
    timeline = uuid4()

    # Set up pageserver for import
    env = neon_env_builder.init_start()
    env.neon_cli.create_tenant(tenant)

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

    # Importing corrupt backup fails
    with pytest.raises(Exception):
        import_tar(corrupt_base_tar, wal_tar)

    # Clean up
    # TODO it should clean itself
    env.pageserver.http_client().timeline_detach(tenant, timeline)

    # Importing correct backup works
    import_tar(base_tar, wal_tar)

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql('select count(*) from t') == [(30000000, )]


@pytest.mark.timeout(600)
def test_import_from_pageserver(test_output_dir, pg_bin, vanilla_pg, neon_env_builder):

    num_rows = 3000
    neon_env_builder.num_safekeepers = 1
    neon_env_builder
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
    env.neon_cli.create_tenant(tenant)
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

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql('select count(*) from tbl') == [(num_rows, )]
