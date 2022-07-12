import pytest
from fixtures.zenith_fixtures import ZenithEnvBuilder, wait_for_upload, wait_for_last_record_lsn
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
from fixtures.zenith_fixtures import pg_distrib_dir


@pytest.mark.timeout(600)
def test_import_from_vanilla(test_output_dir, pg_bin, vanilla_pg, zenith_env_builder):
    # Put data in vanilla pg
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user zenith_admin with password 'postgres' superuser")
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
    zenith_env_builder.enable_local_fs_remote_storage()
    env = zenith_env_builder.init_start()
    env.pageserver.http_client().tenant_create(tenant)

    def import_tar(base, wal):
        env.zenith_cli.raw_cli([
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
