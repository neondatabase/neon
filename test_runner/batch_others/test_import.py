from fixtures.neon_fixtures import NeonEnvBuilder
from uuid import UUID, uuid4
import tarfile
import os
import shutil
from pathlib import Path
import json


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

    # Get start_lsn and end_lsn
    with open(os.path.join(basebackup_dir, "backup_manifest")) as f:
        manifest = json.load(f)
        start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
        end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    node_name = "import_from_vanilla"
    tenant = uuid4()
    timeline = uuid4()

    # Import to pageserver
    env = neon_env_builder.init_start()
    env.neon_cli.create_tenant(tenant)
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
        os.path.join(basebackup_dir, "base.tar"),
        "--end-lsn",
        end_lsn,
        "--wal-tarfile",
        os.path.join(basebackup_dir, "pg_wal.tar"),
    ])

    # Check it worked
    pg = env.postgres.create_start(node_name, tenant_id=tenant)
    assert pg.safe_psql('select count(*) from t') == [(30000000, )]
