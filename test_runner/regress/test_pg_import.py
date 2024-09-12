import os
from pathlib import Path

from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    AbstractNeonCli,
    NeonEnvBuilder,
    PgBin,
    VanillaPostgres,
)
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import RemoteStorageKind
from fixtures.utils import query_scalar, subprocess_capture

num_rows = 1000

class ImportCli(AbstractNeonCli):
    """
    A typed wrapper around the `import` utility CLI tool.
    """

    COMMAND = "import"

    def run_import(self, pgdatadir: Path, dest_dir: Path, tenant_id: TenantId, timeline_id: TimelineId):
        res = self.raw_cli(["--tenant-id", str(tenant_id), "--timeline-id", str(timeline_id), str(pgdatadir), str(dest_dir)])
        res.check_returncode()


def test_pg_import(test_output_dir, pg_bin, vanilla_pg, neon_env_builder):
    # Put data in vanilla pg
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    vanilla_pg.safe_psql(
        """create table t as select 'long string to consume some space' || g
     from generate_series(1,300000) g"""
    )
    assert vanilla_pg.safe_psql("select count(*) from t") == [(300000,)]

    vanilla_pg.stop()


    # We have a Postgres data directory to import now
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    # Set up pageserver for import

    # Run pg_import utility, pointing directly to a directory in the remote storage dir
    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()

    dst_path = env.pageserver_remote_storage.root
    tline_path = env.pageserver_remote_storage.timeline_path(tenant_id, timeline_id)
    tline_path.mkdir(parents=True)

    cli = ImportCli(env)
    cli.run_import(vanilla_pg.pgdatadir, dst_path, tenant_id=tenant_id, timeline_id=timeline_id)

    # TODO: tell pageserver / storage controller that the tenant/timeline now exists
    env.pageserver.tenant_attach(
        tenant_id,
        generation=100,
        override_storage_controller_generation=True,
    )

    env.neon_cli.map_branch("imported", tenant_id, timeline_id)

    endpoint = env.endpoints.create_start(branch_name="imported", tenant_id=tenant_id)
    conn = endpoint.connect()
    cur = conn.cursor()

    assert endpoint.safe_psql("select count(*) from t") == [(300000,)]

    # test writing after the import
    endpoint.safe_psql("insert into t select g from generate_series(1, 1000) g")
    assert endpoint.safe_psql("select count(*) from t") == [(301000,)]

    endpoint.stop()
    endpoint.start()
    assert endpoint.safe_psql("select count(*) from t") == [(301000,)]
