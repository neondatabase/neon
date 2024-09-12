import os
from pathlib import Path

from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    VanillaPostgres,
)
from fixtures.port_distributor import PortDistributor
from fixtures.utils import query_scalar, subprocess_capture

num_rows = 1000

class ImportCli(AbstractNeonCli):
    """
    A typed wrapper around the `import` utility CLI tool.
    """

    COMMAND = "import"

    def import(self, pgdatadir: Path, dest_dir: Path):
        res = self.raw_cli([str(pgdatadir), str(dest_dir)])
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
    env = neon_env_builder.init_start()

    # Set up pageserver for import
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # Run pg_import utility, pointing directly to a directory in the remote storage dir
    tenant = TenantId.generate()
    timeline = TimelineId.generate()

    timelinedir = env.pagserver.workdir() / "tenants" / tenant_id / "timelines" / timeline

    cli = ImportCli(env)
    cli.import(vanilla_pg.pgdata(),

    # TODO: tell pageserver / storage controller that the tenant/timeline now exists
