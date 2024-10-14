from __future__ import annotations

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


# Ensure that regular postgres can start from fullbackup
def test_fullbackup(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    port_distributor: PortDistributor,
    test_output_dir: Path,
):
    env = neon_env_builder.init_start()

    # endpoint needs to be alive until the fullbackup so that we have
    # prev_record_lsn for the vanilla_pg to start in read-write mode
    # for some reason this does not happen if endpoint is shutdown.
    endpoint_main = env.endpoints.create_start("main")

    with endpoint_main.cursor() as cur:
        # data loading may take a while, so increase statement timeout
        cur.execute("SET statement_timeout='300s'")
        cur.execute(
            f"""CREATE TABLE tbl AS SELECT 'long string to consume some space' || g
                    from generate_series(1,{num_rows}) g"""
        )
        cur.execute("CHECKPOINT")

        lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_insert_lsn()"))
        log.info(f"start_backup_lsn = {lsn}")

    # Get and unpack fullbackup from pageserver
    restored_dir_path = env.repo_dir / "restored_datadir"
    os.mkdir(restored_dir_path, 0o750)
    tar_output_file = test_output_dir / "fullbackup.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, env.initial_timeline, lsn, tar_output_file
    )
    subprocess_capture(
        env.repo_dir, ["tar", "-xf", str(tar_output_file), "-C", str(restored_dir_path)]
    )

    # HACK
    # fullbackup returns neon specific pg_control and first WAL segment
    # use resetwal to overwrite it
    pg_resetwal_path = os.path.join(pg_bin.pg_bin_path, "pg_resetwal")
    cmd = [pg_resetwal_path, "-D", str(restored_dir_path)]
    pg_bin.run_capture(cmd)

    # Restore from the backup and find the data we inserted
    port = port_distributor.get_port()
    with VanillaPostgres(restored_dir_path, pg_bin, port, init=False) as vanilla_pg:
        vanilla_pg.start()
        num_rows_found = vanilla_pg.safe_psql("select count(*) from tbl;", user="cloud_admin")[0][0]
        assert num_rows == num_rows_found
