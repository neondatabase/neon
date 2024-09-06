"""
Run the regression tests on the cloud instance of Neon
"""

import os
from pathlib import Path

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import RemotePostgres
from fixtures.pg_version import PgVersion


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_cloud_regress(remote_pg: RemotePostgres, pg_version: PgVersion, pg_distrib_dir: Path):
    """
    Run the regression tests
    """
    with psycopg2.connect(remote_pg.connstr()) as conn:
        with conn.cursor() as cur:
            cur = conn.cursor()
            log.info("Creating the extension")
            cur.execute("CREATE EXTENSION IF NOT EXISTS regress_so")
            conn.commit()

            # This is also a workaround for the full path problem
            # If we specify the full path in the command, the library won't be downloaded
            # So we specify the name only for the first time
            log.info("Creating a C function to check availability of regress.so")
            cur.execute(
                "CREATE FUNCTION get_columns_length(oid[]) "
                "RETURNS int AS 'regress.so' LANGUAGE C STRICT STABLE PARALLEL SAFE;"
            )
            conn.rollback()
            artifact_prefix = f"/tmp/neon/pg_install/v{pg_version}"
            regress_bin = f"{artifact_prefix}/lib/postgresql/pgxs/src/test/regress/pg_regress"
            runpath = pg_distrib_dir / f"build/{pg_version.v_prefixed}/src/test/regress"

            env_vars = {
                "PGHOST": remote_pg.default_options["host"],
                "PGPORT": str(
                    remote_pg.default_options["port"]
                    if "port" in remote_pg.default_options
                    else 5432
                ),
                "PGUSER": remote_pg.default_options["user"],
                "PGPASSWORD": remote_pg.default_options["password"],
                "PGDATABASE": remote_pg.default_options["dbname"],
            }
            regress_cmd = [
                regress_bin,
                "--inputdir=.",
                f"--bindir={artifact_prefix}/bin",
                "--dlpath=/usr/local/lib",
                "--max-concurrent-tests=20",
                "--schedule=./parallel_schedule",
                "--max-connections=5",
            ]
            remote_pg.pg_bin.run(regress_cmd, env=env_vars, cwd=runpath)
