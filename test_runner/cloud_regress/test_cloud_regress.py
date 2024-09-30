"""
Run the regression tests on the cloud instance of Neon
"""

from pathlib import Path
from typing import Any

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import RemotePostgres
from fixtures.pg_version import PgVersion


@pytest.fixture
def setup(remote_pg: RemotePostgres):
    """
    Setup and teardown of the tests
    """
    with psycopg2.connect(remote_pg.connstr()) as conn:
        with conn.cursor() as cur:
            log.info("Creating the extension")
            cur.execute("CREATE EXTENSION IF NOT EXISTS regress_so")
            conn.commit()
            # TODO: Migrate to branches and remove this code
            log.info("Looking for subscriptions in the regress database")
            cur.execute(
                "SELECT subname FROM pg_catalog.pg_subscription WHERE "
                "subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname='regression');"
            )
            if cur.rowcount > 0:
                with psycopg2.connect(
                    dbname="regression",
                    host=remote_pg.default_options["host"],
                    user=remote_pg.default_options["user"],
                    password=remote_pg.default_options["password"],
                ) as regress_conn:
                    with regress_conn.cursor() as regress_cur:
                        for sub in cur:
                            regress_cur.execute(f"ALTER SUBSCRIPTION {sub[0]} DISABLE")
                            regress_cur.execute(
                                f"ALTER SUBSCRIPTION {sub[0]} SET (slot_name = NONE)"
                            )
                            regress_cur.execute(f"DROP SUBSCRIPTION {sub[0]}")
                        regress_conn.commit()

    yield
    # TODO: Migrate to branches and remove this code
    log.info("Looking for extra roles...")
    with psycopg2.connect(remote_pg.connstr()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rolname FROM pg_catalog.pg_roles WHERE oid > 16384 AND rolname <> 'neondb_owner'"
            )
            roles: list[Any] = []
            for role in cur:
                log.info("Role found: %s", role[0])
                roles.append(role[0])
            for role in roles:
                cur.execute(f"DROP ROLE {role}")
            conn.commit()


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_cloud_regress(
    setup,
    remote_pg: RemotePostgres,
    pg_version: PgVersion,
    pg_distrib_dir: Path,
    base_dir: Path,
    test_output_dir: Path,
):
    """
    Run the regression tests
    """
    regress_bin = (
        pg_distrib_dir / f"{pg_version.v_prefixed}/lib/postgresql/pgxs/src/test/regress/pg_regress"
    )
    test_path = base_dir / f"vendor/postgres-{pg_version.v_prefixed}/src/test/regress"

    env_vars = {
        "PGHOST": remote_pg.default_options["host"],
        "PGPORT": str(
            remote_pg.default_options["port"] if "port" in remote_pg.default_options else 5432
        ),
        "PGUSER": remote_pg.default_options["user"],
        "PGPASSWORD": remote_pg.default_options["password"],
        "PGDATABASE": remote_pg.default_options["dbname"],
    }
    regress_cmd = [
        str(regress_bin),
        f"--inputdir={test_path}",
        f"--bindir={pg_distrib_dir}/{pg_version.v_prefixed}/bin",
        "--dlpath=/usr/local/lib",
        "--max-concurrent-tests=20",
        f"--schedule={test_path}/parallel_schedule",
        "--max-connections=5",
    ]
    remote_pg.pg_bin.run(regress_cmd, env=env_vars, cwd=test_output_dir)
