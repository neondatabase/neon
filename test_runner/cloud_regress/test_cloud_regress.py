"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI
from fixtures.neon_fixtures import RemotePostgres
from fixtures.pg_version import PgVersion
from fixtures.utils import PgConnectParam


@pytest.fixture
def setup(neon_api: NeonAPI):
    """
    Setup and teardown of the tests
    """
    project = os.getenv("PROJECT_ID")
    assert project is not None, "PROJECT_ID undefined"
    branches = neon_api.get_branches(project)
    log.info("Branches: %s", branches)
    primary_branch_id = None
    for branch in branches["branches"]:
        if branch["primary"]:
            primary_branch_id = branch["id"]
            break
    assert primary_branch_id is not None, "Cannot get the primary branch"
    current_branch_id = neon_api.create_branch_with_endpoint(
        project, primary_branch_id, datetime.now().strftime("test-%y%m%d%H%M")
    )["branch"]["id"]
    uri = neon_api.get_connection_uri(project, current_branch_id)["uri"]
    log.info("Branch ID: %s", current_branch_id)

    pgconn = PgConnectParam(uri)

    yield pgconn

    log.info("Delete branch %s", current_branch_id)
    neon_api.delete_branch(project, current_branch_id)


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

    regress_cmd = [
        str(regress_bin),
        f"--inputdir={test_path}",
        f"--bindir={pg_distrib_dir}/{pg_version.v_prefixed}/bin",
        "--dlpath=/usr/local/lib",
        "--max-concurrent-tests=20",
        f"--schedule={test_path}/parallel_schedule",
        "--max-connections=5",
    ]
    remote_pg.pg_bin.run(regress_cmd, env=setup.env_vars(), cwd=test_output_dir)
