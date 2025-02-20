"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.pg_version import PgVersion
from fixtures.neon_api import NeonAPI


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_api_random(
    pg_version: PgVersion,
    pg_distrib_dir: Path,
    base_dir: Path,
    test_output_dir: Path,
    neon_api: NeonAPI,
):
    """
    Run the random API tests
    """
    project_id = os.getenv("PROJECT_ID")
    log.info("Project ID: %s", project_id)
    res = neon_api.create_branch(project_id)
    log.info("%s", res)
    assert True
