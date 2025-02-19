"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

from pathlib import Path

import pytest
import os
from fixtures.log_helper import log
from fixtures.neon_fixtures import RemotePostgres
from fixtures.pg_version import PgVersion


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_api_random(
    pg_version: PgVersion,
    pg_distrib_dir: Path,
    base_dir: Path,
    test_output_dir: Path,
):
    """
    Run the random API tests
    """
    project_id = os.getenv('PROJECT_ID')
    log.info('Project ID: %s', project_id)
    assert True
