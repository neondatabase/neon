"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pytest
from fixtures.log_helper import log
from fixtures.pg_version import PgVersion
from fixtures.neon_api import NeonAPI


class RandomNeonProject:
    def __init__(self, project_id: str, neon_api: NeonAPI):
        self.project_id = project_id
        self.neon_api = neon_api

    def __get_branches_info(self) -> list[Any]:
        return self.neon_api.get_branches(self.project_id)["branches"]

    def get_main_branch_id(self) -> str:
        for branch in self.__get_branches_info():
            if "parent_id" not in branch:
                return branch["id"]
        raise RuntimeError(f"The main branch is not found for the project {self.project_id}")

    def get_leaf_branches(self) -> list[str]:
        parents, branches, main = set(), set(), set()
        for branch in self.__get_branches_info():
            branches.add(branch["id"])
            if "parent_id" in branch:
                parents.add(branch["parent_id"])
            else:
                main.add(branch["id"])
        return list(branches - parents - main)

    def create_branch(self, parent_branch_id: str | None = None):
        return self.neon_api.create_branch(self.project_id, parent_branch_id)["branch"]["id"]

    def wait(self):
        return self.neon_api.wait_for_operation_to_finish(self.project_id)


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
    project = RandomNeonProject(project_id, neon_api)
    br1 = project.create_branch()
    log.info("created branch %s", br1)
    project.wait()
    time.sleep(10)
    br2 = project.create_branch(br1)
    log.info("created branch %s", br2)
    project.wait()
    log.info("leaf branches: %s", project.get_leaf_branches())
    assert True
