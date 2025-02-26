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

class NeonEndpoint:
    def __init__(self):
        self.en


class NeonBranch:
    def __init__(self, project, branch: dict[str, Any]):
        self.id = branch["id"]
        self.desc = branch
        self.project = project
        self.neon_api = project.neon_api
        self.project_id = branch["project_id"]

    def create_ro_endpoint(self):
        return self.neon_api.create_endpoint(self.project_id, self.id, "read_only", {})[
            "endpoint"
        ]["id"]


class NeonProject:
    def __init__(self, neon_api: NeonAPI, pg_version: PgVersion):
        self.neon_api = neon_api
        proj = self.neon_api.create_project(pg_version)
        self.id = proj["id"]
        self.name = proj["name"]

    def __get_branches_info(self) -> list[Any]:
        return self.neon_api.get_branches(self.id)["branches"]

    def get_main_branch(self) -> NeonBranch:
        for branch in self.__get_branches_info():
            if "parent_id" not in branch:
                return NeonBranch(self, branch)
        raise RuntimeError(f"The main branch is not found for the project {self.project_id}")

    def get_leaf_branches(self) -> list[NeonBranch]:
        parents, branches, main = set(), set(), set()
        branches_raw = self.__get_branches_info()
        for branch in branches_raw:
            branches.add(branch["id"])
            if "parent_id" in branch:
                parents.add(branch["parent_id"])
            else:
                main.add(branch["id"])
        leafs = branches - parents - main
        return [NeonBranch(self, branch) for branch in branches_raw if branch["id"] in leafs]

    def get_branches(self) -> list[NeonBranch]:
        return [NeonBranch(self, branch) for branch in self.__get_branches_info()]

    def create_branch(self, parent_id: str | None = None) -> NeonBranch:
        return NeonBranch(self, self.neon_api.create_branch(self.project_id, parent_id=parent_id)["branch"])

    def get_ro_endpoints(self):
        return [
            _["id"]
            for _ in self.neon_api.get_endpoints(self.project_id)["endpoints"]
            if _["type"] == "read_only"
        ]

    def wait(self):
        return self.neon_api.wait_for_operation_to_finish(self.project_id)

@pytest.fixture()
def setup_class(
        pg_version: PgVersion,
        neon_api: NeonAPI,
):
    log.info("set up")
    yield neon_api
    log.info("tear down")


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_api_random(
        setup_class,
    test_output_dir: Path,
):
    """
    Run the random API tests
    """
    assert True
