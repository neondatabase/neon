"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI
from fixtures.pg_version import PgVersion


class NeonEndpoint:
    def __init__(self, project, endpoint):
        self.project = project
        self.id = endpoint["id"]
        self.branch = project.branches[endpoint["branch_id"]]
        self.type = endpoint["type"]
        self.branch.endpoints[self.id] = self
        self.project.endpoints[self.id] = self

    def delete(self):
        self.project.delete_endpoint(self.id)


class NeonBranch:
    def __init__(self, project, branch: dict[str, Any]):
        self.id = branch["id"]
        self.desc = branch
        self.project = project
        self.neon_api = project.neon_api
        self.project_id = branch["project_id"]
        self.parent = self.project.branches[branch["parent_id"]] if "parent_id" in branch else None
        self.children = {}
        self.endpoints = {}

    def create_branch(self):
        return self.project.create_branch(self.id)

    def create_ro_endpoint(self):
        return NeonEndpoint(
            self.project,
            self.neon_api.create_endpoint(self.project_id, self.id, "read_only", {})["endpoint"][
                "id"
            ],
        )


class NeonProject:
    def __init__(self, neon_api: NeonAPI, pg_version: PgVersion):
        self.neon_api = neon_api
        proj = self.neon_api.create_project(
            pg_version, f"Automatic random API test {os.getenv('GITHUB_RUN_ID')}"
        )
        self.id = proj["project"]["id"]
        self.name = proj["project"]["name"]
        self.connection_uri = proj["connection_uris"][0]["connection_uri"]
        self.pg_version = pg_version
        self.main_branch = NeonBranch(self, proj["branch"])
        self.branches = {self.main_branch.id: self.main_branch}
        self.leaf_branches = {}
        self.endpoints = {}
        for endpoint in proj["endpoints"]:
            NeonEndpoint(self, endpoint)

    def delete(self):
        self.neon_api.delete_project(self.id)

    def __get_branches_info(self) -> list[Any]:
        return self.neon_api.get_branches(self.id)["branches"]

    def create_branch(self, parent_id: str | None = None) -> NeonBranch:
        new_branch = NeonBranch(
            self, self.neon_api.create_branch(self.id, parent_id=parent_id)["branch"]
        )
        self.leaf_branches[new_branch.id] = new_branch
        self.branches[new_branch.id] = new_branch
        if parent_id and parent_id in self.leaf_branches:
            self.leaf_branches.pop(parent_id)
        if parent_id is None:
            self.main_branch.children[new_branch.id] = new_branch
        else:
            self.branches[parent_id].children[new_branch.id] = new_branch
        return new_branch

    def delete_branch(self, branch_id: str) -> None:
        if branch_id == self.main_branch.id:
            raise RuntimeError("Cannot delete the main branch")
        if branch_id not in self.leaf_branches:
            raise RuntimeError(f"The branch {branch_id}, probably, has ancestors")
        if branch_id not in self.branches:
            raise RuntimeError(f"The branch with id {branch_id} is not found")
        self.neon_api.delete_branch(self.id, branch_id)
        self.branches[branch_id].parent.children.pop(branch_id)
        self.leaf_branches.pop(branch_id)
        self.branches.pop(branch_id)

    def delete_endpoint(self, endpoint_id: str) -> None:
        self.endpoints[endpoint_id].branch.endpoints.pop(endpoint_id)
        self.endpoints.pop(endpoint_id)
        self.neon_api.delete_endpoint(self.id, endpoint_id)

    def wait(self):
        return self.neon_api.wait_for_operation_to_finish(self.id)


@pytest.fixture()
def setup_class(
    pg_version: PgVersion,
    neon_api: NeonAPI,
):
    project = NeonProject(neon_api, pg_version)
    log.info("Created a project with id %s, name %s", project.id, project.name)
    yield project
    log.info("Removing the project")
    project.delete()


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_api_random(
    setup_class,
    test_output_dir: Path,
):
    """
    Run the random API tests
    """
    project = setup_class
    project.wait()
    br = project.main_branch.create_branch()
    log.info("created a branch with id %s", br.id)
    br2 = br.create_branch()
    log.info("created a branch with id %s, a child of %s", br2.id, br.id)
    project.wait()
    q = [project.main_branch]
    while q:
        br = q.pop()
        log.info("branch id: %s", br.id)
        q.extend(br.children.values())
    assert True
