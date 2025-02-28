"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
import random
import subprocess
import time
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI
from fixtures.neon_fixtures import PgBin
from fixtures.pg_version import PgVersion


class NeonEndpoint:
    def __init__(self, project, endpoint):
        self.project = project
        self.id = endpoint["id"]
        self.branch = project.branches[endpoint["branch_id"]]
        self.type = endpoint["type"]
        self.branch.endpoints[self.id] = self
        self.project.endpoints[self.id] = self
        self.host = endpoint["host"]
        self.benchmark = None

    def delete(self):
        self.project.delete_endpoint(self.id)

    def connect_env(self) -> dict[str, str]:
        env = self.branch.connect_env
        env["PGHOST"] = self.host
        return env

    def start_benchmark(self, clients=10):
        return self.project.start_benchmark(self.id, clients=clients)

    def check_benchmark(self):
        self.project.check_benchmark(self.id)

    def terminate_benchmark(self):
        self.project.terminate_benchmark(self.id)


class NeonBranch:
    def __init__(self, project, branch: dict[str, Any]):
        self.id = branch["branch"]["id"]
        self.desc = branch
        self.project = project
        self.neon_api = project.neon_api
        self.project_id = branch["branch"]["project_id"]
        self.parent = self.project.branches[branch["branch"]["parent_id"]] if "parent_id" in branch["branch"] else None
        self.children = {}
        self.endpoints = {}
        self.connection_parameters = branch["connection_uris"][0]["connection_parameters"]
        self.benchmark = None

    def __str__(self):
        return f"Branch {self.id}, parent: {self.parent}"

    def create_child_branch(self):
        return self.project.create_branch(self.id)

    def create_ro_endpoint(self):
        return NeonEndpoint(
            self.project,
            self.neon_api.create_endpoint(self.project_id, self.id, "read_only", {})["endpoint"],
        )

    def delete(self):
        self.project.delete_branch(self.id)

    def connect_env(self) -> dict[str, str]:
        env = {"PGHOST": self.connection_parameters["host"], "PGUSER": self.connection_parameters["role"],
               "PGDATABASE": self.connection_parameters["database"],
               "PGPASSWORD": self.connection_parameters["password"]}
        return env

    def start_benchmark(self, clients=10):
        return self.project.start_benchmark(self.id, clients=clients)

    def check_benchmark(self):
        self.project.check_benchmark(self.id)

    def terminate_benchmark(self):
        self.project.terminate_benchmark(self.id)


class NeonProject:
    def __init__(self, neon_api: NeonAPI, pg_bin: PgBin, pg_version: PgVersion):
        self.neon_api = neon_api
        self.pg_bin = pg_bin
        proj = self.neon_api.create_project(
            pg_version, f"Automatic random API test {os.getenv('GITHUB_RUN_ID')}"
        )
        self.id: str = proj["project"]["id"]
        self.name: str = proj["project"]["name"]
        self.connection_uri: str = proj["connection_uris"][0]["connection_uri"]
        self.connection_parameters: dict[str, str] = proj["connection_uris"][0]["connection_parameters"]
        self.pg_version: PgVersion = pg_version
        self.main_branch: NeonBranch = NeonBranch(self, proj)
        self.main_branch.connection_parameters = self.connection_parameters
        self.branches: dict[str, NeonBranch] = {self.main_branch.id: self.main_branch}
        self.leaf_branches: dict[str, NeonBranch] = {}
        self.endpoints: dict[str, NeonEndpoint] = {}
        for endpoint in proj["endpoints"]:
            NeonEndpoint(self, endpoint)
        self.neon_api.wait_for_operation_to_finish(self.id)
        self.benchmarks: dict[str, subprocess.Popen] = {}

    def delete(self):
        self.neon_api.delete_project(self.id)

    def __get_branches_info(self) -> list[Any]:
        return self.neon_api.get_branches(self.id)["branches"]

    def create_branch(self, parent_id: str | None = None) -> NeonBranch:
        new_branch = NeonBranch(
            self, self.neon_api.create_branch(self.id, parent_id=parent_id)
        )
        self.leaf_branches[new_branch.id] = new_branch
        self.branches[new_branch.id] = new_branch
        if parent_id and parent_id in self.leaf_branches:
            self.leaf_branches.pop(parent_id)
        if parent_id is None:
            self.main_branch.children[new_branch.id] = new_branch
        else:
            self.branches[parent_id].children[new_branch.id] = new_branch
        self.wait()
        return new_branch

    def delete_branch(self, branch_id: str) -> None:
        parent = self.branches[branch_id].parent
        if branch_id == self.main_branch.id:
            raise RuntimeError("Cannot delete the main branch")
        if branch_id not in self.leaf_branches:
            raise RuntimeError(f"The branch {branch_id}, probably, has ancestors")
        if branch_id not in self.branches:
            raise RuntimeError(f"The branch with id {branch_id} is not found")
        self.neon_api.delete_branch(self.id, branch_id)
        if len(parent.children) == 1:
            self.leaf_branches[parent.id] = parent
        parent.children.pop(branch_id)
        self.leaf_branches.pop(branch_id)
        self.branches.pop(branch_id)
        self.wait()

    def delete_endpoint(self, endpoint_id: str) -> None:
        self.endpoints[endpoint_id].branch.endpoints.pop(endpoint_id)
        self.endpoints.pop(endpoint_id)
        self.neon_api.delete_endpoint(self.id, endpoint_id)
        self.wait()

    def start_benchmark(self, target: str, clients: int=10) -> subprocess.Popen:
        if target in self.benchmarks:
            raise RuntimeError(f"Benchmark was already started for {target}")
        target_endpoint = target.startswith("ep")
        read_only = target_endpoint and self.endpoints[target].type == "read_only"
        cmd = ["pgbench", f"-c{clients}", "10800", "-Mprepared"]
        if read_only:
            cmd.append("-S")
        if target_endpoint:
            log.info("connect_env: %s", self.endpoints[target].connect_env())
        else:
            log.info("connect_env: %s", self.branches[target].connect_env())
        pgbench = self.pg_bin.run_nonblocking(cmd, env=self.endpoints[target].connect_env if target_endpoint else self.branches[target].connect_env())
        self.benchmarks[target] = pgbench
        if target_endpoint:
            self.endpoints[target].benchmark = pgbench
        else:
            self.branches[target].benchmark = pgbench
        return pgbench

    def check_all_benchmarks(self) -> None:
        for target in self.benchmarks.keys():
            self.check_benchmark(target)

    def check_benchmark(self, target):
        rc = self.benchmarks[target].poll()
        if rc is not None:
            raise RuntimeError(f"The benchmark for {target} ended with code {rc}")

    def terminate_benchmark(self, target):
        target_endpoint = target.startswith("ep")
        self.check_benchmark(target)
        self.benchmarks[target].terminate()
        self.benchmarks.pop(target)
        if target_endpoint:
            self.endpoints[target].benchmark = None
        else:
            self.branches[target].benchmark = None

    def wait(self):
        return self.neon_api.wait_for_operation_to_finish(self.id)


@pytest.fixture()
def setup_class(
    pg_version: PgVersion,
    pg_bin: PgBin,
    neon_api: NeonAPI,
):
    project = NeonProject(neon_api, pg_bin, pg_version)
    log.info("Created a project with id %s, name %s", project.id, project.name)
    yield pg_bin, project
    log.info("Removing the project")
    project.delete()


def do_action(project, action):
    if action == "new_branch":
        parent = random.choice(list(project.branches.values()))
        child = parent.create_child_branch()
        log.info("Created branch %s, parent: %s", child.id, parent.id)
        child.start_benchmark()
    elif action == "delete_branch":
        if project.leaf_branches:
            target = random.choice(list(project.leaf_branches.values()))
            log.info("Trying to delete branch %s", target)
            target.terminate_benchmark()
            target.delete()
        else:
            log.info("Leaf branches not found, skipping")
    elif action == "new_ro_endpoint":
        ep = random.choice(list(project.branches.values())).create_ro_endpoint()
        log.info("Created the RO endpoint with id %s branch: %s", ep.id, ep.branch.id)
        ep.start_benchmark()
    elif action == "delete_ro_endpoint":
        ro_endpoints = [_ for _ in project.endpoints.values() if _.type == "read_only"]
        if ro_endpoints:
            target = random.choice(ro_endpoints)
            target.terminate_benchmark()
            log.info("endpoint %s deleted", target.id)
        else:
            log.info("no read_only endpoints present, skipping")



@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_api_random(
    setup_class,
    pg_distrib_dir: Path,
    test_output_dir: Path,
):
    """
    Run the random API tests
    """
    if seed_env := os.getenv("RANDOM_SEED"):
        seed = int(seed_env)
    else:
        seed = int(time.time())
    log.info("Using random seed: %s", seed)
    random.seed(seed)
    pg_bin, project = setup_class
    # Here we can assign weights by repeating actions
    ACTIONS = ('new_branch', 'new_branch', 'new_branch', 'new_branch', 'new_ro_endpoint', 'new_ro_endpoint', 'new_ro_endpoint', "delete_ro_endpoint", "delete_ro_endpoint", "delete_branch", "delete branch")
    ACTIONS_LIMIT = 250
    pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", "-s100"], env=project.main_branch.connect_env())
    for _ in range(ACTIONS_LIMIT):
        do_action(project, random.choice(ACTIONS))
    assert True
