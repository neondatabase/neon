"""
Run the random API tests on the cloud instance of Neon
"""

from __future__ import annotations

import os
import random
import subprocess
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
from fixtures.log_helper import log
from requests import HTTPError

if TYPE_CHECKING:
    from pathlib import Path

    from fixtures.neon_api import NeonAPI
    from fixtures.neon_fixtures import PgBin
    from fixtures.pg_version import PgVersion


class NeonEndpoint:
    """
    Neon Endpoint
    Gets the output of the API call of an endpoint creation
    """

    def __init__(self, project: NeonProject, endpoint: dict[str, Any]):
        self.project: NeonProject = project
        self.id: str = endpoint["id"]
        # The branch endpoint belongs to
        self.branch: NeonBranch = project.branches[endpoint["branch_id"]]
        self.type: str = endpoint["type"]
        # add itself to the list of endpoints of the branch
        self.branch.endpoints[self.id] = self
        self.project.endpoints[self.id] = self
        self.host: str = endpoint["host"]
        self.benchmark: subprocess.Popen[Any] | None = None
        # The connection environment is used when running benchmark
        self.connect_env: dict[str, str] | None = None
        if self.branch.connect_env:
            self.connect_env = self.branch.connect_env.copy()
            self.connect_env["PGHOST"] = self.host

    def delete(self):
        self.project.delete_endpoint(self.id)

    def start_benchmark(self, clients=10):
        return self.project.start_benchmark(self.id, clients=clients)

    def check_benchmark(self):
        self.project.check_benchmark(self.id)

    def terminate_benchmark(self):
        self.project.terminate_benchmark(self.id)


class NeonBranch:
    """
    Neon Branch
    Gets the output of the API call of the Neon Public API call of a branch creation as a first parameter
    is_reset defines if the branch is a reset one i.e. created as a result of the reset API Call
    """

    def __init__(self, project, branch: dict[str, Any], is_reset=False):
        self.id: str = branch["branch"]["id"]
        self.desc = branch
        self.project: NeonProject = project
        self.neon_api: NeonAPI = project.neon_api
        self.project_id: str = branch["branch"]["project_id"]
        self.parent: NeonBranch | None = (
            self.project.branches[branch["branch"]["parent_id"]]
            if "parent_id" in branch["branch"]
            else None
        )
        if is_reset:
            self.project.reset_branches.add(self.id)
        elif self.parent:
            self.project.leaf_branches[self.id] = self
        if self.parent is not None and self.parent.id in self.project.leaf_branches:
            self.project.leaf_branches.pop(self.parent.id)
        self.project.branches[self.id] = self
        self.children: dict[str, NeonBranch] = {}
        if self.parent is not None:
            self.parent.children[self.id] = self
        self.endpoints: dict[str, NeonEndpoint] = {}
        self.connection_parameters: dict[str, str] | None = (
            branch["connection_uris"][0]["connection_parameters"]
            if "connection_uris" in branch
            else None
        )
        self.benchmark: subprocess.Popen[Any] | None = None
        self.updated_at: datetime = datetime.fromisoformat(branch["branch"]["updated_at"])
        self.connect_env: dict[str, str] | None = None
        if self.connection_parameters:
            self.connect_env = {
                "PGHOST": self.connection_parameters["host"],
                "PGUSER": self.connection_parameters["role"],
                "PGDATABASE": self.connection_parameters["database"],
                "PGPASSWORD": self.connection_parameters["password"],
                "PGSSLMODE": "require",
            }

    def __str__(self):
        """
        Prints the branch's name with all the predecessors
        (r) means the branch is a reset one
        """
        return f"{self.id}{'(r)' if self.id in self.project.reset_branches else ''}, parent: {self.parent}"

    def create_child_branch(self) -> NeonBranch | None:
        return self.project.create_branch(self.id)

    def create_ro_endpoint(self) -> NeonEndpoint:
        return NeonEndpoint(
            self.project,
            self.neon_api.create_endpoint(self.project_id, self.id, "read_only", {})["endpoint"],
        )

    def delete(self) -> None:
        self.project.delete_branch(self.id)

    def start_benchmark(self, clients=10) -> subprocess.Popen[Any]:
        return self.project.start_benchmark(self.id, clients=clients)

    def check_benchmark(self) -> None:
        self.project.check_benchmark(self.id)

    def terminate_benchmark(self) -> None:
        self.project.terminate_benchmark(self.id)

    def restore_random_time(self) -> None:
        """
        Does PITR, i.e. calls the reset API call on the same branch to the random time in the past
        """
        min_time = self.updated_at + timedelta(seconds=1)
        max_time = datetime.now(UTC) - timedelta(seconds=1)
        target_time = (min_time + (max_time - min_time) * random.random()).replace(microsecond=0)
        res = self.restore(
            self.id,
            source_timestamp=target_time.isoformat().replace("+00:00", "Z"),
            preserve_under_name=self.project.gen_restore_name(),
        )
        if res is None:
            return
        self.updated_at = datetime.fromisoformat(res["branch"]["updated_at"])
        parent_id: str = res["branch"]["parent_id"]
        # Creates an object for the parent branch
        # After the reset operation a new parent branch is created
        parent = NeonBranch(
            self.project, self.neon_api.get_branch_details(self.project_id, parent_id), True
        )
        self.project.branches[parent_id] = parent
        self.parent = parent
        parent.children[self.id] = self
        self.project.wait()

    def restore(
        self,
        source_branch_id: str,
        source_lsn: str | None = None,
        source_timestamp: str | None = None,
        preserve_under_name: str | None = None,
    ) -> dict[str, Any] | None:
        endpoints = [ep for ep in self.endpoints.values() if ep.type == "read_only"]
        # Terminate all the benchmarks running to prevent errors. Errors in benchmark during pgbench are expected
        for ep in endpoints:
            ep.terminate_benchmark()
        self.terminate_benchmark()
        try:
            res: dict[str, Any] = self.neon_api.restore_branch(
                self.project_id,
                self.id,
                source_branch_id,
                source_lsn,
                source_timestamp,
                preserve_under_name,
            )
        except HTTPError as he:
            if (
                he.response.status_code == 422
                and he.response.json()["code"] == "BRANCHES_LIMIT_EXCEEDED"
            ):
                log.info("Branch limit exceeded, skipping")
                return None
            else:
                raise HTTPError(he) from he
        self.project.wait()
        self.start_benchmark()
        for ep in endpoints:
            ep.start_benchmark()
        return res


class NeonProject:
    """
    The project object
    Calls the Public API to create a Neon Project
    """

    def __init__(self, neon_api: NeonAPI, pg_bin: PgBin, pg_version: PgVersion):
        self.neon_api = neon_api
        self.pg_bin = pg_bin
        proj = self.neon_api.create_project(
            pg_version, f"Automatic random API test GITHUB_RUN_ID={os.getenv('GITHUB_RUN_ID')}"
        )
        self.id: str = proj["project"]["id"]
        self.name: str = proj["project"]["name"]
        self.connection_uri: str = proj["connection_uris"][0]["connection_uri"]
        self.connection_parameters: dict[str, str] = proj["connection_uris"][0][
            "connection_parameters"
        ]
        self.pg_version: PgVersion = pg_version
        # Leaf branches are the branches, which do not have children
        self.leaf_branches: dict[str, NeonBranch] = {}
        self.branches: dict[str, NeonBranch] = {}
        self.reset_branches: set[str] = set()
        self.main_branch: NeonBranch = NeonBranch(self, proj)
        self.main_branch.connection_parameters = self.connection_parameters
        self.endpoints: dict[str, NeonEndpoint] = {}
        for endpoint in proj["endpoints"]:
            NeonEndpoint(self, endpoint)
        self.neon_api.wait_for_operation_to_finish(self.id)
        self.benchmarks: dict[str, subprocess.Popen[Any]] = {}
        self.restore_num: int = 0
        self.restart_pgbench_on_console_errors: bool = False

    def delete(self):
        self.neon_api.delete_project(self.id)

    def create_branch(self, parent_id: str | None = None) -> NeonBranch | None:
        self.wait()
        try:
            branch_def = self.neon_api.create_branch(self.id, parent_id=parent_id)
        except HTTPError as he:
            if (
                he.response.status_code == 422
                and he.response.json()["code"] == "BRANCHES_LIMIT_EXCEEDED"
            ):
                log.info("Branch limit exceeded, skipping")
                return None
            else:
                raise HTTPError(he) from he
        new_branch = NeonBranch(self, branch_def)
        self.wait()
        return new_branch

    def delete_branch(self, branch_id: str) -> None:
        parent = self.branches[branch_id].parent
        if not parent or branch_id == self.main_branch.id:
            raise RuntimeError("Cannot delete the main branch")
        if branch_id not in self.leaf_branches and branch_id not in self.reset_branches:
            raise RuntimeError(f"The branch {branch_id}, probably, has ancestors")
        if branch_id not in self.branches:
            raise RuntimeError(f"The branch with id {branch_id} is not found")
        endpoints_to_delete = [
            ep for ep in self.branches[branch_id].endpoints.values() if ep.type == "read_only"
        ]
        for ep in endpoints_to_delete:
            ep.delete()
        if branch_id not in self.reset_branches:
            self.terminate_benchmark(branch_id)
        self.neon_api.delete_branch(self.id, branch_id)
        if len(parent.children) == 1 and parent.id != self.main_branch.id:
            self.leaf_branches[parent.id] = parent
        parent.children.pop(branch_id)
        if branch_id in self.leaf_branches:
            self.leaf_branches.pop(branch_id)
        else:
            self.reset_branches.remove(branch_id)
        self.branches.pop(branch_id)
        self.wait()
        if parent.id in self.reset_branches:
            parent.delete()

    def delete_endpoint(self, endpoint_id: str) -> None:
        self.terminate_benchmark(endpoint_id)
        self.neon_api.delete_endpoint(self.id, endpoint_id)
        self.endpoints[endpoint_id].branch.endpoints.pop(endpoint_id)
        self.endpoints.pop(endpoint_id)
        self.wait()

    def start_benchmark(self, target: str, clients: int = 10) -> subprocess.Popen[Any]:
        if target in self.benchmarks:
            raise RuntimeError(f"Benchmark was already started for {target}")
        is_endpoint = target.startswith("ep")
        read_only = is_endpoint and self.endpoints[target].type == "read_only"
        cmd = ["pgbench", f"-c{clients}", "-T10800", "-Mprepared"]
        if read_only:
            cmd.extend(["-S", "-n"])
        target_object = self.endpoints[target] if is_endpoint else self.branches[target]
        if target_object.connect_env is None:
            raise RuntimeError(f"The connection environment is not defined for {target}")
        log.info(
            "running pgbench on %s, cmd: %s, host: %s",
            target,
            cmd,
            target_object.connect_env["PGHOST"],
        )
        pgbench = self.pg_bin.run_nonblocking(
            cmd, env=target_object.connect_env, stderr_pipe=subprocess.PIPE
        )
        self.benchmarks[target] = pgbench
        target_object.benchmark = pgbench
        time.sleep(2)
        return pgbench

    def check_all_benchmarks(self) -> None:
        for target in tuple(self.benchmarks.keys()):
            self.check_benchmark(target)

    def check_benchmark(self, target) -> None:
        rc = self.benchmarks[target].poll()
        if rc is not None:
            _, err = self.benchmarks[target].communicate()
            log.error("STDERR: %s", err)
            # if the benchmark failed due to irresponsible Control plane,
            # just restart it
            if self.restart_pgbench_on_console_errors and (
                "ERROR:  Couldn't connect to compute node" in err
                or "ERROR:  Console request failed" in err
                or "ERROR:  Control plane request failed" in err
            ):
                log.info("Restarting benchmark for %s", target)
                self.benchmarks.pop(target)
                self.start_benchmark(target)
                return
            raise RuntimeError(f"The benchmark for {target} ended with code {rc}")

    def terminate_benchmark(self, target):
        log.info("Terminating the benchmark %s", target)
        target_endpoint = target.startswith("ep")
        self.check_benchmark(target)
        self.benchmarks[target].terminate()
        self.benchmarks.pop(target)
        if target_endpoint:
            self.endpoints[target].benchmark = None
        else:
            self.branches[target].benchmark = None

    def wait(self):
        """
        Wait for all the operations to be finished
        """
        return self.neon_api.wait_for_operation_to_finish(self.id)

    def gen_restore_name(self):
        self.restore_num += 1
        return f"restore{self.restore_num}"


@pytest.fixture()
def setup_class(
    pg_version: PgVersion,
    pg_bin: PgBin,
    neon_api: NeonAPI,
):
    neon_api.retry_if_possible = True
    project = NeonProject(neon_api, pg_bin, pg_version)
    log.info("Created a project with id %s, name %s", project.id, project.name)
    yield pg_bin, project
    log.info("Retried 524 errors: %s", neon_api.retries524)
    log.info("Retried 4xx errors: %s", neon_api.retries4xx)
    if neon_api.retries524 > 0:
        print(f"::warning::Retried on 524 error {neon_api.retries524} times")
    if neon_api.retries4xx > 0:
        print(f"::warning::Retried on 4xx error {neon_api.retries4xx} times")
    log.info("Removing the project")
    project.delete()


def do_action(project: NeonProject, action: str) -> None:
    """
    Runs the action
    """
    log.info("Action: %s", action)
    if action == "new_branch":
        log.info("Trying to create a new branch")
        parent = project.branches[
            random.choice(list(set(project.branches.keys()) - project.reset_branches))
        ]
        log.info("Parent: %s", parent)
        child = parent.create_child_branch()
        if child is None:
            return
        log.info("Created branch %s", child)
        child.start_benchmark()
    elif action == "delete_branch":
        if project.leaf_branches:
            target = random.choice(list(project.leaf_branches.values()))
            log.info("Trying to delete branch %s", target)
            target.delete()
        else:
            log.info("Leaf branches not found, skipping")
    elif action == "new_ro_endpoint":
        ep = random.choice(
            [br for br in project.branches.values() if br.id not in project.reset_branches]
        ).create_ro_endpoint()
        log.info("Created the RO endpoint with id %s branch: %s", ep.id, ep.branch.id)
        ep.start_benchmark()
    elif action == "delete_ro_endpoint":
        ro_endpoints: list[NeonEndpoint] = [
            endpoint for endpoint in project.endpoints.values() if endpoint.type == "read_only"
        ]
        if ro_endpoints:
            target_ep: NeonEndpoint = random.choice(ro_endpoints)
            target_ep.delete()
            log.info("endpoint %s deleted", target_ep.id)
        else:
            log.info("no read_only endpoints present, skipping")
    elif action == "restore_random_time":
        if project.leaf_branches:
            br: NeonBranch = random.choice(list(project.leaf_branches.values()))
            log.info("Restore %s", br)
            br.restore_random_time()
        else:
            log.info("No leaf branches found")
    else:
        raise ValueError(f"The action {action} is unknown")


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
        seed = 0
    if seed == 0:
        seed = int(time.time())
    log.info("Using random seed: %s", seed)
    random.seed(seed)
    pg_bin, project = setup_class
    # Here we can assign weights
    ACTIONS = (
        ("new_branch", 1.5),
        ("new_ro_endpoint", 1.4),
        ("delete_ro_endpoint", 0.8),
        ("delete_branch", 1.0),
        ("restore_random_time", 1.2),
    )
    if num_ops_env := os.getenv("NUM_OPERATIONS"):
        num_operations = int(num_ops_env)
    else:
        num_operations = 250
    pg_bin.run(["pgbench", "-i", "-I", "dtGvp", "-s100"], env=project.main_branch.connect_env)
    for _ in range(num_operations):
        log.info("Starting action #%s", _ + 1)
        do_action(
            project, random.choices([a[0] for a in ACTIONS], weights=[w[1] for w in ACTIONS])[0]
        )
        project.check_all_benchmarks()
    assert True
