from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, cast, final

import requests

from fixtures.log_helper import log

if TYPE_CHECKING:
    from typing import Any, Literal

    from fixtures.pg_version import PgVersion


def connstr_to_env(connstr: str) -> dict[str, str]:
    # postgresql://neondb_owner:npg_kuv6Rqi1cB@ep-old-silence-w26pxsvz-pooler.us-east-2.aws.neon.build/neondb?sslmode=require&channel_binding=...'
    parts = re.split(r":|@|\/|\?", connstr.removeprefix("postgresql://"))
    return {
        "PGUSER": parts[0],
        "PGPASSWORD": parts[1],
        "PGHOST": parts[2],
        "PGDATABASE": parts[3],
    }


def connection_parameters_to_env(params: dict[str, str]) -> dict[str, str]:
    return {
        "PGHOST": params["host"],
        "PGDATABASE": params["database"],
        "PGUSER": params["role"],
        "PGPASSWORD": params["password"],
    }


# Some API calls not yet implemented.
# You may want to copy not-yet-implemented methods from the PR https://github.com/neondatabase/neon/pull/11305
@final
class NeonAPI:
    def __init__(self, neon_api_key: str, neon_api_base_url: str):
        self.__neon_api_key = neon_api_key
        self.__neon_api_base_url = neon_api_base_url.strip("/")
        self.retry_if_possible = False
        self.attempts = 10
        self.sleep_before_retry = 1
        self.retries524 = 0
        self.retries4xx = 0

    def __request(
        self, method: str | bytes, endpoint: str, retry404: bool = False, **kwargs: Any
    ) -> requests.Response:
        kwargs["headers"] = kwargs.get("headers", {})
        kwargs["headers"]["Authorization"] = f"Bearer {self.__neon_api_key}"

        for attempt in range(self.attempts):
            retry = False
            resp = requests.request(method, f"{self.__neon_api_base_url}{endpoint}", **kwargs)
            if resp.status_code >= 400:
                log.error(
                    "%s %s returned a %d: %s",
                    method,
                    endpoint,
                    resp.status_code,
                    resp.text if resp.status_code != 524 else "CloudFlare error page",
                )
            else:
                log.debug("%s %s returned a %d: %s", method, endpoint, resp.status_code, resp.text)
            if not self.retry_if_possible:
                resp.raise_for_status()
                break
            elif resp.status_code >= 400:
                if resp.status_code == 404 and retry404:
                    retry = True
                    self.retries4xx += 1
                elif resp.status_code == 422 and resp.json()["message"] == "branch not ready yet":
                    retry = True
                    self.retries4xx += 1
                elif resp.status_code == 423 and resp.json()["message"] in {
                    "endpoint is in some transitive state, could not suspend",
                    "project already has running conflicting operations, scheduling of new ones is prohibited",
                }:
                    retry = True
                    self.retries4xx += 1
                elif resp.status_code == 524:
                    log.info("The request was timed out")
                    retry = True
                    self.retries524 += 1
            if retry:
                log.info("Retrying, attempt %s/%s", attempt + 1, self.attempts)
                time.sleep(self.sleep_before_retry)
                continue
            else:
                resp.raise_for_status()
            break
        else:
            raise RuntimeError("Max retry count is reached")

        return resp

    def create_project(
        self,
        pg_version: PgVersion | None = None,
        name: str | None = None,
        branch_name: str | None = None,
        branch_role_name: str | None = None,
        branch_database_name: str | None = None,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "project": {
                "branch": {},
            },
        }
        if name:
            data["project"]["name"] = name
        if pg_version:
            data["project"]["pg_version"] = int(pg_version)
        if branch_name:
            data["project"]["branch"]["name"] = branch_name
        if branch_role_name:
            data["project"]["branch"]["role_name"] = branch_role_name
        if branch_database_name:
            data["project"]["branch"]["database_name"] = branch_database_name

        resp = self.__request(
            "POST",
            "/projects",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def get_project_details(self, project_id: str) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_project_limits(self, project_id: str) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/limits",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def delete_project(
        self,
        project_id: str,
    ) -> dict[str, Any]:
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def create_branch(
        self,
        project_id: str,
        branch_name: str | None = None,
        parent_id: str | None = None,
        parent_lsn: str | None = None,
        parent_timestamp: str | None = None,
        protected: bool | None = None,
        archived: bool | None = None,
        init_source: str | None = None,
        add_endpoint: bool = True,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if add_endpoint:
            data["endpoints"] = [{"type": "read_write"}]
        data["branch"] = {}
        if parent_id:
            data["branch"]["parent_id"] = parent_id
        if branch_name:
            data["branch"]["name"] = branch_name
        if parent_lsn is not None:
            data["branch"]["parent_lsn"] = parent_lsn
        if parent_timestamp is not None:
            data["branch"]["parent_timestamp"] = parent_timestamp
        if protected is not None:
            data["branch"]["protected"] = protected
        if init_source is not None:
            data["branch"]["init_source"] = init_source
        if archived is not None:
            data["branch"]["archived"] = archived
        if not data["branch"]:
            data.pop("branch")
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )
        return cast("dict[str, Any]", resp.json())

    def get_branch_details(self, project_id: str, branch_id: str) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}",
            # XXX Retry get parent details to work around the issue
            # https://databricks.atlassian.net/browse/LKB-279
            retry404=True,
            headers={
                "Accept": "application/json",
            },
        )
        return cast("dict[str, Any]", resp.json())

    def delete_branch(self, project_id: str, branch_id: str) -> dict[str, Any]:
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}/branches/{branch_id}",
            headers={
                "Accept": "application/json",
            },
        )
        return cast("dict[str, Any]", resp.json())

    def reset_to_parent(self, project_id: str, branch_id: str) -> dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/reset_to_parent",
            headers={
                "Accept": "application/json",
            },
        )
        return cast("dict[str, Any]", resp.json())

    def restore_branch(
        self,
        project_id: str,
        branch_id: str,
        source_branch_id: str,
        source_lsn: str | None,
        source_timestamp: str | None,
        preserve_under_name: str | None,
    ):
        data = {"source_branch_id": source_branch_id}
        if source_lsn:
            data["source_lsn"] = source_lsn
        if source_timestamp:
            data["source_timestamp"] = source_timestamp
        if preserve_under_name:
            data["preserve_under_name"] = preserve_under_name
        log.info("Data: %s", data)
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/restore",
            headers={
                "Accept": "application/json",
            },
            json=data,
        )
        return cast("dict[str, Any]", resp.json())

    def start_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
    ) -> dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/start",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def suspend_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
    ) -> dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/suspend",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def restart_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
    ) -> dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/restart",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def create_endpoint(
        self,
        project_id: str,
        branch_id: str,
        endpoint_type: Literal["read_write", "read_only"],
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "endpoint": {
                "branch_id": branch_id,
            },
        }

        if endpoint_type:
            data["endpoint"]["type"] = endpoint_type
        if settings:
            # otherwise we get 400 "settings must not be nil"
            # TODO(myrrc): fix on cplane side
            if "pg_settings" not in settings:
                settings["pg_settings"] = {}
            data["endpoint"]["settings"] = settings

        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def delete_endpoint(self, project_id: str, endpoint_id: str) -> dict[str, Any]:
        resp = self.__request("DELETE", f"/projects/{project_id}/endpoints/{endpoint_id}")
        return cast("dict[str,Any]", resp.json())

    def get_connection_uri(
        self,
        project_id: str,
        branch_id: str | None = None,
        endpoint_id: str | None = None,
        database_name: str = "neondb",
        role_name: str = "neondb_owner",
        pooled: bool = True,
    ) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/connection_uri",
            params={
                "branch_id": branch_id,
                "endpoint_id": endpoint_id,
                "database_name": database_name,
                "role_name": role_name,
                "pooled": pooled,
            },
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_branches(self, project_id: str) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_endpoints(self, project_id: str) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/endpoints",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_operations(self, project_id: str) -> dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/operations",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.__neon_api_key}",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def wait_for_operation_to_finish(self, project_id: str):
        has_running = True
        while has_running:
            has_running = False
            operations = self.get_operations(project_id)["operations"]
            for op in operations:
                if op["status"] in {"scheduling", "running", "cancelling"}:
                    has_running = True
            time.sleep(0.5)


@final
class NeonApiEndpoint:
    def __init__(self, neon_api: NeonAPI, pg_version: PgVersion, project_id: str | None):
        self.neon_api = neon_api
        self.project_id: str
        self.endpoint_id: str
        self.connstr: str

        if project_id is None:
            project = neon_api.create_project(pg_version)
            neon_api.wait_for_operation_to_finish(cast("str", project["project"]["id"]))
            self.project_id = project["project"]["id"]
            self.endpoint_id = project["endpoints"][0]["id"]
            self.connstr = project["connection_uris"][0]["connection_uri"]
            self.pgbench_env = connection_parameters_to_env(
                cast("dict[str, str]", project["connection_uris"][0]["connection_parameters"])
            )
            self.is_new = True
        else:
            project = neon_api.get_project_details(project_id)
            if int(project["project"]["pg_version"]) != int(pg_version):
                raise Exception(
                    f"A project with the provided ID exists, but it's not of the specified version (expected {pg_version}, got {project['project']['pg_version']})"
                )
            self.project_id = project_id
            eps = neon_api.get_endpoints(project_id)["endpoints"]
            self.endpoint_id = eps[0]["id"]
            self.connstr = neon_api.get_connection_uri(
                project_id, endpoint_id=self.endpoint_id, pooled=False
            )["uri"]
            pw = self.connstr.split("@")[0].split(":")[-1]
            self.pgbench_env = {
                "PGHOST": eps[0]["host"],
                "PGDATABASE": "neondb",
                "PGUSER": "neondb_owner",
                "PGPASSWORD": pw,
            }
            self.is_new = False

    def restart(self):
        self.neon_api.restart_endpoint(self.project_id, self.endpoint_id)
        self.neon_api.wait_for_operation_to_finish(self.project_id)

    def get_synthetic_storage_size(self) -> int:
        return int(
            self.neon_api.get_project_details(self.project_id)["project"]["synthetic_storage_size"]
        )
