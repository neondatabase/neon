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

    def get_operation(self, project_id: str, operation_id: str) -> dict[str, Any]:
        """
        Retrieves a specific operation.

        Args:
            project_id: The Neon project ID
            operation_id: The operation ID

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/operations/{operation_id}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def list_projects(self) -> dict[str, Any]:
        """
        Lists all projects.

        Returns:
            The API response containing the list of projects
        """
        resp = self.__request(
            "GET",
            "/projects",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def update_project(
        self,
        project_id: str,
        name: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Updates a project.

        Args:
            project_id: The Neon project ID
            name: New name for the project (optional)
            settings: Project settings to update (optional)

        Returns:
            The API response containing the updated project details
        """
        data: dict[str, Any] = {
            "project": {},
        }

        if name is not None:
            data["project"]["name"] = name

        if settings is not None:
            data["project"]["settings"] = settings

        resp = self.__request(
            "PATCH",
            f"/projects/{project_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def get_project_consumption(
        self,
        project_id: str,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieves project consumption data.

        Args:
            project_id: The Neon project ID
            from_time: Start time for consumption data (optional)
            to_time: End time for consumption data (optional)

        Returns:
            The API response containing the project consumption data
        """
        params = {}
        if from_time is not None:
            params["from"] = from_time
        if to_time is not None:
            params["to"] = to_time

        resp = self.__request(
            "GET",
            f"/projects/{project_id}/consumption",
            headers={
                "Accept": "application/json",
            },
            params=params,
        )

        return cast("dict[str, Any]", resp.json())

    def get_project_vpc_endpoints(self, project_id: str) -> dict[str, Any]:
        """
        Retrieves project VPC endpoints.

        Args:
            project_id: The Neon project ID

        Returns:
            The API response containing the project VPC endpoints
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/vpc_endpoints",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def create_branch(
        self,
        project_id: str,
        parent_id: str | None = None,
        name: str | None = None,
        endpoints: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Creates a new branch.

        Args:
            project_id: The Neon project ID
            parent_id: The parent branch ID (optional)
            name: Name for the branch (optional)
            endpoints: List of endpoints to create with the branch (optional)

        Returns:
            The API response containing the created branch details and operations
        """
        data: dict[str, Any] = {
            "branch": {},
        }

        if parent_id is not None:
            data["branch"]["parent_id"] = parent_id

        if name is not None:
            data["branch"]["name"] = name

        if endpoints is not None:
            data["endpoints"] = endpoints

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
        """
        Retrieves details of a specific branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the branch details
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def update_branch(
        self,
        project_id: str,
        branch_id: str,
        name: str | None = None,
        default: bool | None = None,
    ) -> dict[str, Any]:
        """
        Updates the specified branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            name: New name for the branch (optional)
            default: Whether to set this branch as the default branch (optional)

        Returns:
            The API response containing the updated branch details and operations
        """
        data: dict[str, Any] = {
            "branch": {},
        }

        if name is not None:
            data["branch"]["name"] = name

        if default is not None:
            data["branch"]["default"] = default

        resp = self.__request(
            "PATCH",
            f"/projects/{project_id}/branches/{branch_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def delete_branch(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Deletes a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}/branches/{branch_id}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def reset_branch(
        self,
        project_id: str,
        branch_id: str,
        lsn: str | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        """
        Resets a branch to a specified LSN or timestamp.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            lsn: The Log Sequence Number (LSN) to reset to (optional)
            timestamp: The timestamp to reset to (optional)

        Returns:
            The API response containing the operation details
        """
        params = {}
        if lsn is not None:
            params["lsn"] = lsn
        if timestamp is not None:
            params["timestamp"] = timestamp

        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/reset",
            headers={
                "Accept": "application/json",
            },
            params=params,
        )

        return cast("dict[str, Any]", resp.json())

    def reset_branch_to_parent(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Resets a branch to the current state of the parent branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/reset_to_parent",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def restore_branch(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Restores a branch that was previously deleted.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/restore",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_schema(
        self,
        project_id: str,
        branch_id: str,
        db_name: str,
        lsn: str | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieves the schema from the specified database.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            db_name: Name of the database for which the schema is retrieved
            lsn: The Log Sequence Number (LSN) for which the schema is retrieved (optional)
            timestamp: The point in time for which the schema is retrieved (optional)

        Returns:
            The API response containing the database schema
        """
        params = {
            "db_name": db_name,
        }

        if lsn is not None:
            params["lsn"] = lsn

        if timestamp is not None:
            params["timestamp"] = timestamp

        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/schema",
            headers={
                "Accept": "application/json",
            },
            params=params,
        )

        return cast("dict[str, Any]", resp.json())

    def compare_branch_schema(
        self,
        project_id: str,
        branch_id: str,
        target_branch_id: str,
        db_name: str,
        target_db_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Compares schemas between two branches.

        Args:
            project_id: The Neon project ID
            branch_id: The source branch ID
            target_branch_id: The target branch ID to compare with
            db_name: Name of the database in the source branch
            target_db_name: Name of the database in the target branch (optional)

        Returns:
            The API response containing the schema comparison
        """
        params = {
            "target_branch_id": target_branch_id,
            "db_name": db_name,
        }

        if target_db_name is not None:
            params["target_db_name"] = target_db_name

        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/compare_schema",
            headers={
                "Accept": "application/json",
            },
            params=params,
        )

        return cast("dict[str, Any]", resp.json())

    def set_branch_as_default(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Sets a branch as the default branch for the project.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/set_as_default",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_endpoints(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Retrieves endpoints associated with a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the branch endpoints
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/endpoints",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_databases(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Retrieves databases associated with a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the branch databases
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/databases",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def create_branch_database(
        self, project_id: str, branch_id: str, database_name: str, owner_name: str | None = None
    ) -> dict[str, Any]:
        """
        Creates a new database in a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            database_name: Name of the database to create
            owner_name: Name of the role that will own the database (optional)

        Returns:
            The API response containing the created database details
        """
        data: dict[str, Any] = {
            "database": {
                "name": database_name,
            },
        }

        if owner_name is not None:
            data["database"]["owner_name"] = owner_name

        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/databases",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_database(
        self, project_id: str, branch_id: str, database_name: str
    ) -> dict[str, Any]:
        """
        Retrieves details of a specific database in a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            database_name: Name of the database to retrieve

        Returns:
            The API response containing the database details
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/databases/{database_name}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def delete_branch_database(
        self, project_id: str, branch_id: str, database_name: str
    ) -> dict[str, Any]:
        """
        Deletes a database from a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            database_name: Name of the database to delete

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}/branches/{branch_id}/databases/{database_name}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_roles(self, project_id: str, branch_id: str) -> dict[str, Any]:
        """
        Retrieves roles associated with a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID

        Returns:
            The API response containing the branch roles
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/roles",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def create_branch_role(self, project_id: str, branch_id: str, role_name: str) -> dict[str, Any]:
        """
        Creates a new role in a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            role_name: Name of the role to create

        Returns:
            The API response containing the created role details
        """
        data: dict[str, Any] = {
            "role": {
                "name": role_name,
            },
        }

        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/roles",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_role(self, project_id: str, branch_id: str, role_name: str) -> dict[str, Any]:
        """
        Retrieves details of a specific role in a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            role_name: Name of the role to retrieve

        Returns:
            The API response containing the role details
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/roles/{role_name}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def delete_branch_role(self, project_id: str, branch_id: str, role_name: str) -> dict[str, Any]:
        """
        Deletes a role from a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            role_name: Name of the role to delete

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}/branches/{branch_id}/roles/{role_name}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_branch_role_password(
        self, project_id: str, branch_id: str, role_name: str
    ) -> dict[str, Any]:
        """
        Retrieves the password for a specific role in a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            role_name: Name of the role

        Returns:
            The API response containing the role password
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches/{branch_id}/roles/{role_name}/reveal_password",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def reset_branch_role_password(
        self, project_id: str, branch_id: str, role_name: str
    ) -> dict[str, Any]:
        """
        Resets the password for a specific role in a branch.

        Args:
            project_id: The Neon project ID
            branch_id: The branch ID
            role_name: Name of the role

        Returns:
            The API response containing the new role password
        """
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/branches/{branch_id}/roles/{role_name}/reset_password",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_endpoint(self, project_id: str, endpoint_id: str) -> dict[str, Any]:
        """
        Retrieves details of a specific endpoint.

        Args:
            project_id: The Neon project ID
            endpoint_id: The endpoint ID

        Returns:
            The API response containing the endpoint details
        """
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/endpoints/{endpoint_id}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def update_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
        branch_id: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Updates an endpoint.

        Args:
            project_id: The Neon project ID
            endpoint_id: The endpoint ID
            branch_id: The branch ID to connect the endpoint to (optional)
            settings: Endpoint settings to update (optional)

        Returns:
            The API response containing the updated endpoint details
        """
        data: dict[str, Any] = {
            "endpoint": {},
        }

        if branch_id is not None:
            data["endpoint"]["branch_id"] = branch_id

        if settings is not None:
            data["endpoint"]["settings"] = settings

        resp = self.__request(
            "PATCH",
            f"/projects/{project_id}/endpoints/{endpoint_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def delete_endpoint(self, project_id: str, endpoint_id: str) -> dict[str, Any]:
        """
        Deletes an endpoint.

        Args:
            project_id: The Neon project ID
            endpoint_id: The endpoint ID

        Returns:
            The API response containing the operation details
        """
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}/endpoints/{endpoint_id}",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())

    def get_endpoint_stats(
        self,
        project_id: str,
        endpoint_id: str,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieves statistics for a specific endpoint.

        Args:
            project_id: The Neon project ID
            endpoint_id: The endpoint ID
            from_time: Start time for statistics data (optional)
            to_time: End time for statistics data (optional)

        Returns:
            The API response containing the endpoint statistics
        """
        data: dict[str, Any] = {}

        if from_time is not None:
            data["from"] = from_time

        if to_time is not None:
            data["to"] = to_time

        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/stats",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("dict[str, Any]", resp.json())

    def authenticate_passwordless_session(
        self, project_id: str, endpoint_id: str
    ) -> dict[str, Any]:
        """
        Authenticates a passwordless session for an endpoint.

        Args:
            project_id: The Neon project ID
            endpoint_id: The endpoint ID

        Returns:
            The API response containing the authentication details
        """
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/passwordless_auth",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("dict[str, Any]", resp.json())


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
