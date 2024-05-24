from __future__ import annotations

from typing import TYPE_CHECKING, cast

import requests
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from typing import Any, Literal, Optional


def neon_create_project(
    neon_api_key: str,
    neon_api_base_url: str,
    pg_version: Optional[PgVersion] = None,
    name: Optional[str] = None,
    branch_name: Optional[str] = None,
    branch_role_name: Optional[str] = None,
    branch_database_name: Optional[str] = None,
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
        data["project"]["branch"]["branch_name"] = branch_name
    if branch_role_name:
        data["project"]["branch"]["branch_role_name"] = branch_role_name
    if branch_database_name:
        data["project"]["branch"]["branch_database_name"] = branch_database_name

    resp = requests.post(
        f"{neon_api_base_url}/projects",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
        json=data,
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_start_endpoint(
    neon_api_key: str,
    neon_api_base_url: str,
    project_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    resp = requests.post(
        f"{neon_api_base_url}/projects/{project_id}/endpoints/{endpoint_id}/start",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_suspend_endpoint(
    neon_api_key: str,
    neon_api_base_url: str,
    project_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    resp = requests.post(
        f"{neon_api_base_url}/projects/{project_id}/endpoints/{endpoint_id}/suspend",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_restart_endpoint(
    neon_api_key: str,
    neon_api_base_url: str,
    project_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    resp = requests.post(
        f"{neon_api_base_url}/projects/{project_id}/endpoints/{endpoint_id}/restart",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_create_endpoint(
    neon_api_key: str,
    neon_api_base_url: str,
    project_id: str,
    branch_id: str,
    endpoint_type: Optional[Literal["read_write", "read_only"]] = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "endpoint": {
            "branch_id": branch_id,
        },
    }
    if endpoint_type:
        data["endpoint"]["type"] = endpoint_type

    resp = requests.post(
        f"{neon_api_base_url}/projects/{project_id}/endpoints",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
        json=data,
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_get_connection_uri(
    neon_api_key: str,
    neon_api_base_url: str,
    project_id: str,
    branch_id: Optional[str] = None,
    endpoint_id: Optional[str] = None,
    database_name: str = "neondb",
    role_name: str = "neondb_owner",
    pooled: bool = True,
) -> dict[str, Any]:
    resp = requests.get(
        f"{neon_api_base_url}/projects/{project_id}/connection_uri",
        params={
            "branch_id": branch_id,
            "endpoint_id": endpoint_id,
            "database_name": database_name,
            "role_name": role_name,
            "pooled": pooled,
        },
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_get_branches(neon_api_key: str, neon_api_base_url: str, project_id: str) -> dict[str, Any]:
    resp = requests.get(
        f"{neon_api_base_url}/projects/{project_id}/branches",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_get_endpoints(
    neon_api_key: str, neon_api_base_url: str, project_id: str
) -> dict[str, Any]:
    resp = requests.get(
        f"{neon_api_base_url}/projects/{project_id}/endpoints",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())
