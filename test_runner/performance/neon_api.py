from __future__ import annotations

from typing import TYPE_CHECKING, cast

import requests
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from typing import Any, Optional


def neon_create_project(
    neon_api_key: str, neon_api_base_url: str, pg_version: PgVersion
) -> dict[str, Any]:
    resp = requests.post(
        f"{neon_api_base_url}/projects",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
        json={
            "project": {
                "pg_version": int(pg_version),
            },
        },
    )

    assert resp.status_code == 200

    return cast("dict[str, Any]", resp.json())


def neon_create_ro_endpoint(
    neon_api_key: str,
    neon_api_base_url: str,
    project_id: str,
    branch_id: str,
) -> dict[str, Any]:
    resp = requests.post(
        f"{neon_api_base_url}/projects/{project_id}/endpoints",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {neon_api_key}",
        },
        json={
            "endpoint": {
                "branch_id": branch_id,
            },
        },
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
