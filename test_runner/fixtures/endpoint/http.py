from __future__ import annotations

import urllib.parse
from enum import StrEnum
from typing import TYPE_CHECKING, final

import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from typing_extensions import override

from fixtures.log_helper import log
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from requests import PreparedRequest


COMPUTE_AUDIENCE = "compute"
"""
The value to place in the `aud` claim.
"""


@final
class ComputeClaimsScope(StrEnum):
    ADMIN = "admin"


@final
class BearerAuth(AuthBase):
    """
    Auth implementation for bearer authorization in HTTP requests through the
    requests HTTP client library.
    """

    def __init__(self, jwt: str):
        self.__jwt = jwt

    @override
    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = "Bearer " + self.__jwt
        return request


@final
class EndpointHttpClient(requests.Session):
    def __init__(
        self,
        external_port: int,
        internal_port: int,
        jwt: str,
    ):
        super().__init__()
        self.external_port: int = external_port
        self.internal_port: int = internal_port
        self.auth = BearerAuth(jwt)

        self.mount("http://", HTTPAdapter())

    def dbs_and_roles(self):
        res = self.get(f"http://localhost:{self.external_port}/dbs_and_roles", auth=self.auth)
        res.raise_for_status()
        return res.json()

    def prewarm_lfc_status(self) -> dict[str, str]:
        res = self.get(f"http://localhost:{self.external_port}/lfc/prewarm")
        res.raise_for_status()
        json: dict[str, str] = res.json()
        return json

    def prewarm_lfc(self):
        self.post(f"http://localhost:{self.external_port}/lfc/prewarm").raise_for_status()

        def prewarmed():
            json = self.prewarm_lfc_status()
            status, err = json["status"], json.get("error")
            assert status == "completed", f"{status}, error {err}"

        wait_until(prewarmed)

    def offload_lfc(self):
        url = f"http://localhost:{self.external_port}/lfc/offload"
        self.post(url).raise_for_status()

        def offloaded():
            res = self.get(url)
            res.raise_for_status()
            json = res.json()
            status, err = json["status"], json.get("error")
            assert status == "completed", f"{status}, error {err}"

        wait_until(offloaded)

    def database_schema(self, database: str):
        res = self.get(
            f"http://localhost:{self.external_port}/database_schema?database={urllib.parse.quote(database, safe='')}",
            auth=self.auth,
        )
        res.raise_for_status()
        return res.text

    def extensions(self, extension: str, version: str, database: str):
        body = {
            "extension": extension,
            "version": version,
            "database": database,
        }
        res = self.post(f"http://localhost:{self.internal_port}/extensions", json=body)
        res.raise_for_status()
        return res.json()

    def set_role_grants(self, database: str, role: str, schema: str, privileges: list[str]):
        res = self.post(
            f"http://localhost:{self.internal_port}/grants",
            json={"database": database, "schema": schema, "role": role, "privileges": privileges},
        )
        res.raise_for_status()
        return res.json()

    def metrics(self) -> str:
        res = self.get(f"http://localhost:{self.external_port}/metrics")
        res.raise_for_status()
        log.debug("raw compute metrics: %s", res.text)
        return res.text

    # Current compute status.
    def status(self):
        res = self.get(f"http://localhost:{self.external_port}/status", auth=self.auth)
        res.raise_for_status()
        return res.json()

    # Compute startup-related metrics.
    def metrics_json(self):
        res = self.get(f"http://localhost:{self.external_port}/metrics.json", auth=self.auth)
        res.raise_for_status()
        return res.json()

    def configure_failpoints(self, *args: tuple[str, str]) -> None:
        body: list[dict[str, str]] = []

        for fp in args:
            body.append(
                {
                    "name": fp[0],
                    "action": fp[1],
                }
            )

        res = self.post(f"http://localhost:{self.internal_port}/failpoints", json=body)
        res.raise_for_status()
