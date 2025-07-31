from __future__ import annotations

import urllib.parse
from enum import StrEnum
from typing import TYPE_CHECKING, Any, final

import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from requests.exceptions import ReadTimeout
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
    ADMIN = "compute_ctl:admin"


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
        self.prewarm_url = f"http://localhost:{external_port}/lfc/prewarm"
        self.offload_url = f"http://localhost:{external_port}/lfc/offload"

    def dbs_and_roles(self):
        res = self.get(f"http://localhost:{self.external_port}/dbs_and_roles", auth=self.auth)
        res.raise_for_status()
        return res.json()

    def autoscaling_metrics(self):
        res = self.get(f"http://localhost:{self.external_port}/autoscaling_metrics")
        res.raise_for_status()
        log.debug("raw compute metrics: %s", res.text)
        return res.text

    def prewarm_lfc_status(self) -> dict[str, str]:
        res = self.get(self.prewarm_url)
        res.raise_for_status()
        json: dict[str, str] = res.json()
        return json

    def prewarm_lfc(self, from_endpoint_id: str | None = None) -> dict[str, str]:
        """
        Prewarm LFC cache from given endpoint and wait till it finishes or errors
        """
        params = {"from_endpoint": from_endpoint_id} if from_endpoint_id else dict()
        self.post(self.prewarm_url, params=params).raise_for_status()
        return self.prewarm_lfc_wait()

    def cancel_prewarm_lfc(self):
        """
        Cancel LFC prewarm if any is ongoing
        """
        self.delete(self.prewarm_url).raise_for_status()

    def prewarm_lfc_wait(self) -> dict[str, str]:
        """
        Wait till LFC prewarm returns with error or success.
        If prewarm was not requested before calling this function, it will error
        """
        statuses = "failed", "completed", "skipped", "cancelled"

        def prewarmed():
            json = self.prewarm_lfc_status()
            status, err = json["status"], json.get("error")
            assert status in statuses, f"{status}, {err=}"

        wait_until(prewarmed, timeout=60)
        res = self.prewarm_lfc_status()
        assert res["status"] != "failed", res
        return res

    def offload_lfc_status(self) -> dict[str, str]:
        res = self.get(self.offload_url)
        res.raise_for_status()
        json: dict[str, str] = res.json()
        return json

    def offload_lfc(self) -> dict[str, str]:
        """
        Offload LFC cache to endpoint storage and wait till offload finishes or errors
        """
        self.post(self.offload_url).raise_for_status()
        return self.offload_lfc_wait()

    def offload_lfc_wait(self) -> dict[str, str]:
        """
        Wait till LFC offload returns with error or success.
        If offload was not requested before calling this function, it will error
        """
        statuses = "failed", "completed", "skipped"

        def offloaded():
            json = self.offload_lfc_status()
            status, err = json["status"], json.get("error")
            assert status in statuses, f"{status}, {err=}"

        wait_until(offloaded, timeout=60)
        res = self.offload_lfc_status()
        assert res["status"] != "failed", res
        return res

    def promote(self, promote_spec: dict[str, Any], disconnect: bool = False) -> dict[str, str]:
        url = f"http://localhost:{self.external_port}/promote"
        if disconnect:
            try:  # send first request to start promote and disconnect
                self.post(url, json=promote_spec, timeout=0.001)
            except ReadTimeout:
                pass  # wait on second request which returns on promotion finish
        res = self.post(url, json=promote_spec)
        json: dict[str, str] = res.json()
        return json

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
