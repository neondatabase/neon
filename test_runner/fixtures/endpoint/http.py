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

    def prewarm_lfc_status(self) -> dict[str, str]:
        res = self.get(self.prewarm_url)
        res.raise_for_status()
        json: dict[str, str] = res.json()
        return json

    def prewarm_lfc(self, from_endpoint_id: str | None = None):
        params = {"from_endpoint": from_endpoint_id} if from_endpoint_id else dict()
        self.post(self.prewarm_url, params=params).raise_for_status()
        self.prewarm_lfc_wait()

    def prewarm_lfc_wait(self):
        def prewarmed():
            json = self.prewarm_lfc_status()
            status, err = json["status"], json.get("error")
            assert status == "completed", f"{status}, {err=}"

        wait_until(prewarmed, timeout=60)

    def offload_lfc_status(self) -> dict[str, str]:
        res = self.get(self.offload_url)
        res.raise_for_status()
        json: dict[str, str] = res.json()
        return json

    def offload_lfc(self):
        self.post(self.offload_url).raise_for_status()
        self.offload_lfc_wait()

    def offload_lfc_wait(self):
        def offloaded():
            json = self.offload_lfc_status()
            status, err = json["status"], json.get("error")
            assert status == "completed", f"{status}, {err=}"

        wait_until(offloaded)

    def promote(self, safekeepers_lsn: dict[str, Any], disconnect: bool = False):
        url = f"http://localhost:{self.external_port}/promote"
        if disconnect:
            try:  # send first request to start promote and disconnect
                self.post(url, data=safekeepers_lsn, timeout=0.001)
            except ReadTimeout:
                pass  # wait on second request which returns on promotion finish
        res = self.post(url, data=safekeepers_lsn)
        res.raise_for_status()
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

    def configure_failpoints(
        self, *args: tuple[str, str] | list[dict[str, str | dict[str, str]]]
    ) -> None:
        """Configure failpoints for testing purposes.

        Args:
            *args: Can be one of:
                - Variable number of (name, actions) tuples
                - Single list of dicts with keys: name, actions, and optionally context_matchers

        Examples:
            # Basic failpoints
            client.configure_failpoints(("test_fp", "return(error)"))
            client.configure_failpoints(("fp1", "return"), ("fp2", "sleep(1000)"))

            # Probability-based failpoint
            client.configure_failpoints(("test_fp", "50%return(error)"))

            # Context-based failpoint
            client.configure_failpoints([{
                "name": "test_fp",
                "actions": "return(error)",
                "context_matchers": {"tenant_id": ".*test.*"}
            }])
        """

        request_body: list[dict[str, Any]] = []

        if (
            len(args) == 1
            and isinstance(args[0], list)
            and args[0]
            and isinstance(args[0][0], dict)
        ):
            # Handle list of dicts (context-based failpoints)
            failpoint_configs = args[0]
            for config in failpoint_configs:
                server_config: dict[str, Any] = {
                    "name": config["name"],
                    "actions": config["actions"],
                }
                if "context_matchers" in config:
                    server_config["context_matchers"] = config["context_matchers"]
                request_body.append(server_config)
        else:
            # Handle tuples (basic failpoints)
            for fp in args:
                request_body.append(
                    {
                        "name": fp[0],
                        "actions": fp[1],
                    }
                )

        res = self.post(f"http://localhost:{self.internal_port}/failpoints", json=request_body)
        res.raise_for_status()
