from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import jwt

from fixtures.common_types import TenantId


@dataclass
class AuthKeys:
    priv: str

    def generate_token(self, *, scope: TokenScope, **token_data: Any) -> str:
        token_data = {key: str(val) for key, val in token_data.items()}
        token = jwt.encode({"scope": scope, **token_data}, self.priv, algorithm="EdDSA")
        # cast(Any, self.priv)

        # jwt.encode can return 'bytes' or 'str', depending on Python version or type
        # hinting or something (not sure what). If it returned 'bytes', convert it to 'str'
        # explicitly.
        if isinstance(token, bytes):
            token = token.decode()

        return token

    def generate_pageserver_token(self) -> str:
        return self.generate_token(scope=TokenScope.PAGE_SERVER_API)

    def generate_safekeeper_token(self) -> str:
        return self.generate_token(scope=TokenScope.SAFEKEEPER_DATA)

    # generate token giving access to only one tenant
    def generate_tenant_token(self, tenant_id: TenantId) -> str:
        return self.generate_token(scope=TokenScope.TENANT, tenant_id=str(tenant_id))


class TokenScope(StrEnum):
    ADMIN = "admin"
    PAGE_SERVER_API = "pageserverapi"
    GENERATIONS_API = "generations_api"
    SAFEKEEPER_DATA = "safekeeperdata"
    TENANT = "tenant"
    SCRUBBER = "scrubber"
    INFRA = "infra"
