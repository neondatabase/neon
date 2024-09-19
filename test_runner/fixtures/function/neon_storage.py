from typing import Iterator, Optional, Dict, Any

import pytest
from _pytest.fixtures import FixtureRequest

from fixtures.neon_fixtures import NeonEnv
from fixtures.shared_fixtures import TTenant, TTimeline

@pytest.fixture(scope="function")
def tenant_config() -> Dict[str, Any]:
    return dict()

@pytest.fixture(scope="function")
def tenant(
    neon_shared_env: NeonEnv,
    request: FixtureRequest,
    tenant_config: Optional[Dict[str, Any]] = None,
) -> Iterator[TTenant]:
    tenant = TTenant(env=neon_shared_env, name=request.node.name, config=tenant_config)
    yield tenant
    tenant.shutdown_resources()


@pytest.fixture(scope="function")
def timeline(
    tenant: TTenant,
) -> Iterator[TTimeline]:
    timeline = tenant.default_timeline
    yield timeline


@pytest.fixture(scope="function")
def exclusive_tenant(
    neon_env: NeonEnv,
    request: FixtureRequest,
    tenant_config: Optional[Dict[str, Any]] = None,
) -> Iterator[TTenant]:
    tenant = TTenant(env=neon_env, name=request.node.name, config=tenant_config)
    yield tenant
    tenant.shutdown_resources()


@pytest.fixture(scope="function")
def exclusive_timeline(
    exclusive_tenant: TTenant,
) -> Iterator[TTimeline]:
    timeline = exclusive_tenant.default_timeline
    yield timeline
