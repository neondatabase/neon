from dataclasses import dataclass
from typing import Generator, Optional

import pytest
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverApiException, TenantConfig
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.types import TenantId
from fixtures.utils import wait_until


@pytest.fixture
def positive_env(neon_env_builder: NeonEnvBuilder) -> NeonEnv:
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.LOCAL_FS,
        test_name="test_attach_tenant_config",
    )
    env = neon_env_builder.init_start()

    # eviction might be the first one after an attach to access the layers
    env.pageserver.allowed_errors.append(
        ".*unexpectedly on-demand downloading remote layer remote.* for task kind Eviction"
    )
    assert isinstance(env.remote_storage, LocalFsStorage)
    return env


@dataclass
class NegativeTests:
    neon_env: NeonEnv
    tenant_id: TenantId
    config_pre_detach: TenantConfig


@pytest.fixture
def negative_env(neon_env_builder: NeonEnvBuilder) -> Generator[NegativeTests, None, None]:
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.LOCAL_FS,
        test_name="test_attach_tenant_config",
    )
    env = neon_env_builder.init_start()
    assert isinstance(env.remote_storage, LocalFsStorage)

    ps_http = env.pageserver.http_client()
    (tenant_id, _) = env.neon_cli.create_tenant()
    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == {}
    config_pre_detach = ps_http.tenant_config(tenant_id)
    assert tenant_id in [TenantId(t["id"]) for t in ps_http.tenant_list()]
    ps_http.tenant_detach(tenant_id)
    assert tenant_id not in [TenantId(t["id"]) for t in ps_http.tenant_list()]

    yield NegativeTests(env, tenant_id, config_pre_detach)

    assert tenant_id not in [
        TenantId(t["id"]) for t in ps_http.tenant_list()
    ], "tenant should not be attached after negative test"

    env.pageserver.allowed_errors.append(".*Error processing HTTP request: Bad request")

    def log_contains_bad_request():
        env.pageserver.log_contains(".*Error processing HTTP request: Bad request")

    wait_until(50, 0.1, log_contains_bad_request)


def test_null_body(negative_env: NegativeTests):
    """
    If we send `null` in the body, the request should be rejected with status 400.
    """
    env = negative_env.neon_env
    tenant_id = negative_env.tenant_id
    ps_http = env.pageserver.http_client()

    res = ps_http.post(
        f"{ps_http.base_url}/v1/tenant/{tenant_id}/attach",
        data=b"null",
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 400


def test_null_config(negative_env: NegativeTests):
    """
    If the `config` field is `null`, the request should be rejected with status 400.
    """

    env = negative_env.neon_env
    tenant_id = negative_env.tenant_id
    ps_http = env.pageserver.http_client()

    res = ps_http.post(
        f"{ps_http.base_url}/v1/tenant/{tenant_id}/attach",
        data=b'{"config": null}',
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 400


def test_config_with_unknown_keys_is_bad_request(negative_env: NegativeTests):
    """
    If we send a config with unknown keys, the request should be rejected with status 400.
    """

    env = negative_env.neon_env
    tenant_id = negative_env.tenant_id
    ps_http = env.pageserver.http_client()

    config_with_unknown_keys = {
        "compaction_period": "1h",
        "this_key_does_not_exist": "some value",
    }

    with pytest.raises(PageserverApiException) as e:
        ps_http.tenant_attach(tenant_id, config=config_with_unknown_keys)
    assert e.type == PageserverApiException
    assert e.value.status_code == 400


@pytest.mark.parametrize("content_type", [None, "application/json"])
def test_empty_body(positive_env: NeonEnv, content_type: Optional[str]):
    """
    For backwards-compatibility: if we send an empty body,
    the request should be accepted and the config should be the default config.
    """
    env = positive_env
    ps_http = env.pageserver.http_client()
    (tenant_id, _) = env.neon_cli.create_tenant()
    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == {}
    config_pre_detach = ps_http.tenant_config(tenant_id)
    assert tenant_id in [TenantId(t["id"]) for t in ps_http.tenant_list()]
    ps_http.tenant_detach(tenant_id)
    assert tenant_id not in [TenantId(t["id"]) for t in ps_http.tenant_list()]

    ps_http.post(
        f"{ps_http.base_url}/v1/tenant/{tenant_id}/attach",
        data=b"",
        headers=None if content_type else {"Content-Type": "application/json"},
    ).raise_for_status()

    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == {}
    assert ps_http.tenant_config(tenant_id).effective_config == config_pre_detach.effective_config


def test_fully_custom_config(positive_env: NeonEnv):
    """
    If we send a valid config in the body, the request should be accepted and the config should be applied.
    """
    env = positive_env

    fully_custom_config = {
        "compaction_period": "1h",
        "compaction_threshold": 13,
        "compaction_target_size": 1048576,
        "checkpoint_distance": 10000,
        "checkpoint_timeout": "13m",
        "eviction_policy": {
            "kind": "LayerAccessThreshold",
            "period": "20s",
            "threshold": "23h",
        },
        "evictions_low_residence_duration_metric_threshold": "2days",
        "gc_feedback": True,
        "gc_horizon": 23 * (1024 * 1024),
        "gc_period": "2h 13m",
        "image_creation_threshold": 7,
        "pitr_interval": "1m",
        "lagging_wal_timeout": "23m",
        "max_lsn_wal_lag": 230000,
        "min_resident_size_override": 23,
        "trace_read_requests": True,
        "walreceiver_connect_timeout": "13m",
    }

    ps_http = env.pageserver.http_client()

    initial_tenant_config = ps_http.tenant_config(env.initial_tenant)
    assert initial_tenant_config.tenant_specific_overrides == {}
    assert set(initial_tenant_config.effective_config.keys()) == set(
        fully_custom_config.keys()
    ), "ensure we cover all config options"

    (tenant_id, _) = env.neon_cli.create_tenant()
    ps_http.set_tenant_config(tenant_id, fully_custom_config)
    our_tenant_config = ps_http.tenant_config(tenant_id)
    assert our_tenant_config.tenant_specific_overrides == fully_custom_config
    assert set(our_tenant_config.effective_config.keys()) == set(
        fully_custom_config.keys()
    ), "ensure we cover all config options"
    assert {
        k: initial_tenant_config.effective_config[k] != our_tenant_config.effective_config[k]
        for k in fully_custom_config.keys()
    } == {
        k: True for k in fully_custom_config.keys()
    }, "ensure our custom config has different values than the default config for all config options, so we know we overrode everything"

    ps_http.tenant_detach(tenant_id)
    ps_http.tenant_attach(tenant_id, config=fully_custom_config)

    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == fully_custom_config
    assert set(ps_http.tenant_config(tenant_id).effective_config.keys()) == set(
        fully_custom_config.keys()
    ), "ensure we cover all config options"
