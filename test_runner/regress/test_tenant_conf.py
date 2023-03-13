from contextlib import closing

import psycopg2.extras
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    LocalFsStorage,
    NeonEnvBuilder,
    RemoteStorageKind,
    assert_tenant_status,
    wait_for_upload,
)
from fixtures.types import Lsn
from fixtures.utils import wait_until


def test_tenant_config(neon_env_builder: NeonEnvBuilder):
    """Test per tenant configuration"""
    # set some non-default global config
    neon_env_builder.pageserver_config_override = """
page_cache_size=444;
wait_lsn_timeout='111 s';
tenant_config={checkpoint_distance = 10000, compaction_target_size = 1048576}"""

    env = neon_env_builder.init_start()
    http_client = env.pageserver.http_client()

    # Check that we raise on misspelled configs
    invalid_conf_key = "some_invalid_setting_name_blah_blah_123"
    try:
        env.neon_cli.create_tenant(
            conf={
                invalid_conf_key: "20000",
            }
        )
    except Exception as e:
        assert invalid_conf_key in str(e)
    else:
        raise AssertionError("Expected validation error")

    new_conf = {
        "checkpoint_distance": "20000",
        "gc_period": "30sec",
    }
    tenant, _ = env.neon_cli.create_tenant(conf=new_conf)

    env.neon_cli.create_timeline("test_tenant_conf", tenant_id=tenant)
    env.postgres.create_start(
        "test_tenant_conf",
        "main",
        tenant,
    )

    # check the configuration of the default tenant
    # it should match global configuration
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as pscur:
            log.info(f"show {env.initial_tenant}")
            pscur.execute(f"show {env.initial_tenant}")
            res = pscur.fetchone()
            assert all(
                i in res.items()
                for i in {
                    "checkpoint_distance": 10000,
                    "compaction_target_size": 1048576,
                    "compaction_period": 20,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 60 * 60,
                    "image_creation_threshold": 3,
                    "pitr_interval": 604800,  # 7 days
                }.items()
            ), f"Unexpected res: {res}"
    default_tenant_config = http_client.tenant_config(tenant_id=env.initial_tenant)
    assert (
        not default_tenant_config.tenant_specific_overrides
    ), "Should have no specific settings yet"
    effective_config = default_tenant_config.effective_config
    assert effective_config["checkpoint_distance"] == 10000
    assert effective_config["compaction_target_size"] == 1048576
    assert effective_config["compaction_period"] == "20s"
    assert effective_config["compaction_threshold"] == 10
    assert effective_config["gc_horizon"] == 67108864
    assert effective_config["gc_period"] == "1h"
    assert effective_config["image_creation_threshold"] == 3
    assert effective_config["pitr_interval"] == "7days"

    # check the configuration of the new tenant
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as pscur:
            pscur.execute(f"show {tenant}")
            res = pscur.fetchone()
            log.info(f"res: {res}")
            assert all(
                i in res.items()
                for i in {
                    "checkpoint_distance": 20000,
                    "compaction_target_size": 1048576,
                    "compaction_period": 20,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 30,
                    "image_creation_threshold": 3,
                    "pitr_interval": 604800,
                }.items()
            ), f"Unexpected res: {res}"
    new_tenant_config = http_client.tenant_config(tenant_id=tenant)
    new_specific_config = new_tenant_config.tenant_specific_overrides
    assert new_specific_config["checkpoint_distance"] == 20000
    assert new_specific_config["gc_period"] == "30s"
    assert len(new_specific_config) == len(
        new_conf
    ), f"No more specific properties were expected, but got: {new_specific_config}"
    new_effective_config = new_tenant_config.effective_config
    assert (
        new_effective_config["checkpoint_distance"] == 20000
    ), "Specific 'checkpoint_distance' config should override the default value"
    assert (
        new_effective_config["gc_period"] == "30s"
    ), "Specific 'gc_period' config should override the default value"
    assert new_effective_config["compaction_target_size"] == 1048576
    assert new_effective_config["compaction_period"] == "20s"
    assert new_effective_config["compaction_threshold"] == 10
    assert new_effective_config["gc_horizon"] == 67108864
    assert new_effective_config["image_creation_threshold"] == 3
    assert new_effective_config["pitr_interval"] == "7days"

    # update the config and ensure that it has changed
    conf_update = {
        "checkpoint_distance": "15000",
        "gc_period": "80sec",
        "compaction_period": "80sec",
        "image_creation_threshold": "2",
    }
    env.neon_cli.config_tenant(
        tenant_id=tenant,
        conf=conf_update,
    )

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as pscur:
            pscur.execute(f"show {tenant}")
            res = pscur.fetchone()
            log.info(f"after config res: {res}")
            assert all(
                i in res.items()
                for i in {
                    "checkpoint_distance": 15000,
                    "compaction_target_size": 1048576,
                    "compaction_period": 80,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 80,
                    "image_creation_threshold": 2,
                    "pitr_interval": 604800,
                }.items()
            ), f"Unexpected res: {res}"
    updated_tenant_config = http_client.tenant_config(tenant_id=tenant)
    updated_specific_config = updated_tenant_config.tenant_specific_overrides
    assert updated_specific_config["checkpoint_distance"] == 15000
    assert updated_specific_config["gc_period"] == "1m 20s"
    assert updated_specific_config["compaction_period"] == "1m 20s"
    assert len(updated_specific_config) == len(
        conf_update
    ), f"No more specific properties were expected, but got: {updated_specific_config}"
    updated_effective_config = updated_tenant_config.effective_config
    assert (
        updated_effective_config["checkpoint_distance"] == 15000
    ), "Specific 'checkpoint_distance' config should override the default value"
    assert (
        updated_effective_config["gc_period"] == "1m 20s"
    ), "Specific 'gc_period' config should override the default value"
    assert (
        updated_effective_config["compaction_period"] == "1m 20s"
    ), "Specific 'compaction_period' config should override the default value"
    assert updated_effective_config["compaction_target_size"] == 1048576
    assert updated_effective_config["compaction_threshold"] == 10
    assert updated_effective_config["gc_horizon"] == 67108864
    assert updated_effective_config["image_creation_threshold"] == 2
    assert updated_effective_config["pitr_interval"] == "7days"

    # restart the pageserver and ensure that the config is still correct
    env.pageserver.stop()
    env.pageserver.start()

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as pscur:
            pscur.execute(f"show {tenant}")
            res = pscur.fetchone()
            log.info(f"after restart res: {res}")
            assert all(
                i in res.items()
                for i in {
                    "checkpoint_distance": 15000,
                    "compaction_target_size": 1048576,
                    "compaction_period": 80,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 80,
                    "image_creation_threshold": 2,
                    "pitr_interval": 604800,
                }.items()
            ), f"Unexpected res: {res}"
    restarted_tenant_config = http_client.tenant_config(tenant_id=tenant)
    assert (
        restarted_tenant_config == updated_tenant_config
    ), "Updated config should not change after the restart"

    # update the config with very short config and make sure no trailing chars are left from previous config
    final_conf = {
        "pitr_interval": "1 min",
    }
    env.neon_cli.config_tenant(
        tenant_id=tenant,
        conf=final_conf,
    )
    final_tenant_config = http_client.tenant_config(tenant_id=tenant)
    final_specific_config = final_tenant_config.tenant_specific_overrides
    assert final_specific_config["pitr_interval"] == "1m"
    assert len(final_specific_config) == len(
        final_conf
    ), f"No more specific properties were expected, but got: {final_specific_config}"
    final_effective_config = final_tenant_config.effective_config
    assert (
        final_effective_config["pitr_interval"] == "1m"
    ), "Specific 'pitr_interval' config should override the default value"
    assert final_effective_config["checkpoint_distance"] == 10000
    assert final_effective_config["compaction_target_size"] == 1048576
    assert final_effective_config["compaction_period"] == "20s"
    assert final_effective_config["compaction_threshold"] == 10
    assert final_effective_config["gc_horizon"] == 67108864
    assert final_effective_config["gc_period"] == "1h"
    assert final_effective_config["image_creation_threshold"] == 3

    # restart the pageserver and ensure that the config is still correct
    env.pageserver.stop()
    env.pageserver.start()

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as pscur:
            pscur.execute(f"show {tenant}")
            res = pscur.fetchone()
            log.info(f"after restart res: {res}")
            assert all(
                i in res.items()
                for i in {
                    "compaction_period": 20,
                    "pitr_interval": 60,
                }.items()
            ), f"Unexpected res: {res}"


def test_creating_tenant_conf_after_attach(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.LOCAL_FS,
        test_name="test_creating_tenant_conf_after_attach",
    )

    env = neon_env_builder.init_start()
    assert isinstance(env.remote_storage, LocalFsStorage)

    # tenant is created with defaults, as in without config file
    (tenant_id, timeline_id) = env.neon_cli.create_tenant()
    config_path = env.repo_dir / "tenants" / str(tenant_id) / "config"
    assert config_path.exists(), "config file is always initially created"

    http_client = env.pageserver.http_client()

    detail = http_client.timeline_detail(tenant_id, timeline_id)
    last_record_lsn = Lsn(detail["last_record_lsn"])
    assert last_record_lsn.lsn_int != 0, "initdb must have executed"

    wait_for_upload(http_client, tenant_id, timeline_id, last_record_lsn)

    http_client.tenant_detach(tenant_id)

    assert not config_path.exists(), "detach did not remove config file"

    http_client.tenant_attach(tenant_id)
    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: assert_tenant_status(http_client, tenant_id, "Active"),
    )

    env.neon_cli.config_tenant(tenant_id, {"gc_horizon": "1000000"})
    contents_first = config_path.read_text()
    env.neon_cli.config_tenant(tenant_id, {"gc_horizon": "0"})
    contents_later = config_path.read_text()

    # dont test applying the setting here, we have that another test case to show it
    # we just care about being able to create the file
    assert len(contents_first) > len(contents_later)
