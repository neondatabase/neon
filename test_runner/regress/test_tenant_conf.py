from contextlib import closing

import psycopg2.extras
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


def test_tenant_config(neon_env_builder: NeonEnvBuilder):
    """Test per tenant configuration"""
    # set some non-default global config
    neon_env_builder.pageserver_config_override = """
page_cache_size=444;
wait_lsn_timeout='111 s';
tenant_config={checkpoint_distance = 10000, compaction_target_size = 1048576}"""

    env = neon_env_builder.init_start()

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

    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "checkpoint_distance": "20000",
            "gc_period": "30sec",
        }
    )

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
                    "compaction_period": 1,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 100,
                    "image_creation_threshold": 3,
                    "pitr_interval": 2592000,
                }.items()
            )

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
                    "compaction_period": 1,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 30,
                    "image_creation_threshold": 3,
                    "pitr_interval": 2592000,
                }.items()
            )

    # update the config and ensure that it has changed
    env.neon_cli.config_tenant(
        tenant_id=tenant,
        conf={
            "checkpoint_distance": "15000",
            "gc_period": "80sec",
        },
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
                    "compaction_period": 1,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 80,
                    "image_creation_threshold": 3,
                    "pitr_interval": 2592000,
                }.items()
            )

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
                    "compaction_period": 1,
                    "compaction_threshold": 10,
                    "gc_horizon": 67108864,
                    "gc_period": 80,
                    "image_creation_threshold": 3,
                    "pitr_interval": 2592000,
                }.items()
            )
