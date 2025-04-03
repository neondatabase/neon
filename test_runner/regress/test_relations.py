from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fixtures.neon_fixtures import (
        NeonEnvBuilder,
    )


def test_pageserver_reldir_v2(
    neon_env_builder: NeonEnvBuilder,
):
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "rel_size_v2_enabled": "false",
        }
    )

    endpoint = env.endpoints.create_start("main")
    # Create a relation in v1
    endpoint.safe_psql("CREATE TABLE foo1 (id INTEGER PRIMARY KEY, val text)")
    endpoint.safe_psql("CREATE TABLE foo2 (id INTEGER PRIMARY KEY, val text)")

    assert (
        env.pageserver.http_client().timeline_detail(env.initial_tenant, env.initial_timeline)[
            "rel_size_migration"
        ]
        == "legacy"
    )

    # Ensure the pageserver accepts the table creation SQLs before the migration. In theory, we can also do
    # a "wait_flush_lsn" here, but it's easier to just do a restart.
    env.pageserver.restart()

    # Switch to v2
    env.pageserver.http_client().update_tenant_config(
        env.initial_tenant,
        {
            "rel_size_v2_enabled": True,
        },
    )

    assert (
        env.pageserver.http_client().timeline_detail(env.initial_tenant, env.initial_timeline)[
            "rel_size_migration"
        ]
        == "legacy"
    )

    # Check if both relations are still accessible
    endpoint.safe_psql("SELECT * FROM foo1")
    endpoint.safe_psql("SELECT * FROM foo2")

    # Restart the endpoint
    endpoint.stop()
    endpoint.start()

    # Check if both relations are still accessible again after restart
    endpoint.safe_psql("SELECT * FROM foo1")
    endpoint.safe_psql("SELECT * FROM foo2")

    # Create a relation in v2
    endpoint.safe_psql("CREATE TABLE foo3 (id INTEGER PRIMARY KEY, val text)")
    endpoint.safe_psql("CREATE TABLE foo4 (id INTEGER PRIMARY KEY, val text)")
    # Delete a relation in v1
    endpoint.safe_psql("DROP TABLE foo1")

    # Check if both relations are still accessible
    endpoint.safe_psql("SELECT * FROM foo2")
    endpoint.safe_psql("SELECT * FROM foo3")
    endpoint.safe_psql("SELECT * FROM foo4")

    # Restart the endpoint
    endpoint.stop()
    # This will acquire a basebackup, which lists all relations.
    endpoint.start()

    # Check if both relations are still accessible
    endpoint.safe_psql("DROP TABLE IF EXISTS foo1")
    endpoint.safe_psql("SELECT * FROM foo2")
    endpoint.safe_psql("SELECT * FROM foo3")
    endpoint.safe_psql("SELECT * FROM foo4")
    endpoint.safe_psql("DROP TABLE foo3")
    endpoint.stop()
    endpoint.start()

    # Check if relations are still accessible
    endpoint.safe_psql("DROP TABLE IF EXISTS foo1")
    endpoint.safe_psql("SELECT * FROM foo2")
    endpoint.safe_psql("DROP TABLE IF EXISTS foo3")
    endpoint.safe_psql("SELECT * FROM foo4")

    # Set the config to false to emulate the case where the config is not persisted when the tenant gets detached/attached.
    env.pageserver.http_client().update_tenant_config(
        env.initial_tenant,
        {
            "rel_size_v2_enabled": False,
        },
    )

    # Check if the relation is still accessible
    endpoint.safe_psql("SELECT * FROM foo2")
    endpoint.safe_psql("SELECT * FROM foo4")

    env.pageserver.restart()

    assert (
        env.pageserver.http_client().timeline_detail(env.initial_tenant, env.initial_timeline)[
            "rel_size_migration"
        ]
        == "migrating"
    )
