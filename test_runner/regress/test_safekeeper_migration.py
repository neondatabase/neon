from __future__ import annotations

from fixtures.neon_fixtures import NeonEnvBuilder


def test_safekeeper_migration_simple(neon_env_builder: NeonEnvBuilder):
    """
    Simple safekeeper migration test.
    Creates 3 safekeepers. The timeline is configuret to use only one safekeeper.
    1. Go through all safekeepers, migrate the timeline to it.
    2. Stop the other safekeepers. Validate that the insert is successful.
    3. Start the other safekeepers again and go to the next safekeeper.
    4. Validate that the table contains all inserted values.
    """
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 1,
    }
    env = neon_env_builder.init_start()

    ep = env.endpoints.create(
        "main", tenant_id=env.initial_tenant
    )
    # We specify all safekeepers, so compute will connect to all of them.
    # Only thouse from the current membership configuration will be used.
    ep.start(safekeeper_generation=1, safekeepers=[1, 2, 3])
    ep.safe_psql("CREATE TABLE t(a int)")

    for active_sk in range(1, 4):
        env.storage_controller.migrate_safekeepers(env.initial_tenant, env.initial_timeline, [active_sk])
        
        other_sks = [sk for sk in range(1, 4) if sk != active_sk]

        ep.safe_psql(f"INSERT INTO t VALUES ({2 * active_sk - 1})")

        for sk in other_sks:
            env.safekeepers[sk - 1].stop()

        ep.safe_psql(f"INSERT INTO t VALUES ({2 * active_sk})")

        for sk in other_sks:
            env.safekeepers[sk - 1].start()

    assert ep.safe_psql("SELECT * FROM t") == [(i,) for i in range(1, 7)]
