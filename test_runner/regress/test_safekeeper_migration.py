from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fixtures.neon_fixtures import StorageControllerApiException

if TYPE_CHECKING:
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
    # TODO(diko): pageserver spams with various errors during safekeeper migration.
    # Fix the code so it handles the migration better.
    env.pageserver.allowed_errors.extend(
        [
            ".*Timeline .* was cancelled and cannot be used anymore.*",
            ".*Timeline .* has been deleted.*",
            ".*Timeline .* was not found in global map.*",
            ".*wal receiver task finished with an error.*",
        ]
    )

    ep = env.endpoints.create("main", tenant_id=env.initial_tenant)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["new_sk_set"] is None
    assert len(mconf["sk_set"]) == 1
    assert mconf["generation"] == 1

    ep.start(safekeeper_generation=1, safekeepers=mconf["sk_set"])
    ep.safe_psql("CREATE EXTENSION neon_test_utils;")
    ep.safe_psql("CREATE TABLE t(a int)")

    for active_sk in range(1, 4):
        env.storage_controller.migrate_safekeepers(
            env.initial_tenant, env.initial_timeline, [active_sk]
        )

        other_sks = [sk for sk in range(1, 4) if sk != active_sk]

        for sk in other_sks:
            env.safekeepers[sk - 1].stop()

        ep.safe_psql(f"INSERT INTO t VALUES ({active_sk})")

        for sk in other_sks:
            env.safekeepers[sk - 1].start()

    ep.clear_buffers()

    assert ep.safe_psql("SELECT * FROM t") == [(i,) for i in range(1, 4)]

    # 1 initial generation + 2 migrations on each loop iteration.
    expected_gen = 1 + 2 * 3

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["generation"] == expected_gen

    assert ep.safe_psql("SHOW neon.safekeepers")[0][0].startswith(f"g#{expected_gen}:")

    # Restart and check again to make sure data is persistent.
    ep.stop()
    ep.start(safekeeper_generation=1, safekeepers=[3])

    assert ep.safe_psql("SELECT * FROM t") == [(i,) for i in range(1, 4)]


def test_new_sk_set_validation(neon_env_builder: NeonEnvBuilder):
    """
    Test that safekeeper_migrate validates the new_sk_set before starting the migration.
    """
    neon_env_builder.num_safekeepers = 2
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 2,
    }
    env = neon_env_builder.init_start()

    def expect_fail(sk_set: list[int], match: str):
        with pytest.raises(StorageControllerApiException, match=match):
            env.storage_controller.migrate_safekeepers(
                env.initial_tenant, env.initial_timeline, sk_set
            )
        # Check that we failed before commiting to the database.
        mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
        assert mconf["generation"] == 1

    expect_fail([], "must have at least 2 safekeepers")
    expect_fail([1], "must have at least 2 safekeepers")
    expect_fail([1, 1], "duplicate")
    expect_fail([1, 100500], "does not exist")
