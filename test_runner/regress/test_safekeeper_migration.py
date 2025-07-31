from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest
import requests
from fixtures.log_helper import log
from fixtures.neon_fixtures import StorageControllerApiException

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder

# TODO(diko): pageserver spams with various errors during safekeeper migration.
# Fix the code so it handles the migration better.
PAGESERVER_ALLOWED_ERRORS = [
    ".*Timeline .* was cancelled and cannot be used anymore.*",
    ".*Timeline .* has been deleted.*",
    ".*Timeline .* was not found in global map.*",
    ".*wal receiver task finished with an error.*",
]


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
    env.pageserver.allowed_errors.extend(PAGESERVER_ALLOWED_ERRORS)

    ep = env.endpoints.create("main", tenant_id=env.initial_tenant)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["new_sk_set"] is None
    assert len(mconf["sk_set"]) == 1
    assert mconf["generation"] == 1

    current_sk = mconf["sk_set"][0]

    ep.start(safekeeper_generation=1, safekeepers=mconf["sk_set"])
    ep.safe_psql("CREATE EXTENSION neon_test_utils;")
    ep.safe_psql("CREATE TABLE t(a int)")

    expected_gen = 1

    for active_sk in range(1, 4):
        env.storage_controller.migrate_safekeepers(
            env.initial_tenant, env.initial_timeline, [active_sk]
        )

        if active_sk != current_sk:
            expected_gen += 2
            current_sk = active_sk

        other_sks = [sk for sk in range(1, 4) if sk != active_sk]

        for sk in other_sks:
            env.safekeepers[sk - 1].stop()

        ep.safe_psql(f"INSERT INTO t VALUES ({active_sk})")

        for sk in other_sks:
            env.safekeepers[sk - 1].start()

    ep.clear_buffers()

    assert ep.safe_psql("SELECT * FROM t") == [(i,) for i in range(1, 4)]

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
    neon_env_builder.num_safekeepers = 3
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

    expect_fail([], "safekeeper set is empty")
    expect_fail([1], "must have at least 2 safekeepers")
    expect_fail([1, 1], "duplicate safekeeper")
    expect_fail([1, 100500], "does not exist")

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    sk_set = mconf["sk_set"]
    assert len(sk_set) == 2

    decom_sk = [sk.id for sk in env.safekeepers if sk.id not in sk_set][0]
    env.storage_controller.safekeeper_scheduling_policy(decom_sk, "Decomissioned")

    expect_fail([sk_set[0], decom_sk], "decomissioned")


def test_safekeeper_migration_common_set_failpoints(neon_env_builder: NeonEnvBuilder):
    """
    Test that safekeeper migration handles failures well.

    Two main conditions are checked:
    1. safekeeper migration handler can be retried on different failures.
    2. writes do not stuck if sk_set and new_sk_set have a quorum in common.
    """
    neon_env_builder.num_safekeepers = 4
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 3,
    }
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(PAGESERVER_ALLOWED_ERRORS)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert len(mconf["sk_set"]) == 3
    assert mconf["generation"] == 1

    ep = env.endpoints.create("main", tenant_id=env.initial_tenant)
    ep.start(safekeeper_generation=1, safekeepers=mconf["sk_set"])
    ep.safe_psql("CREATE EXTENSION neon_test_utils;")
    ep.safe_psql("CREATE TABLE t(a int)")

    excluded_sk = mconf["sk_set"][-1]
    added_sk = [sk.id for sk in env.safekeepers if sk.id not in mconf["sk_set"]][0]
    new_sk_set = mconf["sk_set"][:-1] + [added_sk]
    log.info(f"migrating sk set from {mconf['sk_set']} to {new_sk_set}")

    failpoints = [
        "sk-migration-after-step-3",
        "sk-migration-after-step-4",
        "sk-migration-after-step-5",
        "sk-migration-after-step-7",
        "sk-migration-after-step-8",
        "sk-migration-step-9-after-set-membership",
        "sk-migration-step-9-mid-exclude",
        "sk-migration-step-9-after-exclude",
        "sk-migration-after-step-9",
    ]

    for i, fp in enumerate(failpoints):
        env.storage_controller.configure_failpoints((fp, "return(1)"))

        with pytest.raises(StorageControllerApiException, match=f"failpoint {fp}"):
            env.storage_controller.migrate_safekeepers(
                env.initial_tenant, env.initial_timeline, new_sk_set
            )
        ep.safe_psql(f"INSERT INTO t VALUES ({i})")

        env.storage_controller.configure_failpoints((fp, "off"))

    # No failpoints, migration should succeed.
    env.storage_controller.migrate_safekeepers(env.initial_tenant, env.initial_timeline, new_sk_set)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["new_sk_set"] is None
    assert mconf["sk_set"] == new_sk_set
    assert mconf["generation"] == 3

    ep.clear_buffers()
    assert ep.safe_psql("SELECT * FROM t") == [(i,) for i in range(len(failpoints))]
    assert ep.safe_psql("SHOW neon.safekeepers")[0][0].startswith("g#3:")

    # Check that we didn't forget to remove the timeline on the excluded safekeeper.
    with pytest.raises(requests.exceptions.HTTPError) as exc:
        env.safekeepers[excluded_sk - 1].http_client().timeline_status(
            env.initial_tenant, env.initial_timeline
        )
    assert exc.value.response.status_code == 404
    assert (
        f"timeline {env.initial_tenant}/{env.initial_timeline} deleted" in exc.value.response.text
    )


def test_sk_generation_aware_tombstones(neon_env_builder: NeonEnvBuilder):
    """
    Test that safekeeper respects generations:
    1. Check that migration back and forth between two safekeepers works.
    2. Check that sk refuses to execute requests with stale generation.
    """
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 1,
    }
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(PAGESERVER_ALLOWED_ERRORS)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["new_sk_set"] is None
    assert len(mconf["sk_set"]) == 1
    cur_sk = mconf["sk_set"][0]

    second_sk, third_sk = [sk.id for sk in env.safekeepers if sk.id != cur_sk]
    cur_gen = 1

    # Pull the timeline manually to third_sk, so the timeline exists there with stale generation.
    # This is needed for the test later.
    env.get_safekeeper(third_sk).pull_timeline(
        [env.get_safekeeper(cur_sk)], env.initial_tenant, env.initial_timeline
    )

    def expect_deleted(sk_id: int):
        with pytest.raises(requests.exceptions.HTTPError, match="Not Found") as exc:
            env.get_safekeeper(sk_id).http_client().timeline_status(
                env.initial_tenant, env.initial_timeline
            )
        assert exc.value.response.status_code == 404
        assert re.match(r".*timeline .* deleted.*", exc.value.response.text)

    def get_mconf(sk_id: int):
        status = (
            env.get_safekeeper(sk_id)
            .http_client()
            .timeline_status(env.initial_tenant, env.initial_timeline)
        )
        assert status.mconf is not None
        return status.mconf

    def migrate():
        nonlocal cur_sk, second_sk, cur_gen
        env.storage_controller.migrate_safekeepers(
            env.initial_tenant, env.initial_timeline, [second_sk]
        )
        cur_sk, second_sk = second_sk, cur_sk
        cur_gen += 2

    # Migrate the timeline back and forth between cur_sk and second_sk.
    for _i in range(3):
        migrate()
        # Timeline should exist on cur_sk.
        assert get_mconf(cur_sk).generation == cur_gen
        # Timeline should be deleted on second_sk.
        expect_deleted(second_sk)

    # Remember current mconf.
    mconf = get_mconf(cur_sk)

    # Migrate the timeline one more time.
    # It increases the generation by 2.
    migrate()

    # Check that sk refuses to execute the exclude request with the old mconf.
    with pytest.raises(requests.exceptions.HTTPError, match="Conflict") as exc:
        env.get_safekeeper(cur_sk).http_client().timeline_exclude(
            env.initial_tenant, env.initial_timeline, mconf
        )
    assert re.match(r".*refused to switch into excluding mconf.*", exc.value.response.text)
    # We shouldn't have deleted the timeline.
    assert get_mconf(cur_sk).generation == cur_gen

    # Check that sk refuses to execute the pull_timeline request with the old mconf.
    # Note: we try to pull from third_sk, which has a timeline with stale generation.
    # Thus, we bypass some preliminary generation checks and actually test tombstones.
    with pytest.raises(requests.exceptions.HTTPError, match="Conflict") as exc:
        env.get_safekeeper(second_sk).pull_timeline(
            [env.get_safekeeper(third_sk)], env.initial_tenant, env.initial_timeline, mconf
        )
    assert re.match(r".*Timeline .* deleted.*", exc.value.response.text)
    # The timeline should remain deleted.
    expect_deleted(second_sk)


def test_safekeeper_migration_stale_timeline(neon_env_builder: NeonEnvBuilder):
    """
    Test that safekeeper migration handles stale timeline correctly by migrating to
    a safekeeper with a stale timeline.
    1. Check that we are waiting for the stale timeline to catch up with the commit lsn.
       The migration might fail if there is no compute to advance the WAL.
    2. Check that we rely on last_log_term (and not the current term) when waiting for the
       sync_position on step 7.
    3. Check that migration succeeds if the compute is running.
    """
    neon_env_builder.num_safekeepers = 2
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 1,
    }
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(PAGESERVER_ALLOWED_ERRORS)
    env.storage_controller.allowed_errors.append(".*not enough successful .* to reach quorum.*")

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)

    active_sk = env.get_safekeeper(mconf["sk_set"][0])
    other_sk = [sk for sk in env.safekeepers if sk.id != active_sk.id][0]

    ep = env.endpoints.create("main", tenant_id=env.initial_tenant)
    ep.start(safekeeper_generation=1, safekeepers=[active_sk.id])
    ep.safe_psql("CREATE TABLE t(a int)")
    ep.safe_psql("INSERT INTO t VALUES (0)")

    # Pull the timeline to other_sk, so other_sk now has a "stale" timeline on it.
    other_sk.pull_timeline([active_sk], env.initial_tenant, env.initial_timeline)

    # Advance the WAL on active_sk.
    ep.safe_psql("INSERT INTO t VALUES (1)")

    # The test is more tricky if we have the same last_log_term but different term/flush_lsn.
    # Stop the active_sk during the endpoint shutdown because otherwise compute_ctl runs
    # sync_safekeepers and advances last_log_term on active_sk.
    active_sk.stop()
    ep.stop(mode="immediate")
    active_sk.start()

    active_sk_status = active_sk.http_client().timeline_status(
        env.initial_tenant, env.initial_timeline
    )
    other_sk_status = other_sk.http_client().timeline_status(
        env.initial_tenant, env.initial_timeline
    )

    # other_sk should have the same last_log_term, but a stale flush_lsn.
    assert active_sk_status.last_log_term == other_sk_status.last_log_term
    assert active_sk_status.flush_lsn > other_sk_status.flush_lsn

    commit_lsn = active_sk_status.flush_lsn

    # Bump the term on other_sk to make it higher than active_sk.
    # This is to make sure we don't use current term instead of last_log_term in the algorithm.
    other_sk.http_client().term_bump(
        env.initial_tenant, env.initial_timeline, active_sk_status.term + 100
    )

    # TODO(diko): now it fails because the timeline on other_sk is stale and there is no compute
    # to catch up it with active_sk. It might be fixed in https://databricks.atlassian.net/browse/LKB-946
    # if we delete stale timelines before starting the migration.
    # But the rest of the test is still valid: we should not lose committed WAL after the migration.
    with pytest.raises(
        StorageControllerApiException, match="not enough successful .* to reach quorum"
    ):
        env.storage_controller.migrate_safekeepers(
            env.initial_tenant, env.initial_timeline, [other_sk.id]
        )

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["new_sk_set"] == [other_sk.id]
    assert mconf["sk_set"] == [active_sk.id]
    assert mconf["generation"] == 2

    # Start the endpoint, so it advances the WAL on other_sk.
    ep.start(safekeeper_generation=2, safekeepers=[active_sk.id, other_sk.id])
    # Now the migration should succeed.
    env.storage_controller.migrate_safekeepers(
        env.initial_tenant, env.initial_timeline, [other_sk.id]
    )

    # Check that we didn't lose committed WAL.
    assert (
        other_sk.http_client().timeline_status(env.initial_tenant, env.initial_timeline).flush_lsn
        >= commit_lsn
    )
    assert ep.safe_psql("SELECT * FROM t") == [(0,), (1,)]


def test_pull_from_most_advanced_sk(neon_env_builder: NeonEnvBuilder):
    """
    Test that we pull the timeline from the most advanced safekeeper during the
    migration and do not lose committed WAL.
    """
    neon_env_builder.num_safekeepers = 4
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 3,
    }
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(PAGESERVER_ALLOWED_ERRORS)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)

    sk_set = mconf["sk_set"]
    assert len(sk_set) == 3

    other_sk = [sk.id for sk in env.safekeepers if sk.id not in sk_set][0]

    ep = env.endpoints.create("main", tenant_id=env.initial_tenant)
    ep.start(safekeeper_generation=1, safekeepers=sk_set)
    ep.safe_psql("CREATE TABLE t(a int)")
    ep.safe_psql("INSERT INTO t VALUES (0)")

    # Stop one sk, so we have a lagging WAL on it.
    env.get_safekeeper(sk_set[0]).stop()
    # Advance the WAL on the other sks.
    ep.safe_psql("INSERT INTO t VALUES (1)")

    # Stop other sks to make sure compute_ctl doesn't advance the last_log_term on them during shutdown.
    for sk_id in sk_set[1:]:
        env.get_safekeeper(sk_id).stop()
    ep.stop(mode="immediate")
    for sk_id in sk_set:
        env.get_safekeeper(sk_id).start()

    # Bump the term on the lagging sk to make sure we don't use it to choose the most advanced sk.
    env.get_safekeeper(sk_set[0]).http_client().term_bump(
        env.initial_tenant, env.initial_timeline, 100
    )

    def get_commit_lsn(sk_set: list[int]):
        flush_lsns = []
        last_log_terms = []
        for sk_id in sk_set:
            sk = env.get_safekeeper(sk_id)
            status = sk.http_client().timeline_status(env.initial_tenant, env.initial_timeline)
            flush_lsns.append(status.flush_lsn)
            last_log_terms.append(status.last_log_term)

        # In this test we assume that all sks have the same last_log_term.
        assert len(set(last_log_terms)) == 1

        flush_lsns.sort(reverse=True)
        commit_lsn = flush_lsns[len(sk_set) // 2]

        log.info(f"sk_set: {sk_set}, flush_lsns: {flush_lsns}, commit_lsn: {commit_lsn}")
        return commit_lsn

    commit_lsn_before_migration = get_commit_lsn(sk_set)

    # Make two migrations, so the lagging sk stays in the sk_set, but other sks are replaced.
    new_sk_set1 = [sk_set[0], sk_set[1], other_sk]  # remove sk_set[2], add other_sk
    new_sk_set2 = [sk_set[0], other_sk, sk_set[2]]  # remove sk_set[1], add sk_set[2] back
    env.storage_controller.migrate_safekeepers(
        env.initial_tenant, env.initial_timeline, new_sk_set1
    )
    env.storage_controller.migrate_safekeepers(
        env.initial_tenant, env.initial_timeline, new_sk_set2
    )

    commit_lsn_after_migration = get_commit_lsn(new_sk_set2)

    # We should not lose committed WAL.
    # If we have choosen the lagging sk to pull the timeline from, this might fail.
    assert commit_lsn_before_migration <= commit_lsn_after_migration

    ep.start(safekeeper_generation=5, safekeepers=new_sk_set2)
    assert ep.safe_psql("SELECT * FROM t") == [(0,), (1,)]


def test_abort_safekeeper_migration(neon_env_builder: NeonEnvBuilder):
    """
    Test that safekeeper migration can be aborted.
    1. Insert failpoints and ensure the abort successfully reverts the timeline state.
    2. Check that endpoint is operational after the abort.
    """
    neon_env_builder.num_safekeepers = 2
    neon_env_builder.storage_controller_config = {
        "timelines_onto_safekeepers": True,
        "timeline_safekeeper_count": 1,
    }
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(PAGESERVER_ALLOWED_ERRORS)

    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert len(mconf["sk_set"]) == 1
    cur_sk = mconf["sk_set"][0]
    cur_gen = 1

    ep = env.endpoints.create("main", tenant_id=env.initial_tenant)
    ep.start(safekeeper_generation=1, safekeepers=mconf["sk_set"])
    ep.safe_psql("CREATE EXTENSION neon_test_utils;")
    ep.safe_psql("CREATE TABLE t(a int)")
    ep.safe_psql("INSERT INTO t VALUES (1)")

    another_sk = [sk.id for sk in env.safekeepers if sk.id != cur_sk][0]

    failpoints = [
        "sk-migration-after-step-3",
        "sk-migration-after-step-4",
        "sk-migration-after-step-5",
        "sk-migration-after-step-7",
    ]

    for fp in failpoints:
        env.storage_controller.configure_failpoints((fp, "return(1)"))

        with pytest.raises(StorageControllerApiException, match=f"failpoint {fp}"):
            env.storage_controller.migrate_safekeepers(
                env.initial_tenant, env.initial_timeline, [another_sk]
            )
        cur_gen += 1

        env.storage_controller.configure_failpoints((fp, "off"))

        # We should have a joint mconf after the failure.
        mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
        assert mconf["generation"] == cur_gen
        assert mconf["sk_set"] == [cur_sk]
        assert mconf["new_sk_set"] == [another_sk]

        env.storage_controller.abort_safekeeper_migration(env.initial_tenant, env.initial_timeline)
        cur_gen += 1

        # Abort should revert the timeline to the previous sk_set and increment the generation.
        mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
        assert mconf["generation"] == cur_gen
        assert mconf["sk_set"] == [cur_sk]
        assert mconf["new_sk_set"] is None

        assert ep.safe_psql("SHOW neon.safekeepers")[0][0].startswith(f"g#{cur_gen}:")
        ep.safe_psql(f"INSERT INTO t VALUES ({cur_gen})")

    # After step-8 the final mconf is committed and the migration is not abortable anymore.
    # So the abort should not abort anything.
    env.storage_controller.configure_failpoints(("sk-migration-after-step-8", "return(1)"))

    with pytest.raises(StorageControllerApiException, match="failpoint sk-migration-after-step-8"):
        env.storage_controller.migrate_safekeepers(
            env.initial_tenant, env.initial_timeline, [another_sk]
        )
    cur_gen += 2

    env.storage_controller.configure_failpoints((fp, "off"))

    env.storage_controller.abort_safekeeper_migration(env.initial_tenant, env.initial_timeline)

    # The migration is fully committed, no abort should have been performed.
    mconf = env.storage_controller.timeline_locate(env.initial_tenant, env.initial_timeline)
    assert mconf["generation"] == cur_gen
    assert mconf["sk_set"] == [another_sk]
    assert mconf["new_sk_set"] is None

    ep.safe_psql(f"INSERT INTO t VALUES ({cur_gen})")
    ep.clear_buffers()
    assert ep.safe_psql("SELECT * FROM t") == [(i + 1,) for i in range(cur_gen) if i % 2 == 0]
