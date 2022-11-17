import time
from typing import List, Tuple

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PageserverApiException,
    wait_for_last_flush_lsn,
)
from fixtures.types import Lsn


def test_empty_tenant_size(neon_simple_env: NeonEnv):
    env = neon_simple_env
    (tenant_id, _) = env.neon_cli.create_tenant()
    http_client = env.pageserver.http_client()
    size = http_client.tenant_size(tenant_id)

    # we should never have zero, because there should be the initdb however
    # this is questionable if we should have anything in this case, as the
    # gc_cutoff is negative
    assert (
        size == 0
    ), "initial implementation returns zero tenant_size before last_record_lsn is past gc_horizon"

    with env.postgres.create_start("main", tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1
        size = http_client.tenant_size(tenant_id)
        assert size == 0, "starting idle compute should not change the tenant size"

    # the size should be the same, until we increase the size over the
    # gc_horizon
    size = http_client.tenant_size(tenant_id)
    assert size == 0, "tenant_size should not be affected by shutdown of compute"


def test_single_branch_get_tenant_size_grows(neon_env_builder: NeonEnvBuilder):
    """
    Operate on single branch reading the tenants size after each transaction.
    """

    # gc and compaction is not wanted automatically
    # the pitr_interval here is quite problematic, so we cannot really use it.
    # it'd have to be calibrated per test executing env.

    # there was a bug which was hidden if the create table and first batch of
    # inserts is larger than gc_horizon. for example 0x20000 here hid the fact
    # that there next_gc_cutoff could be smaller than initdb_lsn, which will
    # obviously lead to issues when calculating the size.
    gc_horizon = 0x30000
    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='1h', gc_period='1h', pitr_interval='0sec', gc_horizon={gc_horizon}}}"

    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    branch_name, timeline_id = env.neon_cli.list_timelines(tenant_id)[0]

    http_client = env.pageserver.http_client()

    collected_responses: List[Tuple[Lsn, int]] = []

    with env.postgres.create_start(branch_name, tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t0 (i BIGINT NOT NULL)")

        batch_size = 100

        i = 0
        while True:
            with pg.cursor() as cur:
                cur.execute(
                    f"INSERT INTO t0(i) SELECT i FROM generate_series({batch_size} * %s, ({batch_size} * (%s + 1)) - 1) s(i)",
                    (i, i),
                )

            i += 1

            current_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

            size = http_client.tenant_size(tenant_id)

            if len(collected_responses) > 0:
                prev = collected_responses[-1][1]
                if size == 0:
                    assert prev == 0
                else:
                    assert size > prev

            collected_responses.append((current_lsn, size))

            if len(collected_responses) > 2:
                break

        while True:
            with pg.cursor() as cur:
                cur.execute(
                    f"UPDATE t0 SET i = -i WHERE i IN (SELECT i FROM t0 WHERE i > 0 LIMIT {batch_size})"
                )
                updated = cur.rowcount

            if updated == 0:
                break

            current_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

            size = http_client.tenant_size(tenant_id)
            prev = collected_responses[-1][1]
            assert size > prev, "tenant_size should grow with updates"
            collected_responses.append((current_lsn, size))

        while True:
            with pg.cursor() as cur:
                cur.execute(f"DELETE FROM t0 WHERE i IN (SELECT i FROM t0 LIMIT {batch_size})")
                deleted = cur.rowcount

            if deleted == 0:
                break

            current_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

            size = http_client.tenant_size(tenant_id)
            prev = collected_responses[-1][1]
            assert (
                size > prev
            ), "even though rows have been deleted, the tenant_size should increase"
            collected_responses.append((current_lsn, size))

        with pg.cursor() as cur:
            cur.execute("DROP TABLE t0")

        current_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

        size = http_client.tenant_size(tenant_id)
        prev = collected_responses[-1][1]
        assert size > prev, "dropping table grows tenant_size"
        collected_responses.append((current_lsn, size))

    # this isn't too many lines to forget for a while. observed while
    # developing these tests that locally the value is a bit more than what we
    # get in the ci.
    for lsn, size in collected_responses:
        log.info(f"collected: {lsn}, {size}")

    env.pageserver.stop()
    env.pageserver.start()

    size_after = http_client.tenant_size(tenant_id)
    prev = collected_responses[-1][1]

    assert size_after == prev, "size after restarting pageserver should not have changed"


def test_get_tenant_size_with_multiple_branches(neon_env_builder: NeonEnvBuilder):
    """
    Reported size goes up while branches or rows are being added, goes down after removing branches.
    """

    gc_horizon = 128 * 1024

    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='1h', gc_period='1h', pitr_interval='0sec', gc_horizon={gc_horizon}}}"

    env = neon_env_builder.init_start()

    # FIXME: we have a race condition between GC and delete timeline. GC might fail with this
    # error. Similar to https://github.com/neondatabase/neon/issues/2671
    env.pageserver.allowed_errors.append(".*InternalServerError\\(No such file or directory.*")

    tenant_id = env.initial_tenant
    main_branch_name, main_timeline_id = env.neon_cli.list_timelines(tenant_id)[0]

    http_client = env.pageserver.http_client()

    main_pg = env.postgres.create_start(main_branch_name, tenant_id=tenant_id)

    batch_size = 10000

    with main_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, {batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, main_pg, tenant_id, main_timeline_id)
    size_at_branch = http_client.tenant_size(tenant_id)
    assert size_at_branch > 0

    first_branch_timeline_id = env.neon_cli.create_branch(
        "first-branch", main_branch_name, tenant_id
    )

    # unsure why this happens, the size difference is more than a page alignment
    size_after_first_branch = http_client.tenant_size(tenant_id)
    assert size_after_first_branch > size_at_branch
    assert size_after_first_branch - size_at_branch == gc_horizon

    first_branch_pg = env.postgres.create_start("first-branch", tenant_id=tenant_id)

    with first_branch_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, {batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, first_branch_pg, tenant_id, first_branch_timeline_id)
    size_after_growing_first_branch = http_client.tenant_size(tenant_id)
    assert size_after_growing_first_branch > size_after_first_branch

    with main_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 2*{batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, main_pg, tenant_id, main_timeline_id)
    size_after_continuing_on_main = http_client.tenant_size(tenant_id)
    assert size_after_continuing_on_main > size_after_growing_first_branch

    second_branch_timeline_id = env.neon_cli.create_branch(
        "second-branch", main_branch_name, tenant_id
    )
    size_after_second_branch = http_client.tenant_size(tenant_id)
    assert size_after_second_branch > size_after_continuing_on_main

    second_branch_pg = env.postgres.create_start("second-branch", tenant_id=tenant_id)

    with second_branch_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t2 AS SELECT i::bigint n FROM generate_series(0, 3*{batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, second_branch_pg, tenant_id, second_branch_timeline_id)
    size_after_growing_second_branch = http_client.tenant_size(tenant_id)
    assert size_after_growing_second_branch > size_after_second_branch

    with second_branch_pg.cursor() as cur:
        cur.execute("DROP TABLE t0")
        cur.execute("DROP TABLE t1")
        cur.execute("VACUUM FULL")

    wait_for_last_flush_lsn(env, second_branch_pg, tenant_id, second_branch_timeline_id)
    size_after_thinning_branch = http_client.tenant_size(tenant_id)
    assert (
        size_after_thinning_branch > size_after_growing_second_branch
    ), "tenant_size should grow with dropped tables and full vacuum"

    first_branch_pg.stop_and_destroy()
    second_branch_pg.stop_and_destroy()
    main_pg.stop()
    env.pageserver.stop()
    env.pageserver.start()

    # chance of compaction and gc on startup might have an effect on the
    # tenant_size but so far this has been reliable, even though at least gc
    # and tenant_size race for the same locks
    size_after = http_client.tenant_size(tenant_id)
    assert size_after == size_after_thinning_branch

    # teardown, delete branches, and the size should be going down
    deleted = False
    for _ in range(10):
        try:
            http_client.timeline_delete(tenant_id, first_branch_timeline_id)
            deleted = True
            break
        except PageserverApiException as e:
            # compaction is ok but just retry if this fails; related to #2442
            if "cannot lock compaction critical section" in str(e):
                # also ignore it in the log
                env.pageserver.allowed_errors.append(".*cannot lock compaction critical section.*")
                time.sleep(1)
                continue
            raise

    assert deleted

    size_after_deleting_first = http_client.tenant_size(tenant_id)
    assert size_after_deleting_first < size_after_thinning_branch

    http_client.timeline_delete(tenant_id, second_branch_timeline_id)
    size_after_deleting_second = http_client.tenant_size(tenant_id)
    assert size_after_deleting_second < size_after_deleting_first

    assert size_after_deleting_second < size_after_continuing_on_main
    assert size_after_deleting_second > size_after_first_branch
