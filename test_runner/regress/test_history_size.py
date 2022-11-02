import os
import time
from pathlib import Path
from typing import Any, List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PageserverApiException,
    PageserverHttpClient,
    wait_for_last_flush_lsn,
)
from fixtures.types import Lsn, TenantId


def test_empty_tenant_history_size(neon_simple_env: NeonEnv):
    env = neon_simple_env
    (tenant_id, _) = env.neon_cli.create_tenant()
    http_client = env.pageserver.http_client()
    size = history_size(http_client, tenant_id)[0]
    # we should never have zero, because there should be the initdb however
    # this is questionable if we should have anything in this case, as the
    # gc_cutoff is negative
    assert size == 0

    with env.postgres.create_start("main", tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1
        # running compute should not change the size
        size = history_size(http_client, tenant_id)[0]
        assert size == 0

    # after compute has been shut down, the size should be the same, until we
    # increase the size over the gc_horizon
    size = history_size(http_client, tenant_id)[0]
    assert size == 0


def history_size(http_client: PageserverHttpClient, tenant_id: TenantId) -> Tuple[int, Any]:
    res = http_client.get(f"http://localhost:{http_client.port}/v1/tenant/{tenant_id}/history_size")
    http_client.verbose_error(res)
    res = res.json()
    assert isinstance(res, dict)
    assert TenantId(res["id"]) == tenant_id
    history_size = res["history_size"]
    assert type(history_size) == int
    return (history_size, res.get("inputs", None))


def test_single_branch_history_size_grows(neon_env_builder: NeonEnvBuilder):
    """
    Operate on single branch reading the tenants history size after each transaction.
    """

    # gc and compaction is not wanted automatically
    # the pitr_interval here is quite problematic, so we cannot really use it.
    # it'd have to be calibrated per test executing env.
    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='1h', gc_period='1h', pitr_interval='0sec', gc_horizon={128 * 1024}}}"
    # in this test we don't run gc or compaction, but the history_size is
    # expected to use the "next gc" cutoff, so the small amounts should work

    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    branch_name, timeline_id = env.neon_cli.list_timelines(tenant_id)[0]

    http_client = env.pageserver.http_client()

    collected_responses: List[Tuple[Lsn, Any]] = []

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

            size, _ = history_size(http_client, tenant_id)

            if len(collected_responses) > 0:
                last_size = collected_responses[-1][1]
                if size == 0:
                    assert last_size == 0
                else:
                    assert size > last_size

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

            size, _ = history_size(http_client, tenant_id)
            assert size > collected_responses[-1][1]
            collected_responses.append((current_lsn, size))

        while True:
            with pg.cursor() as cur:
                cur.execute(f"DELETE FROM t0 WHERE i IN (SELECT i FROM t0 LIMIT {batch_size})")
                deleted = cur.rowcount

            if deleted == 0:
                break

            current_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

            size, _ = history_size(http_client, tenant_id)
            # even though rows have been deleted, the size should not decrease
            assert size > collected_responses[-1][1]
            collected_responses.append((current_lsn, size))

        with pg.cursor() as cur:
            cur.execute("DROP TABLE t0")

        current_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

        size, _ = history_size(http_client, tenant_id)

        assert size > collected_responses[-1][1]
        collected_responses.append((current_lsn, size))

    for lsn, size in collected_responses:
        log.info(f"collected: {lsn}, {size}")

    env.pageserver.stop()
    env.pageserver.start()

    size_after = history_size(http_client, tenant_id)[0]

    assert (
        size_after == collected_responses[-1][1]
    ), "size after restarting pageserver should not have changed"


def test_history_size_with_multiple_branches(neon_env_builder: NeonEnvBuilder):
    """
    Reported size goes up while branches or rows are being added, goes down after removing branches.
    """

    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='1h', gc_period='1h', pitr_interval='0sec', gc_horizon={128 * 1024}}}"

    env = neon_env_builder.init_start()

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
    size_at_branch = history_size(http_client, tenant_id)[0]
    assert size_at_branch > 0

    first_branch_timeline_id = env.neon_cli.create_branch(
        "first-branch", main_branch_name, tenant_id
    )

    # unsure why this happens
    size_after_branch = history_size(http_client, tenant_id)[0]
    assert size_after_branch > size_at_branch

    first_branch_pg = env.postgres.create_start("first-branch", tenant_id=tenant_id)

    with first_branch_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, {batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, first_branch_pg, tenant_id, first_branch_timeline_id)
    size_after_growing_branch = history_size(http_client, tenant_id)[0]
    assert size_after_growing_branch > size_after_branch

    with main_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 2*{batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, main_pg, tenant_id, main_timeline_id)
    size_after_continuing_on_main = history_size(http_client, tenant_id)[0]
    assert size_after_continuing_on_main > size_after_growing_branch

    second_branch_timeline_id = env.neon_cli.create_branch(
        "second-branch", main_branch_name, tenant_id
    )
    size_after_branch = history_size(http_client, tenant_id)[0]
    assert size_after_branch > size_after_continuing_on_main

    second_branch_pg = env.postgres.create_start("second-branch", tenant_id=tenant_id)

    with second_branch_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t2 AS SELECT i::bigint n FROM generate_series(0, 3*{batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, second_branch_pg, tenant_id, second_branch_timeline_id)
    size_after_growing_branch = history_size(http_client, tenant_id)[0]
    assert size_after_growing_branch > size_after_branch

    with second_branch_pg.cursor() as cur:
        cur.execute("DROP TABLE t0")
        cur.execute("DROP TABLE t1")
        cur.execute("VACUUM FULL")

    wait_for_last_flush_lsn(env, second_branch_pg, tenant_id, second_branch_timeline_id)
    size_after_thinning_branch = history_size(http_client, tenant_id)[0]
    # size doesn't yet go down
    assert size_after_thinning_branch > size_after_growing_branch

    first_branch_pg.stop_and_destroy()
    second_branch_pg.stop_and_destroy()
    main_pg.stop()
    env.pageserver.stop()
    env.pageserver.start()
    size_after = history_size(http_client, tenant_id)[0]

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
                time.sleep(1)
                continue
            raise

    assert deleted

    size_after_deleting_first = history_size(http_client, tenant_id)[0]
    assert size_after_deleting_first < size_after_thinning_branch

    http_client.timeline_delete(tenant_id, second_branch_timeline_id)
    size_after_deleting_second = history_size(http_client, tenant_id)[0]
    assert size_after_deleting_second < size_after_deleting_first


def test_history_size_with_broken_timeline(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='1h', gc_period='1h', pitr_interval='0sec', gc_horizon={128 * 1024}}}"
    env = neon_env_builder.init_start()
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
    size_at_branch = history_size(http_client, tenant_id)[0]
    assert size_at_branch > 0

    first_branch_timeline_id = env.neon_cli.create_branch(
        "first-branch", main_branch_name, tenant_id
    )
    size_after_branch = history_size(http_client, tenant_id)[0]
    assert size_after_branch > size_at_branch

    first_branch_pg = env.postgres.create_start("first-branch", tenant_id=tenant_id)

    with first_branch_pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, {batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, first_branch_pg, tenant_id, first_branch_timeline_id)
    size_after_growing_branch = history_size(http_client, tenant_id)[0]
    assert size_after_growing_branch > size_at_branch

    first_branch_pg.stop()
    main_pg.stop()

    http_client.timeline_checkpoint(tenant_id, first_branch_timeline_id)

    env.pageserver.stop()

    layer_removed = False
    for path in Path.iterdir(env.timeline_dir(tenant_id, first_branch_timeline_id)):
        log.info(f"iterating timeline dir: {path.name}")
        if not layer_removed and path.name.startswith("00000"):
            os.remove(path)
            layer_removed = True
            break
    assert layer_removed

    env.pageserver.start()
    main_pg.start()
    first_branch_pg.start()

    # was expecting that this timeline would be broken
    with first_branch_pg.cursor() as cur:
        with pytest.raises(Exception, match="""relation "t1" does not exist"""):
            cur.execute("SELECT count(1) FROM t1")

    size_after_breaking_timeline = history_size(http_client, tenant_id)[0]
    assert size_after_breaking_timeline == size_after_growing_branch
