from pathlib import Path
from typing import List, Tuple

from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.types import Lsn


# Helper for tests that compare timeline_inputs
# We don't want to compare the exact values, because they can be unstable
# and cause flaky tests. So replace the values with useful invariants.
def mask_model_inputs(x):
    if isinstance(x, dict):
        newx = {}
        for k, v in x.items():
            if k == "size":
                if v is None or v == 0:
                    # no change
                    newx[k] = v
                elif v < 0:
                    newx[k] = "<0"
                else:
                    newx[k] = ">0"
            elif k.endswith("lsn") or k.endswith("cutoff") or k == "last_record":
                if v is None or v == 0 or v == "0/0":
                    # no change
                    newx[k] = v
                else:
                    newx[k] = "masked"
            else:
                newx[k] = mask_model_inputs(v)
        return newx
    elif isinstance(x, list):
        newlist = [mask_model_inputs(v) for v in x]
        return newlist
    else:
        return x


def test_empty_tenant_size(neon_simple_env: NeonEnv, test_output_dir: Path):
    env = neon_simple_env
    (tenant_id, _) = env.neon_cli.create_tenant()
    http_client = env.pageserver.http_client()
    initial_size = http_client.tenant_size(tenant_id)

    # we should never have zero, because there should be the initdb "changes"
    assert initial_size > 0, "initial implementation returns ~initdb tenant_size"

    main_branch_name = "main"

    branch_name, main_timeline_id = env.neon_cli.list_timelines(tenant_id)[0]
    assert branch_name == main_branch_name

    with env.postgres.create_start(
        main_branch_name,
        tenant_id=tenant_id,
        config_lines=["autovacuum=off", "checkpoint_timeout=10min"],
    ) as pg:
        with pg.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1
        size = http_client.tenant_size(tenant_id)
        # we've disabled the autovacuum and checkpoint
        # so background processes should not change the size.
        # If this test will flake we should probably loosen the check
        assert size == initial_size, "starting idle compute should not change the tenant size"

    # the size should be the same, until we increase the size over the
    # gc_horizon
    size, inputs = http_client.tenant_size_and_modelinputs(tenant_id)
    assert size == initial_size, "tenant_size should not be affected by shutdown of compute"

    expected_inputs = {
        "segments": [
            {
                "segment": {"parent": None, "lsn": 23694408, "size": 25362432, "needed": True},
                "timeline_id": f"{main_timeline_id}",
                "kind": "BranchStart",
            },
            {
                "segment": {"parent": 0, "lsn": 23694528, "size": None, "needed": True},
                "timeline_id": f"{main_timeline_id}",
                "kind": "BranchEnd",
            },
        ],
        "timeline_inputs": [
            {
                "timeline_id": f"{main_timeline_id}",
                "ancestor_lsn": "0/0",
                "last_record": "0/1698CC0",
                "latest_gc_cutoff": "0/1698C48",
                "horizon_cutoff": "0/0",
                "pitr_cutoff": "0/0",
                "next_gc_cutoff": "0/0",
                "retention_param_cutoff": None,
            }
        ],
    }
    expected_inputs = mask_model_inputs(expected_inputs)
    actual_inputs = mask_model_inputs(inputs)

    assert expected_inputs == actual_inputs

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_branched_empty_timeline_size(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    Issue found in production. Because the ancestor branch was under
    gc_horizon, the branchpoint was "dangling" and the computation could not be
    done.

    Assuming gc_horizon = 50
    root:    I      0---10------>20
    branch:              |-------------------I---------->150
                                   gc_horizon
    """
    env = neon_simple_env
    (tenant_id, _) = env.neon_cli.create_tenant()
    http_client = env.pageserver.http_client()

    initial_size = http_client.tenant_size(tenant_id)

    first_branch_timeline_id = env.neon_cli.create_branch("first-branch", tenant_id=tenant_id)

    with env.postgres.create_start("first-branch", tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute(
                "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
        wait_for_last_flush_lsn(env, pg, tenant_id, first_branch_timeline_id)

    size_after_branching = http_client.tenant_size(tenant_id)
    log.info(f"size_after_branching: {size_after_branching}")

    assert size_after_branching > initial_size

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_branched_from_many_empty_parents_size(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    More general version of test_branched_empty_timeline_size

    Assuming gc_horizon = 50

    root:  I 0------10
    first: I        10
    nth_0: I        10
    nth_1: I        10
    nth_n:          10------------I--------100
    """
    env = neon_simple_env
    (tenant_id, _) = env.neon_cli.create_tenant()
    http_client = env.pageserver.http_client()

    initial_size = http_client.tenant_size(tenant_id)

    first_branch_name = "first"
    env.neon_cli.create_branch(first_branch_name, tenant_id=tenant_id)

    size_after_branching = http_client.tenant_size(tenant_id)

    # this might be flaky like test_get_tenant_size_with_multiple_branches
    # https://github.com/neondatabase/neon/issues/2962
    assert size_after_branching == initial_size

    last_branch_name = first_branch_name
    last_branch = None

    for i in range(0, 4):
        latest_branch_name = f"nth_{i}"
        last_branch = env.neon_cli.create_branch(
            latest_branch_name, ancestor_branch_name=last_branch_name, tenant_id=tenant_id
        )
        last_branch_name = latest_branch_name

        size_after_branching = http_client.tenant_size(tenant_id)
        assert size_after_branching == initial_size

    assert last_branch is not None

    with env.postgres.create_start(last_branch_name, tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute(
                "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
        wait_for_last_flush_lsn(env, pg, tenant_id, last_branch)

    size_after_writes = http_client.tenant_size(tenant_id)
    assert size_after_writes > initial_size

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


# @pytest.mark.skip("This should work, but is left out because assumed covered by other tests")
def test_branch_point_within_horizon(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    gc_horizon = 15

    main:          0--I-10------>20
    branch:              |-------------------I---------->150
                                   gc_horizon
    """

    env = neon_simple_env
    gc_horizon = 20_000
    (tenant_id, main_id) = env.neon_cli.create_tenant(conf={"gc_horizon": str(gc_horizon)})
    http_client = env.pageserver.http_client()

    with env.postgres.create_start("main", tenant_id=tenant_id) as pg:
        initdb_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, main_id)
        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000) s(i)")
        flushed_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, main_id)

    size_before_branching = http_client.tenant_size(tenant_id)

    assert flushed_lsn.lsn_int - gc_horizon > initdb_lsn.lsn_int

    branch_id = env.neon_cli.create_branch(
        "branch", tenant_id=tenant_id, ancestor_start_lsn=flushed_lsn
    )

    with env.postgres.create_start("branch", tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 1000) s(i)")
        wait_for_last_flush_lsn(env, pg, tenant_id, branch_id)

    size_after = http_client.tenant_size(tenant_id)

    assert size_before_branching < size_after

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


# @pytest.mark.skip("This should work, but is left out because assumed covered by other tests")
def test_parent_within_horizon(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    gc_horizon = 5

    main:          0----10----I->20
    branch:              |-------------------I---------->150
                                   gc_horizon
    """

    env = neon_simple_env
    gc_horizon = 200_000
    (tenant_id, main_id) = env.neon_cli.create_tenant(conf={"gc_horizon": str(gc_horizon)})
    http_client = env.pageserver.http_client()

    with env.postgres.create_start("main", tenant_id=tenant_id) as pg:
        initdb_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, main_id)
        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000) s(i)")

        flushed_lsn = wait_for_last_flush_lsn(env, pg, tenant_id, main_id)

        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t00 AS SELECT i::bigint n FROM generate_series(0, 2000) s(i)")

        wait_for_last_flush_lsn(env, pg, tenant_id, main_id)

    size_before_branching = http_client.tenant_size(tenant_id)

    assert flushed_lsn.lsn_int - gc_horizon > initdb_lsn.lsn_int

    branch_id = env.neon_cli.create_branch(
        "branch", tenant_id=tenant_id, ancestor_start_lsn=flushed_lsn
    )

    with env.postgres.create_start("branch", tenant_id=tenant_id) as pg:
        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 10000) s(i)")
        wait_for_last_flush_lsn(env, pg, tenant_id, branch_id)

    size_after = http_client.tenant_size(tenant_id)

    assert size_before_branching < size_after

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


# @pytest.mark.skip("This should work, but is left out because assumed covered by other tests")
def test_only_heads_within_horizon(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    gc_horizon = small

    main: 0--------10-----I>20
    first:         |-----------------------------I>150
    second:        |---------I>30
    """

    env = neon_simple_env
    (tenant_id, main_id) = env.neon_cli.create_tenant(conf={"gc_horizon": "1024"})
    http_client = env.pageserver.http_client()

    initial_size = http_client.tenant_size(tenant_id)

    first_id = env.neon_cli.create_branch("first", tenant_id=tenant_id)
    second_id = env.neon_cli.create_branch("second", tenant_id=tenant_id)

    ids = {"main": main_id, "first": first_id, "second": second_id}

    latest_size = None

    # gc is not expected to change the results

    for branch_name, amount in [("main", 2000), ("first", 15000), ("second", 3000)]:
        with env.postgres.create_start(branch_name, tenant_id=tenant_id) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, {amount}) s(i)"
                )
            wait_for_last_flush_lsn(env, pg, tenant_id, ids[branch_name])
            size_now = http_client.tenant_size(tenant_id)
            if latest_size is not None:
                assert size_now > latest_size
            else:
                assert size_now > initial_size

            latest_size = size_now

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_single_branch_get_tenant_size_grows(
    neon_env_builder: NeonEnvBuilder, test_output_dir: Path
):
    """
    Operate on single branch reading the tenants size after each transaction.
    """

    # Disable automatic gc and compaction.
    # The pitr_interval here is quite problematic, so we cannot really use it.
    # it'd have to be calibrated per test executing env.

    # there was a bug which was hidden if the create table and first batch of
    # inserts is larger than gc_horizon. for example 0x20000 here hid the fact
    # that there next_gc_cutoff could be smaller than initdb_lsn, which will
    # obviously lead to issues when calculating the size.
    gc_horizon = 0x30000
    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='0s', gc_period='0s', pitr_interval='0sec', gc_horizon={gc_horizon}}}"

    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    branch_name, timeline_id = env.neon_cli.list_timelines(tenant_id)[0]

    http_client = env.pageserver.http_client()

    collected_responses: List[Tuple[Lsn, int]] = []

    size_debug_file = open(test_output_dir / "size_debug.html", "w")

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

            size, sizes = http_client.tenant_size_and_modelinputs(tenant_id)

            size_debug = http_client.tenant_size_debug(tenant_id)
            size_debug_file.write(size_debug)

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

            size, sizes = http_client.tenant_size_and_modelinputs(tenant_id)

            size_debug = http_client.tenant_size_debug(tenant_id)
            size_debug_file.write(size_debug)

            prev = collected_responses[-1][1]
            assert size > prev, f"tenant_size should grow with updates {size} {prev}"
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

    size_debug_file.close()

    size_after = http_client.tenant_size(tenant_id)
    prev = collected_responses[-1][1]

    assert size_after == prev, "size after restarting pageserver should not have changed"

    ps_metrics = parse_metrics(http_client.get_metrics(), "pageserver")
    tenant_metric_filter = {
        "tenant_id": str(tenant_id),
    }

    tenant_size_metric = int(
        ps_metrics.query_one("pageserver_tenant_synthetic_size", filter=tenant_metric_filter).value
    )

    assert tenant_size_metric == size_after, "API size value should be equal to metric size value"


def test_get_tenant_size_with_multiple_branches(
    neon_env_builder: NeonEnvBuilder, test_output_dir: Path
):
    """
    Reported size goes up while branches or rows are being added, goes down after removing branches.
    """

    gc_horizon = 128 * 1024

    neon_env_builder.pageserver_config_override = f"tenant_config={{compaction_period='0s', gc_period='0s', pitr_interval='0sec', gc_horizon={gc_horizon}}}"

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

    size_after_first_branch = http_client.tenant_size(tenant_id)
    assert size_after_first_branch == size_at_branch

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
    assert size_after_second_branch == size_after_continuing_on_main

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

    size_debug_file_before = open(test_output_dir / "size_debug_before.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file_before.write(size_debug)

    # teardown, delete branches, and the size should be going down
    http_client.timeline_delete(tenant_id, first_branch_timeline_id)

    size_after_deleting_first = http_client.tenant_size(tenant_id)
    assert size_after_deleting_first < size_after_thinning_branch

    http_client.timeline_delete(tenant_id, second_branch_timeline_id)
    size_after_deleting_second = http_client.tenant_size(tenant_id)
    assert size_after_deleting_second < size_after_deleting_first

    assert size_after_deleting_second < size_after_continuing_on_main
    assert size_after_deleting_second > size_after_first_branch

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)
