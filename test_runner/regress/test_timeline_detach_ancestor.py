import datetime
import enum
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from threading import Barrier
from typing import List, Tuple

import pytest
from fixtures.common_types import Lsn, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    LogCursor,
    NeonEnvBuilder,
    PgBin,
    flush_ep_to_pageserver,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import HistoricLayerInfo, PageserverApiException
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_timeline_detail_404
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.utils import assert_pageserver_backups_equal, wait_until
from requests import ReadTimeout


def by_end_lsn(info: HistoricLayerInfo) -> Lsn:
    assert info.lsn_end is not None
    return Lsn(info.lsn_end)


def layer_name(info: HistoricLayerInfo) -> str:
    return info.layer_file_name


@enum.unique
class Branchpoint(str, enum.Enum):
    """
    Have branches at these Lsns possibly relative to L0 layer boundary.
    """

    EARLIER = "earlier"
    AT_L0 = "at"
    AFTER_L0 = "after"
    LAST_RECORD_LSN = "head"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def all() -> List["Branchpoint"]:
        return [
            Branchpoint.EARLIER,
            Branchpoint.AT_L0,
            Branchpoint.AFTER_L0,
            Branchpoint.LAST_RECORD_LSN,
        ]


SHUTDOWN_ALLOWED_ERRORS = [
    ".*initial size calculation failed: downloading failed, possibly for shutdown",
    ".*failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
    ".*logical_size_calculation_task:panic.*: Sequential get failed with Bad state \\(not active\\).*",
    ".*Task 'initial size calculation' .* panicked.*",
]


@pytest.mark.parametrize("branchpoint", Branchpoint.all())
@pytest.mark.parametrize("restart_after", [True, False])
@pytest.mark.parametrize("write_to_branch_first", [True, False])
def test_ancestor_detach_branched_from(
    test_output_dir,
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    branchpoint: Branchpoint,
    restart_after: bool,
    write_to_branch_first: bool,
):
    """
    Creates a branch relative to L0 lsn boundary according to Branchpoint. Later the timeline is detached.
    """
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT);")

        after_first_tx = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        # create a single layer for us to remote copy
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    deltas = client.layer_map_info(env.initial_tenant, env.initial_timeline).delta_layers()
    # there is also the in-mem layer, but ignore it for now
    assert len(deltas) == 2, "expecting there to be two deltas: initdb and checkpointed"
    later_delta = max(deltas, key=by_end_lsn)
    assert later_delta.lsn_end is not None

    # -1 as the lsn_end is exclusive.
    last_lsn = Lsn(later_delta.lsn_end).lsn_int - 1

    if branchpoint == Branchpoint.EARLIER:
        branch_at = after_first_tx
        rows = 0
        truncated_layers = 1
    elif branchpoint == Branchpoint.AT_L0:
        branch_at = Lsn(last_lsn)
        rows = 8192
        truncated_layers = 0
    elif branchpoint == Branchpoint.AFTER_L0:
        branch_at = Lsn(last_lsn + 8)
        rows = 8192
        # as there is no 8 byte walrecord, nothing should get copied from the straddling layer
        truncated_layers = 0
    else:
        # this case also covers the implicit flush of ancestor as the inmemory hasn't been flushed yet
        assert branchpoint == Branchpoint.LAST_RECORD_LSN
        branch_at = None
        rows = 16384
        truncated_layers = 0

    name = "new main"

    timeline_id = env.neon_cli.create_branch(
        name, "main", env.initial_tenant, ancestor_start_lsn=branch_at
    )

    recorded = Lsn(client.timeline_detail(env.initial_tenant, timeline_id)["ancestor_lsn"])
    if branch_at is None:
        # fix it up if we need it later (currently unused)
        branch_at = recorded
    else:
        assert branch_at == recorded, "the test should not use unaligned lsns"

    if write_to_branch_first:
        with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
            # make sure the ep is writable
            # with BEFORE_L0, AFTER_L0 there will be a gap in Lsns caused by accurate end_lsn on straddling layers
            ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)

        # branch must have a flush for "PREV_LSN: none"
        client.timeline_checkpoint(env.initial_tenant, timeline_id)
        branch_layers = set(
            map(layer_name, client.layer_map_info(env.initial_tenant, timeline_id).historic_layers)
        )
    else:
        branch_layers = set()

    # run fullbackup to make sure there are no off by one errors
    # take this on the parent
    fullbackup_before = test_output_dir / "fullbackup-before.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, env.initial_timeline, branch_at, fullbackup_before
    )

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert all_reparented == []

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    old_main_info = client.layer_map_info(env.initial_tenant, env.initial_timeline)
    old_main = set(map(layer_name, old_main_info.historic_layers))

    new_main_info = client.layer_map_info(env.initial_tenant, timeline_id)
    new_main = set(map(layer_name, new_main_info.historic_layers))

    new_main_copied_or_truncated = new_main - branch_layers
    new_main_truncated = new_main_copied_or_truncated - old_main

    assert len(new_main_truncated) == truncated_layers
    # could additionally check that the symmetric difference has layers starting at the same lsn
    # but if nothing was copied, then there is no nice rule.
    # there could be a hole in LSNs between copied from the "old main" and the first branch layer.

    # take this on the detached, at same lsn
    fullbackup_after = test_output_dir / "fullbackup-after.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, timeline_id, branch_at, fullbackup_after
    )

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)

    # because we do the fullbackup from ancestor at the branch_lsn, the zenith.signal is always different
    # as there is always "PREV_LSN: invalid" for "before"
    skip_files = {"zenith.signal"}

    assert_pageserver_backups_equal(fullbackup_before, fullbackup_after, skip_files)


def test_ancestor_detach_reparents_earlier(neon_env_builder: NeonEnvBuilder):
    """
    The case from RFC:

                              +-> another branch with same ancestor_lsn as new main
                              |
    old main -------|---------X--------->
                    |         |         |
                    |         |         +-> after
                    |         |
                    |         +-> new main
                    |
                    +-> reparented

    Ends up as:

    old main --------------------------->
                                        |
                                        +-> after

                              +-> another branch with same ancestor_lsn as new main
                              |
    new main -------|---------|->
                    |
                    +-> reparented

    We confirm the end result by being able to delete "old main" after deleting "after".
    """

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT);")
        ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")

        branchpoint_pipe = wait_for_last_flush_lsn(
            env, ep, env.initial_tenant, env.initial_timeline
        )

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        branchpoint_x = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    # as this only gets reparented, we don't need to write to it like new main
    reparented = env.neon_cli.create_branch(
        "reparented", "main", env.initial_tenant, ancestor_start_lsn=branchpoint_pipe
    )

    same_branchpoint = env.neon_cli.create_branch(
        "same_branchpoint", "main", env.initial_tenant, ancestor_start_lsn=branchpoint_x
    )

    timeline_id = env.neon_cli.create_branch(
        "new main", "main", env.initial_tenant, ancestor_start_lsn=branchpoint_x
    )

    after = env.neon_cli.create_branch("after", "main", env.initial_tenant, ancestor_start_lsn=None)

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert set(all_reparented) == {reparented, same_branchpoint}

    env.pageserver.quiesce_tenants()

    # checking the ancestor after is much faster than waiting for the endpoint not start
    expected_result = [
        ("main", env.initial_timeline, None, 16384, 1),
        ("after", after, env.initial_timeline, 16384, 1),
        ("new main", timeline_id, None, 8192, 1),
        ("same_branchpoint", same_branchpoint, timeline_id, 8192, 1),
        ("reparented", reparented, timeline_id, 0, 1),
    ]

    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    for _, queried_timeline, expected_ancestor, _, _ in expected_result:
        details = client.timeline_detail(env.initial_tenant, queried_timeline)
        ancestor_timeline_id = details["ancestor_timeline_id"]
        if expected_ancestor is None:
            assert ancestor_timeline_id is None
        else:
            assert TimelineId(ancestor_timeline_id) == expected_ancestor

        index_part = env.pageserver_remote_storage.index_content(
            env.initial_tenant, queried_timeline
        )
        lineage = index_part["lineage"]
        assert lineage is not None

        assert lineage.get("reparenting_history_overflown", "false") == "false"

        if queried_timeline == timeline_id:
            original_ancestor = lineage["original_ancestor"]
            assert original_ancestor is not None
            assert original_ancestor[0] == str(env.initial_timeline)
            assert original_ancestor[1] == str(branchpoint_x)

            # this does not contain Z in the end, so fromisoformat accepts it
            # it is to be in line with the deletion timestamp.. well, almost.
            when = original_ancestor[2][:26]
            when_ts = datetime.datetime.fromisoformat(when)
            assert when_ts < datetime.datetime.now()
            assert len(lineage.get("reparenting_history", [])) == 0
        elif expected_ancestor == timeline_id:
            assert len(lineage.get("original_ancestor", [])) == 0
            assert lineage["reparenting_history"] == [str(env.initial_timeline)]
        else:
            assert len(lineage.get("original_ancestor", [])) == 0
            assert len(lineage.get("reparenting_history", [])) == 0

    for name, _, _, rows, starts in expected_result:
        with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
            assert ep.safe_psql(f"SELECT count(*) FROM audit WHERE starts = {starts}")[0][0] == 1

    # delete the timelines to confirm detach actually worked
    client.timeline_delete(env.initial_tenant, after)
    wait_timeline_detail_404(client, env.initial_tenant, after, 10, 1.0)

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)


def test_detached_receives_flushes_while_being_detached(neon_env_builder: NeonEnvBuilder):
    """
    Makes sure that the timeline is able to receive writes through-out the detach process.
    """

    env = neon_env_builder.init_start()

    client = env.pageserver.http_client()

    # row counts have been manually verified to cause reconnections and getpage
    # requests when restart_after=False with pg16
    def insert_rows(n: int, ep) -> int:
        ep.safe_psql(
            f"INSERT INTO foo SELECT i::bigint, 'more info!! this is a long string' || i FROM generate_series(0, {n - 1}) g(i);"
        )
        return n

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE EXTENSION neon_test_utils;")
        ep.safe_psql("CREATE TABLE foo (i BIGINT, aux TEXT NOT NULL);")

        rows = insert_rows(256, ep)

        branchpoint = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    timeline_id = env.neon_cli.create_branch(
        "new main", "main", tenant_id=env.initial_tenant, ancestor_start_lsn=branchpoint
    )

    log.info("starting the new main endpoint")
    ep = env.endpoints.create_start("new main", tenant_id=env.initial_tenant)
    assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    def small_txs(ep, queue: Queue[str], barrier):
        extra_rows = 0

        with ep.connect() as conn:
            while True:
                try:
                    queue.get_nowait()
                    break
                except Empty:
                    pass

                if barrier is not None:
                    barrier.wait()
                    barrier = None

                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO foo(i, aux) VALUES (1, 'more info!! this is a long string' || 1);"
                )
                extra_rows += 1
        return extra_rows

    with ThreadPoolExecutor(max_workers=1) as exec:
        queue: Queue[str] = Queue()
        barrier = Barrier(2)

        completion = exec.submit(small_txs, ep, queue, barrier)
        barrier.wait()

        reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
        assert len(reparented) == 0

        env.pageserver.quiesce_tenants()

        queue.put("done")
        extra_rows = completion.result()
        assert extra_rows > 0, "some rows should had been written"
        rows += extra_rows

    assert client.timeline_detail(env.initial_tenant, timeline_id)["ancestor_timeline_id"] is None

    assert ep.safe_psql("SELECT clear_buffer_cache();")
    assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
    assert ep.safe_psql("SELECT SUM(LENGTH(aux)) FROM foo")[0][0] != 0
    ep.stop()

    # finally restart the endpoint and make sure we still have the same answer
    with env.endpoints.create_start("new main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)


def test_compaction_induced_by_detaches_in_history(
    neon_env_builder: NeonEnvBuilder, test_output_dir, pg_bin: PgBin
):
    """
    Assuming the tree of timelines:

    root
    |- child1
       |- ...
          |- wanted_detached_child

    Each detach can add N more L0 per level, this is actually unbounded because
    compaction can be arbitrarily delayed (or detach happen right before one
    starts). If "wanted_detached_child" has already made progress and compacted
    L1s, we want to make sure "compaction in the history" does not leave the
    timeline broken.
    """

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # we want to create layers manually so we don't branch on arbitrary
            # Lsn, but we also do not want to compact L0 -> L1.
            "compaction_threshold": "99999",
            "compaction_period": "0s",
            # shouldn't matter, but just in case
            "gc_period": "0s",
        }
    )
    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)
    client = env.pageserver.http_client()

    def delta_layers(timeline_id: TimelineId):
        # shorthand for more readable formatting
        return client.layer_map_info(env.initial_tenant, timeline_id).delta_layers()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("create table integers (i bigint not null);")
        ep.safe_psql("insert into integers (i) values (42)")
        branch_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        assert len(delta_layers(env.initial_timeline)) == 2

    more_good_numbers = range(0, 3)

    branches: List[Tuple[str, TimelineId]] = [("main", env.initial_timeline)]

    for num in more_good_numbers:
        branch_name = f"br-{len(branches)}"
        branch_timeline_id = env.neon_cli.create_branch(
            branch_name,
            ancestor_branch_name=branches[-1][0],
            tenant_id=env.initial_tenant,
            ancestor_start_lsn=branch_lsn,
        )
        branches.append((branch_name, branch_timeline_id))

        with env.endpoints.create_start(branches[-1][0], tenant_id=env.initial_tenant) as ep:
            ep.safe_psql(
                f"insert into integers (i) select i from generate_series({num}, {num + 100}) as s(i)"
            )
            branch_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, branch_timeline_id)
            client.timeline_checkpoint(env.initial_tenant, branch_timeline_id)

        assert len(delta_layers(branch_timeline_id)) == 1

    # now fill in the final, most growing timeline

    branch_name, branch_timeline_id = branches[-1]
    with env.endpoints.create_start(branch_name, tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("insert into integers (i) select i from generate_series(50, 500) s(i)")

        last_suffix = None
        for suffix in range(0, 4):
            ep.safe_psql(f"create table other_table_{suffix} as select * from integers")
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, branch_timeline_id)
            client.timeline_checkpoint(env.initial_tenant, branch_timeline_id)
            last_suffix = suffix

        assert last_suffix is not None

        assert len(delta_layers(branch_timeline_id)) == 5

        client.patch_tenant_config_client_side(
            env.initial_tenant, {"compaction_threshold": 5}, None
        )

        client.timeline_compact(env.initial_tenant, branch_timeline_id)

        # one more layer
        ep.safe_psql(f"create table other_table_{last_suffix + 1} as select * from integers")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, branch_timeline_id)

        # we need to wait here, because the detaches will do implicit tenant restart,
        # and we could get unexpected layer counts
        client.timeline_checkpoint(env.initial_tenant, branch_timeline_id, wait_until_uploaded=True)

    assert len([filter(lambda x: x.l0, delta_layers(branch_timeline_id))]) == 1

    skip_main = branches[1:]
    branch_lsn = client.timeline_detail(env.initial_tenant, branch_timeline_id)["ancestor_lsn"]

    # take the fullbackup before and after inheriting the new L0s
    fullbackup_before = test_output_dir / "fullbackup-before.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, branch_timeline_id, branch_lsn, fullbackup_before
    )

    for _, timeline_id in skip_main:
        reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
        assert reparented == [], "we have no earlier branches at any level"

    post_detach_l0s = list(filter(lambda x: x.l0, delta_layers(branch_timeline_id)))
    assert len(post_detach_l0s) == 5, "should had inherited 4 L0s, have 5 in total"

    # checkpoint does compaction, which in turn decides to run, because
    # there is now in total threshold number L0s even if they are not
    # adjacent in Lsn space:
    #
    # inherited  flushed during this checkpoint
    #       \\\\ /
    #       1234X5---> lsn
    #           |
    #       l1 layers from "fill in the final, most growing timeline"
    #
    # branch_lsn is between 4 and first X.
    client.timeline_checkpoint(env.initial_tenant, branch_timeline_id)

    post_compact_l0s = list(filter(lambda x: x.l0, delta_layers(branch_timeline_id)))
    assert len(post_compact_l0s) == 1, "only the consecutive inherited L0s should be compacted"

    fullbackup_after = test_output_dir / "fullbackup_after.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, branch_timeline_id, branch_lsn, fullbackup_after
    )

    # we don't need to skip any files, because zenith.signal will be identical
    assert_pageserver_backups_equal(fullbackup_before, fullbackup_after, set())


@pytest.mark.parametrize("sharded", [True, False])
def test_timeline_ancestor_detach_idempotent_success(
    neon_env_builder: NeonEnvBuilder, sharded: bool
):
    shards = 2 if sharded else 1

    neon_env_builder.num_pageservers = shards
    env = neon_env_builder.init_start(initial_tenant_shard_count=shards if sharded else None)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    for ps in pageservers.values():
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    if sharded:
        # FIXME: should this be in the neon_env_builder.init_start?
        env.storage_controller.reconcile_until_idle()
        client = env.storage_controller.pageserver_api()
    else:
        client = env.pageserver.http_client()

    first_branch = env.neon_cli.create_branch("first_branch")

    _ = env.neon_cli.create_branch("second_branch", ancestor_branch_name="first_branch")

    # these two will be reparented, and they should be returned in stable order
    # from pageservers OR otherwise there will be an `error!` logging from
    # storage controller
    reparented1 = env.neon_cli.create_branch("first_reparented", ancestor_branch_name="main")
    reparented2 = env.neon_cli.create_branch("second_reparented", ancestor_branch_name="main")

    first_reparenting_response = client.detach_ancestor(env.initial_tenant, first_branch)
    assert set(first_reparenting_response) == {reparented1, reparented2}

    # FIXME: this should be done by the http req handler
    for ps in pageservers.values():
        ps.quiesce_tenants()

    for _ in range(5):
        # once completed, we can retry this how many times
        assert (
            client.detach_ancestor(env.initial_tenant, first_branch) == first_reparenting_response
        )

    client.tenant_delete(env.initial_tenant)

    with pytest.raises(PageserverApiException) as e:
        client.detach_ancestor(env.initial_tenant, first_branch)
    assert e.value.status_code == 404


@pytest.mark.parametrize("sharded", [True, False])
def test_timeline_ancestor_detach_errors(neon_env_builder: NeonEnvBuilder, sharded: bool):
    # the test is split from test_timeline_ancestor_detach_idempotent_success as only these error cases should create "request was dropped before completing",
    # given the current first error handling
    shards = 2 if sharded else 1

    neon_env_builder.num_pageservers = shards
    env = neon_env_builder.init_start(initial_tenant_shard_count=shards if sharded else None)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    for ps in pageservers.values():
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)
        ps.allowed_errors.append(
            ".* WARN .* path=/v1/tenant/.*/timeline/.*/detach_ancestor request_id=.*: request was dropped before completing"
        )

    client = (
        env.pageserver.http_client() if not sharded else env.storage_controller.pageserver_api()
    )

    with pytest.raises(PageserverApiException, match=".* no ancestors") as info:
        client.detach_ancestor(env.initial_tenant, env.initial_timeline)
    assert info.value.status_code == 409

    _ = env.neon_cli.create_branch("first_branch")

    second_branch = env.neon_cli.create_branch("second_branch", ancestor_branch_name="first_branch")

    # funnily enough this does not have a prefix
    with pytest.raises(PageserverApiException, match="too many ancestors") as info:
        client.detach_ancestor(env.initial_tenant, second_branch)
    assert info.value.status_code == 400


def test_sharded_timeline_detach_ancestor(neon_env_builder: NeonEnvBuilder):
    """
    Sharded timeline detach ancestor; 4 nodes: 1 stuck, 1 restarted, 2 normal.

    Stuck node gets stuck on a pause failpoint for first storage controller request.
    Restarted node remains stuck until explicit restart from test code.

    We retry the request until storage controller gets 200 OK from all nodes.
    """
    branch_name = "soon_detached"
    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    # FIXME: should this be in the neon_env_builder.init_start?
    env.storage_controller.reconcile_until_idle()
    # as we will stop a node, make sure there is no clever rebalancing
    env.storage_controller.tenant_policy_update(env.initial_tenant, body={"scheduling": "Stop"})
    env.storage_controller.allowed_errors.append(".*: Scheduling is disabled by policy Stop .*")

    shards = env.storage_controller.locate(env.initial_tenant)

    utilized_pageservers = {x["node_id"] for x in shards}
    assert len(utilized_pageservers) > 1, "all shards got placed on single pageserver?"

    branch_timeline_id = env.neon_cli.create_branch(branch_name, tenant_id=env.initial_tenant)

    with env.endpoints.create_start(branch_name, tenant_id=env.initial_tenant) as ep:
        ep.safe_psql(
            "create table foo as select 1::bigint, i::bigint from generate_series(1, 10000) v(i)"
        )
        lsn = flush_ep_to_pageserver(env, ep, env.initial_tenant, branch_timeline_id)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    for shard_info in shards:
        node_id = int(shard_info["node_id"])
        shard_id = shard_info["shard_id"]
        detail = pageservers[node_id].http_client().timeline_detail(shard_id, branch_timeline_id)

        assert Lsn(detail["last_record_lsn"]) >= lsn
        assert Lsn(detail["initdb_lsn"]) < lsn
        assert TimelineId(detail["ancestor_timeline_id"]) == env.initial_timeline

    # make one of the nodes get stuck, but continue the initial operation
    # make another of the nodes get stuck, then restart

    stuck = pageservers[int(shards[0]["node_id"])]
    log.info(f"stuck pageserver is id={stuck.id}")
    stuck_http = stuck.http_client()
    stuck_http.configure_failpoints(
        ("timeline-detach-ancestor::before_starting_after_locking_pausable", "pause")
    )

    restarted = pageservers[int(shards[1]["node_id"])]
    log.info(f"restarted pageserver is id={restarted.id}")
    # this might be hit; see `restart_restarted`
    restarted.allowed_errors.append(".*: Cancelled request finished with an error: ShuttingDown")
    assert restarted.id != stuck.id
    restarted_http = restarted.http_client()
    restarted_http.configure_failpoints(
        [
            ("timeline-detach-ancestor::before_starting_after_locking_pausable", "pause"),
        ]
    )

    for info in shards:
        pageserver = pageservers[int(info["node_id"])]
        # the first request can cause these, but does not repeatedly
        pageserver.allowed_errors.append(".*: request was dropped before completing")

    # first request again
    env.storage_controller.allowed_errors.append(".*: request was dropped before completing")

    target = env.storage_controller.pageserver_api()

    with pytest.raises(ReadTimeout):
        target.detach_ancestor(env.initial_tenant, branch_timeline_id, timeout=1)

    stuck_http.configure_failpoints(
        ("timeline-detach-ancestor::before_starting_after_locking_pausable", "off")
    )

    barrier = threading.Barrier(2)

    def restart_restarted():
        barrier.wait()
        # graceful shutdown should just work, because simultaneously unpaused
        restarted.stop()
        # this does not happen always, depends how fast we exit after unpausing
        # restarted.assert_log_contains("Cancelled request finished with an error: ShuttingDown")
        restarted.start()

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(restart_restarted)
        barrier.wait()
        # we have 10s, lets use 1/2 of that to help the shutdown start
        time.sleep(5)
        restarted_http.configure_failpoints(
            ("timeline-detach-ancestor::before_starting_after_locking_pausable", "off")
        )
        fut.result()

    # detach ancestor request handling is not sensitive to http cancellation.
    # this means that the "stuck" is on its way to complete the detach, but the restarted is off
    # now it can either be complete on all nodes, or still in progress with
    # one.
    without_retrying = target.without_status_retrying()

    # this retry loop will be long enough that the tenant can always activate
    reparented = None
    for _ in range(10):
        try:
            reparented = without_retrying.detach_ancestor(env.initial_tenant, branch_timeline_id)
        except PageserverApiException as info:
            assert info.status_code == 503
            time.sleep(2)
        else:
            break

    assert reparented == [], "too many retries (None) or unexpected reparentings"

    for shard_info in shards:
        node_id = int(shard_info["node_id"])
        shard_id = shard_info["shard_id"]

        # TODO: ensure quescing is done on pageserver?
        pageservers[node_id].quiesce_tenants()
        detail = pageservers[node_id].http_client().timeline_detail(shard_id, branch_timeline_id)
        wait_for_last_record_lsn(
            pageservers[node_id].http_client(), shard_id, branch_timeline_id, lsn
        )
        assert detail.get("ancestor_timeline_id") is None

    with env.endpoints.create_start(branch_name, tenant_id=env.initial_tenant) as ep:
        count = int(ep.safe_psql("select count(*) from foo")[0][0])
        assert count == 10000


@pytest.mark.parametrize("mode", ["delete_timeline", "delete_tenant"])
@pytest.mark.parametrize("sharded", [False, True])
def test_timeline_detach_ancestor_interrupted_by_deletion(
    neon_env_builder: NeonEnvBuilder, mode: str, sharded: bool
):
    """
    Timeline ancestor detach interrupted by deleting either:
    - the detached timeline
    - the whole tenant

    after starting the detach.

    What remains not tested by this:
    - shutdown winning over complete

    Shutdown winning over complete needs gc blocking and reparenting any left-overs on retry.
    """

    if sharded and mode == "delete_tenant":
        # the shared/exclusive lock for tenant is blocking this:
        # timeline detach ancestor takes shared, delete tenant takes exclusive
        pytest.skip("tenant deletion while timeline ancestor detach is underway cannot happen")

    shard_count = 2 if sharded else 1

    neon_env_builder.num_pageservers = shard_count

    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count if sharded else None)

    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    detached_timeline = env.neon_cli.create_branch("detached soon", "main")

    failpoint = "timeline-detach-ancestor::before_starting_after_locking_pausable"

    env.storage_controller.reconcile_until_idle()
    shards = env.storage_controller.locate(env.initial_tenant)

    assert len(set(info["node_id"] for info in shards)) == shard_count

    target = env.storage_controller.pageserver_api() if sharded else env.pageserver.http_client()
    target = target.without_status_retrying()

    victim = pageservers[int(shards[-1]["node_id"])]
    victim_http = victim.http_client()
    victim_http.configure_failpoints((failpoint, "pause"))

    def detach_ancestor():
        target.detach_ancestor(env.initial_tenant, detached_timeline)

    def at_failpoint() -> Tuple[str, LogCursor]:
        return victim.assert_log_contains(f"at failpoint {failpoint}")

    def start_delete():
        if mode == "delete_timeline":
            target.timeline_delete(env.initial_tenant, detached_timeline)
        elif mode == "delete_tenant":
            target.tenant_delete(env.initial_tenant)
        else:
            raise RuntimeError(f"unimplemented mode {mode}")

    def at_waiting_on_gate_close(start_offset: LogCursor) -> LogCursor:
        _, offset = victim.assert_log_contains(
            "closing is taking longer than expected", offset=start_offset
        )
        return offset

    def is_deleted():
        try:
            if mode == "delete_timeline":
                target.timeline_detail(env.initial_tenant, detached_timeline)
            elif mode == "delete_tenant":
                target.tenant_status(env.initial_tenant)
            else:
                return False
        except PageserverApiException as e:
            assert e.status_code == 404
            return True
        else:
            raise RuntimeError("waiting for 404")

    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            fut = pool.submit(detach_ancestor)
            _, offset = wait_until(10, 1.0, at_failpoint)

            delete = pool.submit(start_delete)

            wait_until(10, 1.0, lambda: at_waiting_on_gate_close(offset))

            victim_http.configure_failpoints((failpoint, "off"))

            delete.result()

            assert wait_until(10, 1.0, is_deleted), f"unimplemented mode {mode}"

            with pytest.raises(PageserverApiException) as exc:
                fut.result()
            assert exc.value.status_code == 503
        finally:
            victim_http.configure_failpoints((failpoint, "off"))


@pytest.mark.parametrize("mode", ["delete_reparentable_timeline"])
def test_sharded_tad_interleaved_after_partial_success(neon_env_builder: NeonEnvBuilder, mode: str):
    """
    Technically possible storage controller concurrent interleaving timeline
    deletion with timeline detach.

    Deletion is fine, as any sharded pageservers reach the same end state, but
    creating reparentable timeline would create an issue as the two nodes would
    never agree. There is a solution though: the created reparentable timeline
    must be detached.
    """

    assert (
        mode == "delete_reparentable_timeline"
    ), "only one now, but we could have the create just as well, need gc blocking"

    shard_count = 2
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    env.storage_controller.reconcile_until_idle()
    shards = env.storage_controller.locate(env.initial_tenant)
    assert len(set(x["node_id"] for x in shards)) == shard_count

    with env.endpoints.create_start("main") as ep:
        ep.safe_psql("create table foo as select i::bigint from generate_series(1, 1000) t(i)")

        # as the interleaved operation, we will delete this timeline, which was reparenting candidate
        first_branch_lsn = wait_for_last_flush_lsn(
            env, ep, env.initial_tenant, env.initial_timeline
        )
        for ps, shard_id in [(pageservers[int(x["node_id"])], x["shard_id"]) for x in shards]:
            ps.http_client().timeline_checkpoint(shard_id, env.initial_timeline)

        ep.safe_psql("create table bar as select i::bigint from generate_series(1, 2000) t(i)")
        detached_branch_lsn = flush_ep_to_pageserver(
            env, ep, env.initial_tenant, env.initial_timeline
        )

    for ps, shard_id in [(pageservers[int(x["node_id"])], x["shard_id"]) for x in shards]:
        ps.http_client().timeline_checkpoint(shard_id, env.initial_timeline)

    first_branch = env.neon_cli.create_branch(
        "first_branch", ancestor_branch_name="main", ancestor_start_lsn=first_branch_lsn
    )
    detached_branch = env.neon_cli.create_branch(
        "detached_branch", ancestor_branch_name="main", ancestor_start_lsn=detached_branch_lsn
    )

    pausepoint = "timeline-detach-ancestor::before_starting_after_locking_pausable"

    stuck = pageservers[int(shards[0]["node_id"])]
    stuck_http = stuck.http_client().without_status_retrying()
    stuck_http.configure_failpoints((pausepoint, "pause"))

    victim = pageservers[int(shards[-1]["node_id"])]
    victim_http = victim.http_client().without_status_retrying()
    victim_http.configure_failpoints(
        (pausepoint, "pause"),
    )

    # noticed a surprising 409 if the other one would fail instead
    # victim_http.configure_failpoints([
    #     (pausepoint, "pause"),
    #     ("timeline-detach-ancestor::before_starting_after_locking", "return"),
    # ])

    # interleaving a create_timeline which could be reparented will produce two
    # permanently different reparentings: one node has reparented, other has
    # not
    #
    # with deletion there is no such problem
    def detach_timeline():
        env.storage_controller.pageserver_api().detach_ancestor(env.initial_tenant, detached_branch)

    def paused_at_failpoint():
        stuck.assert_log_contains(f"at failpoint {pausepoint}")
        victim.assert_log_contains(f"at failpoint {pausepoint}")

    def first_completed():
        detail = stuck_http.timeline_detail(shards[0]["shard_id"], detached_branch)
        log.info(detail)
        assert detail.get("ancestor_lsn") is None

    def first_branch_gone():
        try:
            env.storage_controller.pageserver_api().timeline_detail(
                env.initial_tenant, first_branch
            )
        except PageserverApiException as e:
            log.info(f"error {e}")
            assert e.status_code == 404
        else:
            log.info("still ok")
            raise RuntimeError("not done yet")

    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            fut = pool.submit(detach_timeline)
            wait_until(10, 1.0, paused_at_failpoint)

            # let stuck complete
            stuck_http.configure_failpoints((pausepoint, "off"))
            wait_until(10, 1.0, first_completed)

            # if we would let victim fail, for some reason there'd be a 409 response instead of 500
            # victim_http.configure_failpoints((pausepoint, "off"))
            # with pytest.raises(PageserverApiException, match=".* 500 Internal Server Error failpoint: timeline-detach-ancestor::before_starting_after_locking") as exc:
            #     fut.result()
            # assert exc.value.status_code == 409

            env.storage_controller.pageserver_api().timeline_delete(
                env.initial_tenant, first_branch
            )
            victim_http.configure_failpoints((pausepoint, "off"))
            wait_until(10, 1.0, first_branch_gone)

            # it now passes, and we should get an error messages about mixed reparenting as the stuck still had something to reparent
            fut.result()

            msg, offset = env.storage_controller.assert_log_contains(
                ".*/timeline/\\S+/detach_ancestor.*: shards returned different results matching=0 .*"
            )
            log.info(f"expected error message: {msg}")
            env.storage_controller.allowed_errors.append(
                ".*: shards returned different results matching=0 .*"
            )

            detach_timeline()

            # FIXME: perhaps the above should be automatically retried, if we get mixed results?
            not_found = env.storage_controller.log_contains(
                ".*/timeline/\\S+/detach_ancestor.*: shards returned different results matching=0 .*",
                offset=offset,
            )

            assert not_found is None
        finally:
            stuck_http.configure_failpoints((pausepoint, "off"))
            victim_http.configure_failpoints((pausepoint, "off"))


# TODO:
# - after starting the operation, pageserver is shutdown, restarted
# - after starting the operation, bottom-most timeline is deleted, pageserver is restarted, gc is inhibited
# - deletion of reparented while reparenting should fail once, then succeed (?)
# - branch near existing L1 boundary, image layers?
# - investigate: why are layers started at uneven lsn? not just after branching, but in general.
#
# TEST: 1. tad which partially succeeds, one returns 500
#       2. create branch below timeline? ~or delete reparented timeline~ (done)
#       3. on retry all should report the same reparented timelines
