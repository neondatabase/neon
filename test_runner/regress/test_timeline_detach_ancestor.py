import datetime
import enum
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from queue import Empty, Queue
from threading import Barrier
from typing import IO, List, Set, Tuple, Union

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import HistoricLayerInfo
from fixtures.pageserver.utils import wait_timeline_detail_404
from fixtures.remote_storage import LocalFsStorage
from fixtures.types import Lsn, TimelineId


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
    pg_distrib_dir,
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

    psql_env = {"LD_LIBRARY_PATH": str(pg_distrib_dir / "lib")}
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
    cmd = [
        "psql",
        "--no-psqlrc",
        env.pageserver.connstr(),
        "-c",
        f"fullbackup {env.initial_tenant} {env.initial_timeline} {branch_at}",
        "-o",
        str(fullbackup_before),
    ]
    pg_bin.run_capture(cmd, env=psql_env)

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert all_reparented == set()

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
    cmd = [
        "psql",
        "--no-psqlrc",
        env.pageserver.connstr(),
        "-c",
        f"fullbackup {env.initial_tenant} {timeline_id} {branch_at}",
        "-o",
        str(fullbackup_after),
    ]
    pg_bin.run_capture(cmd, env=psql_env)

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)

    # because we do the fullbackup from ancestor at the branch_lsn, the zenith.signal is always different
    # as there is always "PREV_LSN: invalid" for "before"
    skip_files = {"zenith.signal"}

    tar_cmp(fullbackup_before, fullbackup_after, skip_files)


def tar_cmp(left: Path, right: Path, skip_files: Set[str]):
    """
    This is essentially:

    lines=$(comm -3 \
        <(mkdir left && cd left && tar xf "$left" && find . -type f -print0 | xargs sha256sum | sort -k2) \
        <(mkdir right && cd right && tar xf "$right" && find . -type f -print0 | xargs sha256sum | sort -k2) \
        | wc -l)
    [ "$lines" = "0" ]

    But in a more mac friendly fashion.
    """
    started_at = time.time()

    def hash_extracted(reader: Union[IO[bytes], None]) -> bytes:
        assert reader is not None
        digest = sha256(usedforsecurity=False)
        while True:
            buf = reader.read(64 * 1024)
            if not buf:
                break
            digest.update(buf)
        return digest.digest()

    def build_hash_list(p: Path) -> List[Tuple[str, bytes]]:
        with tarfile.open(p) as f:
            matching_files = (info for info in f if info.isreg() and info.name not in skip_files)
            ret = list(
                map(lambda info: (info.name, hash_extracted(f.extractfile(info))), matching_files)
            )
            ret.sort(key=lambda t: t[0])
            return ret

    left_list, right_list = map(build_hash_list, [left, right])

    try:
        assert len(left_list) == len(right_list)

        for left_tuple, right_tuple in zip(left_list, right_list):
            assert left_tuple == right_tuple
    finally:
        elapsed = time.time() - started_at
        log.info(f"tar_cmp completed in {elapsed}s")


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
    assert all_reparented == {reparented, same_branchpoint}

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


# TODO:
# - after starting the operation, tenant is deleted
# - after starting the operation, pageserver is shutdown, restarted
# - after starting the operation, bottom-most timeline is deleted, pageserver is restarted, gc is inhibited
# - deletion of reparented while reparenting should fail once, then succeed (?)
# - branch near existing L1 boundary, image layers?
# - investigate: why are layers started at uneven lsn? not just after branching, but in general.
