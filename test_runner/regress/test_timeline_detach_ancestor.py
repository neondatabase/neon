import enum
from typing import List

import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import HistoricLayerInfo
from fixtures.pageserver.utils import wait_timeline_detail_404
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
    BEFORE_L0 = "prev"
    AT_L0 = "at"
    AFTER_L0 = "next"
    LAST_RECORD_LSN = "head"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def all() -> List["Branchpoint"]:
        return [
            Branchpoint.EARLIER,
            Branchpoint.BEFORE_L0,
            Branchpoint.AT_L0,
            Branchpoint.AFTER_L0,
            Branchpoint.LAST_RECORD_LSN,
        ]


@pytest.mark.parametrize("branchpoint", Branchpoint.all())
def test_ancestor_detach_branched_from(neon_env_builder: NeonEnvBuilder, branchpoint: Branchpoint):
    """
    Creates a branch relative to L0 lsn boundary according to Branchpoint. Later the timeline is detached.
    """
    # TODO: parametrize; currently unimplemented over at pageserver
    restart_after = True
    write_to_branch_first = True

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
        }
    )

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
    elif branchpoint == Branchpoint.BEFORE_L0:
        branch_at = Lsn(last_lsn - 8)
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

    # TODO: return value from this
    client.detach_ancestor(env.initial_tenant, timeline_id)

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    old_main = client.layer_map_info(env.initial_tenant, env.initial_timeline)
    old_main = set(map(layer_name, old_main.historic_layers))

    new_main = client.layer_map_info(env.initial_tenant, timeline_id)
    new_main = set(map(layer_name, new_main.historic_layers))

    new_main_copied_or_truncated = new_main - branch_layers
    new_main_truncated = new_main_copied_or_truncated - old_main

    assert len(new_main_truncated) == truncated_layers
    # could additionally check that the symmetric difference has layers starting at the same lsn
    # but if nothing was copied, then there is no nice rule.
    # there could be a hole in LSNs between copied from the "old main" and the first branch layer.

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)


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

    # TODO: support not yet implemented for these
    restart_after = True
    write_to_branch_first = True

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
        }
    )

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

    if write_to_branch_first:
        with env.endpoints.create_start("new main", tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 8192
            with ep.cursor() as cur:
                cur.execute("UPDATE audit SET starts = starts + 1")
                assert cur.rowcount == 1
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)

        client.timeline_checkpoint(env.initial_tenant, timeline_id)

    # TODO: return value from this should say that reparented and same_branchpoint were reparented
    client.detach_ancestor(env.initial_tenant, timeline_id)

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    # checking the ancestor after is much faster than waiting for the endpoint not start
    expected_ancestry = [
        (env.initial_timeline, None),
        (after, env.initial_timeline),
        (timeline_id, None),
        (same_branchpoint, timeline_id),
        (reparented, timeline_id),
    ]

    for id, expected_ancestor in expected_ancestry:
        details = client.timeline_detail(env.initial_tenant, id)
        ancestor_timeline_id = details["ancestor_timeline_id"]
        if expected_ancestor is None:
            assert ancestor_timeline_id is None
        else:
            assert TimelineId(ancestor_timeline_id) == expected_ancestor

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start("after", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start("new main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 8192
        assert ep.safe_psql("SELECT count(*) FROM audit WHERE starts = 2")[0][0] == 1

    with env.endpoints.create_start("same_branchpoint", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 8192
        assert ep.safe_psql("SELECT count(*) FROM audit WHERE starts = 1")[0][0] == 1

    with env.endpoints.create_start("reparented", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 0

    client.timeline_delete(env.initial_tenant, after)
    wait_timeline_detail_404(client, env.initial_tenant, after, 10, 1.0)

    # if this succeeds, the reparenting must've happened
    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)


# TODO:
# - after starting the operation, tenant is deleted
# - after starting the operation, pageserver is shutdown
# - branch near existing L1 boundary, image layers?
# - investigate: why are layers started at uneven lsn? not just after branching, but in general.
