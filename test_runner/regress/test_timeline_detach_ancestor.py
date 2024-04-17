from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import HistoricLayerInfo
from fixtures.types import Lsn


def by_end_lsn(info: HistoricLayerInfo) -> Lsn:
    assert info.lsn_end is not None
    return Lsn(info.lsn_end)


def layer_name(info: HistoricLayerInfo) -> str:
    return info.layer_file_name


def test_ancestor_detach_with_restart_after(neon_env_builder: NeonEnvBuilder):
    """
    Happy path, without gc problems. Does not require live-reparenting
    implementation, because we restart after operation.
    """

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
        }
    )

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT);")
        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        # create a single layer for us to remote copy
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")

        future_branch_at = Lsn(ep.safe_psql("SELECT pg_current_wal_flush_lsn();")[0][0])

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(16384, 24575) g(i);")

    # do 3 branch detaches: first future, +1, then at the boundary, then -1
    branches = []

    deltas = client.layer_map_info(env.initial_tenant, env.initial_timeline).delta_layers()
    assert len(deltas) == 2, "expecting there to be two deltas: initdb and checkpointed"
    later_delta = max(deltas, key=by_end_lsn)
    assert later_delta.lsn_end is not None

    lsn_start = Lsn(later_delta.lsn_start)
    lsn_end = Lsn(later_delta.lsn_end)

    # half-way of a single transaction makes no sense, but we can do it
    lowest_branch_at = lsn_start + Lsn((lsn_end.lsn_int - lsn_start.lsn_int) // 2)
    # the end of delta lsn range is exclusive, so decrement one to get a full copy
    at_delta_end = Lsn(lsn_end.lsn_int - 1)

    after_delta_end = lsn_end + 7
    assert future_branch_at > after_delta_end

    named_lsns = [
        ("fourth_new_main", lowest_branch_at, 0),
        ("third_new_main", at_delta_end, 8192),
        ("second_new_main", after_delta_end, 8192),
        ("new_main", future_branch_at, 16384),
    ]

    for name, lsn, rows in named_lsns:
        timeline_id = env.neon_cli.create_branch(
            name, "main", env.initial_tenant, ancestor_start_lsn=lsn
        )
        branches.append((timeline_id, name, lsn, rows))

    write_to_branch_before_detach = True

    # does false cause a missing previous lsn?
    # no it's zenith.signal check between prev lsn being "invalid" or "none" without a hack

    if write_to_branch_before_detach:
        for timeline_id, name, _, rows in branches:
            with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
                assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
                # make sure the ep is writable
                ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")

            # this is the most important part; this way the PREV_LSN will be available even after detach
            # for "PREV_LSN: none", which would otherwise be "PREV_LSN: invalid".
            client.timeline_checkpoint(env.initial_tenant, timeline_id)

    # TODO: does gc_cutoff move on the timeline? probably not; a hole might appear but we will always keep what is required to reconstruct a branchpoint

    # do 4 detaches in descending LSN order
    branches.reverse()
    prev_lsn = None
    ancestor_deletion_order = []
    for timeline_id, name, lsn, _ in branches:
        if prev_lsn is not None:
            assert prev_lsn > lsn, "expecting to iterate branches in desceding order"
        ancestor = client.timeline_detail(env.initial_tenant, timeline_id)["ancestor_timeline_id"]
        assert ancestor is not None
        client.detach_ancestor(env.initial_tenant, timeline_id)

        env.pageserver.stop()
        env.pageserver.start()

        log.info(f"post detach layers for {name} ({timeline_id}):")

        layers = client.layer_map_info(env.initial_tenant, timeline_id).historic_layers
        layers.sort(key=layer_name)

        for layer in layers:
            log.info(f"- {layer.layer_file_name}")

        ancestor_deletion_order.append(ancestor)

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 24576

    for _, name, _, rows in branches:
        with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    for ancestor in ancestor_deletion_order:
        client.timeline_delete(env.initial_tenant, ancestor)


# TODO:
# - ancestor gets the checkpoint called at detach if necessary
# - branch before existing layer boundary, at, after
# - branch near existing L1 boundary
