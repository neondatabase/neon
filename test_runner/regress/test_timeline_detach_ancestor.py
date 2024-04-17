import enum
from typing import List

import pytest
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
    Creates a branch before, at or after the L0 lsn boundary.
    """
    # TODO: parametrize
    restart_after = True
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
        last_record_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

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
    elif branchpoint == Branchpoint.BEFORE_L0:
        branch_at = Lsn(last_lsn - 8)
        rows = 0
    elif branchpoint == Branchpoint.AT_L0:
        branch_at = Lsn(last_lsn)
        rows = 8192
    elif branchpoint == Branchpoint.AFTER_L0:
        branch_at = Lsn(last_lsn + 8)
        rows = 8192
    else:
        assert branchpoint == Branchpoint.LAST_RECORD_LSN
        branch_at = last_record_lsn
        rows = 16384

    name = "new main"

    timeline_id = env.neon_cli.create_branch(
        name, "main", env.initial_tenant, ancestor_start_lsn=branch_at
    )

    with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
        # make sure the ep is writable
        # with BEFORE_L0, AFTER_L0 there will be a gap in Lsns caused by accurate end_lsn on straddling layers
        ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)

    # branch must have a flush for "PREV_LSN: none"
    client.timeline_checkpoint(env.initial_tenant, timeline_id)

    # TODO: return value from this
    client.detach_ancestor(env.initial_tenant, timeline_id)

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    client.timeline_delete(env.initial_tenant, env.initial_timeline)

    info = client.layer_map_info(env.initial_tenant, timeline_id)
    info.historic_layers.sort(key=layer_name)

    log.info("layers:")
    for layer in info.historic_layers:
        log.info(f"- {layer.layer_file_name}")


# TODO:
# - ancestor gets the checkpoint called at detach if necessary
# - reparenting is done
# - after starting the operation, tenant is deleted
# - after starting the operation, pageserver is shutdown
# - branch near existing L1 boundary
