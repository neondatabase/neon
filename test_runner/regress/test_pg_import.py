import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Optional

import psycopg2
import psycopg2.errors
import pytest
from fixtures.common_types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, VanillaPostgres
from fixtures.pageserver.http import HistoricLayerInfo, PageserverHttpClient
from fixtures.remote_storage import RemoteStorageKind

num_rows = 1000


class RelBlockSize(Enum):
    ONE_STRIPE_SIZE = 1
    TWO_STRPES_PER_SHARD = 2
    MULTIPLE_RELATION_SEGMENTS = 3


smoke_params = [
    # unsharded (the stripe size needs to be given for rel block size calculations)
    *[(None, 1024, s) for s in RelBlockSize],
    # many shards, small stripe size to speed up test
    *[(8, 1024, s) for s in RelBlockSize],
]


@pytest.mark.parametrize("shard_count,stripe_size,rel_block_size", smoke_params)
def test_pgdata_import_smoke(
    vanilla_pg: VanillaPostgres,
    neon_env_builder: NeonEnvBuilder,
    shard_count: Optional[int],
    stripe_size: int,
    rel_block_size: RelBlockSize,
):
    #
    # Put data in vanilla pg
    #

    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")

    log.info("create relblock data")
    if rel_block_size == RelBlockSize.ONE_STRIPE_SIZE:
        target_relblock_size = stripe_size * 8192
    elif rel_block_size == RelBlockSize.TWO_STRPES_PER_SHARD:
        target_relblock_size = (shard_count or 1) * stripe_size * 8192 * 2
    elif rel_block_size == RelBlockSize.MULTIPLE_RELATION_SEGMENTS:
        target_relblock_size = int(((2.333 * 1024 * 1024 * 1024) // 8192) * 8192)
    else:
        raise ValueError

    # fillfactor so we don't need to produce that much data
    # 900 byte per row is > 10% => 1 row per page
    vanilla_pg.safe_psql("""create table t (data char(900)) with (fillfactor = 10)""")

    nrows = 0
    while True:
        relblock_size = vanilla_pg.safe_psql_scalar("select pg_relation_size('t')")
        log.info(
            f"relblock size: {relblock_size/8192} pages (target: {target_relblock_size//8192}) pages"
        )
        if relblock_size >= target_relblock_size:
            break
        addrows = int((target_relblock_size - relblock_size) // 8192)
        assert addrows >= 1, "forward progress"
        vanilla_pg.safe_psql(f"insert into t select generate_series({nrows+1}, {nrows + addrows})")
        nrows += addrows
    expect_nrows = nrows
    expect_sum = (
        (nrows) * (nrows + 1) // 2
    )  # https://stackoverflow.com/questions/43901484/sum-of-the-integers-from-1-to-n

    def validate_vanilla_equivalence(ep):
        # TODO: would be nicer to just compare pgdump
        assert ep.safe_psql("select count(*), sum(data::bigint)::bigint from t") == [
            (expect_nrows, expect_sum)
        ]

    validate_vanilla_equivalence(vanilla_pg)

    vanilla_pg.stop()

    #
    # We have a Postgres data directory to import now
    #

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id, shard_count=shard_count, shard_stripe_size=stripe_size
    )

    timeline_id = TimelineId.generate()
    log.info("starting import")
    start = time.monotonic()
    env.storage_controller.timeline_import_from_pgdata(tenant_id, vanilla_pg.pgdatadir, timeline_id)
    env.storage_controller.allowed_errors.append(
        # FIXME: the import operation should not be a blocking HTTP call, but something
        # that can be polled for completion => we don't have such a transitory state in the system yet.
        ".*import_pgdata.*Shared lock by TimelineImportFromPgdata was held for"
    )
    import_duration = time.monotonic() - start
    log.info(f"import complete; duration={import_duration:.2f}s")

    env.neon_cli.map_branch("imported", tenant_id, timeline_id)

    #
    # Get some timeline details for later.
    #
    locations = env.storage_controller.locate(tenant_id)
    [shard_zero] = [
        loc for loc in locations if TenantShardId.parse(loc["shard_id"]).shard_number == 0
    ]
    shard_zero_ps = env.get_pageserver(shard_zero["node_id"])
    shard_zero_http = shard_zero_ps.http_client()
    shard_zero_timeline_info = shard_zero_http.timeline_detail(shard_zero["shard_id"], timeline_id)
    initdb_lsn = Lsn(shard_zero_timeline_info["initdb_lsn"])
    latest_gc_cutoff_lsn = Lsn(shard_zero_timeline_info["latest_gc_cutoff_lsn"])
    last_record_lsn = Lsn(shard_zero_timeline_info["last_record_lsn"])
    disk_consistent_lsn = Lsn(shard_zero_timeline_info["disk_consistent_lsn"])
    _remote_consistent_lsn = Lsn(shard_zero_timeline_info["remote_consistent_lsn"])
    remote_consistent_lsn_visible = Lsn(shard_zero_timeline_info["remote_consistent_lsn_visible"])
    # assert remote_consistent_lsn_visible == remote_consistent_lsn TODO: this fails initially and after restart, presumably because `UploadQueue::clean.1` is still `None`
    assert remote_consistent_lsn_visible == disk_consistent_lsn
    assert initdb_lsn == latest_gc_cutoff_lsn
    assert disk_consistent_lsn == initdb_lsn + 8
    assert last_record_lsn == disk_consistent_lsn
    # TODO: assert these values are the same everywhere

    #
    # Validate the imported data
    #

    ro_endpoint = env.endpoints.create_start(
        branch_name="imported", endpoint_id="ro", tenant_id=tenant_id, lsn=last_record_lsn
    )

    validate_vanilla_equivalence(ro_endpoint)

    # ensure the import survives restarts
    ro_endpoint.stop()
    env.pageserver.stop(immediate=True)
    env.pageserver.start()
    ro_endpoint.start()
    validate_vanilla_equivalence(ro_endpoint)

    #
    # validate the layer files in each shard only have the shard-specific data
    # (the implementation would be functional but not efficient without this characteristic)
    #

    shards = env.storage_controller.locate(tenant_id)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futs = []
        for shard in shards:
            shard_ps = env.get_pageserver(shard["node_id"])
            tenant_shard_id = TenantShardId.parse(shard["shard_id"])
            shard_ps_http = shard_ps.http_client()
            shard_layer_map = shard_ps_http.layer_map_info(tenant_shard_id, timeline_id)
            for layer in shard_layer_map.historic_layers:

                def do_layer(
                    shard_ps_http: PageserverHttpClient,
                    tenant_shard_id: TenantShardId,
                    timeline_id: TimelineId,
                    layer: HistoricLayerInfo,
                ):
                    return (
                        layer,
                        shard_ps_http.timeline_layer_scan_disposable_keys(
                            tenant_shard_id, timeline_id, layer.layer_file_name
                        ),
                    )

                futs.append(
                    executor.submit(do_layer, shard_ps_http, tenant_shard_id, timeline_id, layer)
                )
        for fut in futs:
            layer, result = fut.result()
            assert result["disposable_count"] == 0
            assert result["not_disposable_count"] > 0  # sanity check

    #
    # validate that we can write
    #
    rw_endpoint = env.endpoints.create_start(
        branch_name="imported", endpoint_id="rw", tenant_id=tenant_id
    )
    rw_endpoint.safe_psql("create table othertable(values text)")
    rw_lsn = Lsn(rw_endpoint.safe_psql_scalar("select pg_current_wal_flush_lsn()"))

    # TODO: consider using `class Workload` here
    # to do compaction and whatnot?

    #
    # validate that we can branch (important use case)
    #

    # ... at the tip
    _ = env.neon_cli.create_branch("br-tip", "imported", tenant_id, ancestor_start_lsn=rw_lsn)
    br_tip_endpoint = env.endpoints.create_start(
        branch_name="br-tip", endpoint_id="br-tip-ro", tenant_id=tenant_id
    )
    validate_vanilla_equivalence(br_tip_endpoint)
    br_tip_endpoint.safe_psql("select * from othertable")

    # ... at the initdb lsn
    _ = env.neon_cli.create_branch(
        "br-initdb", "imported", tenant_id, ancestor_start_lsn=initdb_lsn
    )
    br_initdb_endpoint = env.endpoints.create_start(
        branch_name="br-initdb", endpoint_id="br-initdb-ro", tenant_id=tenant_id
    )
    validate_vanilla_equivalence(br_initdb_endpoint)
    with pytest.raises(psycopg2.errors.UndefinedTable):
        br_initdb_endpoint.safe_psql("select * from othertable")
