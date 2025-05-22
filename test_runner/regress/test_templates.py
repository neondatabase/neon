import base64
import json
import shutil
import time
from enum import Enum
from pathlib import Path
from threading import Event

import psycopg2
import psycopg2.errors
import pytest
from fixtures.common_types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.fast_import import FastImport
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    PgProtocol,
    VanillaPostgres,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import (
    ImportPgdataIdemptencyKey,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import LocalFsStorage, MockS3Server, RemoteStorageKind
from fixtures.utils import (
    run_only_on_default_postgres,
    shared_buffers_for_max_cu,
    skip_in_debug_build,
    wait_until,
)
from fixtures.workload import Workload
from mypy_boto3_kms import KMSClient
from mypy_boto3_kms.type_defs import EncryptResponseTypeDef
from mypy_boto3_s3 import S3Client
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

num_rows = 1000


def mock_import_bucket(vanilla_pg: VanillaPostgres, path: Path):
    """
    Mock the import S3 bucket into a local directory for a provided vanilla PG instance.
    """
    assert not vanilla_pg.is_running()

    path.mkdir()
    # what cplane writes before scheduling fast_import
    specpath = path / "spec.json"
    specpath.write_text(json.dumps({"branch_id": "somebranch", "project_id": "someproject"}))
    # what fast_import writes
    vanilla_pg.pgdatadir.rename(path / "pgdata")
    statusdir = path / "status"
    statusdir.mkdir()
    (statusdir / "pgdata").write_text(json.dumps({"done": True}))
    (statusdir / "fast_import").write_text(json.dumps({"command": "pgdata", "done": True}))


def test_template_smoke(neon_env_builder: NeonEnvBuilder):
    shard_count = 1
    stripe_size = 1024
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    log.info("create template data")

    template_tenant_id = TenantId.generate()
    template_timeline_id = TimelineId.generate()

    env.create_tenant(shard_count=1, tenant_id=template_tenant_id, timeline_id=template_timeline_id)

    def validate_data_equivalence(ep):
        # TODO: would be nicer to just compare pgdump

        # Enable IO concurrency for batching on large sequential scan, to avoid making
        # this test unnecessarily onerous on CPU. Especially on debug mode, it's still
        # pretty onerous though, so increase statement_timeout to avoid timeouts.
        assert ep.safe_psql_many(
            [
                "set effective_io_concurrency=32;",
                "SET statement_timeout='300s';",
                "select count(*), sum(data::bigint)::bigint from t",
            ]
        ) == [[], [], [(expect_nrows, expect_sum)]]

    # Fill the template with some data
    with env.endpoints.create_start("main", tenant_id=template_tenant_id) as endpoint:
        # fillfactor so we don't need to produce that much data
        # 900 byte per row is > 10% => 1 row per page
        endpoint.safe_psql("""create table t (data char(900)) with (fillfactor = 10)""")

        nrows = 0

        target_relblock_size = 1024 * 8192

        while True:
            relblock_size = endpoint.safe_psql_scalar("select pg_relation_size('t')")
            log.info(
                f"relblock size: {relblock_size / 8192} pages (target: {target_relblock_size // 8192}) pages"
            )
            if relblock_size >= target_relblock_size:
                break
            addrows = int((target_relblock_size - relblock_size) // 8192)
            assert addrows >= 1, "forward progress"
            endpoint.safe_psql(
                f"insert into t select generate_series({nrows + 1}, {nrows + addrows})"
            )
            nrows += addrows
        expect_nrows = nrows
        expect_sum = (
            (nrows) * (nrows + 1) // 2
        )  # https://stackoverflow.com/questions/43901484/sum-of-the-integers-from-1-to-n

        env.pageserver.http_client().timeline_checkpoint(template_tenant_id, template_timeline_id)
        wait_for_last_flush_lsn(env, endpoint, template_tenant_id, template_timeline_id)

        validate_data_equivalence(endpoint)

    # Copy the template to the templates dir and delete the original project
    template_tenant_shard_id = TenantShardId(template_tenant_id, 0, 1)
    from_dir = env.pageserver_remote_storage.tenant_path(template_tenant_shard_id)
    to_dir = env.pageserver_remote_storage.root / "templates" / str(template_tenant_id)
    shutil.copytree(from_dir, to_dir)

    # Do the template creation
    new_tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        new_tenant_id, shard_count=shard_count, shard_stripe_size=stripe_size
    )

    new_timeline_id = TimelineId.generate()
    log.info("starting timeline creation")
    start = time.monotonic()

    import_branch_name = "imported"
    env.storage_controller.timeline_create(
        new_tenant_id,
        {
            "new_timeline_id": str(new_timeline_id),
            "template_tenant_id": str(template_tenant_id),
            "template_timeline_id": str(template_timeline_id),
        },
    )
    env.neon_cli.mappings_map_branch(import_branch_name, new_tenant_id, new_timeline_id)

    # Get some timeline details for later.
    locations = env.storage_controller.locate(new_tenant_id)
    [shard_zero] = [
        loc for loc in locations if TenantShardId.parse(loc["shard_id"]).shard_number == 0
    ]
    shard_zero_ps = env.get_pageserver(shard_zero["node_id"])
    shard_zero_http = shard_zero_ps.http_client()
    shard_zero_timeline_info = shard_zero_http.timeline_detail(shard_zero["shard_id"], new_timeline_id)
    initdb_lsn = Lsn(shard_zero_timeline_info["initdb_lsn"])
    min_readable_lsn = Lsn(shard_zero_timeline_info["min_readable_lsn"])
    last_record_lsn = Lsn(shard_zero_timeline_info["last_record_lsn"])
    disk_consistent_lsn = Lsn(shard_zero_timeline_info["disk_consistent_lsn"])
    _remote_consistent_lsn = Lsn(shard_zero_timeline_info["remote_consistent_lsn"])
    remote_consistent_lsn_visible = Lsn(shard_zero_timeline_info["remote_consistent_lsn_visible"])
    # assert remote_consistent_lsn_visible == remote_consistent_lsn TODO: this fails initially and after restart, presumably because `UploadQueue::clean.1` is still `None`
    #assert remote_consistent_lsn_visible == disk_consistent_lsn
    #assert initdb_lsn == min_readable_lsn
    #assert disk_consistent_lsn == initdb_lsn + 8
    assert last_record_lsn == disk_consistent_lsn
    # TODO: assert these values are the same everywhere

    # Last step: validation

    with env.endpoints.create_start(
        branch_name=import_branch_name,
        endpoint_id="ro",
        tenant_id=new_tenant_id,
        lsn=last_record_lsn,
    ) as ro_endpoint:
        validate_data_equivalence(ro_endpoint)

        # ensure the template survives restarts
        ro_endpoint.stop()
        #env.pageserver.stop(immediate=True)
        #env.pageserver.start()
        ro_endpoint.start()
        validate_data_equivalence(ro_endpoint)

    #
    # validate that we can write
    #
    workload = Workload(env, new_tenant_id, new_timeline_id, branch_name=import_branch_name)
    workload.init()
    workload.write_rows(64)
    workload.validate()

    rw_lsn = Lsn(workload.endpoint().safe_psql_scalar("select pg_current_wal_flush_lsn()"))

    #
    # validate that we can branch (important use case)
    #

    # ... at the tip
    child_timeline_id = env.create_branch(
        new_branch_name="br-tip",
        ancestor_branch_name=import_branch_name,
        tenant_id=new_tenant_id,
        ancestor_start_lsn=rw_lsn,
    )
    child_workload = workload.branch(timeline_id=child_timeline_id, branch_name="br-tip")
    child_workload.validate()

    validate_data_equivalence(child_workload.endpoint())

    # ... at the initdb lsn
    _ = env.create_branch(
        new_branch_name="br-initdb",
        ancestor_branch_name=import_branch_name,
        tenant_id=new_tenant_id,
        ancestor_start_lsn=initdb_lsn,
    )
    br_initdb_endpoint = env.endpoints.create_start(
        branch_name="br-initdb",
        endpoint_id="br-initdb-ro",
        tenant_id=new_tenant_id,
    )
    validate_data_equivalence(br_initdb_endpoint)
    with pytest.raises(psycopg2.errors.UndefinedTable):
        br_initdb_endpoint.safe_psql(f"select * from {workload.table}")
