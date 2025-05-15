import base64
import json
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
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, PgProtocol, VanillaPostgres
from fixtures.pageserver.http import (
    ImportPgdataIdemptencyKey,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import MockS3Server, RemoteStorageKind
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


@skip_in_debug_build("MULTIPLE_RELATION_SEGMENTS has non trivial amount of data")
@pytest.mark.parametrize("shard_count,stripe_size,rel_block_size", smoke_params)
def test_pgdata_import_smoke(
    vanilla_pg: VanillaPostgres,
    neon_env_builder: NeonEnvBuilder,
    shard_count: int | None,
    stripe_size: int,
    rel_block_size: RelBlockSize,
    make_httpserver: HTTPServer,
):
    #
    # Setup fake control plane for import progress
    #
    import_completion_signaled = Event()

    def handler(request: Request) -> Response:
        log.info(f"control plane /import_complete request: {request.json}")
        import_completion_signaled.set()
        return Response(json.dumps({}), status=200)

    cplane_mgmt_api_server = make_httpserver
    cplane_mgmt_api_server.expect_request(
        "/storage/api/v1/import_complete", method="PUT"
    ).respond_with_handler(handler)

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    neon_env_builder.control_plane_hooks_api = (
        f"http://{cplane_mgmt_api_server.host}:{cplane_mgmt_api_server.port}/storage/api/v1/"
    )

    if neon_env_builder.storage_controller_config is None:
        neon_env_builder.storage_controller_config = {}
    neon_env_builder.storage_controller_config["timelines_onto_safekeepers"] = True

    env = neon_env_builder.init_start()

    # The test needs LocalFs support, which is only built in testing mode.
    env.pageserver.is_testing_enabled_or_skip()

    env.pageserver.stop()
    env.pageserver.start()

    # By default our tests run with a tiny shared_buffers=1MB setting. That
    # doesn't allow any prefetching on v17 and above, where the new streaming
    # read machinery keeps buffers pinned while prefetching them.  Use a higher
    # setting to enable prefetching and speed up the tests
    # use shared_buffers size like in production for 8 CU compute
    ep_config = [f"shared_buffers={shared_buffers_for_max_cu(8.0)}"]

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
        segment_size = 16 * 1024 * 1024
        target_relblock_size = segment_size * 8
    else:
        raise ValueError

    # fillfactor so we don't need to produce that much data
    # 900 byte per row is > 10% => 1 row per page
    vanilla_pg.safe_psql("""create table t (data char(900)) with (fillfactor = 10)""")

    nrows = 0
    while True:
        relblock_size = vanilla_pg.safe_psql_scalar("select pg_relation_size('t')")
        log.info(
            f"relblock size: {relblock_size / 8192} pages (target: {target_relblock_size // 8192}) pages"
        )
        if relblock_size >= target_relblock_size:
            break
        addrows = int((target_relblock_size - relblock_size) // 8192)
        assert addrows >= 1, "forward progress"
        vanilla_pg.safe_psql(
            f"insert into t select generate_series({nrows + 1}, {nrows + addrows})"
        )
        nrows += addrows
    expect_nrows = nrows
    expect_sum = (
        (nrows) * (nrows + 1) // 2
    )  # https://stackoverflow.com/questions/43901484/sum-of-the-integers-from-1-to-n

    def validate_vanilla_equivalence(ep):
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

    validate_vanilla_equivalence(vanilla_pg)

    vanilla_pg.stop()

    #
    # We have a Postgres data directory now.
    # Make a localfs remote storage that looks like how after `fast_import` ran.
    # TODO: actually exercise fast_import here
    # TODO: test s3 remote storage
    #
    importbucket_path = neon_env_builder.repo_dir / "importbucket"
    mock_import_bucket(vanilla_pg, importbucket_path)

    #
    # Do the import
    #

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id, shard_count=shard_count, shard_stripe_size=stripe_size
    )

    timeline_id = TimelineId.generate()
    log.info("starting import")
    start = time.monotonic()

    idempotency = ImportPgdataIdemptencyKey.random()
    log.info(f"idempotency key {idempotency}")
    # TODO: teach neon_local CLI about the idempotency & 429 error so we can run inside the loop
    # and check for 429

    import_branch_name = "imported"
    env.storage_controller.timeline_create(
        tenant_id,
        {
            "new_timeline_id": str(timeline_id),
            "import_pgdata": {
                "idempotency_key": str(idempotency),
                "location": {"LocalFs": {"path": str(importbucket_path.absolute())}},
            },
        },
    )
    env.neon_cli.mappings_map_branch(import_branch_name, tenant_id, timeline_id)

    def cplane_notified():
        assert import_completion_signaled.is_set()

    # Generous timeout for the MULTIPLE_RELATION_SEGMENTS test variants
    wait_until(cplane_notified, timeout=90)

    import_duration = time.monotonic() - start
    log.info(f"import complete; duration={import_duration:.2f}s")

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
    min_readable_lsn = Lsn(shard_zero_timeline_info["min_readable_lsn"])
    last_record_lsn = Lsn(shard_zero_timeline_info["last_record_lsn"])
    disk_consistent_lsn = Lsn(shard_zero_timeline_info["disk_consistent_lsn"])
    _remote_consistent_lsn = Lsn(shard_zero_timeline_info["remote_consistent_lsn"])
    remote_consistent_lsn_visible = Lsn(shard_zero_timeline_info["remote_consistent_lsn_visible"])
    # assert remote_consistent_lsn_visible == remote_consistent_lsn TODO: this fails initially and after restart, presumably because `UploadQueue::clean.1` is still `None`
    assert remote_consistent_lsn_visible == disk_consistent_lsn
    assert initdb_lsn == min_readable_lsn
    assert disk_consistent_lsn == initdb_lsn + 8
    assert last_record_lsn == disk_consistent_lsn
    # TODO: assert these values are the same everywhere

    #
    # Validate the resulting remote storage state.
    #

    #
    # Validate the imported data
    #

    ro_endpoint = env.endpoints.create_start(
        branch_name=import_branch_name,
        endpoint_id="ro",
        tenant_id=tenant_id,
        lsn=last_record_lsn,
        config_lines=ep_config,
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
    for shard in shards:
        shard_ps = env.get_pageserver(shard["node_id"])
        result = shard_ps.timeline_scan_no_disposable_keys(shard["shard_id"], timeline_id)
        assert result.tally.disposable_count == 0
        assert result.tally.not_disposable_count > 0, (
            "sanity check, each shard should have some data"
        )

    #
    # validate that we can write
    #
    workload = Workload(env, tenant_id, timeline_id, branch_name=import_branch_name)
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
        tenant_id=tenant_id,
        ancestor_start_lsn=rw_lsn,
    )
    child_workload = workload.branch(timeline_id=child_timeline_id, branch_name="br-tip")
    child_workload.validate()

    validate_vanilla_equivalence(child_workload.endpoint())

    # ... at the initdb lsn
    _ = env.create_branch(
        new_branch_name="br-initdb",
        ancestor_branch_name=import_branch_name,
        tenant_id=tenant_id,
        ancestor_start_lsn=initdb_lsn,
    )
    br_initdb_endpoint = env.endpoints.create_start(
        branch_name="br-initdb",
        endpoint_id="br-initdb-ro",
        tenant_id=tenant_id,
        config_lines=ep_config,
    )
    validate_vanilla_equivalence(br_initdb_endpoint)
    with pytest.raises(psycopg2.errors.UndefinedTable):
        br_initdb_endpoint.safe_psql(f"select * from {workload.table}")


@run_only_on_default_postgres(reason="PG version is irrelevant here")
def test_import_completion_on_restart(
    neon_env_builder: NeonEnvBuilder, vanilla_pg: VanillaPostgres, make_httpserver: HTTPServer
):
    """
    Validate that the storage controller delivers the import completion notification
    eventually even if it was restarted when the import initially completed.
    """
    # Set up mock control plane HTTP server to listen for import completions
    import_completion_signaled = Event()

    def handler(request: Request) -> Response:
        log.info(f"control plane /import_complete request: {request.json}")
        import_completion_signaled.set()
        return Response(json.dumps({}), status=200)

    cplane_mgmt_api_server = make_httpserver
    cplane_mgmt_api_server.expect_request(
        "/storage/api/v1/import_complete", method="PUT"
    ).respond_with_handler(handler)

    # Plug the cplane mock in
    neon_env_builder.control_plane_hooks_api = (
        f"http://{cplane_mgmt_api_server.host}:{cplane_mgmt_api_server.port}/storage/api/v1/"
    )

    # The import will specifiy a local filesystem path mocking remote storage
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    vanilla_pg.start()
    vanilla_pg.stop()

    env = neon_env_builder.init_configs()
    env.start()

    importbucket_path = neon_env_builder.repo_dir / "test_import_completion_bucket"
    mock_import_bucket(vanilla_pg, importbucket_path)

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    idempotency = ImportPgdataIdemptencyKey.random()

    # Pause before sending the notification
    failpoint_name = "timeline-import-pre-cplane-notification"
    env.storage_controller.configure_failpoints((failpoint_name, "pause"))

    env.storage_controller.tenant_create(tenant_id)
    env.storage_controller.timeline_create(
        tenant_id,
        {
            "new_timeline_id": str(timeline_id),
            "import_pgdata": {
                "idempotency_key": str(idempotency),
                "location": {"LocalFs": {"path": str(importbucket_path.absolute())}},
            },
        },
    )

    def hit_failpoint():
        log.info("Checking log for pattern...")
        try:
            assert env.storage_controller.log_contains(f".*at failpoint {failpoint_name}.*")
        except Exception:
            log.exception("Failed to find pattern in log")
            raise

    wait_until(hit_failpoint)
    assert not import_completion_signaled.is_set()

    # Restart the storage controller before signalling control plane.
    # This clears the failpoint and we expect that the import start-up reconciliation
    # kicks in and notifies cplane.
    env.storage_controller.stop()
    env.storage_controller.start()

    def cplane_notified():
        assert import_completion_signaled.is_set()

    wait_until(cplane_notified)


@run_only_on_default_postgres(reason="PG version is irrelevant here")
def test_import_respects_tenant_shutdown(
    neon_env_builder: NeonEnvBuilder, vanilla_pg: VanillaPostgres, make_httpserver: HTTPServer
):
    """
    Validate that importing timelines respect the usual timeline life cycle:
    1. Shut down on tenant shut-down and resumes upon re-attach
    2. Deletion on timeline deletion (TODO)
    """
    # Set up mock control plane HTTP server to listen for import completions
    import_completion_signaled = Event()

    def handler(request: Request) -> Response:
        log.info(f"control plane /import_complete request: {request.json}")
        import_completion_signaled.set()
        return Response(json.dumps({}), status=200)

    cplane_mgmt_api_server = make_httpserver
    cplane_mgmt_api_server.expect_request(
        "/storage/api/v1/import_complete", method="PUT"
    ).respond_with_handler(handler)

    # Plug the cplane mock in
    neon_env_builder.control_plane_hooks_api = (
        f"http://{cplane_mgmt_api_server.host}:{cplane_mgmt_api_server.port}/storage/api/v1/"
    )

    # The import will specifiy a local filesystem path mocking remote storage
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    vanilla_pg.start()
    vanilla_pg.stop()

    env = neon_env_builder.init_configs()
    env.start()

    importbucket_path = neon_env_builder.repo_dir / "test_import_completion_bucket"
    mock_import_bucket(vanilla_pg, importbucket_path)

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    idempotency = ImportPgdataIdemptencyKey.random()

    # Pause before sending the notification
    failpoint_name = "import-timeline-pre-execute-pausable"
    env.pageserver.http_client().configure_failpoints((failpoint_name, "pause"))

    env.storage_controller.tenant_create(tenant_id)
    env.storage_controller.timeline_create(
        tenant_id,
        {
            "new_timeline_id": str(timeline_id),
            "import_pgdata": {
                "idempotency_key": str(idempotency),
                "location": {"LocalFs": {"path": str(importbucket_path.absolute())}},
            },
        },
    )

    def hit_failpoint():
        log.info("Checking log for pattern...")
        try:
            assert env.pageserver.log_contains(f".*at failpoint {failpoint_name}.*")
        except Exception:
            log.exception("Failed to find pattern in log")
            raise

    wait_until(hit_failpoint)
    assert not import_completion_signaled.is_set()

    # Restart the pageserver while an import job is in progress.
    # This clears the failpoint and we expect that the import starts up afresh
    # after the restart and eventually completes.
    env.pageserver.stop()
    env.pageserver.start()

    def cplane_notified():
        assert import_completion_signaled.is_set()

    wait_until(cplane_notified)


def test_fast_import_with_pageserver_ingest(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    mock_s3_server: MockS3Server,
    mock_kms: KMSClient,
    mock_s3_client: S3Client,
    neon_env_builder: NeonEnvBuilder,
    make_httpserver: HTTPServer,
):
    # Prepare KMS and S3
    key_response = mock_kms.create_key(
        Description="Test key",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )
    key_id = key_response["KeyMetadata"]["KeyId"]

    def encrypt(x: str) -> EncryptResponseTypeDef:
        return mock_kms.encrypt(KeyId=key_id, Plaintext=x)

    # Start source postgres and ingest data
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    # Setup pageserver and fake cplane for import progress
    import_completion_signaled = Event()

    def handler(request: Request) -> Response:
        log.info(f"control plane /import_complete request: {request.json}")
        import_completion_signaled.set()
        return Response(json.dumps({}), status=200)

    cplane_mgmt_api_server = make_httpserver
    cplane_mgmt_api_server.expect_request(
        "/storage/api/v1/import_complete", method="PUT"
    ).respond_with_handler(handler)

    neon_env_builder.control_plane_hooks_api = (
        f"http://{cplane_mgmt_api_server.host}:{cplane_mgmt_api_server.port}/storage/api/v1/"
    )

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)
    env = neon_env_builder.init_start()

    env.pageserver.patch_config_toml_nonrecursive(
        {
            # because import_pgdata code uses this endpoint, not the one in common remote storage config
            # TODO: maybe use common remote_storage config in pageserver?
            "import_pgdata_aws_endpoint_url": env.s3_mock_server.endpoint(),
        }
    )
    env.pageserver.stop()
    env.pageserver.start()

    # Encrypt connstrings and put spec into S3
    source_connstring_encrypted = encrypt(vanilla_pg.connstr())
    spec = {
        "encryption_secret": {"KMS": {"key_id": key_id}},
        "source_connstring_ciphertext_base64": base64.b64encode(
            source_connstring_encrypted["CiphertextBlob"]
        ).decode("utf-8"),
        "project_id": "someproject",
        "branch_id": "somebranch",
    }

    bucket = "test-bucket"
    key_prefix = "test-prefix"
    mock_s3_client.create_bucket(Bucket=bucket)
    mock_s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/spec.json", Body=json.dumps(spec))

    # Create timeline with import_pgdata
    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(tenant_id)

    timeline_id = TimelineId.generate()
    log.info("starting import")
    start = time.monotonic()

    idempotency = ImportPgdataIdemptencyKey.random()
    log.info(f"idempotency key {idempotency}")
    # TODO: teach neon_local CLI about the idempotency & 429 error so we can run inside the loop
    # and check for 429

    import_branch_name = "imported"
    env.storage_controller.timeline_create(
        tenant_id,
        {
            "new_timeline_id": str(timeline_id),
            "import_pgdata": {
                "idempotency_key": str(idempotency),
                "location": {
                    "AwsS3": {
                        "region": env.s3_mock_server.region(),
                        "bucket": bucket,
                        "key": key_prefix,
                    }
                },
            },
        },
    )
    env.neon_cli.mappings_map_branch(import_branch_name, tenant_id, timeline_id)

    # Run fast_import
    fast_import.set_aws_creds(
        mock_s3_server, {"RUST_LOG": "info,aws_config=debug,aws_sdk_kms=debug"}
    )
    pg_port = port_distributor.get_port()
    fast_import.run_pgdata(pg_port=pg_port, s3prefix=f"s3://{bucket}/{key_prefix}")

    pgdata_status_obj = mock_s3_client.get_object(Bucket=bucket, Key=f"{key_prefix}/status/pgdata")
    pgdata_status = pgdata_status_obj["Body"].read().decode("utf-8")
    assert json.loads(pgdata_status) == {"done": True}, f"got status: {pgdata_status}"

    job_status_obj = mock_s3_client.get_object(
        Bucket=bucket, Key=f"{key_prefix}/status/fast_import"
    )
    job_status = job_status_obj["Body"].read().decode("utf-8")
    assert json.loads(job_status) == {
        "command": "pgdata",
        "done": True,
    }, f"got status: {job_status}"

    vanilla_pg.stop()

    def validate_vanilla_equivalence(ep):
        res = ep.safe_psql("SELECT count(*), sum(a) FROM foo;", dbname="neondb")
        assert res[0] == (10, 55), f"got result: {res}"

    # Sanity check that data in pgdata is expected:
    pgbin = PgBin(test_output_dir, fast_import.pg_distrib_dir, fast_import.pg_version)
    with VanillaPostgres(
        fast_import.workdir / "pgdata", pgbin, pg_port, False
    ) as new_pgdata_vanilla_pg:
        new_pgdata_vanilla_pg.start()

        # database name and user are hardcoded in fast_import binary, and they are different from normal vanilla postgres
        conn = PgProtocol(dsn=f"postgresql://cloud_admin@localhost:{pg_port}/neondb")
        validate_vanilla_equivalence(conn)

    def cplane_notified():
        assert import_completion_signaled.is_set()

    wait_until(cplane_notified, timeout=60)

    import_duration = time.monotonic() - start
    log.info(f"import complete; duration={import_duration:.2f}s")

    ep = env.endpoints.create_start(branch_name=import_branch_name, tenant_id=tenant_id)

    # check that data is there
    validate_vanilla_equivalence(ep)

    # check that we can do basic ops

    ep.safe_psql("create table othertable(values text)", dbname="neondb")
    rw_lsn = Lsn(ep.safe_psql_scalar("select pg_current_wal_flush_lsn()"))
    ep.stop()

    # ... at the tip
    _ = env.create_branch(
        new_branch_name="br-tip",
        ancestor_branch_name=import_branch_name,
        tenant_id=tenant_id,
        ancestor_start_lsn=rw_lsn,
    )
    br_tip_endpoint = env.endpoints.create_start(
        branch_name="br-tip", endpoint_id="br-tip-ro", tenant_id=tenant_id
    )
    validate_vanilla_equivalence(br_tip_endpoint)
    br_tip_endpoint.safe_psql("select * from othertable", dbname="neondb")
    br_tip_endpoint.stop()

    # ... at the initdb lsn
    locations = env.storage_controller.locate(tenant_id)
    [shard_zero] = [
        loc for loc in locations if TenantShardId.parse(loc["shard_id"]).shard_number == 0
    ]
    shard_zero_ps = env.get_pageserver(shard_zero["node_id"])
    shard_zero_timeline_info = shard_zero_ps.http_client().timeline_detail(
        shard_zero["shard_id"], timeline_id
    )
    initdb_lsn = Lsn(shard_zero_timeline_info["initdb_lsn"])
    _ = env.create_branch(
        new_branch_name="br-initdb",
        ancestor_branch_name=import_branch_name,
        tenant_id=tenant_id,
        ancestor_start_lsn=initdb_lsn,
    )
    br_initdb_endpoint = env.endpoints.create_start(
        branch_name="br-initdb", endpoint_id="br-initdb-ro", tenant_id=tenant_id
    )
    validate_vanilla_equivalence(br_initdb_endpoint)
    with pytest.raises(psycopg2.errors.UndefinedTable):
        br_initdb_endpoint.safe_psql("select * from othertable", dbname="neondb")
    br_initdb_endpoint.stop()

    env.pageserver.stop(immediate=True)


def test_fast_import_binary(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
):
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    pg_port = port_distributor.get_port()
    fast_import.run_pgdata(pg_port=pg_port, source_connection_string=vanilla_pg.connstr())
    vanilla_pg.stop()

    pgbin = PgBin(test_output_dir, fast_import.pg_distrib_dir, fast_import.pg_version)
    with VanillaPostgres(
        fast_import.workdir / "pgdata", pgbin, pg_port, False
    ) as new_pgdata_vanilla_pg:
        new_pgdata_vanilla_pg.start()

        # database name and user are hardcoded in fast_import binary, and they are different from normal vanilla postgres
        conn = PgProtocol(dsn=f"postgresql://cloud_admin@localhost:{pg_port}/neondb")
        res = conn.safe_psql("SELECT count(*) FROM foo;")
        log.info(f"Result: {res}")
        assert res[0][0] == 10


def test_fast_import_event_triggers(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
):
    vanilla_pg.start()
    vanilla_pg.safe_psql("""
        CREATE FUNCTION test_event_trigger_for_drops()
                RETURNS event_trigger LANGUAGE plpgsql AS $$
        DECLARE
            obj record;
        BEGIN
            FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
            LOOP
                RAISE NOTICE '% dropped object: % %.% %',
                            tg_tag,
                            obj.object_type,
                            obj.schema_name,
                            obj.object_name,
                            obj.object_identity;
            END LOOP;
        END
        $$;

        CREATE EVENT TRIGGER test_event_trigger_for_drops
        ON sql_drop
        EXECUTE PROCEDURE test_event_trigger_for_drops();
    """)

    pg_port = port_distributor.get_port()
    p = fast_import.run_pgdata(pg_port=pg_port, source_connection_string=vanilla_pg.connstr())
    assert p.returncode == 0

    vanilla_pg.stop()

    pgbin = PgBin(test_output_dir, fast_import.pg_distrib_dir, fast_import.pg_version)
    with VanillaPostgres(
        fast_import.workdir / "pgdata", pgbin, pg_port, False
    ) as new_pgdata_vanilla_pg:
        new_pgdata_vanilla_pg.start()

        # database name and user are hardcoded in fast_import binary, and they are different from normal vanilla postgres
        conn = PgProtocol(dsn=f"postgresql://cloud_admin@localhost:{pg_port}/neondb")
        res = conn.safe_psql("SELECT count(*) FROM pg_event_trigger;")
        log.info(f"Result: {res}")
        assert res[0][0] == 0, f"Neon does not support importing event triggers, got: {res[0][0]}"


def test_fast_import_restore_to_connstring(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
):
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    pgdatadir = test_output_dir / "destination-pgdata"
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)
    port = port_distributor.get_port()
    with VanillaPostgres(pgdatadir, pg_bin, port) as destination_vanilla_pg:
        destination_vanilla_pg.configure(["shared_preload_libraries='neon_rmgr'"])
        destination_vanilla_pg.start()

        # create another database & role and try to restore there
        destination_vanilla_pg.safe_psql("""
            CREATE ROLE testrole WITH
                LOGIN
                PASSWORD 'testpassword'
                NOSUPERUSER
                NOCREATEDB
                NOCREATEROLE;
        """)
        destination_vanilla_pg.safe_psql("CREATE DATABASE testdb OWNER testrole;")

        destination_connstring = destination_vanilla_pg.connstr(
            dbname="testdb", user="testrole", password="testpassword"
        )
        fast_import.run_dump_restore(
            source_connection_string=vanilla_pg.connstr(),
            destination_connection_string=destination_connstring,
        )
        vanilla_pg.stop()
        conn = PgProtocol(dsn=destination_connstring)
        res = conn.safe_psql("SELECT count(*) FROM foo;")
        log.info(f"Result: {res}")
        assert res[0][0] == 10


def test_fast_import_restore_to_connstring_from_s3_spec(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    mock_s3_server: MockS3Server,
    mock_kms: KMSClient,
    mock_s3_client: S3Client,
):
    # Prepare KMS and S3
    key_response = mock_kms.create_key(
        Description="Test key",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )
    key_id = key_response["KeyMetadata"]["KeyId"]

    def encrypt(x: str) -> EncryptResponseTypeDef:
        return mock_kms.encrypt(KeyId=key_id, Plaintext=x)

    # Start source postgres and ingest data
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    # Start target postgres
    pgdatadir = test_output_dir / "destination-pgdata"
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)
    port = port_distributor.get_port()
    with VanillaPostgres(pgdatadir, pg_bin, port) as destination_vanilla_pg:
        destination_vanilla_pg.configure(["shared_preload_libraries='neon_rmgr'"])
        destination_vanilla_pg.start()

        # Encrypt connstrings and put spec into S3
        source_connstring_encrypted = encrypt(vanilla_pg.connstr())
        destination_connstring_encrypted = encrypt(destination_vanilla_pg.connstr())
        spec = {
            "encryption_secret": {"KMS": {"key_id": key_id}},
            "source_connstring_ciphertext_base64": base64.b64encode(
                source_connstring_encrypted["CiphertextBlob"]
            ).decode("utf-8"),
            "destination_connstring_ciphertext_base64": base64.b64encode(
                destination_connstring_encrypted["CiphertextBlob"]
            ).decode("utf-8"),
        }

        bucket = "test-bucket"
        key_prefix = "test-prefix"
        mock_s3_client.create_bucket(Bucket=bucket)
        mock_s3_client.put_object(
            Bucket=bucket, Key=f"{key_prefix}/spec.json", Body=json.dumps(spec)
        )

        # Run fast_import
        fast_import.set_aws_creds(
            mock_s3_server, {"RUST_LOG": "aws_config=debug,aws_sdk_kms=debug"}
        )
        fast_import.run_dump_restore(s3prefix=f"s3://{bucket}/{key_prefix}")

        job_status_obj = mock_s3_client.get_object(
            Bucket=bucket, Key=f"{key_prefix}/status/fast_import"
        )
        job_status = job_status_obj["Body"].read().decode("utf-8")
        assert json.loads(job_status) == {
            "done": True,
            "command": "dump-restore",
        }, f"got status: {job_status}"
        vanilla_pg.stop()

        res = destination_vanilla_pg.safe_psql("SELECT count(*) FROM foo;")
        log.info(f"Result: {res}")
        assert res[0][0] == 10


def test_fast_import_restore_to_connstring_error_to_s3_bad_destination(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    mock_s3_server: MockS3Server,
    mock_kms: KMSClient,
    mock_s3_client: S3Client,
):
    # Prepare KMS and S3
    key_response = mock_kms.create_key(
        Description="Test key",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )
    key_id = key_response["KeyMetadata"]["KeyId"]

    def encrypt(x: str) -> EncryptResponseTypeDef:
        return mock_kms.encrypt(KeyId=key_id, Plaintext=x)

    # Start source postgres and ingest data
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    # Encrypt connstrings and put spec into S3
    source_connstring_encrypted = encrypt(vanilla_pg.connstr())
    destination_connstring_encrypted = encrypt("postgres://random:connection@string:5432/neondb")
    spec = {
        "encryption_secret": {"KMS": {"key_id": key_id}},
        "source_connstring_ciphertext_base64": base64.b64encode(
            source_connstring_encrypted["CiphertextBlob"]
        ).decode("utf-8"),
        "destination_connstring_ciphertext_base64": base64.b64encode(
            destination_connstring_encrypted["CiphertextBlob"]
        ).decode("utf-8"),
    }

    bucket = "test-bucket"
    key_prefix = "test-prefix"
    mock_s3_client.create_bucket(Bucket=bucket)
    mock_s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/spec.json", Body=json.dumps(spec))

    # Run fast_import
    fast_import.set_aws_creds(mock_s3_server, {"RUST_LOG": "aws_config=debug,aws_sdk_kms=debug"})
    fast_import.run_dump_restore(s3prefix=f"s3://{bucket}/{key_prefix}")

    job_status_obj = mock_s3_client.get_object(
        Bucket=bucket, Key=f"{key_prefix}/status/fast_import"
    )
    job_status = job_status_obj["Body"].read().decode("utf-8")
    assert json.loads(job_status) == {
        "command": "dump-restore",
        "done": False,
        "error": "pg_restore failed",
    }, f"got status: {job_status}"
    vanilla_pg.stop()


def test_fast_import_restore_to_connstring_error_to_s3_kms_error(
    test_output_dir,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    mock_s3_server: MockS3Server,
    mock_kms: KMSClient,
    mock_s3_client: S3Client,
):
    # Prepare KMS and S3
    key_response = mock_kms.create_key(
        Description="Test key",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )
    key_id = key_response["KeyMetadata"]["KeyId"]

    def encrypt(x: str) -> EncryptResponseTypeDef:
        return mock_kms.encrypt(KeyId=key_id, Plaintext=x)

    # Encrypt connstrings and put spec into S3
    spec = {
        "encryption_secret": {"KMS": {"key_id": key_id}},
        "source_connstring_ciphertext_base64": base64.b64encode(b"invalid encrypted string").decode(
            "utf-8"
        ),
    }

    bucket = "test-bucket"
    key_prefix = "test-prefix"
    mock_s3_client.create_bucket(Bucket=bucket)
    mock_s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/spec.json", Body=json.dumps(spec))

    # Run fast_import
    fast_import.set_aws_creds(mock_s3_server, {"RUST_LOG": "aws_config=debug,aws_sdk_kms=debug"})
    fast_import.run_dump_restore(s3prefix=f"s3://{bucket}/{key_prefix}")

    job_status_obj = mock_s3_client.get_object(
        Bucket=bucket, Key=f"{key_prefix}/status/fast_import"
    )
    job_status = job_status_obj["Body"].read().decode("utf-8")
    assert json.loads(job_status) == {
        "command": "dump-restore",
        "done": False,
        "error": "decrypt source connection string",
    }, f"got status: {job_status}"
