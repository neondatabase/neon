import os
import pprint
import shutil
from typing import Optional

import pytest
from fixtures.common_types import TenantId, TenantShardId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    StorageScrubber,
)
from fixtures.pageserver.utils import (
    list_prefix,
    remote_storage_delete_key,
    remote_storage_download_index_part,
    remote_storage_get_lastest_index_key,
)
from fixtures.remote_storage import S3Storage, s3_storage
from fixtures.workload import Workload


@pytest.mark.parametrize("shard_count", [None, 4])
def test_scrubber_tenant_snapshot(neon_env_builder: NeonEnvBuilder, shard_count: Optional[int]):
    """
    Test the `tenant-snapshot` subcommand, which grabs data from remote storage

    This is only a support/debug tool, but worth testing to ensure the tool does not regress.
    """

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = shard_count if shard_count is not None else 1

    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    branch = "main"

    # Do some work
    workload = Workload(env, tenant_id, timeline_id, branch)
    workload.init()

    # Multiple write/flush passes to generate multiple layers
    for _n in range(0, 3):
        workload.write_rows(128)

    # Do some more work after a restart, so that we have multiple generations
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    for _n in range(0, 3):
        workload.write_rows(128)

    # If we're doing multiple shards, split: this is important to exercise
    # the scrubber's ability to understand the references from child shards to parent shard's layers
    if shard_count is not None:
        tenant_shard_ids = env.storage_controller.tenant_shard_split(
            tenant_id, shard_count=shard_count
        )

        # Write after shard split: this will result in shards containing a mixture of owned
        # and parent layers in their index.
        workload.write_rows(128)
    else:
        tenant_shard_ids = [TenantShardId(tenant_id, 0, 0)]

    output_path = neon_env_builder.test_output_dir / "snapshot"
    os.makedirs(output_path)

    scrubber = StorageScrubber(neon_env_builder)
    scrubber.tenant_snapshot(tenant_id, output_path)

    assert len(os.listdir(output_path)) > 0

    workload.stop()

    # Stop pageservers
    for pageserver in env.pageservers:
        pageserver.stop()

    # Drop all shards' local storage
    for tenant_shard_id in tenant_shard_ids:
        pageserver = env.get_tenant_pageserver(tenant_shard_id)
        shutil.rmtree(pageserver.timeline_dir(tenant_shard_id, timeline_id))

    # Replace remote storage contents with the snapshot we downloaded
    assert isinstance(env.pageserver_remote_storage, S3Storage)

    remote_tenant_path = env.pageserver_remote_storage.tenant_path(tenant_id)

    # Delete current remote storage contents
    bucket = env.pageserver_remote_storage.bucket_name
    remote_client = env.pageserver_remote_storage.client
    deleted = 0
    for object in remote_client.list_objects_v2(Bucket=bucket, Prefix=remote_tenant_path)[
        "Contents"
    ]:
        key = object["Key"]
        remote_client.delete_object(Key=key, Bucket=bucket)
        deleted += 1
    assert deleted > 0

    # Upload from snapshot
    for root, _dirs, files in os.walk(output_path):
        for file in files:
            full_local_path = os.path.join(root, file)
            full_remote_path = (
                env.pageserver_remote_storage.tenants_path()
                + "/"
                + full_local_path.removeprefix(f"{output_path}/")
            )
            remote_client.upload_file(full_local_path, bucket, full_remote_path)

    for pageserver in env.pageservers:
        pageserver.start()

    # Check we can read everything
    workload.validate()


@pytest.mark.parametrize("shard_count", [None, 4])
def test_scrubber_physical_gc(neon_env_builder: NeonEnvBuilder, shard_count: Optional[int]):
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.neon_cli.create_tenant(tenant_id, timeline_id, shard_count=shard_count)

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()

    # We will end up with an index per shard, per cycle, plus one for the initial startup
    n_cycles = 4
    expect_indices_per_shard = n_cycles + 1
    shard_count = 1 if shard_count is None else shard_count

    # For each cycle, detach and attach the tenant to bump the generation, and do some writes to generate uploads
    for _i in range(0, n_cycles):
        env.storage_controller.tenant_policy_update(tenant_id, {"placement": "Detached"})
        env.storage_controller.reconcile_until_idle()

        env.storage_controller.tenant_policy_update(tenant_id, {"placement": {"Attached": 0}})
        env.storage_controller.reconcile_until_idle()

        # This write includes remote upload, will generate an index in this generation
        workload.write_rows(1)

    # With a high min_age, the scrubber should decline to delete anything
    gc_summary = StorageScrubber(neon_env_builder).pageserver_physical_gc(min_age_secs=3600)
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0

    # If targeting a different tenant, the scrubber shouldn't do anything
    gc_summary = StorageScrubber(neon_env_builder).pageserver_physical_gc(
        min_age_secs=1, tenant_ids=[TenantId.generate()]
    )
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0

    #  With a low min_age, the scrubber should go ahead and clean up all but the latest 2 generations
    gc_summary = StorageScrubber(neon_env_builder).pageserver_physical_gc(min_age_secs=1)
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == (expect_indices_per_shard - 2) * shard_count


@pytest.mark.parametrize("shard_count", [None, 4])
def test_scrubber_scan_pageserver_metadata(
    neon_env_builder: NeonEnvBuilder, shard_count: Optional[int]
):
    """
    Create some layers. Delete an object listed in index. Run scrubber and see if it detects the defect.
    """

    # Use s3_storage so we could test out scrubber.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = shard_count if shard_count is not None else 1
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    # Create some layers.

    workload = Workload(env, env.initial_tenant, env.initial_timeline)
    workload.init()

    for _ in range(3):
        workload.write_rows(128)

    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    for _ in range(3):
        workload.write_rows(128)

    # Get the latest index for a particular timeline.

    tenant_shard_id = TenantShardId(env.initial_tenant, 0, shard_count if shard_count else 0)

    assert isinstance(env.pageserver_remote_storage, S3Storage)
    timeline_path = env.pageserver_remote_storage.timeline_path(
        tenant_shard_id, env.initial_timeline
    )
    objects = list_prefix(env.pageserver_remote_storage, prefix=f"{timeline_path}/").get(
        "Contents", []
    )
    keys = [obj["Key"] for obj in objects]
    index_keys = list(filter(lambda s: s.startswith(f"/{timeline_path}/index_part"), keys))

    latest_index_key = remote_storage_get_lastest_index_key(index_keys)
    log.info(f"{latest_index_key=}")

    index = remote_storage_download_index_part(env.pageserver_remote_storage, latest_index_key)

    assert len(index.layer_metadata) > 0
    it = iter(index.layer_metadata.items())

    # Delete a layer file that is listed in the index.
    layer, metadata = next(it)
    log.info(f"Deleting {timeline_path}/{layer.to_str()}")
    delete_response = remote_storage_delete_key(
        env.pageserver_remote_storage,
        f"{timeline_path}/{layer.to_str()}-{metadata.generation:08x}",
    )
    log.info(f"delete response: {delete_response}")

    # Check scan summary. Expect it to be a L0 layer so only emit warnings.
    scan_summary = StorageScrubber(neon_env_builder).scan_metadata()
    log.info(f"{pprint.pformat(scan_summary)}")
    assert len(scan_summary["with_warnings"]) > 0
