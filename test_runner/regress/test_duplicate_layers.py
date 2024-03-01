import time

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_for_upload_queue_empty,
    wait_until_tenant_active,
)
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from requests.exceptions import ConnectionError


def test_duplicate_layers(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # use a failpoint to return all L0s as L1s
    message = ".*duplicated L1 layer layer=.*"
    env.pageserver.allowed_errors.append(message)

    # Use aggressive compaction and checkpoint settings
    tenant_id, _ = env.neon_cli.create_tenant(
        conf={
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            "compaction_period": "5 s",
            "compaction_threshold": "3",
        }
    )

    pageserver_http.configure_failpoints(("compact-level0-phase1-return-same", "return"))

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    connstr = endpoint.connstr(options="-csynchronous_commit=off")
    pg_bin.run_capture(["pgbench", "-i", "-s1", connstr])

    time.sleep(10)  # let compaction to be performed
    env.pageserver.assert_log_contains("compact-level0-phase1-return-same")


def test_actually_duplicated_l1(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Test sets fail point at the end of first compaction phase: after
    flushing new L1 layer but before deletion of L0 layers.

    The L1 used to be overwritten, but with crash-consistency via remote
    index_part.json, we end up deleting the not yet uploaded L1 layer on
    startup.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            "compaction_period": "0 s",
            "compaction_threshold": "3",
        }
    )
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.initial_tenant, env.initial_timeline

    pageserver_http.configure_failpoints(("after-timeline-compacted-first-L1", "exit"))

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    connstr = endpoint.connstr(options="-csynchronous_commit=off")
    pg_bin.run_capture(["pgbench", "-i", "-s1", connstr])

    lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop()

    # make sure we receive no new wal after this, so that we'll write over the same L1 file.
    endpoint.stop()
    for sk in env.safekeepers:
        sk.stop()

    # hit the exit failpoint
    with pytest.raises(ConnectionError, match="Remote end closed connection without response"):
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    env.pageserver.stop()

    # now the duplicate L1 has been created, but is not yet uploaded
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    # path = env.remote_storage.timeline_path(tenant_id, timeline_id)
    l1_found = None
    for path in env.pageserver.timeline_dir(tenant_id, timeline_id).iterdir():
        if path.name == "metadata" or path.name.startswith("ephemeral-"):
            continue

        if len(path.suffixes) > 0:
            # temp files
            continue

        [key_range, lsn_range] = path.name.split("__", maxsplit=1)

        if "-" not in lsn_range:
            # image layer
            continue

        [key_start, key_end] = key_range.split("-", maxsplit=1)

        if key_start == "0" * 36 and key_end == "F" * 36:
            # L0
            continue

        if l1_found is not None:
            raise RuntimeError(f"found multiple L1: {l1_found.name} and {path.name}")
        l1_found = path

    assert l1_found is not None, "failed to find L1 locally"

    uploaded = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, l1_found.name
    )
    assert not uploaded.exists(), "to-be-overwritten should not yet be uploaded"

    env.pageserver.start()
    wait_until_tenant_active(pageserver_http, tenant_id)

    assert not l1_found.exists(), "partial compaction result should had been removed during startup"

    # wait for us to catch up again
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, lsn)

    pageserver_http.timeline_compact(tenant_id, timeline_id)

    # give time for log flush
    time.sleep(1)

    message = f".*duplicated L1 layer layer={l1_found.name}"
    found_msg = env.pageserver.log_contains(message)
    # resident or evicted, it should not be overwritten, however it should had been non-existing at startup
    assert (
        found_msg is None
    ), "layer should had been removed during startup, did it live on as evicted?"

    assert l1_found.exists(), "the L1 reappears"

    wait_for_upload_queue_empty(pageserver_http, tenant_id, timeline_id)

    uploaded = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, l1_found.name
    )
    assert uploaded.exists(), "the L1 is uploaded"
