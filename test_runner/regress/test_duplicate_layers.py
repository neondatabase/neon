import time

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin
from fixtures.pageserver.utils import wait_for_upload_queue_empty
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from requests.exceptions import ConnectionError


@pytest.mark.timeout(600)
def test_duplicate_layers(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

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
    assert env.pageserver.log_contains("compact-level0-phase1-return-same")

    pg_bin.run_capture(["pgbench", "-P1", "-N", "-c5", "-T200", "-Mprepared", connstr])


def test_actually_duplicated_l1(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    This test sets fail point at the end of first compaction phase:
    after flushing new L1 layers but before deletion of L0 layers
    it should cause generation of duplicate L1 layer by compaction after restart.
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

    # hit the exit failpoint
    with pytest.raises(ConnectionError, match="Remote end closed connection without response"):
        pageserver_http.timeline_compact(tenant_id, timeline_id)
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

        assert l1_found is None, f"found multiple L1: {l1_found.name} and {path.name}"
        l1_found = path

    assert l1_found is not None, "failed to find L1 locally"
    original_created_at = l1_found.stat()[8]

    uploaded = env.pageserver_remote_storage.timeline_path(tenant_id, timeline_id) / l1_found.name
    assert not uploaded.exists(), "to-be-overwritten should not yet be uploaded"

    # give room for fs timestamps
    time.sleep(1)

    env.pageserver.start()
    warning = f".*duplicated L1 layer layer={l1_found.name}"
    env.pageserver.allowed_errors.append(warning)

    pageserver_http.timeline_compact(tenant_id, timeline_id)
    # give time for log flush
    time.sleep(1)

    found_warning = env.pageserver.log_contains(warning)
    assert found_warning, "no layer was duplicated, has this been fixed already?"

    log.info(f"found warning: {found_warning}")

    overwritten_at = l1_found.stat()[8]
    assert original_created_at < overwritten_at, "expected the L1 to be overwritten"

    wait_for_upload_queue_empty(pageserver_http, tenant_id, timeline_id)

    uploaded_at = uploaded.stat()[8]
    assert overwritten_at <= uploaded_at, "expected the L1 to finally be uploaded"

    # why does compaction not wait for uploads? probably so that we can compact
    # faster than we can upload in some cases.
    #
    # timeline_compact should wait for uploads as well
