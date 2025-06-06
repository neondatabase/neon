from __future__ import annotations

import threading
import time
from contextlib import closing
from enum import StrEnum
from typing import TYPE_CHECKING

import pytest
import requests
from fixtures.common_types import Lsn, TimelineId
from fixtures.log_helper import log
from fixtures.remote_storage import S3Storage, s3_storage
from fixtures.safekeeper_utils import is_segment_offloaded
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import (
        Endpoint,
        NeonEnvBuilder,
    )


@pytest.mark.parametrize("auth_enabled", [False, True])
def test_safekeeper_delete_timeline(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    # FIXME: are these expected?
    env.pageserver.allowed_errors.extend(
        [
            ".*Timeline .* was not found in global map.*",
            ".*Timeline .* has been deleted.*",
            ".*Timeline .* was cancelled and cannot be used anymore.*",
        ]
    )

    # Create two tenants: one will be deleted, other should be preserved.
    tenant_id = env.initial_tenant
    timeline_id_1 = env.create_branch("br1")  # Active, delete explicitly
    timeline_id_2 = env.create_branch("br2")  # Inactive, delete explicitly
    timeline_id_3 = env.create_branch("br3")  # Active, delete with the tenant
    timeline_id_4 = env.create_branch("br4")  # Inactive, delete with the tenant

    tenant_id_other, timeline_id_other = env.create_tenant()

    # Populate branches
    endpoint_1 = env.endpoints.create_start("br1")
    endpoint_2 = env.endpoints.create_start("br2")
    endpoint_3 = env.endpoints.create_start("br3")
    endpoint_4 = env.endpoints.create_start("br4")
    endpoint_other = env.endpoints.create_start("main", tenant_id=tenant_id_other)
    for endpoint in [endpoint_1, endpoint_2, endpoint_3, endpoint_4, endpoint_other]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key)")
    sk = env.safekeepers[0]
    sk_data_dir = sk.data_dir
    if not auth_enabled:
        sk_http = sk.http_client()
        sk_http_other = sk_http
    else:
        sk_http = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        sk_http_other = sk.http_client(
            auth_token=env.auth_keys.generate_tenant_token(tenant_id_other)
        )
        sk_http_noauth = sk.http_client(gen_sk_wide_token=False)
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_1)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Stop branches which should be inactive and restart Safekeeper to drop its in-memory state.
    endpoint_2.stop_and_destroy()
    endpoint_4.stop_and_destroy()
    sk.stop()
    sk.start()

    # Ensure connections to Safekeeper are established
    for endpoint in [endpoint_1, endpoint_3, endpoint_other]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO t (key) VALUES (1)")

    # Stop all computes gracefully before safekeepers stop responding to them
    endpoint_1.stop_and_destroy()
    endpoint_3.stop_and_destroy()

    # Remove initial tenant's br1 (active)
    assert sk_http.timeline_delete(tenant_id, timeline_id_1)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Ensure repeated deletion succeeds
    assert not sk_http.timeline_delete(tenant_id, timeline_id_1)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    if auth_enabled:
        # Ensure we cannot delete the other tenant
        for sk_h in [sk_http, sk_http_noauth]:
            with pytest.raises(sk_h.HTTPError, match="Forbidden|Unauthorized"):
                assert sk_h.timeline_delete(tenant_id_other, timeline_id_other)
            with pytest.raises(sk_h.HTTPError, match="Forbidden|Unauthorized"):
                assert sk_h.tenant_delete_force(tenant_id_other)
        assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant's br2 (inactive)
    assert sk_http.timeline_delete(tenant_id, timeline_id_2)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_2)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove non-existing branch, should succeed
    assert not sk_http.timeline_delete(tenant_id, TimelineId("00" * 16))["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_2)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant fully (two branches are active)
    response = sk_http.tenant_delete_force(tenant_id)
    assert response[str(timeline_id_3)]["dir_existed"]
    assert not (sk_data_dir / str(tenant_id)).exists()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant again.
    response = sk_http.tenant_delete_force(tenant_id)
    # assert response == {}
    assert not (sk_data_dir / str(tenant_id)).exists()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Ensure the other tenant still works
    sk_http_other.timeline_status(tenant_id_other, timeline_id_other)
    with closing(endpoint_other.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO t (key) VALUES (123)")


def test_safekeeper_delete_timeline_under_load(neon_env_builder: NeonEnvBuilder):
    """
    Test deleting timelines on a safekeeper while they're under load.

    This should not happen under normal operation, but it can happen if
    there is some rogue compute/pageserver that is writing/reading to a
    safekeeper that we're migrating a timeline away from, or if the timeline
    is being deleted while such a rogue client is running.
    """
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    # Create two endpoints that will generate load
    timeline_id_a = env.create_branch("deleteme_a")
    timeline_id_b = env.create_branch("deleteme_b")

    endpoint_a = env.endpoints.create("deleteme_a")
    endpoint_a.start()
    endpoint_b = env.endpoints.create("deleteme_b")
    endpoint_b.start()

    # Get tenant and timeline IDs
    tenant_id = env.initial_tenant

    # Start generating load on both timelines
    def generate_load(endpoint: Endpoint):
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS t(key int, value text)")
                while True:
                    try:
                        cur.execute("INSERT INTO t SELECT generate_series(1,1000), 'data'")
                    except:  # noqa
                        # Ignore errors since timeline may be deleted
                        break

    t_a = threading.Thread(target=generate_load, args=(endpoint_a,))
    t_b = threading.Thread(target=generate_load, args=(endpoint_b,))
    try:
        t_a.start()
        t_b.start()

        # Let the load run for a bit
        log.info("Warming up...")
        time.sleep(2)

        # Safekeeper errors will propagate to the pageserver: it is correct that these are
        # logged at error severity because they indicate the pageserver is trying to read
        # a timeline that it shouldn't.
        env.pageserver.allowed_errors.extend(
            [
                ".*Timeline.*was cancelled.*",
                ".*Timeline.*has been deleted.*",
                ".*Timeline.*was not found.*",
            ]
        )

        # Try deleting timelines while under load
        sk = env.safekeepers[0]
        sk_http = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))

        # Delete first timeline
        log.info(f"Deleting {timeline_id_a}...")
        assert sk_http.timeline_delete(tenant_id, timeline_id_a, only_local=True)["dir_existed"]

        # Delete second timeline
        log.info(f"Deleting {timeline_id_b}...")
        assert sk_http.timeline_delete(tenant_id, timeline_id_b, only_local=True)["dir_existed"]

        # Verify timelines are gone from disk
        sk_data_dir = sk.data_dir
        assert not (sk_data_dir / str(tenant_id) / str(timeline_id_a)).exists()
        # assert not (sk_data_dir / str(tenant_id) / str(timeline_id_b)).exists()

    finally:
        log.info("Stopping endpoints...")
        # Stop endpoints with immediate mode because we deleted the timeline out from under the compute, which may cause it to hang
        endpoint_a.stop(mode="immediate")
        endpoint_b.stop(mode="immediate")
        log.info("Joining threads...")
        t_a.join()
        t_b.join()


class RemoteDeleteFailpoint(StrEnum):
    PAUSE = "sk-delete-timeline-remote-pause"
    FAIL = "sk-delete-timeline-remote"


@pytest.mark.parametrize("failpoint", [RemoteDeleteFailpoint.PAUSE, RemoteDeleteFailpoint.FAIL])
def test_safekeeper_delete_remote_errors(
    neon_env_builder: NeonEnvBuilder, failpoint: RemoteDeleteFailpoint
):
    """
    Test that errors and delays during remote deletion are handled correctly.
    """

    # Configure safekeepers with ultra-fast eviction policy
    neon_env_builder.safekeeper_extra_opts = [
        "--enable-offload",
        "--delete-offloaded-wal",
        "--control-file-save-interval",
        "1s",
    ]
    neon_env_builder.enable_safekeeper_remote_storage(s3_storage())
    env = neon_env_builder.init_start()

    # FIXME: pageserver is intermittently emitting this
    env.pageserver.allowed_errors.extend(
        [
            ".*unsupported command START_WAL_PUSH in START_WAL_PUSH.*",
        ]
    )

    timeline_id_a = env.create_branch("deleteme_a")
    endpoint_a = env.endpoints.create("deleteme_a")
    endpoint_a.start()
    with closing(endpoint_a.connect()) as conn:
        with conn.cursor() as cur:
            # roughly fills one segment
            cur.execute("create table t(key int, value text)")
            cur.execute("insert into t select generate_series(1,250000), 'payload'")
    endpoint_a.stop()

    # Ensure something is uploaded to remote storage
    def assert_is_uploaded():
        assert is_segment_offloaded(
            env.safekeepers[0], env.initial_tenant, timeline_id_a, Lsn("0/2000000")
        )

    wait_until(assert_is_uploaded)

    def list_timeline_remote():
        assert isinstance(env.safekeepers_remote_storage, S3Storage)
        prefix = f"{env.safekeepers_remote_storage.safekeeper_timeline_path(env.initial_tenant, timeline_id_a)}/"

        listing = env.safekeepers_remote_storage.client.list_objects_v2(
            Bucket=env.safekeepers_remote_storage.bucket_name,
            Prefix=prefix,
        )
        return listing.get("Contents", [])

    assert list_timeline_remote() != []

    sk_http = env.safekeepers[0].http_client()
    env.pageserver.http_client().timeline_delete(env.initial_tenant, timeline_id_a)

    # Set up failpoint
    if failpoint == RemoteDeleteFailpoint.PAUSE:
        sk_http.configure_failpoints((failpoint, "pause"))
    elif failpoint == RemoteDeleteFailpoint.FAIL:
        sk_http.configure_failpoints((failpoint, "return"))
    else:
        raise NotImplementedError(f"Unknown failpoint: {failpoint}")

    # Delete the timeline - this should hit the configured failpoint
    if failpoint == RemoteDeleteFailpoint.PAUSE:
        # Expect time out
        with pytest.raises(requests.exceptions.ReadTimeout, match="timed out"):
            sk_http.timeline_delete(env.initial_tenant, timeline_id_a, timeout=5)

        # Assert deletion didn't happy yet
        assert list_timeline_remote() != []

        # Unblock the background task that should still be running
        sk_http.configure_failpoints((failpoint, "off"))

        # Expect that after unblocking, remote deletion proceeds
        def assert_remote_deleted():
            assert list_timeline_remote() == []

        wait_until(assert_remote_deleted)

    elif failpoint == RemoteDeleteFailpoint.FAIL:
        # Expect immediate failure
        with pytest.raises(sk_http.HTTPError, match="Internal Server Error"):
            sk_http.timeline_delete(env.initial_tenant, timeline_id_a)

        sk_http.configure_failpoints((failpoint, "off"))
    else:
        raise NotImplementedError(f"Unknown failpoint: {failpoint}")

    # Retry should succeed
    sk_http.timeline_delete(env.initial_tenant, timeline_id_a)

    # Remote storage should be empty
    assert list_timeline_remote() == []
