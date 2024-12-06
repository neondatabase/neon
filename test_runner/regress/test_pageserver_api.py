from __future__ import annotations

from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.neon_fixtures import (
    DEFAULT_BRANCH_NAME,
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.utils import wait_until


def check_client(env: NeonEnv, client: PageserverHttpClient):
    pg_version = env.pg_version
    initial_tenant = env.initial_tenant

    client.check_status()

    # check initial tenant is there
    assert initial_tenant in {TenantId(t["id"]) for t in client.tenant_list()}

    # create new tenant and check it is also there
    tenant_id = TenantId.generate()
    env.pageserver.tenant_create(
        tenant_id,
        generation=env.storage_controller.attach_hook_issue(tenant_id, env.pageserver.id),
        auth_token=client.auth_token,
    )
    assert tenant_id in {TenantId(t["id"]) for t in client.tenant_list()}

    timelines = client.timeline_list(tenant_id)
    assert len(timelines) == 0, "initial tenant should not have any timelines"

    # create timeline
    timeline_id = TimelineId.generate()
    client.timeline_create(
        pg_version=pg_version,
        tenant_id=tenant_id,
        new_timeline_id=timeline_id,
    )

    timelines = client.timeline_list(tenant_id)
    assert len(timelines) > 0

    # check it is there
    assert timeline_id in {TimelineId(b["timeline_id"]) for b in client.timeline_list(tenant_id)}
    for timeline in timelines:
        timeline_id = TimelineId(timeline["timeline_id"])
        timeline_details = client.timeline_detail(
            tenant_id=tenant_id,
            timeline_id=timeline_id,
            include_non_incremental_logical_size=True,
        )

        assert TenantId(timeline_details["tenant_id"]) == tenant_id
        assert TimelineId(timeline_details["timeline_id"]) == timeline_id


def test_pageserver_http_get_wal_receiver_not_found(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.pageserver.http_client() as client:
        tenant_id, timeline_id = env.create_tenant()

        timeline_details = client.timeline_detail(
            tenant_id=tenant_id, timeline_id=timeline_id, include_non_incremental_logical_size=True
        )

        assert (
            timeline_details.get("wal_source_connstr") is None
        ), "Should not be able to connect to WAL streaming without PG compute node running"
        assert (
            timeline_details.get("last_received_msg_lsn") is None
        ), "Should not be able to connect to WAL streaming without PG compute node running"
        assert (
            timeline_details.get("last_received_msg_ts") is None
        ), "Should not be able to connect to WAL streaming without PG compute node running"


def expect_updated_msg_lsn(
    client: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    prev_msg_lsn: Lsn | None,
) -> Lsn:
    timeline_details = client.timeline_detail(tenant_id, timeline_id=timeline_id)

    # a successful `timeline_details` response must contain the below fields
    assert "wal_source_connstr" in timeline_details.keys()
    assert "last_received_msg_lsn" in timeline_details.keys()
    assert "last_received_msg_ts" in timeline_details.keys()

    assert (
        timeline_details["last_received_msg_lsn"] is not None
    ), "the last received message's LSN is empty"

    last_msg_lsn = Lsn(timeline_details["last_received_msg_lsn"])
    assert (
        prev_msg_lsn is None or prev_msg_lsn < last_msg_lsn
    ), f"the last received message's LSN {last_msg_lsn} hasn't been updated compared to the previous message's LSN {prev_msg_lsn}"

    return last_msg_lsn


# Test the WAL-receiver related fields in the response to `timeline_details` API call
#
# These fields used to be returned by a separate API call, but they're part of
# `timeline_details` now.
def test_pageserver_http_get_wal_receiver_success(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.pageserver.http_client() as client:
        tenant_id, timeline_id = env.create_tenant()
        endpoint = env.endpoints.create_start(DEFAULT_BRANCH_NAME, tenant_id=tenant_id)

        # insert something to force sk -> ps message
        endpoint.safe_psql("CREATE TABLE t(key int primary key, value text)")
        # Wait to make sure that we get a latest WAL receiver data.
        # We need to wait here because it's possible that we don't have access to
        # the latest WAL yet, when the `timeline_detail` API is first called.
        # See: https://github.com/neondatabase/neon/issues/1768.
        lsn = wait_until(lambda: expect_updated_msg_lsn(client, tenant_id, timeline_id, None))

        # Make a DB modification then expect getting a new WAL receiver's data.
        endpoint.safe_psql("INSERT INTO t VALUES (1, 'hey')")
        wait_until(lambda: expect_updated_msg_lsn(client, tenant_id, timeline_id, lsn))


def test_pageserver_http_api_client(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.pageserver.http_client() as client:
        check_client(env, client)


def test_pageserver_http_api_client_auth_enabled(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    pageserver_token = env.auth_keys.generate_pageserver_token()

    with env.pageserver.http_client(auth_token=pageserver_token) as client:
        check_client(env, client)
