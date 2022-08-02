from typing import Optional
from uuid import uuid4, UUID
import pytest
from fixtures.utils import lsn_from_hex
from fixtures.neon_fixtures import (
    DEFAULT_BRANCH_NAME,
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserverHttpClient,
    NeonPageserverApiException,
    wait_until,
)


# test that we cannot override node id
def test_pageserver_init_node_id(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init()
    with pytest.raises(
            Exception,
            match="node id can only be set during pageserver init and cannot be overridden"):
        env.pageserver.start(overrides=['--pageserver-config-override=id=10'])


def check_client(client: NeonPageserverHttpClient, initial_tenant: UUID):
    client.check_status()

    # check initial tenant is there
    assert initial_tenant.hex in {t['id'] for t in client.tenant_list()}

    # create new tenant and check it is also there
    tenant_id = uuid4()
    client.tenant_create(tenant_id)
    assert tenant_id.hex in {t['id'] for t in client.tenant_list()}

    timelines = client.timeline_list(tenant_id)
    assert len(timelines) == 0, "initial tenant should not have any timelines"

    # create timeline
    timeline_id = uuid4()
    client.timeline_create(tenant_id=tenant_id, new_timeline_id=timeline_id)

    timelines = client.timeline_list(tenant_id)
    assert len(timelines) > 0

    # check it is there
    assert timeline_id.hex in {b['timeline_id'] for b in client.timeline_list(tenant_id)}
    for timeline in timelines:
        timeline_id_str = str(timeline['timeline_id'])
        timeline_details = client.timeline_detail(tenant_id=tenant_id,
                                                  timeline_id=UUID(timeline_id_str),
                                                  include_non_incremental_logical_size=True)

        assert timeline_details['tenant_id'] == tenant_id.hex
        assert timeline_details['timeline_id'] == timeline_id_str

        local_timeline_details = timeline_details.get('local')
        assert local_timeline_details is not None
        assert local_timeline_details['timeline_state'] == 'Loaded'


def test_pageserver_http_get_wal_receiver_not_found(neon_simple_env: NeonEnv):
    env = neon_simple_env
    client = env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant()

    timeline_details = client.timeline_detail(tenant_id=tenant_id,
                                              timeline_id=timeline_id,
                                              include_non_incremental_logical_size=True)

    assert timeline_details.get('wal_source_connstr') is None, 'Should not be able to connect to WAL streaming without PG compute node running'
    assert timeline_details.get('last_received_msg_lsn') is None, 'Should not be able to connect to WAL streaming without PG compute node running'
    assert timeline_details.get('last_received_msg_ts') is None, 'Should not be able to connect to WAL streaming without PG compute node running'


# Test the WAL-receiver related fields in the response to `timeline_details` API call
#
# These fields used to be returned by a separate API call, but they're part of
# `timeline_details` now.
def test_pageserver_http_get_wal_receiver_success(neon_simple_env: NeonEnv):
    env = neon_simple_env
    client = env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant()
    pg = env.postgres.create_start(DEFAULT_BRANCH_NAME, tenant_id=tenant_id)

    def expect_updated_msg_lsn(prev_msg_lsn: Optional[int]) -> int:
        timeline_details = client.timeline_detail(tenant_id, timeline_id=timeline_id)

        # a successful `timeline_details` response must contain the below fields
        local_timeline_details = timeline_details['local']
        assert "wal_source_connstr" in local_timeline_details.keys()
        assert "last_received_msg_lsn" in local_timeline_details.keys()
        assert "last_received_msg_ts" in local_timeline_details.keys()

        assert local_timeline_details["last_received_msg_lsn"] is not None, "the last received message's LSN is empty"

        last_msg_lsn = lsn_from_hex(local_timeline_details["last_received_msg_lsn"])
        assert prev_msg_lsn is None or prev_msg_lsn < last_msg_lsn, \
            f"the last received message's LSN {last_msg_lsn} hasn't been updated \
            compared to the previous message's LSN {prev_msg_lsn}"

        return last_msg_lsn

    # Wait to make sure that we get a latest WAL receiver data.
    # We need to wait here because it's possible that we don't have access to
    # the latest WAL yet, when the `timeline_detail` API is first called.
    # See: https://github.com/neondatabase/neon/issues/1768.
    lsn = wait_until(number_of_iterations=5, interval=1, func=lambda: expect_updated_msg_lsn(None))

    # Make a DB modification then expect getting a new WAL receiver's data.
    pg.safe_psql("CREATE TABLE t(key int primary key, value text)")
    wait_until(number_of_iterations=5, interval=1, func=lambda: expect_updated_msg_lsn(lsn))


def test_pageserver_http_api_client(neon_simple_env: NeonEnv):
    env = neon_simple_env
    client = env.pageserver.http_client()
    check_client(client, env.initial_tenant)


def test_pageserver_http_api_client_auth_enabled(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    management_token = env.auth_keys.generate_management_token()

    client = env.pageserver.http_client(auth_token=management_token)
    check_client(client, env.initial_tenant)
