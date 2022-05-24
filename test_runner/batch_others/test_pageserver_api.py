from typing import Optional
from uuid import uuid4, UUID
import pytest
from fixtures.zenith_fixtures import (
    DEFAULT_BRANCH_NAME,
    ZenithEnv,
    ZenithEnvBuilder,
    ZenithPageserverHttpClient,
    ZenithPageserverApiException,
    wait_until,
)


# test that we cannot override node id
def test_pageserver_init_node_id(zenith_env_builder: ZenithEnvBuilder):
    env = zenith_env_builder.init()
    with pytest.raises(
            Exception,
            match="node id can only be set during pageserver init and cannot be overridden"):
        env.pageserver.start(overrides=['--pageserver-config-override=id=10'])


def check_client(client: ZenithPageserverHttpClient, initial_tenant: UUID):
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
                                                  timeline_id=UUID(timeline_id_str))

        assert timeline_details['tenant_id'] == tenant_id.hex
        assert timeline_details['timeline_id'] == timeline_id_str

        local_timeline_details = timeline_details.get('local')
        assert local_timeline_details is not None
        assert local_timeline_details['timeline_state'] == 'Loaded'


def test_pageserver_http_get_wal_receiver_not_found(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    client = env.pageserver.http_client()

    tenant_id, timeline_id = env.zenith_cli.create_tenant()

    # no PG compute node is running, so no WAL receiver is running
    with pytest.raises(ZenithPageserverApiException) as e:
        _ = client.wal_receiver_get(tenant_id, timeline_id)
        assert "Not Found" in str(e.value)


def test_pageserver_http_get_wal_receiver_success(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    client = env.pageserver.http_client()

    tenant_id, timeline_id = env.zenith_cli.create_tenant()
    pg = env.postgres.create_start(DEFAULT_BRANCH_NAME, tenant_id=tenant_id)

    def try_get_updated_msg_lsn(prev_msg_lsn: Optional[str]) -> str:
        """Try to get the updated message's LSN from the wal receiver.

        It raises an exception if the latest message's LSN is not found or
        it hasn't been updated compared to the previous message's LSN.
        """
        res = client.wal_receiver_get(tenant_id, timeline_id)
        assert list(res.keys()) == [
            "thread_id",
            "wal_producer_connstr",
            "last_received_msg_lsn",
            "last_received_msg_ts",
        ]

        if res["last_received_msg_lsn"] is None:
            raise Exception("the last received message's LSN is empty")
        elif prev_msg_lsn is not None and prev_msg_lsn >= res["last_received_msg_lsn"]:
            raise Exception(
                f"the last received message's LSN hasn't been updated compared to the previous message's LSN {prev_msg_lsn}"
            )

        assert isinstance(res["last_received_msg_lsn"], str)
        return res["last_received_msg_lsn"]

    # Wait to make sure that we get a latest WAL receiver data.
    # We need to wait here because it's possible that we don't have access to
    # the latest WAL during the time the `wal_receiver_get` API is called.
    # See: https://github.com/neondatabase/neon/issues/1768.
    lsn = wait_until(5, 1, lambda: try_get_updated_msg_lsn(None))

    # Make a DB modification then expect getting a new WAL receiver's data.
    pg.safe_psql("CREATE TABLE t(key int primary key, value text)")
    wait_until(5, 1, lambda: try_get_updated_msg_lsn(lsn))


def test_pageserver_http_api_client(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    client = env.pageserver.http_client()
    check_client(client, env.initial_tenant)


def test_pageserver_http_api_client_auth_enabled(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.pageserver_auth_enabled = True
    env = zenith_env_builder.init_start()

    management_token = env.auth_keys.generate_management_token()

    client = env.pageserver.http_client(auth_token=management_token)
    check_client(client, env.initial_tenant)
