import subprocess
from pathlib import Path
from typing import Optional

import toml
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.neon_fixtures import (
    DEFAULT_BRANCH_NAME,
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.utils import wait_until


def test_pageserver_init_node_id(neon_simple_env: NeonEnv, neon_binpath: Path):
    """
    NB: The neon_local doesn't use `--init` mode anymore, but our production
    deployment still does => https://github.com/neondatabase/aws/pull/1322
    """
    workdir = neon_simple_env.pageserver.workdir
    pageserver_config = workdir / "pageserver.toml"
    pageserver_bin = neon_binpath / "pageserver"

    def run_pageserver(args):
        return subprocess.run(
            [str(pageserver_bin), "-D", str(workdir), *args],
            check=False,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    neon_simple_env.pageserver.stop()

    with open(neon_simple_env.pageserver.config_toml_path, "r") as f:
        ps_config = toml.load(f)

    required_config_keys = [
        "pg_distrib_dir",
        "listen_pg_addr",
        "listen_http_addr",
        "pg_auth_type",
        "http_auth_type",
        # TODO: only needed for NEON_PAGESERVER_PANIC_ON_UNSPECIFIED_COMPACTION_ALGORITHM in https://github.com/neondatabase/neon/pull/7748
        # "tenant_config",
    ]
    required_config_overrides = [
        f"--config-override={toml.dumps({k: ps_config[k]})}" for k in required_config_keys
    ]

    pageserver_config.unlink()

    bad_init = run_pageserver(["--init", *required_config_overrides])
    assert (
        bad_init.returncode == 1
    ), "pageserver should not be able to init new config without the node id"
    assert 'missing config value "id"' in bad_init.stderr
    assert not pageserver_config.exists(), "config file should not be created after init error"

    good_init_cmd = [
        "--init",
        f"--config-override=id={ps_config['id']}",
        *required_config_overrides,
    ]
    completed_init = run_pageserver(good_init_cmd)
    assert (
        completed_init.returncode == 0
    ), "pageserver should be able to create a new config with the node id given"
    assert pageserver_config.exists(), "config file should be created successfully"

    bad_reinit = run_pageserver(good_init_cmd)
    assert bad_reinit.returncode == 1, "pageserver refuses to init if already exists"
    assert "config file already exists" in bad_reinit.stderr


def check_client(env: NeonEnv, client: PageserverHttpClient):
    pg_version = env.pg_version
    initial_tenant = env.initial_tenant

    client.check_status()

    # check initial tenant is there
    assert initial_tenant in {TenantId(t["id"]) for t in client.tenant_list()}

    # create new tenant and check it is also there
    tenant_id = TenantId.generate()
    client.tenant_create(
        tenant_id, generation=env.storage_controller.attach_hook_issue(tenant_id, env.pageserver.id)
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
        tenant_id, timeline_id = env.neon_cli.create_tenant()

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
    prev_msg_lsn: Optional[Lsn],
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
        tenant_id, timeline_id = env.neon_cli.create_tenant()
        endpoint = env.endpoints.create_start(DEFAULT_BRANCH_NAME, tenant_id=tenant_id)

        # insert something to force sk -> ps message
        endpoint.safe_psql("CREATE TABLE t(key int primary key, value text)")
        # Wait to make sure that we get a latest WAL receiver data.
        # We need to wait here because it's possible that we don't have access to
        # the latest WAL yet, when the `timeline_detail` API is first called.
        # See: https://github.com/neondatabase/neon/issues/1768.
        lsn = wait_until(
            number_of_iterations=5,
            interval=1,
            func=lambda: expect_updated_msg_lsn(client, tenant_id, timeline_id, None),
        )

        # Make a DB modification then expect getting a new WAL receiver's data.
        endpoint.safe_psql("INSERT INTO t VALUES (1, 'hey')")
        wait_until(
            number_of_iterations=5,
            interval=1,
            func=lambda: expect_updated_msg_lsn(client, tenant_id, timeline_id, lsn),
        )


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
