import subprocess
from pathlib import Path
from typing import List, Optional

import pytest
from fixtures.neon_fixtures import (
    DEFAULT_BRANCH_NAME,
    CustomLogScope,
    NeonEnv,
    NeonEnvBuilder,
    PageserverHttpClient,
    PageserverLogScope,
    TenantLogScope,
    TimelineLogScope,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import wait_until


# test that we cannot override node id after init
def test_pageserver_init_node_id(
    neon_simple_env: NeonEnv, neon_binpath: Path, pg_distrib_dir: Path
):
    repo_dir = neon_simple_env.repo_dir
    pageserver_config = repo_dir / "pageserver.toml"
    pageserver_bin = neon_binpath / "pageserver"

    def run_pageserver(args):
        return subprocess.run(
            [str(pageserver_bin), "-D", str(repo_dir), *args],
            check=False,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    # remove initial config and stop existing pageserver
    pageserver_config.unlink()
    neon_simple_env.pageserver.stop()

    bad_init = run_pageserver(["--init", "-c", f'pg_distrib_dir="{pg_distrib_dir}"'])
    assert (
        bad_init.returncode == 1
    ), "pageserver should not be able to init new config without the node id"
    assert "missing id" in bad_init.stderr
    assert not pageserver_config.exists(), "config file should not be created after init error"

    completed_init = run_pageserver(
        ["--init", "-c", "id = 12345", "-c", f'pg_distrib_dir="{pg_distrib_dir}"']
    )
    assert (
        completed_init.returncode == 0
    ), "pageserver should be able to create a new config with the node id given"
    assert pageserver_config.exists(), "config file should be created successfully"

    bad_reinit = run_pageserver(
        ["--init", "-c", "id = 12345", "-c", f'pg_distrib_dir="{pg_distrib_dir}"']
    )
    assert (
        bad_reinit.returncode == 1
    ), "pageserver should not be able to init new config without the node id"
    assert "already exists, cannot init it" in bad_reinit.stderr

    bad_update = run_pageserver(["--update-config", "-c", "id = 3"])
    assert bad_update.returncode == 1, "pageserver should not allow updating node id"
    assert "has node id already, it cannot be overridden" in bad_update.stderr


def check_client(client: PageserverHttpClient, initial_tenant: TenantId):
    client.check_status()

    # check initial tenant is there
    assert initial_tenant in {TenantId(t["id"]) for t in client.tenant_list()}

    # create new tenant and check it is also there
    tenant_id = TenantId.generate()
    client.tenant_create(tenant_id)
    assert tenant_id in {TenantId(t["id"]) for t in client.tenant_list()}

    timelines = client.timeline_list(tenant_id)
    assert len(timelines) == 0, "initial tenant should not have any timelines"

    # create timeline
    timeline_id = TimelineId.generate()
    client.timeline_create(tenant_id=tenant_id, new_timeline_id=timeline_id)

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
    ), f"the last received message's LSN {last_msg_lsn} hasn't been updated \
        compared to the previous message's LSN {prev_msg_lsn}"

    return last_msg_lsn


# Test the WAL-receiver related fields in the response to `timeline_details` API call
#
# These fields used to be returned by a separate API call, but they're part of
# `timeline_details` now.
def test_pageserver_http_get_wal_receiver_success(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.pageserver.http_client() as client:
        tenant_id, timeline_id = env.neon_cli.create_tenant()
        pg = env.postgres.create_start(DEFAULT_BRANCH_NAME, tenant_id=tenant_id)

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
        pg.safe_psql("CREATE TABLE t(key int primary key, value text)")
        wait_until(
            number_of_iterations=5,
            interval=1,
            func=lambda: expect_updated_msg_lsn(client, tenant_id, timeline_id, lsn),
        )


def test_pageserver_http_api_client(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.pageserver.http_client() as client:
        check_client(client, env.initial_tenant)


def test_pageserver_http_api_client_auth_enabled(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    pageserver_token = env.auth_keys.generate_pageserver_token()

    with env.pageserver.http_client(auth_token=pageserver_token) as client:
        check_client(client, env.initial_tenant)


def pageserver_predefined_scopes() -> List[PageserverLogScope]:
    return [
        TenantLogScope("debug", TenantId.generate()),
        TimelineLogScope("debug", TenantId.generate(), TimelineId.generate()),
    ]


def pageserver_custom_scopes() -> List[PageserverLogScope]:
    return [CustomLogScope("pageserver=debug", True), CustomLogScope("hyper=debug", True)]


@pytest.mark.parametrize(
    "pageserver_log_scope", pageserver_predefined_scopes() + pageserver_custom_scopes()
)
def test_pageserver_logs_toggle_overrides_env_var(
    neon_env_builder: NeonEnvBuilder, pageserver_log_scope: PageserverLogScope
):
    # Disable global logging, including the pageserver one, using an env var
    neon_env_builder.rust_log_env_var = "error"
    env = neon_env_builder.init_start()
    pageserver = env.pageserver
    pageserver_http = pageserver.http_client()
    initial_log_filter = pageserver_http.current_log_filter()

    # Ensure that pageserver does not produce logs with such setting
    new_timeline_with_no_logs = TimelineId.generate()
    pageserver_http.timeline_create(
        tenant_id=env.initial_tenant, new_timeline_id=new_timeline_with_no_logs
    )
    pageserver_http.tenant_status(env.initial_tenant)
    assert (
        len(pageserver.log_lines()) == 0
    ), "No err log lines expected, others should be turned off"

    # Make a dynamic override via HTTP api and check that it makes pageserver to produce some logs
    pageserver_log_scope.tenant_id = env.initial_tenant
    pageserver_log_scope.timeline_id = env.initial_timeline
    pageserver_log_scope.log_level = "debug"
    pageserver_http.change_log_filter(pageserver_log_scope)
    log_filter_after_update = pageserver_http.current_log_filter()
    assert (
        initial_log_filter != log_filter_after_update
    ), "Turning a filter on should change the log filter"
    # Query pageserver to produce some logs with the override
    pageserver_http.tenant_status(env.initial_tenant)
    pageserver_http.timeline_compact(env.initial_tenant, env.initial_timeline)
    # Disable the override back
    pageserver_log_scope.disable()
    pageserver_http.change_log_filter(pageserver_log_scope)
    log_filter_after_toggle = pageserver_http.current_log_filter()

    assert (
        initial_log_filter == log_filter_after_toggle
    ), "Turning a filter on and off should restore the initial state"

    updated_log_lines = pageserver.log_lines()
    log_lines_count_after_override = len(updated_log_lines)
    assert (
        log_lines_count_after_override > 0
    ), f"Pageserver should produce logs due to the new override, filter: '{pageserver_http.current_log_filter()}'"

    pageserver_http.tenant_status(env.initial_tenant)
    pageserver_http.timeline_compact(env.initial_tenant, env.initial_timeline)
    latest_log_lines = pageserver.log_lines()
    assert (
        len(latest_log_lines) == log_lines_count_after_override
    ), "After override is disabled, no more logs should be produced again"
    assert all(
        str(new_timeline_with_no_logs) not in line for line in latest_log_lines
    ), f"Should not contain unised timeline {new_timeline_with_no_logs} in logs"


def test_pageserver_logs_restart(neon_env_builder: NeonEnvBuilder):
    # Disable global logging, including the pageserver one, using an env var
    neon_env_builder.rust_log_env_var = "error"
    env = neon_env_builder.init_start()
    pageserver = env.pageserver
    pageserver_http = pageserver.http_client()
    initial_log_filter = pageserver_http.current_log_filter()

    current_log_filter = initial_log_filter
    for pageserver_log_scope in pageserver_predefined_scopes():
        pageserver_log_scope.tenant_id = env.initial_tenant
        pageserver_log_scope.timeline_id = env.initial_timeline
        pageserver_log_scope.log_level = "debug"

        pageserver_http.change_log_filter(pageserver_log_scope)
        new_filter = pageserver_http.current_log_filter()
        assert new_filter != current_log_filter, "New filter should appear after the toggle"
        current_log_filter = new_filter

    # Make a dynamic override via HTTP api and check that it makes pageserver to produce some logs
    pageserver_http.tenant_status(env.initial_tenant)
    pageserver_http.timeline_compact(env.initial_tenant, env.initial_timeline)
    env.pageserver.stop()

    updated_log_lines = pageserver.log_lines()
    log_lines_count_after_override = len(updated_log_lines)
    assert (
        log_lines_count_after_override > 0
    ), f"Pageserver should produce logs due to the new override, filter: '{current_log_filter}'"

    env.pageserver.start()
    pageserver_http.tenant_status(env.initial_tenant)
    pageserver_http.timeline_compact(env.initial_tenant, env.initial_timeline)
    assert (
        initial_log_filter == pageserver_http.current_log_filter()
    ), "Pageserver log filter should get back to default after a restart"
    assert (
        len(pageserver.log_lines()) == log_lines_count_after_override
    ), "After pageserver restart, no more log overrides should produce debug logs"


def test_pageserver_logs_reset(neon_env_builder: NeonEnvBuilder):
    # Disable global logging, including the pageserver one, using an env var
    neon_env_builder.rust_log_env_var = "error"
    env = neon_env_builder.init_start()
    pageserver = env.pageserver
    pageserver_http = pageserver.http_client()
    initial_log_filter = pageserver_http.current_log_filter()

    current_log_filter = initial_log_filter
    for pageserver_log_scope in pageserver_custom_scopes():
        pageserver_log_scope.tenant_id = env.initial_tenant
        pageserver_log_scope.timeline_id = env.initial_timeline
        pageserver_log_scope.log_level = "debug"

        pageserver_http.change_log_filter(pageserver_log_scope)
        new_filter = pageserver_http.current_log_filter()
        assert new_filter != current_log_filter, "New filter should appear after the toggle"
        current_log_filter = new_filter

    pageserver_http.tenant_status(env.initial_tenant)
    pageserver_http.timeline_compact(env.initial_tenant, env.initial_timeline)
    pageserver_http.reset_log_filter()
    updated_log_lines = pageserver.log_lines()
    log_lines_count_after_override = len(updated_log_lines)
    assert (
        log_lines_count_after_override > 0
    ), f"Pageserver should produce logs due to the new override, filter: '{current_log_filter}'"

    pageserver_http.tenant_status(env.initial_tenant)
    pageserver_http.timeline_compact(env.initial_tenant, env.initial_timeline)
    assert (
        initial_log_filter == pageserver_http.current_log_filter()
    ), "Pageserver log filter should get back to default after a reset"
    assert (
        len(pageserver.log_lines()) == log_lines_count_after_override
    ), "After pageserver restart, no more log overrides should produce debug logs"
