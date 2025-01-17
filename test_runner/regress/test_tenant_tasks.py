from __future__ import annotations

from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.pageserver.utils import (
    assert_tenant_state,
    timeline_delete_wait_completed,
    wait_until_tenant_active,
)
from fixtures.utils import wait_until


def get_only_element(l):  # noqa: E741
    assert len(l) == 1
    return l[0]


# Test that gc and compaction tenant tasks start and stop correctly
def test_tenant_tasks(neon_env_builder: NeonEnvBuilder):
    name = "test_tenant_tasks"
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    def get_state(tenant):
        all_states = client.tenant_list()
        matching = [t for t in all_states if TenantId(t["id"]) == tenant]
        return get_only_element(matching)["state"]

    def delete_all_timelines(tenant: TenantId):
        timelines = [TimelineId(t["timeline_id"]) for t in client.timeline_list(tenant)]
        for t in timelines:
            timeline_delete_wait_completed(client, tenant, t)

    # Create tenant, start compute
    tenant, _ = env.create_tenant()
    env.create_timeline(name, tenant_id=tenant)
    endpoint = env.endpoints.create_start(name, tenant_id=tenant)
    assert_tenant_state(
        client,
        tenant,
        expected_state="Active",
        message="Pageserver should activate a tenant and start background jobs if timelines are loaded",
    )

    # Stop compute
    endpoint.stop()

    # Delete all timelines on all tenants.
    #
    # FIXME: we used to check that the background jobs are stopped when all timelines
    # are removed, but we don't stop them anymore. Not sure if this test still makes sense
    # or we should just remove it.
    for tenant_info in client.tenant_list():
        tenant_id = TenantId(tenant_info["id"])
        delete_all_timelines(tenant_id)
        wait_until_tenant_active(client, tenant_id, iterations=10, period=0.2)

    # Assert that all tasks finish quickly after tenant is detached
    task_starts = client.get_metric_value("pageserver_tenant_task_events_total", {"event": "start"})
    assert task_starts is not None
    assert int(task_starts) > 0
    client.tenant_detach(tenant)
    client.tenant_detach(env.initial_tenant)

    def assert_tasks_finish():
        tasks_started = client.get_metric_value(
            "pageserver_tenant_task_events_total", {"event": "start"}
        )
        tasks_ended = client.get_metric_value(
            "pageserver_tenant_task_events_total", {"event": "stop"}
        )
        tasks_panicked = client.get_metric_value(
            "pageserver_tenant_task_events_total", {"event": "panic"}
        )
        log.info(f"started {tasks_started}, ended {tasks_ended}, panicked {tasks_panicked}")
        assert tasks_started == tasks_ended
        assert tasks_panicked is None or int(tasks_panicked) == 0

    wait_until(assert_tasks_finish)
