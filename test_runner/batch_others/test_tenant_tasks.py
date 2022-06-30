from fixtures.neon_fixtures import NeonEnvBuilder, wait_until
from uuid import UUID
import time


def get_only_element(l):
    assert len(l) == 1
    return l[0]


# Test that gc and compaction tenant tasks start and stop correctly
def test_tenant_tasks(neon_env_builder: NeonEnvBuilder):
    # The gc and compaction loops don't bother to watch for tenant state
    # changes while sleeping, so we use small periods to make this test
    # run faster. With default settings we'd have to wait longer for tasks
    # to notice state changes and shut down.
    # TODO fix this behavior in the pageserver
    tenant_config = "{gc_period = '1 s', compaction_period = '1 s'}"
    neon_env_builder.pageserver_config_override = f"tenant_config={tenant_config}"
    name = "test_tenant_tasks"
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    def get_state(tenant):
        all_states = client.tenant_list()
        matching = [t for t in all_states if t["id"] == tenant.hex]
        return get_only_element(matching)["state"]

    def get_metric_value(name):
        metrics = client.get_metrics()
        relevant = [line for line in metrics.splitlines() if line.startswith(name)]
        if len(relevant) == 0:
            return 0
        line = get_only_element(relevant)
        value = line.lstrip(name).strip()
        return int(value)

    def detach_all_timelines(tenant):
        timelines = [UUID(t["timeline_id"]) for t in client.timeline_list(tenant)]
        for t in timelines:
            client.timeline_detach(tenant, t)

    def assert_idle(tenant):
        assert get_state(tenant) == "Idle"

    # Create tenant, start compute
    tenant, _ = env.neon_cli.create_tenant()
    timeline = env.neon_cli.create_timeline(name, tenant_id=tenant)
    pg = env.postgres.create_start(name, tenant_id=tenant)
    assert (get_state(tenant) == "Active")

    # Stop compute, detach timelines
    # TODO tenant should go idle even if we don't explicitly detach
    pg.stop()
    detach_all_timelines(tenant)
    wait_until(10, 0.2, lambda: assert_idle(tenant))

    # Detach all tenants and wait for them to go idle
    # TODO they should be already idle since there are no active computes
    for tenant_info in client.tenant_list():
        tenant_id = UUID(tenant_info["id"])
        detach_all_timelines(tenant_id)
        wait_until(10, 0.2, lambda: assert_idle(tenant_id))

    # Assert that all tasks finish quickly after tenants go idle
    def assert_tasks_finish():
        tasks_started = get_metric_value('pageserver_tenant_task_events{event="start"}')
        tasks_ended = get_metric_value('pageserver_tenant_task_events{event="stop"}')
        tasks_panicked = get_metric_value('pageserver_tenant_task_events{event="panic"}')
        assert tasks_started == tasks_ended
        assert tasks_panicked == 0

    wait_until(10, 0.2, assert_tasks_finish)
