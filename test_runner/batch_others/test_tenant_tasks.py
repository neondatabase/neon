from fixtures.neon_fixtures import NeonEnvBuilder
from uuid import UUID


def get_only_element(l):
    assert len(l) == 1
    return l[0]


def test_tenant_tasks(neon_env_builder: NeonEnvBuilder):
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
        line = get_only_element(relevant)
        value = line.lstrip(name).strip()
        return int(value)

    def detach_all_timelines(tenant):
        timelines = [UUID(t["timeline_id"]) for t in client.timeline_list(tenant)]
        for t in timelines:
            client.timeline_detach(tenant, t)

    # Create tenant, start compute
    tenant, _ = env.neon_cli.create_tenant()
    timeline = env.neon_cli.create_timeline(name, tenant_id=tenant)
    pg = env.postgres.create_start(name, tenant_id=tenant)
    assert(get_state(tenant) == "Active")

    # Stop compute, detach timelines
    # TODO tenant should go idle even if we don't explicitly detach
    pg.stop()
    detach_all_timelines(tenant)

    import time; time.sleep(1)
    assert(get_state(tenant) == "Idle")

    # Detach all tenants
    for tenant_info in client.tenant_list():
        tenant_id = UUID(tenant_info["id"])
        detach_all_timelines(tenant_id)

        # XXX this fails. Why?
        assert get_state(tenant_id) == "Idle"

    tasks_started = get_metric_value('pageserver_tenant_task_events{event="start"}')
    tasks_ended = get_metric_value('pageserver_tenant_task_events{event="stop"}')
    assert tasks_started == tasks_ended
