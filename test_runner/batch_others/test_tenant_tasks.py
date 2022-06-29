from fixtures.neon_fixtures import NeonEnvBuilder
from uuid import UUID


def test_tenant_tasks(neon_env_builder: NeonEnvBuilder):
    name = "test_tenant_tasks"
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    def get_state(tenant):
        all_states = client.tenant_list()
        matching = [t for t in all_states if t["id"] == tenant.hex]
        assert len(matching) == 1
        return matching[0]["state"]

    tenant, _ = env.neon_cli.create_tenant()
    timeline = env.neon_cli.create_timeline(name, tenant_id=tenant)
    pg = env.postgres.create_start(name, tenant_id=tenant)

    assert(get_state(tenant) == "Active")

    timelines = [
        UUID(t["timeline_id"])
        for t in client.timeline_list(tenant)
    ]

    # print(timelines)
    # prints 2 timelines, the initial one and the one I created

    # # Detach all timelines
    # for t in timelines:
    #     client.timeline_detach(tenant, t)

    pg.stop()
    import time; time.sleep(10)
    assert(get_state(tenant) == "Idle")  # This assertion fails
