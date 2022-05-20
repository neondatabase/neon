from fixtures.zenith_fixtures import ZenithEnv
import concurrent.futures


def test_create_multiple_timelines_parallel(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env

    tenant_id, _ = env.zenith_cli.create_tenant()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(env.zenith_cli.create_timeline, f"test-timeline-{i}", tenant_id) for i in range(4)]
        for future in futures:
            future.result()
