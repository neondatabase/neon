import pytest
import concurrent.futures
from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnvBuilder, ZenithEnv
from fixtures.log_helper import log
import os


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_broken_timeline(zenith_env_builder: ZenithEnvBuilder):
    # One safekeeper is enough for this test.
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    tenant_timelines = []

    for n in range(4):
        tenant_id_uuid, timeline_id_uuid = env.zenith_cli.create_tenant()
        tenant_id = tenant_id_uuid.hex
        timeline_id = timeline_id_uuid.hex

        pg = env.postgres.create_start(f'main', tenant_id=tenant_id_uuid)
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100), 'payload'")

                cur.execute("SHOW neon.timeline_id")
                timeline_id = cur.fetchone()[0]
        pg.stop()
        tenant_timelines.append((tenant_id, timeline_id, pg))

    # Stop the pageserver
    env.pageserver.stop()

    # Leave the first timeline alone, but corrupt the others in different ways
    (tenant0, timeline0, pg0) = tenant_timelines[0]

    # Corrupt metadata file on timeline 1
    (tenant1, timeline1, pg1) = tenant_timelines[1]
    metadata_path = "{}/tenants/{}/timelines/{}/metadata".format(env.repo_dir, tenant1, timeline1)
    print(f'overwriting metadata file at {metadata_path}')
    f = open(metadata_path, "w")
    f.write("overwritten with garbage!")
    f.close()

    # Missing layer files file on timeline 2. (This would actually work
    # if we had Cloud Storage enabled in this test.)
    (tenant2, timeline2, pg2) = tenant_timelines[2]
    timeline_path = "{}/tenants/{}/timelines/{}/".format(env.repo_dir, tenant2, timeline2)
    for filename in os.listdir(timeline_path):
        if filename.startswith('00000'):
            # Looks like a layer file. Remove it
            os.remove(f'{timeline_path}/{filename}')

    # Corrupt layer files file on timeline 3
    (tenant3, timeline3, pg3) = tenant_timelines[3]
    timeline_path = "{}/tenants/{}/timelines/{}/".format(env.repo_dir, tenant3, timeline3)
    for filename in os.listdir(timeline_path):
        if filename.startswith('00000'):
            # Looks like a layer file. Corrupt it
            f = open(f'{timeline_path}/{filename}', "w")
            f.write("overwritten with garbage!")
            f.close()

    env.pageserver.start()

    # Tenant 0 should still work
    pg0.start()
    with closing(pg0.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM t")
            assert cur.fetchone()[0] == 100

    # But all others are broken
    for n in range(1, 4):
        (tenant, timeline, pg) = tenant_timelines[n]
        with pytest.raises(Exception, match="Cannot load local timeline") as err:
            pg.start()
        log.info(f'compute startup failed as expected: {err}')


def test_create_multiple_timelines_parallel(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env

    tenant_id, _ = env.zenith_cli.create_tenant()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(env.zenith_cli.create_timeline,
                            f"test-create-multiple-timelines-{i}",
                            tenant_id) for i in range(4)
        ]
        for future in futures:
            future.result()


def test_fix_broken_timelines_on_startup(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env

    tenant_id, _ = env.zenith_cli.create_tenant()

    # Introduce failpoint when creating a new timeline
    env.pageserver.safe_psql(f"failpoints before-checkpoint-new-timeline=return")
    with pytest.raises(Exception, match="before-checkpoint-new-timeline"):
        _ = env.zenith_cli.create_timeline("test_fix_broken_timelines", tenant_id)

    # Restart the page server
    env.zenith_cli.pageserver_stop(immediate=True)
    env.zenith_cli.pageserver_start()

    # Check that the "broken" timeline is not loaded
    timelines = env.zenith_cli.list_timelines(tenant_id)
    assert len(timelines) == 1
