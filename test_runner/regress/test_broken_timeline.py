import concurrent.futures
import os
from typing import List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, Postgres
from fixtures.types import ZTenantId, ZTimelineId


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_broken_timeline(neon_env_builder: NeonEnvBuilder):
    # One safekeeper is enough for this test.
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    tenant_timelines: List[Tuple[ZTenantId, ZTimelineId, Postgres]] = []

    for n in range(4):
        tenant_id, timeline_id = env.neon_cli.create_tenant()

        pg = env.postgres.create_start("main", tenant_id=tenant_id)
        with pg.cursor() as cur:
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100), 'payload'")
        pg.stop()
        tenant_timelines.append((tenant_id, timeline_id, pg))

    # Stop the pageserver
    env.pageserver.stop()

    # Leave the first timeline alone, but corrupt the others in different ways
    (tenant0, timeline0, pg0) = tenant_timelines[0]

    # Corrupt metadata file on timeline 1
    (tenant1, timeline1, pg1) = tenant_timelines[1]
    metadata_path = "{}/tenants/{}/timelines/{}/metadata".format(env.repo_dir, tenant1, timeline1)
    print(f"overwriting metadata file at {metadata_path}")
    f = open(metadata_path, "w")
    f.write("overwritten with garbage!")
    f.close()

    # Missing layer files file on timeline 2. (This would actually work
    # if we had Cloud Storage enabled in this test.)
    (tenant2, timeline2, pg2) = tenant_timelines[2]
    timeline_path = "{}/tenants/{}/timelines/{}/".format(env.repo_dir, tenant2, timeline2)
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            # Looks like a layer file. Remove it
            os.remove(f"{timeline_path}/{filename}")

    # Corrupt layer files file on timeline 3
    (tenant3, timeline3, pg3) = tenant_timelines[3]
    timeline_path = "{}/tenants/{}/timelines/{}/".format(env.repo_dir, tenant3, timeline3)
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            # Looks like a layer file. Corrupt it
            f = open(f"{timeline_path}/{filename}", "w")
            f.write("overwritten with garbage!")
            f.close()

    env.pageserver.start()

    # Tenant 0 should still work
    pg0.start()
    assert pg0.safe_psql("SELECT COUNT(*) FROM t")[0][0] == 100

    # But all others are broken

    # First timeline would not get loaded into pageserver due to corrupt metadata file
    (_tenant, _timeline, pg) = tenant_timelines[1]
    with pytest.raises(
        Exception, match=f"Could not get timeline {timeline1} in tenant {tenant1}"
    ) as err:
        pg.start()
    log.info(f"compute startup failed eagerly for timeline with corrupt metadata: {err}")

    # Yet other timelines will fail when their layers will be queried during basebackup: we don't check layer file contents on startup, when loading the timeline
    for n in range(2, 4):
        (_tenant, _timeline, pg) = tenant_timelines[n]
        with pytest.raises(Exception, match="extracting base backup failed") as err:
            pg.start()
        log.info(
            f"compute startup failed lazily for timeline with corrupt layers, during basebackup preparation: {err}"
        )


def test_create_multiple_timelines_parallel(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant_id, _ = env.neon_cli.create_tenant()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                env.neon_cli.create_timeline, f"test-create-multiple-timelines-{i}", tenant_id
            )
            for i in range(4)
        ]
        for future in futures:
            future.result()


def test_fix_broken_timelines_on_startup(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant_id, _ = env.neon_cli.create_tenant()

    # Introduce failpoint when creating a new timeline
    env.pageserver.safe_psql("failpoints before-checkpoint-new-timeline=return")
    with pytest.raises(Exception, match="before-checkpoint-new-timeline"):
        _ = env.neon_cli.create_timeline("test_fix_broken_timelines", tenant_id)

    # Restart the page server
    env.neon_cli.pageserver_stop(immediate=True)
    env.neon_cli.pageserver_start()

    # Check that tenant with "broken" timeline is not loaded.
    with pytest.raises(Exception, match=f"Failed to get repo for tenant {tenant_id}"):
        env.neon_cli.list_timelines(tenant_id)
