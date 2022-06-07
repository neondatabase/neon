from fixtures.neon_fixtures import NeonEnvBuilder
import time
from pathlib import Path
import os
import shutil


# TODO this function just generates timeline.zip. Make a function that checks
# whether the timeline can be loaded, and separate this "generate timeline.zip"
# functionality elsewhere.
def test_snapshot_pageserver(neon_env_builder: NeonEnvBuilder, test_output_dir):
    # 81920 is the minimum compaction_target size. I choose a larger one to get some images.
    neon_env_builder.pageserver_config_override = """
tenant_config={checkpoint_distance = 3, compaction_target_size = 81920, compaction_period = "1 s"}"""

    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_snapshot_pageserver", "main")
    pg = env.postgres.create_start("test_snapshot_pageserver")

    tenant = env.initial_tenant.hex
    timeline = pg.safe_psql("SHOW neon.timeline_id")[0][0]

    pg.safe_psql("CREATE TABLE foo(i int);")
    for i in range(100):
        pg.safe_psql(f"INSERT INTO foo VALUES ({i});")
        # Sleep so compaction kicks in (every second)
        # TODO trigger manual compaction instead?
        time.sleep(0.1)

    # make some hot pages so we get image layers
    for k in range(6):
        pg.safe_psql(f"UPDATE foo SET i = i + 1")
        # Sleep so compaction kicks in (every second)
        time.sleep(0.3)

    # One more checkpoint
    psconn = env.pageserver.safe_psql(f"checkpoint {tenant} {timeline}")

    # Zip the timeline
    # TODO check it contains L0, L1, at least one image
    path = Path(test_output_dir) / "repo" / "tenants" / tenant / "timelines" / timeline
    shutil.make_archive("timeline", "zip", path)
