import concurrent.futures
import threading

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
)
from fixtures.types import TenantId, TimelineId


def test_sharding_split_big_tenant(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Check that splitting works as expected for a tenant with a reasonable amount of data, larger
    than we use in a typical test.
    """
    neon_env_builder.num_pageservers = 4
    env = neon_env_builder.init_configs()
    neon_env_builder.start()
    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.neon_cli.create_tenant(
        tenant_id, timeline_id, shard_count=1, placement_policy='{"Attached":1}'
    )

    # TODO: a large scale/size
    expect_size = 100e6
    scale = 500

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        with env.endpoints.create_start(
            "main",
            tenant_id=tenant_id,
        ) as ep:
            options = "-cstatement_timeout=0 " + ep.default_options.get("options", "")
            connstr = ep.connstr(password=None, options=options)
            password = ep.default_options.get("password", None)
            environ = {}
            if password is not None:
                environ["PGPASSWORD"] = password
            args = ["pgbench", f"-s{scale}", "-i", "-I", "dtGvp", connstr]

            # Write a lot of data into the tenant
            pg_bin.run(args, env=environ)

            # Confirm that we have created a physical size as large as expected
            timeline_info = env.storage_controller.pageserver_api().timeline_detail(
                tenant_id, timeline_id
            )
            log.info(f"Timeline after init: {timeline_info}")
            assert timeline_info["current_physical_size"] > expect_size

            background_job_duration = 30
            background_stop = threading.Event()

            def background_load():
                while not background_stop.is_set():
                    args = [
                        "pgbench",
                        "-N",
                        "-c4",
                        f"-T{background_job_duration}",
                        "-P2",
                        "--progress-timestamp",
                        connstr,
                    ]
                    pg_bin.run(args, env=environ)

            bg_fut = executor.submit(background_load)

            # Do a split while the endpoint is alive
            env.storage_controller.tenant_shard_split(tenant_id, shard_count=4)

            # Pump the scheduler to do all the changes it would do in the background
            # after a shard split.
            env.storage_controller.reconcile_until_idle(timeout_secs=300)

            background_stop.set()
            bg_fut.result(timeout=background_job_duration * 2)
