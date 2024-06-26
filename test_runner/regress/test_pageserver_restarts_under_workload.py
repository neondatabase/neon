# This test spawns pgbench in a thread in the background and concurrently restarts pageserver,
# checking how client is able to transparently restore connection to pageserver
#
import random
import threading
import time

import pytest
from fixtures.common_types import TenantShardId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, PgBin
from fixtures.remote_storage import RemoteStorageKind
from fixtures.utils import wait_until


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_pageserver_restarts_under_workload(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    env.neon_cli.create_branch("test_pageserver_restarts")
    endpoint = env.endpoints.create_start("test_pageserver_restarts")
    n_restarts = 10
    scale = 10

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", f"-T{n_restarts}", connstr])

    thread = threading.Thread(target=run_pgbench, args=(endpoint.connstr(),), daemon=True)
    thread.start()

    for _ in range(n_restarts):
        # Stop the pageserver gracefully and restart it.
        time.sleep(1)
        env.pageserver.stop()
        env.pageserver.start()

    thread.join()


@pytest.mark.timeout(600)
def test_pageserver_migration_under_workload(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    tenant_conf = {
        # We want layer rewrites to happen as soon as possible (this is the most stressful
        # case for the system), so set PITR interval to something tiny.
        "pitr_interval": "5s",
        # Scaled down thresholds.  We will run at ~1GB scale but would like to emulate
        # the behavior of a system running at ~100GB scale.
        "checkpoint_distance": f"{1024 * 1024}",
        "compaction_threshold": "1",
        "compaction_target_size": f"{1024 * 1024}",
        "image_creation_threshold": "2",
        "image_layer_creation_check_threshold": "0",
        # Do compaction & GC super frequently so that they are likely to be running while we are migrating
        "compaction_period": "1s",
        "gc_period": "1s",
    }

    neon_env_builder.num_pageservers = 2
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_pageserver_remote_storage(
        RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)

    for ps in env.pageservers:
        ps.allowed_errors.extend(
            [
                ".*Dropped remote consistent LSN updates.*",
                ".*Dropping stale deletions.*",
                # A stress test may hit the flush_ms timeout while transitioning a tenant to AttachedStale
                ".*Timed out waiting for flush to remote storage, proceeding anyway.*",
            ]
        )

    timeline_id = env.neon_cli.create_branch("branch")
    endpoint = env.endpoints.create_start("branch")
    n_migrations = 50
    scale = 100

    test_stop = threading.Event()

    def run_pgbench(connstr: str):
        ex = None
        while not test_stop.is_set():
            log.info(f"Starting pgbench on {connstr}")
            try:
                pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
                pg_bin.run_capture(["pgbench", "-T10", connstr])
            except Exception as e:
                # We don't stop the loop running pgbench when it fails: we still want to run the test
                # to completion to see if the pageserver itself hit any issues when under load, even if
                # the client is not getting a clean experience.
                ex = e
                log.warning(f"pgbench failed: {e}")

        if ex is not None:
            raise ex

    def safekeeper_restarts():
        while not test_stop.is_set():
            for safekeeper in env.safekeepers:
                if test_stop.is_set():
                    break

                log.info(f"Restarting safekeeper {safekeeper.id}")
                safekeeper.stop()
                time.sleep(2)
                safekeeper.start()
                time.sleep(5)

    # Run pgbench in the background to ensure some data is there to ingest, and some getpage requests are running
    pgbench_thread = threading.Thread(target=run_pgbench, args=(endpoint.connstr(),), daemon=True)
    pgbench_thread.start()

    # Restart safekeepers in the background, so that pageserver's ingestion is not totally smooth, and sometimes
    # during a tenant shutdown we might be in the process of finding a new safekeeper to download from
    safekeeper_thread = threading.Thread(target=safekeeper_restarts, daemon=True)
    safekeeper_thread.start()

    # TODO: inject some randomized timing noise:
    # - before-downloading-layer-stream-pausable
    # - flush-frozen-pausable

    def assert_getpage_reqs(ps_attached, init_getpage_reqs):
        """
        Assert that the getpage request counter has advanced on a particular node
        """
        getpage_reqs = (
            env.get_pageserver(ps_attached)
            .http_client()
            .get_metric_value(
                "pageserver_smgr_query_seconds_global_count", {"smgr_query_type": "get_page_at_lsn"}
            )
        )
        log.info(f"getpage reqs({ps_attached}): {getpage_reqs} (vs init {init_getpage_reqs})")
        assert getpage_reqs is not None
        assert getpage_reqs > init_getpage_reqs

    rng = random.Random(0xDEADBEEF)

    # Ping-pong the tenant between pageservers: this repeatedly exercises the transitions between secondarry and
    # attached, and implicitly exercises the tenant/timeline shutdown() methods under load
    tenant_id = env.initial_tenant
    ps_attached = env.get_tenant_pageserver(tenant_id).id
    ps_secondary = [p for p in env.pageservers if p.id != ps_attached][0].id
    for _ in range(n_migrations):
        # Snapshot of request counter before migration
        init_getpage_reqs = (
            env.get_pageserver(ps_secondary)
            .http_client()
            .get_metric_value(
                "pageserver_smgr_query_seconds_global_count", {"smgr_query_type": "get_page_at_lsn"}
            )
        )
        if init_getpage_reqs is None:
            init_getpage_reqs = 0

        env.storage_controller.tenant_shard_migrate(TenantShardId(tenant_id, 0, 0), ps_secondary)
        ps_attached, ps_secondary = ps_secondary, ps_attached

        # Make sure we've seen some getpage requests: avoid migrating when a client isn't really doing anything
        wait_until(
            30,
            1,
            lambda ps_attached=ps_attached,  # type: ignore
            init_getpage_reqs=init_getpage_reqs: assert_getpage_reqs(
                ps_attached, init_getpage_reqs
            ),
        )

        # Evict some layers, so that we're exercising getpage requests that involve a layer download
        env.get_pageserver(ps_attached).evict_random_layers(rng, tenant_id, timeline_id, 0.1)

        # Since this test exists to detach shutdown issues, be strict on any warnings logged about delays in shutdown.
        # Do this each loop iteration so that on failures we don't bury the issue behind many more iterations of logs.
        for ps in env.pageservers:
            assert not ps.log_contains(
                "(kept the gate from closing|closing is taking longer than expected)"
            )

    test_stop.set()
    pgbench_thread.join()
    safekeeper_thread.join()
