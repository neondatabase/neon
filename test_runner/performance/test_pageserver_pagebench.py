import json
import os
import shutil
import subprocess
from pathlib import Path
import time
from typing import List, Tuple

import pytest
from fixtures.benchmark_fixture import NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    SnapshotDir,
    last_flush_lsn_upload,
)
from fixtures.pageserver.utils import wait_until_tenant_active, wait_until_tenant_state
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.types import TenantId, TimelineId
import fixtures.pageserver.remote_storage


@pytest.fixture(scope="function")
@pytest.mark.timeout(1000)
def snapshotting_env(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_snapshot_dir: SnapshotDir,
) -> Tuple[NeonEnv, TimelineId, List[TenantId]]:
    """
    The fixture prepares environment or restores it from a snapshot.

    The logic is the following:
    - if the snapshot directory exists, the snapshot is restored from it
    - if there is no snapshot, the environment is initialized from scratch and stored in a snapshot
    - if the fixture is executed on CI (it has CI=true in the environment), the snapshot is not saved
    """

    save_snapshot = os.getenv("CI", "false") != "true"

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # create our template tenant
    tenant_config_mgmt_api = {
        "gc_period": "0s",
        "checkpoint_timeout": "10 years",
        "compaction_period": "20 s",
        "compaction_threshold": 10,
        "compaction_target_size": 134217728,
        "checkpoint_distance": 268435456,
        "image_creation_threshold": 3,
    }

    if test_snapshot_dir.is_initialized():
        save_snapshot = False
        env = neon_env_builder.from_repo_dir(test_snapshot_dir.path)
        ps_http = env.pageserver.http_client()
        tenants = list(
            {TenantId(t.name) for t in (test_snapshot_dir.path.glob("pageserver_*/tenants/*"))}
        )
        template_timeline = env.initial_timeline

        neon_env_builder.start()
    else:
        env = neon_env_builder.init_start()

        remote_storage = env.pageserver_remote_storage
        assert isinstance(remote_storage, LocalFsStorage)

        ps_http = env.pageserver.http_client()
        # clean up the useless default tenant
        ps_http.tenant_delete(env.initial_tenant)

        tenant_config_cli = {k: str(v) for k, v in tenant_config_mgmt_api.items()}

        template_tenant, template_timeline = env.neon_cli.create_tenant(
            conf=tenant_config_cli, set_default=True
        )
        with env.endpoints.create_start("main", tenant_id=template_tenant) as ep:
            pg_bin.run_capture(["pgbench", "-i", "-s5", ep.connstr()])
            last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
        ps_http.tenant_detach(template_tenant)

        # duplicate the template 20 times tenants in localfs storage
        tenants = fixtures.pageserver.remote_storage.duplicate_tenant(env, template_tenant, 20)

        # In theory we could just attach all the tenants, force on-demand downloads via mgmt API, and be done.
        # However, on-demand downloads are quite slow ATM.
        # => do the on-demand downloads in Python.
        assert ps_http.tenant_list() == []
        # make the attach fail after it created enough on-disk state to retry loading
        # the tenant next startup, but before it can start background loops that would start download
        ps_http.configure_failpoints(("attach-before-activate", "return"))
        env.pageserver.allowed_errors.append(
            ".*attach failed, setting tenant state to Broken: attach-before-activate.*"
        )
        for tenant in tenants:
            env.pageserver.tenant_attach(
                tenant,
                config=tenant_config_mgmt_api.copy(),
            )
            wait_until_tenant_state(ps_http, tenant, "Broken", 3)
        env.pageserver.stop()  # clears the failpoint as a side-effect
        tenant_timelines = list(map(lambda tenant: (tenant, template_timeline), tenants))
        fixtures.pageserver.remote_storage.copy_all_remote_layer_files_to_local_tenant_dir(
            env, tenant_timelines
        )
        env.pageserver.start()

    for tenant in tenants:
        wait_until_tenant_active(ps_http, tenant)

    # ensure all layers are resident for predictiable performance
    for tenant in tenants:
        for timeline in ps_http.tenant_status(tenant)["timelines"]:
            info = ps_http.layer_map_info(tenant, timeline)
            for layer in info.historic_layers:
                assert not layer.remote

    # take snapshot after download all layers so tenant dir restoration is fast
    if save_snapshot:
        shutil.copytree(env.repo_dir, test_snapshot_dir.path)
        test_snapshot_dir.set_initialized()

    return env, template_timeline, tenants


def test_getpage_throughput(
    snapshotting_env: Tuple[NeonEnv, TimelineId, List[TenantId]],
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
):
    env, template_timeline, tenants = snapshotting_env
    ps_http = env.pageserver.http_client()

    # run the benchmark with one client per timeline, each doing 10k requests to random keys.
    duration = "10s"
    cmd = [
        str(env.neon_binpath / "pagebench"),
        "get-page-latest-lsn",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--page-service-connstring",
        env.pageserver.connstr(password=None),
        "--runtime",
        duration,
        # "--per-target-rate-limit", "50",
        *[f"{tenant}/{template_timeline}" for tenant in tenants],
    ]
    log.info(f"command: {' '.join(cmd)}")
    basepath = pg_bin.run_capture(cmd, with_command_header=False)
    results_path = Path(basepath + ".stdout")
    log.info(f"Benchmark results at: {results_path}")

    with open(results_path, "r") as f:
        results = json.load(f)

    log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")

    zenbenchmark.record_pagebench_results("get-page-latest-lsn", results, duration)
