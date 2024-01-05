from dataclasses import dataclass
import json
import os
import shutil
import subprocess
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Tuple

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

@dataclass
class SingleTimeline:
    env: NeonEnv
    timeline_id: TimelineId
    tenants: List[TenantId]

def single_timeline(
    neon_env_builder: NeonEnvBuilder,
    snapshot_dir: SnapshotDir,
    setup_template: Callable[[NeonEnv], Tuple[TenantId, TimelineId, Dict[str, Any]]],
    ncopies: int,
) ->  SingleTimeline:
    """
    Create (or rehydrate from `snapshot_dir`) an env with `ncopies` copies
    of a template tenant with a single timeline.
    """

    save_snapshot = os.getenv("CI", "false") != "true"

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    if snapshot_dir.is_initialized():
        save_snapshot = False
        env = neon_env_builder.from_repo_dir(snapshot_dir.path)
        ps_http = env.pageserver.http_client()
        tenants = list(
            {TenantId(t.name) for t in (snapshot_dir.path.glob("pageserver_*/tenants/*"))}
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

        template_tenant, template_timeline, template_config = setup_template(env)

        env.pageserver.http_client().tenant_detach(template_tenant)
        # duplicate the template 20 times tenants in localfs storage
        tenants = fixtures.pageserver.remote_storage.duplicate_tenant(env, template_tenant, ncopies)

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
                config=template_config.copy(),
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
        shutil.copytree(env.repo_dir, snapshot_dir.path)
        snapshot_dir.set_initialized()

    return SingleTimeline(env, template_timeline, tenants)
