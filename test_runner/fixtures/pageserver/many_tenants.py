from __future__ import annotations

import concurrent.futures
from typing import TYPE_CHECKING

import fixtures.pageserver.remote_storage
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


def single_timeline(
    neon_env_builder: NeonEnvBuilder,
    setup_template: Callable[[NeonEnv], tuple[TenantId, TimelineId, dict[str, Any]]],
    ncopies: int,
) -> NeonEnv:
    """
    Create `ncopies` duplicates of a template tenant that has a single timeline.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()

    remote_storage = env.pageserver_remote_storage
    assert isinstance(remote_storage, LocalFsStorage)

    ps_http = env.pageserver.http_client()
    # clean up the useless default tenant
    ps_http.tenant_delete(env.initial_tenant)

    log.info("invoking callback to create template tenant")
    template_tenant, template_timeline, template_config = setup_template(env)
    log.info(
        f"template tenant is template_tenant={template_tenant} template_timeline={template_timeline}"
    )

    log.info("detach template tenant form pageserver")
    env.pageserver.tenant_detach(template_tenant)

    log.info(f"duplicating template tenant {ncopies} times in remote storage")
    tenants = fixtures.pageserver.remote_storage.duplicate_tenant(env, template_tenant, ncopies)

    # In theory we could just attach all the tenants, force on-demand downloads via mgmt API, and be done.
    # However, on-demand downloads are quite slow ATM.
    # => do the on-demand downloads in Python.
    log.info("python-side on-demand download the layer files into local tenant dir")
    tenant_timelines = list(map(lambda tenant: (tenant, template_timeline), tenants))
    fixtures.pageserver.remote_storage.copy_all_remote_layer_files_to_local_tenant_dir(
        env, tenant_timelines
    )

    log.info("attach duplicated tenants to pageserver")
    # In theory we could just attach all the tenants, force on-demand downloads via mgmt API, and be done.
    # However, on-demand downloads are quite slow ATM.
    # => do the on-demand downloads in Python.
    assert ps_http.tenant_list() == []

    def attach(tenant):
        env.pageserver.tenant_attach(
            tenant,
            config=template_config.copy(),
            generation=100,
            override_storage_controller_generation=True,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=22) as executor:
        executor.map(attach, tenants)

    # Benchmarks will start the pageserver explicitly themselves
    env.pageserver.stop()

    return env
