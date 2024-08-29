import concurrent.futures
import time
from typing import Any, Callable, Dict, Tuple

import fixtures.pageserver.remote_storage
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.utils import (
    wait_until_tenant_state,
)
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind


def single_timeline(
    neon_env_builder: NeonEnvBuilder,
    setup_template: Callable[[NeonEnv], Tuple[TenantId, TimelineId, Dict[str, Any]]],
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
    env.pageserver.allowed_errors.append(
        # tenant detach causes this because the underlying attach-hook removes the tenant from storage controller entirely
        ".*Dropped remote consistent LSN updates.*",
    )

    log.info(f"duplicating template tenant {ncopies} times in S3")
    tenants = fixtures.pageserver.remote_storage.duplicate_tenant(env, template_tenant, ncopies)

    log.info("attach duplicated tenants to pageserver")
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

    def attach_broken(tenant):
        env.pageserver.tenant_attach(
            tenant,
            config=template_config.copy(),
            generation=100,
            override_storage_controller_generation=True,
        )
        time.sleep(0.1)
        wait_until_tenant_state(ps_http, tenant, "Broken", 10)

    with concurrent.futures.ThreadPoolExecutor(max_workers=22) as executor:
        executor.map(attach_broken, tenants)

    env.pageserver.stop(
        immediate=True
    )  # clears the failpoint as a side-effect; immediate to avoid hitting neon_local's timeout
    tenant_timelines = list(map(lambda tenant: (tenant, template_timeline), tenants))
    log.info("python-side on-demand download the layer files into local tenant dir")
    fixtures.pageserver.remote_storage.copy_all_remote_layer_files_to_local_tenant_dir(
        env, tenant_timelines
    )

    return env
