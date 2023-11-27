from pathlib import Path
import shutil
import subprocess
from typing import List
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
)
from fixtures.types import TenantId
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.log_helper import log
from fixtures.pageserver.utils import wait_until_tenant_active


def test_pageserver_startup_many_tenants(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Usage

    TEST_OUTPUT=/mnt/many_tenants NEON_BIN=$PWD/target/release DEFAULT_PG_VERSION=15 ./scripts/pytest --preserve-database-files --timeout=0 ./test_runner/performance/test_pageserver_startup_many_tenants.py


    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    neon_env_builder.enable_generations = True

    env = neon_env_builder.init_start()
    remote_storage = env.pageserver_remote_storage
    assert isinstance(remote_storage, LocalFsStorage)

    # create our template tenant
    tenant_config_mgmt_api = {
        "gc_period": "0s",
        "checkpoint_timeout": "3650 day",
        "compaction_period": "20 s",
        "compaction_threshold": 10,
        "compaction_target_size": 134217728,
        "checkpoint_distance": 268435456,
        "image_creation_threshold": 3,
    }
    tenant_config_cli = {k: str(v) for k, v in tenant_config_mgmt_api.items()}

    ps_http = env.pageserver.http_client()

    template_tenant, template_timeline = env.neon_cli.create_tenant(conf=tenant_config_cli)
    template_tenant_gen = int(ps_http.tenant_status(template_tenant)["generation"])
    ep = env.endpoints.create_start("main", tenant_id=template_tenant)
    ep.safe_psql("create table foo(b text)")
    for i in range(0, 8):
        ep.safe_psql("insert into foo(b) values ('some text')")
        last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
    ep.stop_and_destroy()
    ps_http.tenant_detach(template_tenant)

    # tear down processes that could mess with us
    env.pageserver.stop()
    for sk in env.safekeepers:
        sk.stop()

    # duplicate the tenant in remote storage
    src_timelines_dir: Path = remote_storage.tenant_path(template_tenant) / "timelines"
    assert src_timelines_dir.is_dir(), f"{src_timelines_dir} is not a directory"
    tenants = [template_tenant]
    for i in range(0, 20_000):
        new_tenant = TenantId.generate()
        tenants.append(new_tenant)
        log.info("Duplicating tenant #%s: %s", i, new_tenant)

        dst_timelines_dir: Path = remote_storage.tenant_path(new_tenant) / "timelines"
        dst_timelines_dir.parent.mkdir(parents=False, exist_ok=False)
        dst_timelines_dir.mkdir(parents=False, exist_ok=False)

        for tl in src_timelines_dir.iterdir():
            src_tl_dir = src_timelines_dir / tl.name
            assert src_tl_dir.is_dir(), f"{src_tl_dir} is not a directory"
            dst_tl_dir = dst_timelines_dir / tl.name
            dst_tl_dir.mkdir(parents=False, exist_ok=False)
            for file in tl.iterdir():
                shutil.copy2(file, dst_tl_dir)
                if "__" in file.name:
                    cmd: List[str] = [
                        str(
                            env.neon_binpath / "pagectl"
                        ),  # TODO: abstract this like the other binaries
                        "layer",
                        "rewrite-summary",
                        str(dst_tl_dir / file.name),
                        "--new-tenant-id",
                        str(new_tenant),
                    ]
                    subprocess.run(cmd, check=True)
                else:
                    # index_part etc need no patching
                    pass

    env.pageserver.start()
    assert ps_http.tenant_list() == []
    for tenant in tenants:
        ps_http.tenant_attach(
            tenant, config=tenant_config_mgmt_api, generation=template_tenant_gen + 1
        )
    for tenant in tenants:
        wait_until_tenant_active(ps_http, tenant)

    # ensure all layers are resident for predictiable performance
    # TODO: ensure all kinds of eviction are disabled (per-tenant, disk-usage-based)
    for tenant in tenants:
        ps_http.download_all_layers(tenant, template_timeline)
