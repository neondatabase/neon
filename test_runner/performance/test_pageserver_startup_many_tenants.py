import queue
import shutil
import subprocess
import threading
from pathlib import Path
from typing import List, Optional

from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
)
from fixtures.pageserver.utils import wait_until_tenant_active
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.types import TenantId


def duplicate_tenant(
    env: NeonEnv, remote_storage: LocalFsStorage, template_tenant: TenantId, new_tenant: TenantId
):
    src_timelines_dir: Path = remote_storage.tenant_path(template_tenant) / "timelines"
    assert src_timelines_dir.is_dir(), f"{src_timelines_dir} is not a directory"

    assert isinstance(remote_storage, LocalFsStorage)
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
    return None


def test_pageserver_startup_many_tenants(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Usage

    TEST_OUTPUT=/mnt/many_tenants NEON_BIN=$PWD/target/release DEFAULT_PG_VERSION=15 ./scripts/pytest --preserve-database-files --timeout=0 ./test_runner/performance/test_pageserver_startup_many_tenants.py

    Then

    export NEON_REPO_DIR=/mnt/many_tenants/test_pageserver_startup_many_tenants/repo

    # edit $NEON_REPO_DIR/pageserver_1/pageserver.toml to use metric collection,
    # with intervals from prod:
    #
    # metric_collection_endpoint = "https://127.0.0.1:6666"
    # metric_collection_interval: 10min
    # cached_metric_collection_interval: 0s

    # run a fake metric collection endpoint in some other terminal using
    # python3 -m http.server 6666

    # then start pageserver
    ./target/release/neon_local start



    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    neon_env_builder.enable_generations = True

    env = neon_env_builder.init_start()
    remote_storage = env.pageserver_remote_storage
    assert isinstance(remote_storage, LocalFsStorage)

    # cleanup initial tenant
    env.pageserver.tenant_detach(env.initial_tenant)

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
    ep = env.endpoints.create_start("main", tenant_id=template_tenant)
    ep.safe_psql("create table foo(b text)")
    for _i in range(0, 8):
        ep.safe_psql("insert into foo(b) values ('some text')")
        last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
    ep.stop_and_destroy()
    env.pageserver.tenant_detach(template_tenant)

    # duplicate the tenant in remote storage
    def worker(queue: queue.Queue[Optional[TenantId]]):
        while True:
            tenant_id = queue.get()
            if tenant_id is None:
                return
            assert isinstance(remote_storage, LocalFsStorage)
            duplicate_tenant(env, remote_storage, template_tenant, tenant_id)

    new_tenants: List[TenantId] = [TenantId.generate() for _ in range(0, 20_000)]
    duplications: queue.Queue[Optional[TenantId]] = queue.Queue()
    for t in new_tenants:
        duplications.put(t)
    workers = []
    for _ in range(0, 8):
        w = threading.Thread(target=worker, args=[duplications])
        workers.append(w)
        w.start()
        duplications.put(None)
    for w in workers:
        w.join()

    # for evaluation, use the same background loop periods as in prod
    benchmark_tenant_config = {k: v for k, v in tenant_config_mgmt_api.items()}
    del benchmark_tenant_config["compaction_period"]
    del benchmark_tenant_config["gc_period"]
    benchmark_tenant_config["eviction_policy"] = {
        "kind": "LayerAccessThreshold",
        "period": "10m",
        # don't do evictions
        "threshold": "1000d",
    }

    assert ps_http.tenant_list() == []
    for tenant in new_tenants:
        env.pageserver.tenant_attach(tenant, config=benchmark_tenant_config)
    for tenant in new_tenants:
        wait_until_tenant_active(ps_http, tenant)

    # ensure all layers are resident for predictiable performance
    # TODO: ensure all kinds of eviction are disabled (per-tenant, disk-usage-based)
    for tenant in new_tenants:
        ps_http.download_all_layers(tenant, template_timeline)
