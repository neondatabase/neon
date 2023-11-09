
from pathlib import Path
import shutil
import subprocess
from fixtures.compare_fixtures import NeonCompare
from fixtures.remote_storage import LocalFsStorage
from fixtures.neon_fixtures import last_flush_lsn_upload
from fixtures.pageserver.utils import wait_until_tenant_active
from fixtures.types import TenantId
from fixtures.log_helper import log

def test_getpage_throughput(neon_compare: NeonCompare):
    env = neon_compare.env

    import pdb
    pdb.set_trace()

    remote_storage = env.pageserver_remote_storage
    assert isinstance(remote_storage, LocalFsStorage)

    ps_http = env.pageserver.http_client()

    # clean up initial tenant
    ps_http.tenant_detach(env.initial_tenant)

    # create our template tenant
    tenant_config = {
        "gc_period" : '0s',
        "checkpoint_timeout" : '3650 day',
        "compaction_period" : '20 s',
        "compaction_threshold" : "10",
        "compaction_target_size" : "134217728",
        "checkpoint_distance" : "268435456",
        "image_creation_threshold" : "3",
    }
    template_tenant, template_timeline = env.neon_cli.create_tenant(conf=tenant_config)
    with env.endpoints.create_start("main", tenant_id=template_tenant) as ep:
        neon_compare.pg_bin.run_capture(["pgbench", "-i", "-s50", ep.connstr()])
        last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
    template_tenant_gen = ps_http.tenant_status(template_tenant)["generation"]
    ps_http.tenant_detach(template_tenant)

    # stop PS just for good measure
    env.pageserver.stop()

    # duplicate the tenant in remote stora
    src_timelines_dir: Path = remote_storage.tenant_path(template_tenant) / "timelines"
    assert src_timelines_dir.is_dir(), f"{src_timelines_dir} is not a directory"

    tenants = [template_tenant]

    for i in range(0, 10):
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
                    cmd = [
                        env.neon_binpath / "pagectl",  # TODO: abstract this like the other binaries
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
        ps_http.tenant_attach(tenant, generation=template_tenant_gen+1, config=tenant_config)
    for tenant in tenants:
        wait_until_tenant_active(ps_http, tenant)

    # ensure all layers are resident for predictiable performance
    # TODO: ensure all kinds of eviction are disabled (per-tenant, disk-usage-based)
    for tenant in tenants:
        ps_http.download_all_layers(tenant, template_timeline)

    # run the benchmark
    cmd = [
        env.neon_binpath / "getpage_bench_libpq",
        "--mgmt-api-endpoint", ps_http.base_url,
        "--page-service-connstring", env.pageserver.connstr(password=None),
        "--num-tasks", "1",
        "--num-requests", "100000",
        *tenants,
    ]
    basepath = neon_compare.pg_bin.run_capture(cmd)
    log.info("Benchmark results: %s", basepath + ".stdout")
