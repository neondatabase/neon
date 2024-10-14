from __future__ import annotations

import concurrent.futures
import os
import queue
import shutil
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from fixtures.common_types import TenantId, TimelineId
from fixtures.neon_fixtures import NeonEnv
from fixtures.pageserver.common_types import (
    InvalidFileName,
    parse_layer_file_name,
)
from fixtures.remote_storage import LocalFsStorage

if TYPE_CHECKING:
    from typing import Any


def duplicate_one_tenant(env: NeonEnv, template_tenant: TenantId, new_tenant: TenantId):
    remote_storage = env.pageserver_remote_storage
    assert isinstance(remote_storage, LocalFsStorage)

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
                env.pagectl.raw_cli(
                    [
                        "layer",
                        "rewrite-summary",
                        str(dst_tl_dir / file.name),
                        "--new-tenant-id",
                        str(new_tenant),
                    ]
                )
            else:
                # index_part etc need no patching
                pass
    return None


def duplicate_tenant(env: NeonEnv, template_tenant: TenantId, ncopies: int) -> list[TenantId]:
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    def work(tenant_id):
        duplicate_one_tenant(env, template_tenant, tenant_id)

    new_tenants: list[TenantId] = [TenantId.generate() for _ in range(0, ncopies)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(work, new_tenants)
    return new_tenants


def local_layer_name_from_remote_name(remote_name: str) -> str:
    try:
        return parse_layer_file_name(remote_name).to_str()
    except InvalidFileName as e:
        comps = remote_name.rsplit("-", 1)
        if len(comps) == 1:
            raise InvalidFileName("no generation suffix found") from e
        else:
            assert len(comps) == 2
            layer_file_name, _generation = comps
            try:
                return parse_layer_file_name(layer_file_name).to_str()
            except InvalidFileName:
                raise


def copy_all_remote_layer_files_to_local_tenant_dir(
    env: NeonEnv, tenant_timelines: list[tuple[TenantId, TimelineId]]
):
    remote_storage = env.pageserver_remote_storage
    assert isinstance(remote_storage, LocalFsStorage)
    work: queue.Queue[Any] = queue.Queue()
    for tenant, timeline in tenant_timelines:
        remote_timeline_path = remote_storage.timeline_path(tenant, timeline)
        local_timeline_path = env.pageserver.timeline_dir(tenant, timeline)
        local_timeline_path.mkdir(parents=True, exist_ok=True)
        downloads = {}
        for remote_layer in remote_timeline_path.glob("*__*"):
            local_name = local_layer_name_from_remote_name(remote_layer.name)
            assert local_name not in downloads, "remote storage must have had split brain"
            downloads[local_name] = remote_layer
        for local_name, remote_path in downloads.items():
            work.put((remote_path, local_timeline_path / local_name))

    def copy_layer_worker(queue):
        while True:
            item = queue.get()
            if item is None:
                return
            remote_path, local_path = item
            # not copy2, so it looks like a recent download, in case that's relevant to e.g. eviction
            shutil.copy(remote_path, local_path, follow_symlinks=False)

    workers = []
    n_threads = os.cpu_count() or 1
    for _ in range(0, n_threads):
        w = threading.Thread(target=copy_layer_worker, args=[work])
        workers.append(w)
        w.start()
        work.put(None)
    for w in workers:
        w.join()
