# Usage from top of repo:
#  poetry run python3 ./scripts/ps_duplicate_tenant.py c66e2e233057f7f05563caff664ecb14 .neon/remote_storage_local_fs
import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.append("test_runner")

from fixtures.pageserver.http import PageserverHttpClient  # noqa: E402
from fixtures.types import TenantId  # noqa: E402

parser = argparse.ArgumentParser(description="Duplicate tenant script.")
parser.add_argument("initial_tenant", type=str, help="Initial tenant")
parser.add_argument("remote_storage_local_fs_root", type=Path, help="Remote storage local fs root")
parser.add_argument("--ncopies", type=int, help="Number of copies")
parser.add_argument("--numthreads", type=int, default=1, help="Number of threads")
parser.add_argument("--port", type=int, default=9898, help="Pageserver management api port")

args = parser.parse_args()

initial_tenant = args.initial_tenant
remote_storage_local_fs_root: Path = args.remote_storage_local_fs_root
ncopies = args.ncopies
numthreads = args.numthreads

new_tenant = TenantId.generate()
print(f"New tenant: {new_tenant}")

client = PageserverHttpClient(args.port, lambda: None)

src_tenant_gen = int(client.tenant_status(initial_tenant)["generation"])

assert remote_storage_local_fs_root.is_dir(), f"{remote_storage_local_fs_root} is not a directory"

src_timelines_dir: Path = remote_storage_local_fs_root / "tenants" / initial_tenant / "timelines"
assert src_timelines_dir.is_dir(), f"{src_timelines_dir} is not a directory"

dst_timelines_dir: Path = remote_storage_local_fs_root / "tenants" / str(new_tenant) / "timelines"
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
                "./target/debug/pagectl",  # TODO: abstract this like the other binaries
                "layer",
                "rewrite-summary",
                str(dst_tl_dir / file.name),
                "--new-tenant-id",
                str(new_tenant),
            ]
            subprocess.run(cmd, check=True)

client.tenant_attach(new_tenant, generation=src_tenant_gen)

while True:
    status = client.tenant_status(new_tenant)
    if status["state"]["slug"] == "Active":
        break
    print("Waiting for tenant to be active..., is: " + status["state"]["slug"])
    time.sleep(1)

print("Tenant is active: " + str(new_tenant))
