import json
from pathlib import Path
import subprocess
import testgres
import sys


def from_backup_at(backup_dir: Path):
    manifest = json.loads((backup_dir / "data" / "backup_manifest").read_text())
    start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
    end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    cmd = (
        "target/debug/neon_local timeline import "
        f"--tenant-id {tenant_id} "
        f"--base-lsn {start_lsn} "
        f"--end-lsn {end_lsn} "
        f"--base-tarfile {backup_dir / 'data' / 'base.tar'} "
        f"--timeline-id {timeline_id} "
        f"--node-name {node_name}"
    )

    r = subprocess.check_output(cmd.split())
    print(r)


def main(tenant_id, timeline_id, node_name):
    node = testgres.get_new_node()
    node.init(allow_streaming=True).start()
    node.pgbench_init(scale=2)

    backup = node.backup(backup_format="t")
    backup_dir = Path(backup.base_dir)

    from_backup_at(backup_dir)
    print(backup_dir)


if __name__ == "__main__":
    tenant_id = "56fc742b0993a7adfd63fe37daa8a6ed"  # sys.argv[1]
    timeline_id = "56fc742b0993a7adfd63fe37daa8a7ed"  # sys.argv[2]
    node_name = sys.argv[3]

    dir = sys.argv[4]
    from_backup_at(Path(dir))

    # main(tenant_id, timeline_id, node_name)
