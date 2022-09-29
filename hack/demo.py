#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import testgres


def make_tarfile(output_filename, source_dir):
    cmd = ["tar", r"--transform=s/\.\///", "-C", str(source_dir), "-cf", str(output_filename), "."]
    print("Command: ", " ".join(cmd))
    r = subprocess.check_output(cmd).decode()
    print(textwrap.indent(r, "> "))


def create_tenant(tenant_id):
    cmd = f"neon_local tenant create --tenant-id {tenant_id}"
    print("Run command:", cmd)
    r = subprocess.check_output(cmd.split()).decode()
    print(textwrap.indent(r, "> "))


def from_backup_at(args, backup_dir: Path):
    manifest = json.loads((backup_dir / "data" / "backup_manifest").read_text())
    start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
    end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]
    print("LSNs:", start_lsn, end_lsn)

    print("Make tarball")
    tar = Path("/tmp/base.tar")
    make_tarfile(tar, backup_dir / "data")

    cmd = (
        "neon_local timeline import "
        f"--tenant-id {args.tenant_id} "
        f"--base-lsn {start_lsn} "
        f"--end-lsn {end_lsn} "
        f"--base-tarfile {tar} "
        f"--timeline-id {args.timeline_id} "
        f"--node-name {args.node}"
    )

    print("Run neon_local")
    r = subprocess.check_output(cmd.split()).decode()
    print(textwrap.indent(r, "> "))


def debug_prints(node):
    print("RELID:", node.execute("select 'foo'::regclass::oid")[0][0])
    print("DBs:", node.execute("table pg_database"))
    print("foo:", node.execute("table foo"))


def main(args):
    print("Create a node")
    node = testgres.get_new_node()
    node.init(unix_sockets=False, allow_streaming=True).start()
    node.execute(
        """
        create table foo as select 1;
    """
    )
    debug_prints(node)
    # node.pgbench_init(scale=1)

    print("Create a backup")
    backup = node.backup()
    backup_dir = Path(backup.base_dir)

    # pr = backup.spawn_primary().start()
    # debug_prints(pr)
    # exit(1)

    print("Import a backup")
    create_tenant(args.tenant_id)
    from_backup_at(args, backup_dir)

    print("Backup dir:", backup_dir)
    print("Tenant:", args.tenant_id)
    print("Timeline:", args.timeline_id)
    print("Node:", args.node)

    cmd = f"neon_local pg start --tenant-id={args.tenant_id} --timeline-id={args.timeline_id} {args.node}".split()
    r = subprocess.check_output(cmd).decode()
    print(textwrap.indent(r, "> "))

    cmd = ["psql", "host=127.0.0.1 port=55433 user=cloud_admin dbname=postgres"]
    subprocess.call(cmd)


if __name__ == "__main__":
    tenant_id = uuid.uuid4().hex

    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant-id", default=tenant_id)
    parser.add_argument("--timeline-id", default=tenant_id)
    parser.add_argument("node")

    args = parser.parse_args(sys.argv[1:])
    main(args)
