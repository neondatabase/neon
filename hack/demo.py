#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import testgres


def run_command(args):
    print('> Cmd:', ' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    ret = p.wait()
    output = p.stdout.read().strip()
    if output:
        print(textwrap.indent(output, '>> '))
    if ret != 0:
        raise subprocess.CalledProcessError(ret, args)


def make_tarfile(output_filename, source_dir):
    print("* Packing the backup into a tarball")
    cmd = ["tar", r"--transform=s/\.\///", "-C", str(source_dir), "-cf", str(output_filename), "."]
    run_command(cmd)


def create_tenant(tenant_id):
    print("* Creating a new tenant")
    cmd = ["neon_local", "tenant", "create", f"--tenant-id={tenant_id}"]
    run_command(cmd)


def import_backup(args, backup_dir: Path):
    tar = Path('/tmp/base.tar')
    make_tarfile(tar, backup_dir / 'data')

    print("* Importing the timeline into the pageserver")

    manifest = json.loads((backup_dir / "data" / "backup_manifest").read_text())
    start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
    end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]
    print("> LSNs:", start_lsn, end_lsn)

    cmd = (
        "neon_local timeline import "
        f"--tenant-id {args.tenant_id} "
        f"--base-lsn {start_lsn} "
        f"--end-lsn {end_lsn} "
        f"--base-tarfile {tar} "
        f"--timeline-id {args.timeline_id} "
        f"--node-name {args.node}"
    )

    run_command(cmd.split())


def debug_prints(node):
    tuples = node.execute("table foo")
    oid = node.execute("select 'foo'::regclass::oid")[0][0]
    print("> foo's tuples:", tuples, "&", "oid:", oid)
    print("> DBs:", node.execute("select oid, datname from pg_database"))


def main(args):
    print("* Creating a node")
    node = testgres.get_new_node()
    node.init(unix_sockets=False, allow_streaming=True).start()
    node.execute("create table foo as select 1")
    debug_prints(node)
    # node.pgbench_init(scale=1)

    print("* Creating a backup")
    backup = node.backup()
    backup_dir = Path(backup.base_dir)
    print("> Backup dir:", backup_dir)

    # pr = backup.spawn_primary().start()
    # debug_prints(pr)
    # exit(1)

    create_tenant(args.tenant_id)
    import_backup(args, backup_dir)

    print("> Tenant:", args.tenant_id)
    print("> Timeline:", args.timeline_id)
    print("> Node:", args.node)

    print("* Starting postgres")
    cmd = ["neon_local", "pg", "start", f"--tenant-id={args.tenant_id}", f"--timeline-id={args.timeline_id}", args.node]
    run_command(cmd)

    print("* Opening psql session...")
    cmd = ["psql", f"host=127.0.0.1 port=55432 user={os.getlogin()} dbname=postgres"]
    subprocess.call(cmd)


if __name__ == "__main__":
    tenant_id = uuid.uuid4().hex

    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant-id", default=tenant_id)
    parser.add_argument("--timeline-id", default=tenant_id)
    parser.add_argument("node")

    args = parser.parse_args(sys.argv[1:])
    main(args)
