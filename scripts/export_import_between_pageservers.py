#
# Script to export tenants from one pageserver and import them into another page server.
#
# Outline of steps:
# 1. Get `(last_lsn, prev_lsn)` from old pageserver
# 2. Get `fullbackup` from old pageserver, which creates a basebackup tar file
# 3. We import the basebackup into a new pageserver
# 4. We export again via fullbackup, now from the new pageserver and compare the returned
#    tar file with the one we imported. This confirms that we imported everything that was
#    exported, but doesn't guarantee correctness (what if we didn't **export** everything
#    initially?)
# 5. We wait for the new pageserver's remote_consistent_lsn to catch up
#
# For more context on how to use this, see:
# https://github.com/neondatabase/cloud/wiki/Storage-format-migration

import argparse
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
import requests

###############################################
### client-side utils copied from test fixtures
###############################################

Env = Dict[str, str]

_global_counter = 0


def global_counter() -> int:
    """A really dumb global counter.
    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
    _global_counter += 1
    return _global_counter


def subprocess_capture(capture_dir: str, cmd: List[str], **kwargs: Any) -> str:
    """Run a process and capture its output
    Output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.
    If those files already exist, we will overwrite them.
    Returns basepath for files with captured output.
    """
    assert type(cmd) is list
    base = os.path.basename(cmd[0]) + "_{}".format(global_counter())
    basepath = os.path.join(capture_dir, base)
    stdout_filename = basepath + ".stdout"
    stderr_filename = basepath + ".stderr"

    with open(stdout_filename, "w") as stdout_f:
        with open(stderr_filename, "w") as stderr_f:
            print('(capturing output to "{}.stdout")'.format(base))
            subprocess.run(cmd, **kwargs, stdout=stdout_f, stderr=stderr_f)

    return basepath


class PgBin:
    """A helper class for executing postgres binaries"""

    def __init__(self, log_dir: Path, pg_distrib_dir, pg_version):
        self.log_dir = log_dir
        self.pg_bin_path = os.path.join(str(pg_distrib_dir), "v{}".format(pg_version), "bin")
        self.env = os.environ.copy()
        self.env["LD_LIBRARY_PATH"] = os.path.join(
            str(pg_distrib_dir), "v{}".format(pg_version), "lib"
        )

    def _fixpath(self, command: List[str]):
        if "/" not in command[0]:
            command[0] = os.path.join(self.pg_bin_path, command[0])

    def _build_env(self, env_add: Optional[Env]) -> Env:
        if env_add is None:
            return self.env
        env = self.env.copy()
        env.update(env_add)
        return env

    def run(self, command: List[str], env: Optional[Env] = None, cwd: Optional[str] = None):
        """
        Run one of the postgres binaries.
        The command should be in list form, e.g. ['pgbench', '-p', '55432']
        All the necessary environment variables will be set.
        If the first argument (the command name) doesn't include a path (no '/'
        characters present), then it will be edited to include the correct path.
        If you want stdout/stderr captured to files, use `run_capture` instead.
        """

        self._fixpath(command)
        print('Running command "{}"'.format(" ".join(command)))
        env = self._build_env(env)
        subprocess.run(command, env=env, cwd=cwd, check=True)

    def run_capture(
        self,
        command: List[str],
        env: Optional[Env] = None,
        cwd: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """
        Run one of the postgres binaries, with stderr and stdout redirected to a file.
        This is just like `run`, but for chatty programs. Returns basepath for files
        with captured output.
        """

        self._fixpath(command)
        print('Running command "{}"'.format(" ".join(command)))
        env = self._build_env(env)
        return subprocess_capture(
            str(self.log_dir), command, env=env, cwd=cwd, check=True, **kwargs
        )


class NeonPageserverApiException(Exception):
    pass


class NeonPageserverHttpClient(requests.Session):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port

    def verbose_error(self, res: requests.Response):
        try:
            res.raise_for_status()
        except requests.RequestException as e:
            try:
                msg = res.json()["msg"]
            except:  # noqa: E722
                msg = ""
            raise NeonPageserverApiException(msg) from e

    def check_status(self):
        self.get(f"http://{self.host}:{self.port}/v1/status").raise_for_status()

    def tenant_list(self):
        res = self.get(f"http://{self.host}:{self.port}/v1/tenant")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_create(self, new_tenant_id: uuid.UUID, ok_if_exists):
        res = self.post(
            f"http://{self.host}:{self.port}/v1/tenant",
            json={
                "new_tenant_id": new_tenant_id.hex,
            },
        )

        if res.status_code == 409:
            if ok_if_exists:
                print(f"could not create tenant: already exists for id {new_tenant_id}")
            else:
                res.raise_for_status()
        elif res.status_code == 201:
            print(f"created tenant {new_tenant_id}")
        else:
            self.verbose_error(res)

        return new_tenant_id

    def timeline_list(self, tenant_id: uuid.UUID):
        res = self.get(f"http://{self.host}:{self.port}/v1/tenant/{tenant_id.hex}/timeline")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def timeline_detail(self, tenant_id: uuid.UUID, timeline_id: uuid.UUID) -> Dict[Any, Any]:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id.hex}/timeline/{timeline_id.hex}?include-non-incremental-logical-size=1"
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json


def lsn_to_hex(num: int) -> str:
    """Convert lsn from int to standard hex notation."""
    return "{:X}/{:X}".format(num >> 32, num & 0xFFFFFFFF)


def lsn_from_hex(lsn_hex: str) -> int:
    """Convert lsn from hex notation to int."""
    l, r = lsn_hex.split("/")
    return (int(l, 16) << 32) + int(r, 16)


def remote_consistent_lsn(
    pageserver_http_client: NeonPageserverHttpClient, tenant: uuid.UUID, timeline: uuid.UUID
) -> int:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail["remote_consistent_lsn"]
    if lsn_str is None:
        # No remote information at all. This happens right after creating
        # a timeline, before any part of it has been uploaded to remote
        # storage yet.
        return 0
    else:
        assert isinstance(lsn_str, str)
        return lsn_from_hex(lsn_str)


def wait_for_upload(
    pageserver_http_client: NeonPageserverHttpClient,
    tenant: uuid.UUID,
    timeline: uuid.UUID,
    lsn: int,
):
    """waits for local timeline upload up to specified lsn"""
    for i in range(10):
        current_lsn = remote_consistent_lsn(pageserver_http_client, tenant, timeline)
        if current_lsn >= lsn:
            return
        print(
            "waiting for remote_consistent_lsn to reach {}, now {}, iteration {}".format(
                lsn_to_hex(lsn), lsn_to_hex(current_lsn), i + 1
            )
        )
        time.sleep(1)

    raise Exception(
        "timed out while waiting for remote_consistent_lsn to reach {}, was {}".format(
            lsn_to_hex(lsn), lsn_to_hex(current_lsn)
        )
    )


##############
# End of utils
##############


def pack_base(log_dir, restored_dir, output_tar):
    """Create tar file from basebackup, being careful to produce relative filenames."""
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    # We actually cd into the dir and call tar from there. If we call tar from
    # outside we won't encode filenames as relative, and they won't parse well
    # on import.
    subprocess_capture(log_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)


def get_rlsn(pageserver_connstr, tenant_id, timeline_id):
    conn = psycopg2.connect(pageserver_connstr)
    conn.autocommit = True
    with conn.cursor() as cur:
        cmd = f"get_last_record_rlsn {tenant_id} {timeline_id}"
        cur.execute(cmd)
        res = cur.fetchone()
        prev_lsn = res[0]
        last_lsn = res[1]
    conn.close()

    return last_lsn, prev_lsn


def import_timeline(
    args,
    psql_path,
    pageserver_connstr,
    pageserver_http,
    tenant_id,
    timeline_id,
    last_lsn,
    prev_lsn,
    tar_filename,
    pg_version,
):
    # Import timelines to new pageserver
    import_cmd = f"import basebackup {tenant_id} {timeline_id} {last_lsn} {last_lsn} {pg_version}"
    full_cmd = rf"""cat {tar_filename} | {psql_path} {pageserver_connstr} -c '{import_cmd}' """

    stderr_filename2 = os.path.join(args.work_dir, f"import_{tenant_id}_{timeline_id}.stderr")
    stdout_filename = os.path.join(args.work_dir, f"import_{tenant_id}_{timeline_id}.stdout")

    print(f"Running: {full_cmd}")

    with open(stdout_filename, "w") as stdout_f:
        with open(stderr_filename2, "w") as stderr_f:
            print(f"(capturing output to {stdout_filename})")
            pg_bin = PgBin(args.work_dir, args.pg_distrib_dir, pg_version)
            subprocess.run(
                full_cmd,
                stdout=stdout_f,
                stderr=stderr_f,
                env=pg_bin._build_env(None),
                shell=True,
                check=True,
            )

            print("Done import")

    # Wait until pageserver persists the files
    wait_for_upload(
        pageserver_http, uuid.UUID(tenant_id), uuid.UUID(timeline_id), lsn_from_hex(last_lsn)
    )


def export_timeline(
    args,
    psql_path,
    pageserver_connstr,
    tenant_id,
    timeline_id,
    last_lsn,
    prev_lsn,
    tar_filename,
    pg_version,
):
    # Choose filenames
    stderr_filename = os.path.join(args.work_dir, f"{tenant_id}_{timeline_id}.stderr")

    # Construct export command
    query = f"fullbackup {tenant_id} {timeline_id} {last_lsn} {prev_lsn}"
    cmd = [psql_path, "--no-psqlrc", pageserver_connstr, "-c", query]

    # Run export command
    print(f"Running: {cmd}")
    with open(tar_filename, "w") as stdout_f:
        with open(stderr_filename, "w") as stderr_f:
            print(f"(capturing output to {tar_filename})")
            pg_bin = PgBin(args.work_dir, args.pg_distrib_dir, pg_version)
            subprocess.run(
                cmd, stdout=stdout_f, stderr=stderr_f, env=pg_bin._build_env(None), check=True
            )

    # Log more info
    file_size = os.path.getsize(tar_filename)
    print(f"Done export: {tar_filename}, size {file_size}")


def main(args: argparse.Namespace):
    # any psql version will do here. use current DEFAULT_PG_VERSION = 14
    psql_path = str(Path(args.pg_distrib_dir) / "v14" / "bin" / "psql")

    old_pageserver_host = args.old_pageserver_host
    new_pageserver_host = args.new_pageserver_host

    old_http_client = NeonPageserverHttpClient(old_pageserver_host, args.old_pageserver_http_port)
    old_http_client.check_status()
    old_pageserver_connstr = f"postgresql://{old_pageserver_host}:{args.old_pageserver_pg_port}"

    new_http_client = NeonPageserverHttpClient(new_pageserver_host, args.new_pageserver_http_port)
    new_http_client.check_status()
    new_pageserver_connstr = f"postgresql://{new_pageserver_host}:{args.new_pageserver_pg_port}"

    for tenant_id in args.tenants:
        print(f"Tenant: {tenant_id}")
        timelines = old_http_client.timeline_list(uuid.UUID(tenant_id))
        print(f"Timelines: {timelines}")

        # Create tenant in new pageserver
        if args.only_import is False and not args.timelines:
            new_http_client.tenant_create(uuid.UUID(tenant_id), args.ok_if_exists)

        for timeline in timelines:
            # Skip timelines we don't need to export
            if args.timelines and timeline["timeline_id"] not in args.timelines:
                print(f"Skipping timeline {timeline['timeline_id']}")
                continue

            # Choose filenames
            tar_filename = os.path.join(
                args.work_dir, f"{timeline['tenant_id']}_{timeline['timeline_id']}.tar"
            )

            pg_version = timeline["pg_version"]

            # Export timeline from old pageserver
            if args.only_import is False:
                last_lsn, prev_lsn = get_rlsn(
                    old_pageserver_connstr,
                    timeline["tenant_id"],
                    timeline["timeline_id"],
                )
                export_timeline(
                    args,
                    psql_path,
                    old_pageserver_connstr,
                    timeline["tenant_id"],
                    timeline["timeline_id"],
                    last_lsn,
                    prev_lsn,
                    tar_filename,
                    pg_version,
                )

            # Import into new pageserver
            import_timeline(
                args,
                psql_path,
                new_pageserver_connstr,
                new_http_client,
                timeline["tenant_id"],
                timeline["timeline_id"],
                last_lsn,
                prev_lsn,
                tar_filename,
                pg_version,
            )

            # Re-export and compare
            re_export_filename = tar_filename + ".reexport"
            export_timeline(
                args,
                psql_path,
                new_pageserver_connstr,
                timeline["tenant_id"],
                timeline["timeline_id"],
                last_lsn,
                prev_lsn,
                re_export_filename,
                pg_version,
            )

            # Check the size is the same
            old_size = (os.path.getsize(tar_filename),)
            new_size = (os.path.getsize(re_export_filename),)
            if old_size != new_size:
                raise AssertionError(f"Sizes don't match old: {old_size} new: {new_size}")


def non_zero_tcp_port(arg: Any):
    port = int(arg)
    if port < 1 or port > 65535:
        raise argparse.ArgumentTypeError(f"invalid tcp port: {arg}")
    return port


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tenant-id",
        dest="tenants",
        required=True,
        nargs="+",
        help="Id of the tenant to migrate. You can pass multiple arguments",
    )
    parser.add_argument(
        "--timeline-id",
        dest="timelines",
        required=False,
        nargs="+",
        help="Id of the timeline to migrate. You can pass multiple arguments",
    )
    parser.add_argument(
        "--from-host",
        dest="old_pageserver_host",
        required=True,
        help="Host of the pageserver to migrate data from",
    )
    parser.add_argument(
        "--from-http-port",
        dest="old_pageserver_http_port",
        required=False,
        type=int,
        default=9898,
        help="HTTP port of the pageserver to migrate data from. Default: 9898",
    )
    parser.add_argument(
        "--from-pg-port",
        dest="old_pageserver_pg_port",
        required=False,
        type=int,
        default=6400,
        help="pg port of the pageserver to migrate data from. Default: 6400",
    )
    parser.add_argument(
        "--to-host",
        dest="new_pageserver_host",
        required=True,
        help="Host of the pageserver to migrate data to",
    )
    parser.add_argument(
        "--to-http-port",
        dest="new_pageserver_http_port",
        required=False,
        default=9898,
        type=int,
        help="HTTP port of the pageserver to migrate data to. Default: 9898",
    )
    parser.add_argument(
        "--to-pg-port",
        dest="new_pageserver_pg_port",
        required=False,
        default=6400,
        type=int,
        help="pg port of the pageserver to migrate data to. Default: 6400",
    )
    parser.add_argument(
        "--ignore-tenant-exists",
        dest="ok_if_exists",
        required=False,
        help="Ignore error if we are trying to create the tenant that already exists. It can be dangerous if existing tenant already contains some data.",
    )
    parser.add_argument(
        "--pg-distrib-dir",
        dest="pg_distrib_dir",
        required=False,
        default="/usr/local/",
        help="Path where postgres binaries are installed. Default: /usr/local/",
    )
    parser.add_argument(
        "--psql-path",
        dest="psql_path",
        required=False,
        default="/usr/local/v14/bin/psql",
        help="Path to the psql binary. Default: /usr/local/v14/bin/psql",
    )
    parser.add_argument(
        "--only-import",
        dest="only_import",
        required=False,
        default=False,
        action="store_true",
        help="Skip export and tenant creation part",
    )
    parser.add_argument(
        "--work-dir",
        dest="work_dir",
        required=True,
        default=False,
        help="directory where temporary tar files are stored",
    )
    parser.add_argument(
        "--tmp-pg-port",
        dest="tmp_pg_port",
        required=False,
        default=55439,
        type=non_zero_tcp_port,
        help="localhost port to use for temporary postgres instance",
    )
    args = parser.parse_args()
    main(args)
