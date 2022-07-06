### utils copied from test fixtures
import os
import shutil
from pathlib import Path
import tempfile
from contextlib import closing
import psycopg2
import subprocess
import argparse

from typing import Any, List
from psycopg2.extensions import connection as PgConnection
import asyncpg
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, cast, Union, Tuple

Env = Dict[str, str]

_global_counter = 0


def global_counter() -> int:
    """ A really dumb global counter.
    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
    _global_counter += 1
    return _global_counter


def subprocess_capture(capture_dir: str, cmd: List[str], **kwargs: Any) -> str:
    """ Run a process and capture its output
    Output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.
    If those files already exist, we will overwrite them.
    Returns basepath for files with captured output.
    """
    assert type(cmd) is list
    base = os.path.basename(cmd[0]) + '_{}'.format(global_counter())
    basepath = os.path.join(capture_dir, base)
    stdout_filename = basepath + '.stdout'
    stderr_filename = basepath + '.stderr'

    with open(stdout_filename, 'w') as stdout_f:
        with open(stderr_filename, 'w') as stderr_f:
            print('(capturing output to "{}.stdout")'.format(base))
            subprocess.run(cmd, **kwargs, stdout=stdout_f, stderr=stderr_f)

    return basepath


class PgBin:
    """ A helper class for executing postgres binaries """
    def __init__(self, log_dir: Path, pg_distrib_dir):
        self.log_dir = log_dir
        self.pg_bin_path = os.path.join(str(pg_distrib_dir), 'bin')
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] = os.path.join(str(pg_distrib_dir), 'lib')

    def _fixpath(self, command: List[str]):
        if '/' not in command[0]:
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
        print('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        subprocess.run(command, env=env, cwd=cwd, check=True)

    def run_capture(self,
                    command: List[str],
                    env: Optional[Env] = None,
                    cwd: Optional[str] = None,
                    **kwargs: Any) -> str:
        """
        Run one of the postgres binaries, with stderr and stdout redirected to a file.
        This is just like `run`, but for chatty programs. Returns basepath for files
        with captured output.
        """

        self._fixpath(command)
        print('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        return subprocess_capture(str(self.log_dir),
                                  command,
                                  env=env,
                                  cwd=cwd,
                                  check=True,
                                  **kwargs)

class PgProtocol:
    """ Reusable connection logic """
    def __init__(self, **kwargs):
        self.default_options = kwargs

    def connstr(self, **kwargs) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """
        return str(make_dsn(**self.conn_options(**kwargs)))

    def conn_options(self, **kwargs):
        conn_options = self.default_options.copy()
        if 'dsn' in kwargs:
            conn_options.update(parse_dsn(kwargs['dsn']))
        conn_options.update(kwargs)

        # Individual statement timeout in seconds. 2 minutes should be
        # enough for our tests, but if you need a longer, you can
        # change it by calling "SET statement_timeout" after
        # connecting.
        if 'options' in conn_options:
            conn_options['options'] = f"-cstatement_timeout=120s " + conn_options['options']
        else:
            conn_options['options'] = "-cstatement_timeout=120s"
        return conn_options

    # autocommit=True here by default because that's what we need most of the time
    def connect(self, autocommit=True, **kwargs) -> PgConnection:
        """
        Connect to the node.
        Returns psycopg2's connection object.
        This method passes all extra params to connstr.
        """
        conn = psycopg2.connect(**self.conn_options(**kwargs))

        # WARNING: this setting affects *all* tests!
        conn.autocommit = autocommit
        return conn

    async def connect_async(self, **kwargs) -> asyncpg.Connection:
        """
        Connect to the node from async python.
        Returns asyncpg's connection object.
        """

        # asyncpg takes slightly different options than psycopg2. Try
        # to convert the defaults from the psycopg2 format.

        # The psycopg2 option 'dbname' is called 'database' is asyncpg
        conn_options = self.conn_options(**kwargs)
        if 'dbname' in conn_options:
            conn_options['database'] = conn_options.pop('dbname')

        # Convert options='-c<key>=<val>' to server_settings
        if 'options' in conn_options:
            options = conn_options.pop('options')
            for match in re.finditer('-c(\w*)=(\w*)', options):
                key = match.group(1)
                val = match.group(2)
                if 'server_options' in conn_options:
                    conn_options['server_settings'].update({key: val})
                else:
                    conn_options['server_settings'] = {key: val}
        return await asyncpg.connect(**conn_options)

    def safe_psql(self, query: str, **kwargs: Any) -> List[Tuple[Any, ...]]:
        """
        Execute query against the node and return all rows.
        This method passes all extra params to connstr.
        """
        return self.safe_psql_many([query], **kwargs)[0]

    def safe_psql_many(self, queries: List[str], **kwargs: Any) -> List[List[Tuple[Any, ...]]]:
        """
        Execute queries against the node and return all rows.
        This method passes all extra params to connstr.
        """
        result: List[List[Any]] = []
        with closing(self.connect(**kwargs)) as conn:
            with conn.cursor() as cur:
                for query in queries:
                    print(f"Executing query: {query}")
                    cur.execute(query)

                    if cur.description is None:
                        result.append([])  # query didn't return data
                    else:
                        result.append(cast(List[Any], cur.fetchall()))
        return result


class VanillaPostgres(PgProtocol):
    def __init__(self, pgdatadir: Path, pg_bin: PgBin, port: int, init=True):
        super().__init__(host='localhost', port=port, dbname='postgres')
        self.pgdatadir = pgdatadir
        self.pg_bin = pg_bin
        self.running = False
        if init:
            self.pg_bin.run_capture(['initdb', '-D', str(pgdatadir)])
        self.configure([f"port = {port}\n"])

    def configure(self, options: List[str]):
        """Append lines into postgresql.conf file."""
        assert not self.running
        with open(os.path.join(self.pgdatadir, 'postgresql.conf'), 'a') as conf_file:
            conf_file.write("\n".join(options))

    def start(self, log_path: Optional[str] = None):
        assert not self.running
        self.running = True

        if log_path is None:
            log_path = os.path.join(self.pgdatadir, "pg.log")

        self.pg_bin.run_capture(
            ['pg_ctl', '-w', '-D', str(self.pgdatadir), '-l', log_path, 'start'])

    def stop(self):
        assert self.running
        self.running = False
        self.pg_bin.run_capture(['pg_ctl', '-w', '-D', str(self.pgdatadir), 'stop'])

    def get_subdir_size(self, subdir) -> int:
        """Return size of pgdatadir subdirectory in bytes."""
        return get_dir_size(os.path.join(self.pgdatadir, subdir))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.running:
            self.stop()

#
# Simple script to export nodes from one pageserver
# and import them into another page server
#
from os import path
import os
import time
import requests
import uuid
import subprocess
import argparse
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, cast, Union, Tuple


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
                msg = res.json()['msg']
            except:
                msg = ''
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
                'new_tenant_id': new_tenant_id.hex,
            },
        )

        if res.status_code == 409:
            if ok_if_exists:
                print(f'could not create tenant: already exists for id {new_tenant_id}')
            else:
                res.raise_for_status()
        elif res.status_code == 201:
            print(f'created tenant {new_tenant_id}')
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
    """ Convert lsn from int to standard hex notation. """
    return "{:X}/{:X}".format(num >> 32, num & 0xffffffff)


def lsn_from_hex(lsn_hex: str) -> int:
    """ Convert lsn from hex notation to int. """
    l, r = lsn_hex.split('/')
    return (int(l, 16) << 32) + int(r, 16)


def remote_consistent_lsn(pageserver_http_client: NeonPageserverHttpClient,
                          tenant: uuid.UUID,
                          timeline: uuid.UUID) -> int:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    if detail['remote'] is None:
        # No remote information at all. This happens right after creating
        # a timeline, before any part of it has been uploaded to remote
        # storage yet.
        return 0
    else:
        lsn_str = detail['remote']['remote_consistent_lsn']
        assert isinstance(lsn_str, str)
        return lsn_from_hex(lsn_str)


def wait_for_upload(pageserver_http_client: NeonPageserverHttpClient,
                    tenant: uuid.UUID,
                    timeline: uuid.UUID,
                    lsn: int):
    """waits for local timeline upload up to specified lsn"""
    for i in range(10):
        current_lsn = remote_consistent_lsn(pageserver_http_client, tenant, timeline)
        if current_lsn >= lsn:
            return
        print("waiting for remote_consistent_lsn to reach {}, now {}, iteration {}".format(
            lsn_to_hex(lsn), lsn_to_hex(current_lsn), i + 1))
        time.sleep(1)

    raise Exception("timed out while waiting for remote_consistent_lsn to reach {}, was {}".format(
        lsn_to_hex(lsn), lsn_to_hex(current_lsn)))


def pack_base(log_dir, restored_dir, output_tar):
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(log_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)


def reconstruct_paths(log_dir, pg_bin, base_tar):
    """Yeild list of relation paths"""
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", base_tar, "-C", restored_dir])

        port = "55439"  # Probably free
        with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
            vanilla_pg.configure([f"port={port}"])
            vanilla_pg.start(log_path=os.path.join(log_dir, "tmp_pg.log"))

            # Create database based on template0 because we can't connect to template0
            query = "create database template0copy template template0"
            vanilla_pg.safe_psql(query, user="cloud_admin")
            vanilla_pg.safe_psql("CHECKPOINT", user="cloud_admin")

            # Get all databases
            query = "select oid, datname from pg_database"
            oid_dbname_pairs = vanilla_pg.safe_psql(query, user="cloud_admin")
            template0_oid = [
                oid
                for (oid, database) in oid_dbname_pairs
                if database == "template0"
            ][0]

            # Get rel paths for each database
            for oid, database in oid_dbname_pairs:
                if database == "template0":
                    # We can't connect to template0
                    continue

                query = "select relname, pg_relation_filepath(oid) from pg_class"
                result = vanilla_pg.safe_psql(query, user="cloud_admin", dbname=database)
                for relname, filepath in result:
                    if filepath is not None:

                        if database == "template0copy":
                            # Add all template0copy paths to template0
                            prefix = f"base/{oid}/"
                            if filepath.startswith(prefix):
                                suffix = filepath[len(prefix):]
                                yield f"base/{template0_oid}/{suffix}"
                            elif filepath.startswith("global"):
                                print(f"skipping {database} global file {filepath}")
                            else:
                                raise AssertionError
                        else:
                            yield filepath


def touch_missing_rels(log_dir, corrupt_tar, output_tar, paths):
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", corrupt_tar, "-C", restored_dir])

        # Touch files that don't exist
        for path in paths:
            absolute_path = os.path.join(restored_dir, path)
            exists = os.path.exists(absolute_path)
            if not exists:
                print("File {absolute_path} didn't exist. Creating..")
                Path(absolute_path).touch()

        # Repackage
        pack_base(log_dir, restored_dir, output_tar)


def add_missing_rels(base_tar, output_tar, log_dir, pg_bin):
    reconstructed_paths = set(reconstruct_paths(log_dir, pg_bin, base_tar))
    touch_missing_rels(log_dir, base_tar, output_tar, reconstructed_paths)


def main(args: argparse.Namespace):
    psql_path = Path(args.pg_distrib_dir) / "bin" / "psql"

    old_pageserver_host = args.old_pageserver_host
    new_pageserver_host = args.new_pageserver_host
    tenants = args.tenants

    old_http_client = NeonPageserverHttpClient(old_pageserver_host, args.old_pageserver_http_port)
    old_http_client.check_status()
    old_pageserver_connstr = f"postgresql://{old_pageserver_host}:{args.old_pageserver_pg_port}"

    new_http_client = NeonPageserverHttpClient(new_pageserver_host, args.new_pageserver_http_port)
    new_http_client.check_status()
    new_pageserver_connstr = f"postgresql://{new_pageserver_host}:{args.new_pageserver_pg_port}"

    psql_env = {**os.environ, 'LD_LIBRARY_PATH': '/usr/local/lib/'}

    for tenant_id in tenants:
        print(f"Tenant: {tenant_id}")
        timelines = old_http_client.timeline_list(uuid.UUID(tenant_id))
        print(f"Timelines: {timelines}")

        # Create tenant in new pageserver
        if args.only_import is False:
            new_http_client.tenant_create(uuid.UUID(tenant_id), args.ok_if_exists)

        for timeline in timelines:

            # Export timelines from old pageserver
            if args.only_import is False:
                query = f"fullbackup {timeline['tenant_id']} {timeline['timeline_id']} {timeline['local']['last_record_lsn']}"

                cmd = [psql_path, "--no-psqlrc", old_pageserver_connstr, "-c", query]
                print(f"Running: {cmd}")

                tar_filename = path.join(args.work_dir,
                                         f"{timeline['tenant_id']}_{timeline['timeline_id']}.tar")
                incomplete_filename = tar_filename + ".incomplete"
                stderr_filename = path.join(
                    args.work_dir, f"{timeline['tenant_id']}_{timeline['timeline_id']}.stderr")

                with open(incomplete_filename, 'w') as stdout_f:
                    with open(stderr_filename, 'w') as stderr_f:
                        print(f"(capturing output to {incomplete_filename})")
                        subprocess.run(cmd, stdout=stdout_f, stderr=stderr_f, env=psql_env, check=True)

                pg_bin = PgBin(args.work_dir, args.pg_distrib_dir)
                add_missing_rels(incomplete_filename, tar_filename, args.work_dir, pg_bin)

                file_size = os.path.getsize(tar_filename)
                print(f"Done export: {tar_filename}, size {file_size}")

            # Import timelines to new pageserver
            import_cmd = f"import basebackup {timeline['tenant_id']} {timeline['timeline_id']} {timeline['local']['last_record_lsn']} {timeline['local']['last_record_lsn']}"
            tar_filename = path.join(args.work_dir,
                                     f"{timeline['tenant_id']}_{timeline['timeline_id']}.tar")
            full_cmd = rf"""cat {tar_filename} | {psql_path} {new_pageserver_connstr} -c '{import_cmd}' """

            stderr_filename2 = path.join(
                args.work_dir, f"import_{timeline['tenant_id']}_{timeline['timeline_id']}.stderr")
            stdout_filename = path.join(
                args.work_dir, f"import_{timeline['tenant_id']}_{timeline['timeline_id']}.stdout")

            print(f"Running: {full_cmd}")

            with open(stdout_filename, 'w') as stdout_f:
                with open(stderr_filename2, 'w') as stderr_f:
                    print(f"(capturing output to {stdout_filename})")
                    subprocess.run(full_cmd,
                                   stdout=stdout_f,
                                   stderr=stderr_f,
                                   env=psql_env,
                                   shell=True,
                                   check=True)

                    print(f"Done import")

            # Wait until pageserver persists the files
            wait_for_upload(new_http_client, uuid.UUID(timeline['tenant_id']), uuid.UUID(timeline['timeline_id']), lsn_from_hex(timeline['local']['last_record_lsn']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--tenant-id',
        dest='tenants',
        required=True,
        nargs='+',
        help='Id of the tenant to migrate. You can pass multiple arguments',
    )
    parser.add_argument(
        '--from-host',
        dest='old_pageserver_host',
        required=True,
        help='Host of the pageserver to migrate data from',
    )
    parser.add_argument(
        '--from-http-port',
        dest='old_pageserver_http_port',
        required=False,
        type=int,
        default=9898,
        help='HTTP port of the pageserver to migrate data from. Default: 9898',
    )
    parser.add_argument(
        '--from-pg-port',
        dest='old_pageserver_pg_port',
        required=False,
        type=int,
        default=6400,
        help='pg port of the pageserver to migrate data from. Default: 6400',
    )
    parser.add_argument(
        '--to-host',
        dest='new_pageserver_host',
        required=True,
        help='Host of the pageserver to migrate data to',
    )
    parser.add_argument(
        '--to-http-port',
        dest='new_pageserver_http_port',
        required=False,
        default=9898,
        type=int,
        help='HTTP port of the pageserver to migrate data to. Default: 9898',
    )
    parser.add_argument(
        '--to-pg-port',
        dest='new_pageserver_pg_port',
        required=False,
        default=6400,
        type=int,
        help='pg port of the pageserver to migrate data to. Default: 6400',
    )
    parser.add_argument(
        '--ignore-tenant-exists',
        dest='ok_if_exists',
        required=False,
        help=
        'Ignore error if we are trying to create the tenant that already exists. It can be dangerous if existing tenant already contains some data.',
    )
    parser.add_argument(
        '--pg-distrib-dir',
        dest='pg_distrib_dir',
        required=False,
        default='/usr/local/',
        help='Path where postgres binaries are installed. Default: /usr/local/',
    )
    parser.add_argument(
        '--psql-path',
        dest='psql_path',
        required=False,
        default='/usr/local/bin/psql',
        help='Path to the psql binary. Default: /usr/local/bin/psql',
    )
    parser.add_argument(
        '--only-import',
        dest='only_import',
        required=False,
        default=False,
        action='store_true',
        help='Skip export and tenant creation part',
    )
    parser.add_argument(
        '--work-dir',
        dest='work_dir',
        required=True,
        default=False,
        help='directory where temporary tar files are stored',
    )
    args = parser.parse_args()
    main(args)
