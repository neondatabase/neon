import os
import shutil
from pathlib import Path
import tempfile
from contextlib import closing
import psycopg2
import subprocess
import argparse

### utils copied from test fixtures
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


### actual code


def get_rel_paths(log_dir, pg_bin, base_tar):
    """Yeild list of relation paths"""
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", base_tar, "-C", restored_dir])

        port = "55439"  # Probably free
        with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
            vanilla_pg.configure([f"port={port}"])
            vanilla_pg.start()

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


def pack_base(log_dir, restored_dir, output_tar):
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(log_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)


def get_files_in_tar(log_dir, tar):
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", tar, "-C", restored_dir])

        # Find empty files
        empty_files = []
        for root, dirs, files in os.walk(restored_dir):
            for name in files:
                file_path = os.path.join(root, name)
                yield file_path[len(restored_dir) + 1:]


def corrupt(log_dir, base_tar, output_tar):
    """Remove all empty files and repackage. Return paths of files removed."""
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", base_tar, "-C", restored_dir])

        # Find empty files
        empty_files = []
        for root, dirs, files in os.walk(restored_dir):
            for name in files:
                file_path = os.path.join(root, name)
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    empty_files.append(file_path)

        # Delete empty files (just to see if they get recreated)
        for empty_file in empty_files:
            os.remove(empty_file)

        # Repackage
        pack_base(log_dir, restored_dir, output_tar)

        # Return relative paths
        return {
            empty_file[len(restored_dir) + 1:]
            for empty_file in empty_files
        }


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


# TODO this test is not currently called. It needs any ordinary base.tar path as input
def test_add_missing_rels(base_tar):
    output_tar = base_tar + ".fixed"

    # Create new base tar with missing empty files
    corrupt_tar = os.path.join(test_output_dir, "psql_2-corrupted.stdout")
    deleted_files = corrupt(test_output_dir, base_tar, corrupt_tar)
    assert len(set(get_files_in_tar(test_output_dir, base_tar)) -
               set(get_files_in_tar(test_output_dir, corrupt_tar))) > 0

    # Reconstruct paths from the corrupted tar, assert it covers everything important
    reconstructed_paths = set(get_rel_paths(test_output_dir, pg_bin, corrupt_tar))
    paths_missed = deleted_files - reconstructed_paths
    assert paths_missed.issubset({
        "postgresql.auto.conf",
        "pg_ident.conf",
    })

    # Recreate the correct tar by touching files, compare with original tar
    touch_missing_rels(test_output_dir, corrupt_tar, output_tar, reconstructed_paths)
    paths_missed = (set(get_files_in_tar(test_output_dir, base_tar)) -
                    set(get_files_in_tar(test_output_dir, output_tar)))
    assert paths_missed.issubset({
        "postgresql.auto.conf",
        "pg_ident.conf",
    })


# Example command:
# poetry run python scripts/add_missing_rels.py \
#     --base-tar /home/bojan/src/neondatabase/neon/test_output/test_import_from_pageserver/psql_2.stdout \
#     --output-tar output-base.tar \
#     --log-dir /home/bojan/tmp
#     --pg-distrib-dir /home/bojan/src/neondatabase/neon/tmp_install/
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--base-tar',
        dest='base_tar',
        required=True,
        help='base.tar file to add missing rels to (file will not be modified)',
    )
    parser.add_argument(
        '--output-tar',
        dest='output_tar',
        required=True,
        help='path and name for the output base.tar file',
    )
    parser.add_argument(
        '--log-dir',
        dest='log_dir',
        required=True,
        help='directory to save log files in',
    )
    parser.add_argument(
        '--pg-distrib-dir',
        dest='pg_distrib_dir',
        required=True,
        help='directory where postgres is installed',
    )
    args = parser.parse_args()
    base_tar = args.base_tar
    output_tar = args.output_tar
    log_dir = args.log_dir
    pg_bin = PgBin(log_dir, args.pg_distrib_dir)

    reconstructed_paths = set(get_rel_paths(log_dir, pg_bin, base_tar))
    touch_missing_rels(log_dir, base_tar, output_tar, reconstructed_paths)
