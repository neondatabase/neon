import getpass
import os
import psycopg2
import pytest
import shutil
import signal
import subprocess

from contextlib import closing
from pathlib import Path

# Type-related stuff
from psycopg2.extensions import connection as PgConnection
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, cast
from typing_extensions import Literal

from .utils import (get_self_dir, mkdir_if_needed, subprocess_capture)
"""
This file contains pytest fixtures. A fixture is a test resource that can be
summoned by placing its name in the test's arguments.

A fixture is created with the decorator @zenfixture, which is a wrapper around
the standard pytest.fixture with some extra behavior.

There are several environment variables that can control the running of tests:
ZENITH_BIN, POSTGRES_DISTRIB_DIR, etc. See README.md for more information.

To use fixtures in a test file, add this line of code:

>>> pytest_plugins = ("fixtures.zenith_fixtures")

Don't import functions from this file, or pytest will emit warnings. Instead
put directly-importable functions into utils.py or another separate file.
"""

Env = Dict[str, str]
Fn = TypeVar('Fn', bound=Callable[..., Any])

DEFAULT_OUTPUT_DIR = 'test_output'
DEFAULT_POSTGRES_DIR = 'tmp_install'

DEFAULT_PAGESERVER_PORT = 64000


def determine_scope(fixture_name: str, config: Any) -> str:
    return 'session'


def zenfixture(func: Fn) -> Fn:
    """
    This is a python decorator for fixtures with a flexible scope.

    By default every test function will set up and tear down a new
    database. In pytest, this is called fixtures "function" scope.

    If the environment variable TEST_SHARED_FIXTURES is set, then all
    tests will share the same database. State, logs, etc. will be
    stored in a directory called "shared".
    """

    scope: Literal['session', 'function'] = \
        'function' if os.environ.get('TEST_SHARED_FIXTURES') is None else 'session'

    return pytest.fixture(func, scope=scope)


@pytest.fixture(autouse=True, scope='session')
def safety_check() -> None:
    """ Ensure that no unwanted daemons are running before we start testing. """

    # does not use -c as it is not supported on macOS
    cmd = ['pgrep', 'pageserver|postgres|wal_acceptor']
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL)
    if result.returncode == 0:
        # returncode of 0 means it found something.
        # This is bad; we don't want any of those processes polluting the
        # result of the test.
        raise Exception('found interfering processes running')


class PgProtocol:
    """ Reusable connection logic """
    def __init__(self, host: str, port: int, username: Optional[str] = None):
        self.host = host
        self.port = port
        self.username = username or getpass.getuser()

    def connstr(self, *, dbname: str = 'postgres', username: Optional[str] = None) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """

        username = username or self.username
        return f'host={self.host} port={self.port} user={username} dbname={dbname}'

    # autocommit=True here by default because that's what we need most of the time
    def connect(self, *, autocommit: bool = True, **kwargs: Any) -> PgConnection:
        """
        Connect to the node.
        Returns psycopg2's connection object.
        This method passes all extra params to connstr.
        """

        conn = psycopg2.connect(self.connstr(**kwargs))
        # WARNING: this setting affects *all* tests!
        conn.autocommit = autocommit
        return conn

    def safe_psql(self, query: str, **kwargs: Any) -> List[Any]:
        """
        Execute query against the node and return all rows.
        This method passes all extra params to connstr.
        """

        with closing(self.connect(**kwargs)) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description is None:
                    return []  # query didn't return data
                return cast(List[Any], cur.fetchall())


class ZenithCli:
    """
    An object representing the CLI binary named "zenith".

    We also store an environment that will tell the CLI to operate
    on a particular ZENITH_REPO_DIR.
    """
    def __init__(self, binpath: str, repo_dir: str, pg_distrib_dir: str):
        assert os.path.isdir(binpath)
        self.binpath = binpath
        self.bin_zenith = os.path.join(binpath, 'zenith')
        self.env = os.environ.copy()
        self.env['ZENITH_REPO_DIR'] = repo_dir
        self.env['POSTGRES_DISTRIB_DIR'] = pg_distrib_dir

    def run(self, arguments: List[str]) -> Any:
        """
        Run "zenith" with the specified arguments.

        Arguments must be in list form, e.g. ['pg', 'create']

        Return both stdout and stderr, which can be accessed as

        >>> result = zenith_cli.run(...)
        >>> assert result.stderr == ""
        >>> print(result.stdout)
        """

        assert type(arguments) == list

        args = [self.bin_zenith] + arguments
        print('Running command "{}"'.format(' '.join(args)))
        return subprocess.run(args,
                              env=self.env,
                              check=True,
                              universal_newlines=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)


@zenfixture
def zenith_cli(zenith_binpath: str, repo_dir: str, pg_distrib_dir: str) -> ZenithCli:
    return ZenithCli(zenith_binpath, repo_dir, pg_distrib_dir)


class ZenithPageserver(PgProtocol):
    """ An object representing a running pageserver. """
    def __init__(self, zenith_cli: ZenithCli):
        super().__init__(host='localhost', port=DEFAULT_PAGESERVER_PORT)

        self.zenith_cli = zenith_cli
        self.running = False

    def init(self) -> 'ZenithPageserver':
        """
        Initialize the repository, i.e. run "zenith init".
        Returns self.
        """

        self.zenith_cli.run(['init'])
        return self

    def start(self) -> 'ZenithPageserver':
        """
        Start the page server.
        Returns self.
        """

        self.zenith_cli.run(['start'])
        self.running = True
        return self

    def stop(self) -> 'ZenithPageserver':
        """
        Stop the page server.
        Returns self.
        """

        if self.running:
            self.zenith_cli.run(['stop'])
            self.running = False

        return self


@zenfixture
def pageserver(zenith_cli: ZenithCli) -> Iterator[ZenithPageserver]:
    """
    The 'pageserver' fixture provides a Page Server that's up and running.

    If TEST_SHARED_FIXTURES is set, the Page Server instance is shared by all
    the tests. To avoid clashing with other tests, don't use the 'main' branch in
    the tests directly. Instead, create a branch off the 'empty' branch and use
    that.

    By convention, the test branches are named after the tests. For example,
    test called 'test_foo' would create and use branches with the 'test_foo' prefix.
    """

    ps = ZenithPageserver(zenith_cli).init().start()
    # For convenience in tests, create a branch from the freshly-initialized cluster.
    zenith_cli.run(["branch", "empty", "main"])

    yield ps

    # After the yield comes any cleanup code we need.
    print('Starting pageserver cleanup')
    ps.stop()


class Postgres(PgProtocol):
    """ An object representing a running postgres daemon. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str, instance_num: int):
        super().__init__(host='localhost', port=55431 + instance_num)

        self.zenith_cli = zenith_cli
        self.instance_num = instance_num
        self.running = False
        self.repo_dir = repo_dir
        self.branch: Optional[str] = None  # dubious, see asserts below
        # path to conf is <repo_dir>/pgdatadirs/<branch_name>/postgresql.conf

    def create(self,
               branch: str,
               wal_acceptors: Optional[str] = None,
               config_lines: Optional[List[str]] = None) -> 'Postgres':
        """
        Create the pg data directory.
        If wal_acceptors is not None, node will use wal acceptors; config is
        adjusted accordingly.
        Returns self.
        """

        if not config_lines:
            config_lines = []

        self.zenith_cli.run(['pg', 'create', branch])
        self.branch = branch
        if wal_acceptors is not None:
            self.adjust_for_wal_acceptors(wal_acceptors)
        if config_lines is None:
            config_lines = []
        self.config(config_lines)

        return self

    def start(self) -> 'Postgres':
        """
        Start the Postgres instance.
        Returns self.
        """

        assert self.branch is not None
        self.zenith_cli.run(['pg', 'start', self.branch])
        self.running = True

        return self

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        filename = f'pgdatadirs/{self.branch}/postgresql.conf'
        return os.path.join(self.repo_dir, filename)

    def adjust_for_wal_acceptors(self, wal_acceptors: str) -> 'Postgres':
        """
        Adjust instance config for working with wal acceptors instead of
        pageserver (pre-configured by CLI) directly.
        """

        # TODO: reuse config()
        with open(self.config_file_path(), "r") as f:
            cfg_lines = f.readlines()
        with open(self.config_file_path(), "w") as f:
            for cfg_line in cfg_lines:
                # walproposer uses different application_name
                if ("synchronous_standby_names" in cfg_line or
                        # don't ask pageserver to fetch WAL from compute
                        "callmemaybe_connstring" in cfg_line):
                    continue
                f.write(cfg_line)
            f.write("synchronous_standby_names = 'walproposer'\n")
            f.write(f"wal_acceptors = '{wal_acceptors}'\n")
        return self

    def config(self, lines: List[str]) -> 'Postgres':
        """
        Add lines to postgresql.conf.
        Lines should be an array of valid postgresql.conf rows.
        Returns self.
        """

        with open(self.config_file_path(), 'a') as conf:
            for line in lines:
                conf.write(line)
                conf.write('\n')

        return self

    def stop(self) -> 'Postgres':
        """
        Stop the Postgres instance if it's running.
        Returns self.
        """

        if self.running:
            assert self.branch is not None
            self.zenith_cli.run(['pg', 'stop', self.branch])
            self.running = False

        return self

    def stop_and_destroy(self) -> 'Postgres':
        """
        Stop the Postgres instance, then destroy it.
        Returns self.
        """

        assert self.branch is not None
        self.zenith_cli.run(['pg', 'stop', '--destroy', self.branch])

        return self

    def create_start(self,
                     branch: str,
                     wal_acceptors: Optional[str] = None,
                     config_lines: Optional[List[str]] = None) -> 'Postgres':
        """
        Create a Postgres instance, then start it.
        Returns self.
        """

        self.create(branch, wal_acceptors, config_lines).start()

        return self


class PostgresFactory:
    """ An object representing multiple running postgres daemons. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str):
        self.zenith_cli = zenith_cli
        self.repo_dir = repo_dir
        self.num_instances = 0
        self.instances: List[Postgres] = []

    def create_start(self,
                     branch: str = "main",
                     wal_acceptors: Optional[str] = None,
                     config_lines: Optional[List[str]] = None) -> Postgres:

        pg = Postgres(self.zenith_cli, self.repo_dir, self.num_instances + 1)
        self.num_instances += 1
        self.instances.append(pg)

        return pg.create_start(branch, wal_acceptors, config_lines)

    def stop_all(self) -> 'PostgresFactory':
        for pg in self.instances:
            pg.stop()

        return self


@zenfixture
def postgres(zenith_cli: ZenithCli, repo_dir: str) -> Iterator[PostgresFactory]:
    pgfactory = PostgresFactory(zenith_cli, repo_dir)

    yield pgfactory

    # After the yield comes any cleanup code we need.
    print('Starting postgres cleanup')
    pgfactory.stop_all()


class PgBin:
    """ A helper class for executing postgres binaries """
    def __init__(self, log_dir: str, pg_distrib_dir: str):
        self.log_dir = log_dir
        self.pg_install_path = pg_distrib_dir
        self.pg_bin_path = os.path.join(self.pg_install_path, 'bin')
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] = os.path.join(self.pg_install_path, 'lib')

    def _fixpath(self, command: List[str]) -> None:
        if '/' not in command[0]:
            command[0] = os.path.join(self.pg_bin_path, command[0])

    def _build_env(self, env_add: Optional[Env]) -> Env:
        if env_add is None:
            return self.env
        env = self.env.copy()
        env.update(env_add)
        return env

    def run(self, command: List[str], env: Optional[Env] = None, cwd: Optional[str] = None) -> None:
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
                    cwd: Optional[str] = None) -> None:
        """
        Run one of the postgres binaries, with stderr and stdout redirected to a file.

        This is just like `run`, but for chatty programs.
        """

        self._fixpath(command)
        print('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        subprocess_capture(self.log_dir, command, env=env, cwd=cwd, check=True)


@zenfixture
def pg_bin(test_output_dir: str, pg_distrib_dir: str) -> PgBin:
    return PgBin(test_output_dir, pg_distrib_dir)


def read_pid(path):
    """ Read content of file into number """
    return int(Path(path).read_text())


class WalAcceptor:
    """ An object representing a running wal acceptor daemon. """
    def __init__(self, wa_binpath, data_dir, port, num):
        self.wa_binpath = wa_binpath
        self.data_dir = data_dir
        self.port = port
        self.num = num  # identifier for logging

    def start(self) -> 'WalAcceptor':
        # create data directory if not exists
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)

        cmd = [self.wa_binpath]
        cmd.extend(["-D", self.data_dir])
        cmd.extend(["-l", "localhost:{}".format(self.port)])
        cmd.append("--daemonize")
        cmd.append("--no-sync")
        # Tell page server it can receive WAL from this WAL safekeeper
        cmd.extend(["--pageserver", "localhost:{}".format(DEFAULT_PAGESERVER_PORT)])
        cmd.extend(["--recall", "1 second"])
        print('Running command "{}"'.format(' '.join(cmd)))
        subprocess.run(cmd, check=True)

        return self

    def stop(self) -> 'WalAcceptor':
        print('Stopping wal acceptor {}'.format(self.num))
        pidfile_path = os.path.join(self.data_dir, "wal_acceptor.pid")
        try:
            pid = read_pid(pidfile_path)
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass  # pidfile might be obsolete
            # TODO: cleanup pid file on exit in wal acceptor
            return self
            # for _ in range(5):
            # print('waiting wal acceptor {} (pid {}) to stop...', self.num, pid)
            # try:
            # read_pid(pidfile_path)
            # except FileNotFoundError:
            # return  # done
            # time.sleep(1)
            # raise Exception('Failed to wait for wal acceptor {} shutdown'.format(self.num))
        except FileNotFoundError:
            print("Wal acceptor {} is not running".format(self.num))
            return self


class WalAcceptorFactory:
    """ An object representing multiple running wal acceptors. """
    def __init__(self, zenith_binpath, data_dir):
        self.wa_binpath = os.path.join(zenith_binpath, 'wal_acceptor')
        self.data_dir = data_dir
        self.instances = []
        self.initial_port = 54321

    def start_new(self) -> WalAcceptor:
        """
        Start new wal acceptor.
        """

        wa_num = len(self.instances)
        wa = WalAcceptor(self.wa_binpath, os.path.join(self.data_dir, f"wal_acceptor_{wa_num}"),
                         self.initial_port + wa_num, wa_num)
        wa.start()
        self.instances.append(wa)
        return wa

    def start_n_new(self, n: int) -> None:
        """
        Start n new wal acceptors.
        """

        for _ in range(n):
            self.start_new()

    def stop_all(self) -> 'WalAcceptorFactory':
        for wa in self.instances:
            wa.stop()
        return self

    def get_connstrs(self) -> str:
        """ Get list of wal acceptor endpoints suitable for wal_acceptors GUC  """
        return ','.join([f"localhost:{wa.port}" for wa in self.instances])


@zenfixture
def wa_factory(zenith_binpath: str, repo_dir: str) -> Iterator[WalAcceptorFactory]:
    """ Gives WalAcceptorFactory providing wal acceptors. """

    wafactory = WalAcceptorFactory(zenith_binpath, os.path.join(repo_dir, "wal_acceptors"))

    yield wafactory

    # After the yield comes any cleanup code we need.
    print('Starting wal acceptors cleanup')
    wafactory.stop_all()


@zenfixture
def base_dir() -> str:
    """ find the base directory (currently this is the git root) """

    base_dir = os.path.normpath(os.path.join(get_self_dir(), '../..'))
    print('\nbase_dir is', base_dir)
    return base_dir


@zenfixture
def top_output_dir(base_dir: str) -> str:
    """ Compute the top-level directory for all tests. """

    env_test_output = os.environ.get('TEST_OUTPUT')
    if env_test_output is not None:
        output_dir = env_test_output
    else:
        output_dir = os.path.join(base_dir, DEFAULT_OUTPUT_DIR)
    mkdir_if_needed(output_dir)
    return output_dir


@zenfixture
def test_output_dir(request: Any, top_output_dir: str) -> str:
    """ Compute the working directory for an individual test. """

    if os.environ.get('TEST_SHARED_FIXTURES') is None:
        # one directory per test
        test_name = request.node.name
    else:
        # We're running shared fixtures. Share a single directory.
        test_name = 'shared'

    test_output_dir = os.path.join(top_output_dir, test_name)
    print('test_output_dir is', test_output_dir)
    shutil.rmtree(test_output_dir, ignore_errors=True)
    mkdir_if_needed(test_output_dir)
    return test_output_dir


@zenfixture
def repo_dir(request: Any, test_output_dir: str) -> str:
    """
    Compute the test repo_dir.

    "repo_dir" is the place where all of the pageserver files will go.
    It doesn't have anything to do with the git repo.
    """

    repo_dir = os.path.join(test_output_dir, 'repo')
    return repo_dir


@zenfixture
def zenith_binpath(base_dir: str) -> str:
    """ Find the zenith binaries. """

    env_zenith_bin = os.environ.get('ZENITH_BIN')
    if env_zenith_bin:
        zenith_dir = env_zenith_bin
    else:
        zenith_dir = os.path.join(base_dir, 'target/debug')
    if not os.path.exists(os.path.join(zenith_dir, 'pageserver')):
        raise Exception(f'zenith binaries not found at "{zenith_dir}"')
    return zenith_dir


@zenfixture
def pg_distrib_dir(base_dir: str) -> str:
    """ Find the postgres install. """

    env_postgres_bin = os.environ.get('POSTGRES_DISTRIB_DIR')
    if env_postgres_bin:
        pg_dir = env_postgres_bin
    else:
        pg_dir = os.path.normpath(os.path.join(base_dir, DEFAULT_POSTGRES_DIR))
    print('postgres dir is', pg_dir)
    if not os.path.exists(os.path.join(pg_dir, 'bin/postgres')):
        raise Exception(f'postgres not found at "{pg_dir}"')
    return pg_dir
