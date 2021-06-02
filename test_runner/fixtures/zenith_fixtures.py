import getpass
import os
import pytest
import shutil
import subprocess

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Literal

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


class ZenithPageserver:
    """ An object representing a running pageserver. """
    def __init__(self, zenith_cli: ZenithCli):
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

    # The page server speaks the Postgres FE/BE protocol, so you can connect
    # to it with any Postgres client, and run special commands. This function
    # returns a libpq connection string for connecting to it.
    def connstr(self) -> str:
        username = getpass.getuser()
        conn_str = 'host={} port={} dbname=postgres user={}'.format('localhost', 64000, username)
        return conn_str


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


class Postgres:
    """ An object representing a running postgres daemon. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str, instance_num: int):
        self.zenith_cli = zenith_cli
        self.instance_num = instance_num
        self.running = False
        self.username = getpass.getuser()
        self.host = 'localhost'
        self.port = 55431 + instance_num  # TODO: find a better way
        self.repo_dir = repo_dir
        self.branch: Optional[str] = None  # dubious, see asserts below
        # path to conf is <repo_dir>/pgdatadirs/<branch_name>/postgresql.conf

    def create(self, branch: str, config_lines: Optional[List[str]] = None) -> 'Postgres':
        """
        Create the pg data directory.
        Returns self.
        """

        if not config_lines:
            config_lines = []

        self.zenith_cli.run(['pg', 'create', branch])
        self.branch = branch
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

    def config(self, lines: List[str]) -> 'Postgres':
        """
        Add lines to postgresql.conf.
        Lines should be an array of valid postgresql.conf rows.
        Returns self.
        """

        filename = 'pgdatadirs/{}/postgresql.conf'.format(self.branch)
        config_name = os.path.join(self.repo_dir, filename)
        with open(config_name, 'a') as conf:
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

    def create_start(self, branch: str, config_lines: Optional[List[str]] = None) -> 'Postgres':
        """
        Create a Postgres instance, then start it.
        Returns self.
        """

        self.create(branch, config_lines).start()

        return self

    def connstr(self, dbname: str = 'postgres', username: Optional[str] = None) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """

        conn_str = 'host={} port={} dbname={} user={}'.format(self.host, self.port, dbname,
                                                              (username or self.username))

        return conn_str


class PostgresFactory:
    """ An object representing multiple running postgres daemons. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str):
        self.zenith_cli = zenith_cli
        self.host = 'localhost'
        self.repo_dir = repo_dir
        self.num_instances = 0
        self.instances: List[Postgres] = []

    def create_start(self,
                     branch: str = "main",
                     config_lines: Optional[List[str]] = None) -> Postgres:

        pg = Postgres(self.zenith_cli, self.repo_dir, self.num_instances + 1)
        self.num_instances += 1
        self.instances.append(pg)

        return pg.create_start(branch, config_lines)

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


@zenfixture
def base_dir() -> str:
    """ find the base directory (currently this is the git root) """

    base_dir = os.path.normpath(os.path.join(get_self_dir(), '../..'))
    print('base_dir is', base_dir)
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
        raise Exception('zenith binaries not found at "{}"'.format(zenith_dir))
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
        raise Exception('postgres not found at "{}"'.format(pg_dir))
    return pg_dir
