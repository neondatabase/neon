import os
import pytest
import shutil
import subprocess
import sys
from fixtures.utils import (get_self_dir, mkdir_if_needed,
                            subprocess_capture, global_counter)

"""
This file contains pytest fixtures. A fixture is a test resource that can be
summoned by placing its name in the test's arguments.

A fixture is created with the decorator @zenfixture, which is a wrapper around
the standard pytest.fixture with some extra behavior.

There are several environment variables that can control the running of tests:
ZENITH_BIN, POSTGRES_BIN, etc. See README.md for more information.

To use fixtures in a test file, add this line of code:

    pytest_plugins = ("fixtures.zenith_fixtures")

Don't import functions from this file, or pytest will emit warnings. Instead
put directly-importable functions into utils.py or another separate file.
"""

DEFAULT_OUTPUT_DIR = 'test_output'
DEFAULT_POSTGRES_DIR = 'tmp_install'


def determine_scope(fixture_name, config):
    return 'session'


def zenfixture(func):
    """ This is a python decorator for fixtures with a flexible scope.

    By default every test function will set up and tear down a new
    database. In pytest, this is called fixtures "function" scope.

    If the environment variable TEST_SHARED_FIXTURES is set, then all
    tests will share the same database. State, logs, etc. will be
    stored in a directory called "shared".

    """
    if os.environ.get('TEST_SHARED_FIXTURES') is None:
        scope = 'function'
    else:
        scope = 'session'
    return pytest.fixture(func, scope=scope)


@pytest.fixture(autouse=True, scope='session')
def safety_check():
    """ Ensure that no unwanted daemons are running before we start testing. """
    cmd = ['pgrep', '-c', 'pageserver|postgres|wal_acceptor']
    result = subprocess.run(cmd)
    if result.returncode == 0:
        # returncode of 0 means it found something.
        # This is bad; we don't want any of those processes polluting the
        # result of the test.
        raise Exception('found interfering processes running')


class ZenithCli:
    """ An object representing the CLI binary named "zenith".

    We also store an environment that will tell the CLI to operate
    on a particular ZENITH_REPO_DIR.
    """

    def __init__(self, binpath, repo_dir, pg_distrib_dir):
        assert os.path.isdir(binpath)
        self.binpath = binpath
        self.bin_zenith = os.path.join(binpath, 'zenith')
        self.env = os.environ.copy()
        self.env['ZENITH_REPO_DIR'] = repo_dir
        self.env['POSTGRES_BIN'] = pg_distrib_dir

    def run(self, arguments):
        """ Run "zenith" with the specified arguments.

        arguments must be in list form, e.g. ['pg', 'create']
        """
        assert type(arguments) == list
        args = [self.bin_zenith] + arguments
        print('Running command "{}"'.format(' '.join(args)))
        subprocess.run(args, env=self.env, check=True)

    def run_init(self):
        """ Run the "zenith init <dir>" command. """
        self.run(['init'])


@zenfixture
def zenith_cli(zenith_binpath, repo_dir, pg_distrib_dir):
    return ZenithCli(zenith_binpath, repo_dir, pg_distrib_dir)


class ZenithPageserver:
    """ An object representing a running pageserver. """

    def __init__(self, zenith_cli):
        self.zenith_cli = zenith_cli
        self.running = False

    def start(self):
        self.zenith_cli.run(['pageserver', 'start'])
        self.running = True

    def stop(self):
        # FIXME: this is a todo!() in the zenith cli code
        if self.running:
            try:
                subprocess.run(['killall', 'pageserver'])
            except FileNotFoundError:
                print(
                    'WARNING: Failed to terminate pageserver, "killall" not found', file=sys.stderr)


@zenfixture
def pageserver(zenith_cli):
    ps = ZenithPageserver(zenith_cli)
    yield ps
    # After the yield comes any cleanup code we need.
    print('Starting pageserver cleanup')
    ps.stop()


class Postgres:
    """ An object representing a running postgres daemon. """

    def __init__(self, zenith_cli):
        self.zenith_cli = zenith_cli
        self.running = False
        # path to conf is <repo_dir>/pgdatadirs/pg1/postgresql.conf

    def create_start(self):
        """ create the pg data directory, and start the server """
        self.zenith_cli.run(['pg', 'create'])
        # FIXME: where did the name pg1 come from?
        self.zenith_cli.run(['pg', 'start', 'pg1'])
        self.running = True

    def stop(self):
        if self.running:
            self.zenith_cli.run(['pg', 'stop', 'pg1'])


@zenfixture
def postgres(zenith_cli):
    pg = Postgres(zenith_cli)
    yield pg
    # After the yield comes any cleanup code we need.
    print('Starting postgres cleanup')
    pg.stop()


class PgBin:
    """ A helper class for executing postgres binaries """

    def __init__(self, log_dir, pg_distrib_dir):
        self.log_dir = log_dir
        self.pg_install_path = pg_distrib_dir
        self.pg_bin_path = os.path.join(self.pg_install_path, 'bin')
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] = os.path.join(self.pg_install_path, 'lib')

    def _fixpath(self, command):
        if not '/' in command[0]:
            command[0] = os.path.join(self.pg_bin_path, command[0])

    def _build_env(self, env_add):
        if env_add is None:
            return self.env
        env = self.env.copy()
        env.update(env_add)
        return env

    def run(self, command, env=None, cwd=None):
        """ Run one of the postgres binaries.

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

    def run_capture(self, command, env=None, cwd=None):
        """ Run one of the postgres binaries, with stderr and stdout redirected to a file.

        This is just like `run`, but for chatty programs.
        """
        self._fixpath(command)
        print('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        subprocess_capture(self.log_dir, command, env=env, cwd=cwd, check=True)


@zenfixture
def pg_bin(test_output_dir, pg_distrib_dir):
    return PgBin(test_output_dir, pg_distrib_dir)


@zenfixture
def base_dir():
    """ find the base directory (currently this is the git root) """
    base_dir = os.path.normpath(os.path.join(get_self_dir(), '../..'))
    print('base_dir is', base_dir)
    return base_dir


@zenfixture
def top_output_dir(base_dir):
    """ Compute the top-level directory for all tests. """
    env_test_output = os.environ.get('TEST_OUTPUT')
    if env_test_output is not None:
        output_dir = env_test_output
    else:
        output_dir = os.path.join(base_dir, DEFAULT_OUTPUT_DIR)
    mkdir_if_needed(output_dir)
    return output_dir


@zenfixture
def test_output_dir(request, top_output_dir):
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
def repo_dir(request, test_output_dir):
    """ Compute the test repo_dir

    "repo_dir" is the place where all of the pageserver files will go.
    It doesn't have anything to do with the git repo.
    """
    repo_dir = os.path.join(test_output_dir, 'repo')
    return repo_dir


@zenfixture
def zenith_binpath(base_dir):
    """ find the zenith binaries """
    env_zenith_bin = os.environ.get('ZENITH_BIN')
    if env_zenith_bin:
        zenith_dir = env_zenith_bin
    else:
        zenith_dir = os.path.join(base_dir, 'target/debug')
    if not os.path.exists(os.path.join(zenith_dir, 'pageserver')):
        raise Exception('zenith binaries not found at "{}"'.format(zenith_dir))
    return zenith_dir


@zenfixture
def pg_distrib_dir(base_dir):
    """ find the postgress install """
    env_postgres_bin = os.environ.get('POSTGRES_BIN')
    if env_postgres_bin:
        pg_dir = env_postgres_bin
    else:
        pg_dir = os.path.normpath(os.path.join(base_dir, DEFAULT_POSTGRES_DIR))
    print('postgres dir is', pg_dir)
    if not os.path.exists(os.path.join(pg_dir, 'bin/postgres')):
        raise Exception('postgres not found at "{}"'.format(pg_dir))
    return pg_dir


class SimpleTest:
    """ A fixture object that contains the things we need for a simple test.

    This is an object with common fixture members:
        zenith_cli
        pageserver
        postgres
        pg_bin

    Example:

        @zenfixture
        def my_test(zen_simple):
            zen_simple.pg_bin.run(['pgbench', '-i'])

    """

    def __init__(self, zenith_cli, pageserver, postgres, pg_bin):
        self.zenith_cli = zenith_cli
        self.pageserver = pageserver
        self.postgres = postgres
        self.pg_bin = pg_bin

    def start(self):
        """ Start a pageserver and postgres. """
        self.zenith_cli.run_init()
        self.pageserver.start()
        print('pageserver is running')

        self.postgres.create_start()
        print('postgres is running')


@zenfixture
def zen_simple(zenith_cli, pageserver, postgres, pg_bin):
    simple = SimpleTest(zenith_cli, pageserver, postgres, pg_bin)
    simple.start()
    return simple
