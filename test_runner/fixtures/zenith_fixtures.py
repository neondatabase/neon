from __future__ import annotations

from dataclasses import dataclass
from cached_property import cached_property
import asyncpg
import os
import pathlib
import uuid
import jwt
import json
import psycopg2
import pytest
import shutil
import signal
import socket
import subprocess
import time
import filecmp
import tempfile

from contextlib import closing, suppress
from pathlib import Path
from dataclasses import dataclass

# Type-related stuff
from psycopg2.extensions import connection as PgConnection
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, cast
from typing_extensions import Literal

import requests

from .utils import (get_self_dir, mkdir_if_needed, subprocess_capture)
from fixtures.log_helper import log
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

BASE_PORT = 15000
WORKER_PORT_NUM = 100

# These are set in pytest_configure()
base_dir = ""
zenith_binpath = ""
pg_distrib_dir = ""
top_output_dir = ""


def pytest_configure(config):
    """
    Ensure that no unwanted daemons are running before we start testing.
    Check that we do not owerflow available ports range.
    """
    numprocesses = config.getoption('numprocesses')
    if numprocesses is not None and BASE_PORT + numprocesses * WORKER_PORT_NUM > 32768:  # do not use ephemeral ports
        raise Exception('Too many workers configured. Cannot distrubute ports for services.')

    # does not use -c as it is not supported on macOS
    cmd = ['pgrep', 'pageserver|postgres|safekeeper']
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL)
    if result.returncode == 0:
        # returncode of 0 means it found something.
        # This is bad; we don't want any of those processes polluting the
        # result of the test.
        # NOTE this shows as an internal pytest error, there might be a better way
        raise Exception(
            'Found interfering processes running. Stop all Zenith pageservers, nodes, safekeepers, as well as stand-alone Postgres.'
        )

    # find the base directory (currently this is the git root)
    global base_dir
    base_dir = os.path.normpath(os.path.join(get_self_dir(), '../..'))
    log.info(f'base_dir is {base_dir}')

    # Find the zenith binaries.
    global zenith_binpath
    env_zenith_bin = os.environ.get('ZENITH_BIN')
    if env_zenith_bin:
        zenith_binpath = env_zenith_bin
    else:
        zenith_binpath = os.path.join(base_dir, 'target/debug')
    log.info(f'zenith_binpath is {zenith_binpath}')
    if not os.path.exists(os.path.join(zenith_binpath, 'pageserver')):
        raise Exception('zenith binaries not found at "{}"'.format(zenith_dir))

    # Find the postgres installation.
    global pg_distrib_dir
    env_postgres_bin = os.environ.get('POSTGRES_DISTRIB_DIR')
    if env_postgres_bin:
        pg_distrib_dir = env_postgres_bin
    else:
        pg_distrib_dir = os.path.normpath(os.path.join(base_dir, DEFAULT_POSTGRES_DIR))
    log.info(f'pg_distrib_dir is {pg_distrib_dir}')
    if not os.path.exists(os.path.join(pg_distrib_dir, 'bin/postgres')):
        raise Exception('postgres not found at "{}"'.format(pg_distrib_dir))

    # Compute the top-level directory for all tests.
    global top_output_dir
    env_test_output = os.environ.get('TEST_OUTPUT')
    if env_test_output is not None:
        top_output_dir = env_test_output
    else:
        top_output_dir = os.path.join(base_dir, DEFAULT_OUTPUT_DIR)
    mkdir_if_needed(top_output_dir)


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


@zenfixture
def worker_seq_no(worker_id: str):
    # worker_id is a pytest-xdist fixture
    # it can be master or gw<number>
    # parse it to always get a number
    if worker_id == 'master':
        return 0
    assert worker_id.startswith('gw')
    return int(worker_id[2:])


@zenfixture
def worker_base_port(worker_seq_no: int):
    # so we divide ports in ranges of 100 ports
    # so workers have disjoint set of ports for services
    return BASE_PORT + worker_seq_no * WORKER_PORT_NUM


def can_bind(host: str, port: int) -> bool:
    """
    Check whether a host:port is available to bind for listening

    Inspired by the can_bind() perl function used in Postgres tests, in
    vendor/postgres/src/test/perl/PostgresNode.pm
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        # TODO: The pageserver and safekeepers don't use SO_REUSEADDR at the
        # moment. If that changes, we should use start using SO_REUSEADDR here
        # too, to allow reusing ports more quickly.
        # See https://github.com/zenithdb/zenith/issues/801
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            sock.bind((host, port))
            sock.listen()
            return True
        except socket.error:
            log.info(f"Port {port} is in use, skipping")
            return False


class PortDistributor:
    def __init__(self, base_port: int, port_number: int) -> None:
        self.iterator = iter(range(base_port, base_port + port_number))

    def get_port(self) -> int:
        for port in self.iterator:
            if can_bind("localhost", port):
                return port
        else:
            raise RuntimeError(
                'port range configured for test is exhausted, consider enlarging the range')


@zenfixture
def port_distributor(worker_base_port):
    return PortDistributor(base_port=worker_base_port, port_number=WORKER_PORT_NUM)


class PgProtocol:
    """ Reusable connection logic """
    def __init__(self, host: str, port: int, username: Optional[str] = None):
        self.host = host
        self.port = port
        self.username = username or "zenith_admin"

    def connstr(self,
                *,
                dbname: str = 'postgres',
                username: Optional[str] = None,
                password: Optional[str] = None) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """

        username = username or self.username
        res = f'host={self.host} port={self.port} user={username} dbname={dbname}'
        if not password:
            return res
        return f'{res} password={password}'

    # autocommit=True here by default because that's what we need most of the time
    def connect(self,
                *,
                autocommit=True,
                dbname: str = 'postgres',
                username: Optional[str] = None,
                password: Optional[str] = None) -> PgConnection:
        """
        Connect to the node.
        Returns psycopg2's connection object.
        This method passes all extra params to connstr.
        """

        conn = psycopg2.connect(self.connstr(
            dbname=dbname,
            username=username,
            password=password,
        ))
        # WARNING: this setting affects *all* tests!
        conn.autocommit = autocommit
        return conn

    async def connect_async(self,
                            *,
                            dbname: str = 'postgres',
                            username: Optional[str] = None,
                            password: Optional[str] = None) -> asyncpg.Connection:
        """
        Connect to the node from async python.
        Returns asyncpg's connection object.
        """

        conn = await asyncpg.connect(
            host=self.host,
            port=self.port,
            database=dbname,
            user=username or self.username,
            password=password,
        )
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


@dataclass
class AuthKeys:
    pub: bytes
    priv: bytes

    def generate_management_token(self):
        token = jwt.encode({"scope": "pageserverapi"}, self.priv, algorithm="RS256")

        # jwt.encode can return 'bytes' or 'str', depending on Python version or type
        # hinting or something (not sure what). If it returned 'bytes', convert it to 'str'
        # explicitly.
        if isinstance(token, bytes):
            token = token.decode()

        return token

    def generate_tenant_token(self, tenant_id):
        token = jwt.encode({
            "scope": "tenant", "tenant_id": tenant_id
        },
                           self.priv,
                           algorithm="RS256")

        if isinstance(token, bytes):
            token = token.decode()

        return token


class ZenithEnvBuilder:
    """
    Builder object to create a Zenith runtime environment

    You should use the `zenith_env_builder` or `zenith_simple_env` pytest
    fixture to create the ZenithEnv object. That way, the repository is
    created in the right directory, based on the test name, and it's properly
    cleaned up after the test has finished.
    """
    def __init__(self,
                 repo_dir: Path,
                 port_distributor: PortDistributor,
                 num_safekeepers: int = 0,
                 pageserver_auth_enabled: bool = False):
        self.repo_dir = repo_dir
        self.port_distributor = port_distributor
        self.num_safekeepers = num_safekeepers
        self.pageserver_auth_enabled = pageserver_auth_enabled
        self.env: Optional[ZenithEnv] = None

    def init(self) -> ZenithEnv:
        # Cannot create more than one environment from one builder
        assert self.env is None, "environment already initialized"
        self.env = ZenithEnv(self)
        return self.env

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        # Stop all the nodes.
        if self.env:
            log.info('Cleaning up all storage and compute nodes')
            self.env.postgres.stop_all()
            for sk in self.env.safekeepers:
                sk.stop(immediate=True)
            self.env.pageserver.stop(immediate=True)


class ZenithEnv:
    """
    An object representing the Zenith runtime environment. It consists of
    the page server, 0-N safekeepers, and the compute nodes.

    ZenithEnv contains functions for stopping/starting nodes in the
    environment, checking their status, creating tenants, connecting to the
    nodes, creating and destroying compute nodes, etc. The page server and
    the safekeepers are considered fixed in the environment, you cannot
    create or destroy them after the environment is initialized. (That will
    likely change in the future, as we start supporting multiple page
    servers and adding/removing safekeepers on the fly).

    Some notable functions and fields in ZenithEnv:

    postgres - A factory object for creating postgres compute nodes.

    pageserver - An object that contains functions for manipulating and
        connecting to the pageserver

    safekeepers - An array containing objects representing the safekeepers

    pg_bin - pg_bin.run() can be used to execute Postgres client binaries,
        like psql or pg_dump

    initial_tenant - tenant ID of the initial tenant created in the repository

    zenith_cli() - zenith_cli() can be used to run the 'zenith' CLI tool

    create_tenant() - initializes a new tenant in the page server, returns
        the tenant id
    """
    def __init__(self, config: ZenithEnvBuilder):
        self.repo_dir = config.repo_dir
        self.port_distributor = config.port_distributor

        self.postgres = PostgresFactory(self)

        self.safekeepers: List[Safekeeper] = []

        # generate initial tenant ID here instead of letting 'zenith init' generate it,
        # so that we don't need to dig it out of the config file afterwards.
        self.initial_tenant = uuid.uuid4().hex

        # Create a config file corresponding to the options
        toml = f"""
default_tenantid = '{self.initial_tenant}'
        """

        # Create config for pageserver
        pageserver_port = PageserverPort(
            pg=self.port_distributor.get_port(),
            http=self.port_distributor.get_port(),
        )
        pageserver_auth_type = "ZenithJWT" if config.pageserver_auth_enabled else "Trust"

        toml += f"""
[pageserver]
pg_port = {pageserver_port.pg}
http_port = {pageserver_port.http}
auth_type = '{pageserver_auth_type}'
        """

        # Create a corresponding ZenithPageserver object
        self.pageserver = ZenithPageserver(self, port=pageserver_port)

        # Create config and a Safekeeper object for each safekeeper
        for i in range(1, config.num_safekeepers + 1):
            port = SafekeeperPort(
                pg=self.port_distributor.get_port(),
                http=self.port_distributor.get_port(),
            )

            if config.num_safekeepers == 1:
                name = "single"
            else:
                name = f"sk{i}"
            toml += f"""
[[safekeepers]]
name = '{name}'
pg_port = {port.pg}
http_port = {port.http}
sync = false # Disable fsyncs to make the tests go faster
            """
            safekeeper = Safekeeper(env=self, name=name, port=port)
            self.safekeepers.append(safekeeper)

        log.info(f"Config: {toml}")

        # Run 'zenith init' using the config file we constructed
        with tempfile.NamedTemporaryFile(mode='w+') as tmp:
            tmp.write(toml)
            tmp.flush()

            cmd = ['init', f'--config={tmp.name}']
            self.zenith_cli(cmd)

        # Start up the page server and all the safekeepers
        self.pageserver.start()

        for safekeeper in self.safekeepers:
            safekeeper.start()

    def get_safekeeper_connstrs(self) -> str:
        """ Get list of safekeeper endpoints suitable for wal_acceptors GUC  """
        return ','.join([f'localhost:{wa.port.pg}' for wa in self.safekeepers])

    def create_tenant(self, tenant_id: Optional[str] = None):
        if tenant_id is None:
            tenant_id = uuid.uuid4().hex
        res = self.zenith_cli(['tenant', 'create', tenant_id])
        res.check_returncode()
        return tenant_id

    def zenith_cli(self, arguments: List[str]) -> 'subprocess.CompletedProcess[str]':
        """
        Run "zenith" with the specified arguments.

        Arguments must be in list form, e.g. ['pg', 'create']

        Return both stdout and stderr, which can be accessed as

        >>> result = env.zenith_cli(...)
        >>> assert result.stderr == ""
        >>> log.info(result.stdout)
        """

        assert type(arguments) == list

        bin_zenith = os.path.join(str(zenith_binpath), 'zenith')

        args = [bin_zenith] + arguments
        log.info('Running command "{}"'.format(' '.join(args)))
        log.info(f'Running in "{self.repo_dir}"')

        env_vars = os.environ.copy()
        env_vars['ZENITH_REPO_DIR'] = str(self.repo_dir)
        env_vars['POSTGRES_DISTRIB_DIR'] = str(pg_distrib_dir)

        # Intercept CalledProcessError and print more info
        try:
            res = subprocess.run(args,
                                 env=env_vars,
                                 check=True,
                                 universal_newlines=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            log.info(f"Run success: {res.stdout}")
        except subprocess.CalledProcessError as exc:
            # this way command output will be in recorded and shown in CI in failure message
            msg = f"""\
            Run failed: {exc}
              stdout: {exc.stdout}
              stderr: {exc.stderr}
            """
            log.info(msg)

            raise Exception(msg) from exc

        return res

    @cached_property
    def auth_keys(self) -> AuthKeys:
        pub = (Path(self.repo_dir) / 'auth_public_key.pem').read_bytes()
        priv = (Path(self.repo_dir) / 'auth_private_key.pem').read_bytes()
        return AuthKeys(pub=pub, priv=priv)


@zenfixture
def _shared_simple_env(request: Any, port_distributor) -> Iterator[ZenithEnv]:
    """
    Internal fixture backing the `zenith_simple_env` fixture. If TEST_SHARED_FIXTURES
    is set, this is shared by all tests using `zenith_simple_env`.
    """

    if os.environ.get('TEST_SHARED_FIXTURES') is None:
        # Create the environment in the per-test output directory
        repo_dir = os.path.join(get_test_output_dir(request), "repo")
    else:
        # We're running shared fixtures. Share a single directory.
        repo_dir = os.path.join(str(top_output_dir), "shared_repo")
        shutil.rmtree(repo_dir, ignore_errors=True)

    with ZenithEnvBuilder(Path(repo_dir), port_distributor) as builder:

        env = builder.init()

        # For convenience in tests, create a branch from the freshly-initialized cluster.
        env.zenith_cli(["branch", "empty", "main"])

        # Return the builder to the caller
        yield env


@pytest.fixture(scope='function')
def zenith_simple_env(_shared_simple_env: ZenithEnv) -> Iterator[ZenithEnv]:
    """
    Simple Zenith environment, with no authentication and no safekeepers.

    If TEST_SHARED_FIXTURES environment variable is set, we reuse the same
    environment for all tests that use 'zenith_simple_env', keeping the
    page server and safekeepers running. Any compute nodes are stopped after
    each the test, however.
    """
    yield _shared_simple_env

    _shared_simple_env.postgres.stop_all()


@pytest.fixture(scope='function')
def zenith_env_builder(test_output_dir, port_distributor) -> Iterator[ZenithEnvBuilder]:
    """
    Fixture to create a Zenith environment for test.

    To use, define 'zenith_env_builder' fixture in your test to get access to the
    builder object. Set properties on it to describe the environment.
    Finally, initialize and start up the environment by calling
    zenith_env_builder.init().

    After the initialization, you can launch compute nodes by calling
    the functions in the 'env.postgres' factory object, stop/start the
    nodes, etc.
    """

    # Create the environment in the test-specific output dir
    repo_dir = os.path.join(test_output_dir, "repo")

    # Return the builder to the caller
    with ZenithEnvBuilder(Path(repo_dir), port_distributor) as builder:
        yield builder


class ZenithPageserverHttpClient(requests.Session):
    def __init__(self, port: int, auth_token: Optional[str] = None) -> None:
        super().__init__()
        self.port = port
        self.auth_token = auth_token

        if auth_token is not None:
            self.headers['Authorization'] = f'Bearer {auth_token}'

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def branch_list(self, tenant_id: uuid.UUID) -> List[Dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/branch/{tenant_id.hex}")
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def branch_create(self, tenant_id: uuid.UUID, name: str, start_point: str) -> Dict[Any, Any]:
        res = self.post(f"http://localhost:{self.port}/v1/branch",
                        json={
                            'tenant_id': tenant_id.hex,
                            'name': name,
                            'start_point': start_point,
                        })
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def branch_detail(self, tenant_id: uuid.UUID, name: str) -> Dict[Any, Any]:
        res = self.get(f"http://localhost:{self.port}/v1/branch/{tenant_id.hex}/{name}", )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def tenant_list(self) -> List[Dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant")
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_create(self, tenant_id: uuid.UUID):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant",
            json={
                'tenant_id': tenant_id.hex,
            },
        )
        res.raise_for_status()
        return res.json()

    def get_metrics(self) -> str:
        res = self.get(f"http://localhost:{self.port}/metrics")
        res.raise_for_status()
        return res.text


@dataclass
class PageserverPort:
    pg: int
    http: int


class ZenithPageserver(PgProtocol):
    """
    An object representing a running pageserver.

    Initializes the repository via `zenith init`.
    """
    def __init__(self, env: ZenithEnv, port: PageserverPort, enable_auth=False):
        super().__init__(host='localhost', port=port.pg)
        self.env = env
        self.running = False
        self.service_port = port  # do not shadow PgProtocol.port which is just int

    def start(self) -> 'ZenithPageserver':
        """
        Start the page server.
        Returns self.
        """
        assert self.running == False

        self.env.zenith_cli(['pageserver', 'start'])
        self.running = True
        return self

    def stop(self, immediate=False) -> 'ZenithPageserver':
        """
        Stop the page server.
        Returns self.
        """
        cmd = ['pageserver', 'stop']
        if immediate:
            cmd.extend(['-m', 'immediate'])

        log.info(f"Stopping pageserver with {cmd}")
        if self.running:
            self.env.zenith_cli(cmd)
            self.running = False

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop(True)

    def http_client(self, auth_token: Optional[str] = None):
        return ZenithPageserverHttpClient(
            port=self.service_port.http,
            auth_token=auth_token,
        )


class PgBin:
    """ A helper class for executing postgres binaries """
    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        self.pg_bin_path = os.path.join(str(pg_distrib_dir), 'bin')
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] = os.path.join(str(pg_distrib_dir), 'lib')

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
        log.info('Running command "{}"'.format(' '.join(command)))
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
        log.info('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        return subprocess_capture(self.log_dir, command, env=env, cwd=cwd, check=True, **kwargs)


@pytest.fixture(scope='function')
def pg_bin(test_output_dir: str) -> PgBin:
    return PgBin(test_output_dir)


class Postgres(PgProtocol):
    """ An object representing a running postgres daemon. """
    def __init__(self, env: ZenithEnv, tenant_id: str, port: int):
        super().__init__(host='localhost', port=port)

        self.env = env
        self.running = False
        self.node_name: Optional[str] = None  # dubious, see asserts below
        self.pgdata_dir: Optional[str] = None  # Path to computenode PGDATA
        self.tenant_id = tenant_id
        # path to conf is <repo_dir>/pgdatadirs/tenants/<tenant_id>/<node_name>/postgresql.conf

    def create(
        self,
        node_name: str,
        branch: Optional[str] = None,
        config_lines: Optional[List[str]] = None,
    ) -> 'Postgres':
        """
        Create the pg data directory.
        Returns self.
        """

        if not config_lines:
            config_lines = []

        if branch is None:
            branch = node_name

        self.env.zenith_cli([
            'pg',
            'create',
            f'--tenantid={self.tenant_id}',
            f'--port={self.port}',
            node_name,
            branch
        ])
        self.node_name = node_name
        path = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id / self.node_name
        self.pgdata_dir = os.path.join(self.env.repo_dir, path)

        if self.env.safekeepers:
            self.adjust_for_wal_acceptors(self.env.get_safekeeper_connstrs())
        if config_lines is None:
            config_lines = []
        self.config(config_lines)

        return self

    def start(self) -> 'Postgres':
        """
        Start the Postgres instance.
        Returns self.
        """

        assert self.node_name is not None

        log.info(f"Starting postgres node {self.node_name}")

        run_result = self.env.zenith_cli(
            ['pg', 'start', f'--tenantid={self.tenant_id}', f'--port={self.port}', self.node_name])
        self.running = True

        log.info(f"stdout: {run_result.stdout}")

        return self

    def pg_data_dir_path(self) -> str:
        """ Path to data directory """
        assert self.node_name
        path = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id / self.node_name
        return os.path.join(self.env.repo_dir, path)

    def pg_xact_dir_path(self) -> str:
        """ Path to pg_xact dir """
        return os.path.join(self.pg_data_dir_path(), 'pg_xact')

    def pg_twophase_dir_path(self) -> str:
        """ Path to pg_twophase dir """
        return os.path.join(self.pg_data_dir_path(), 'pg_twophase')

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        return os.path.join(self.pg_data_dir_path(), 'postgresql.conf')

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
            f.write("wal_acceptors = '{}'\n".format(wal_acceptors))
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
            assert self.node_name is not None
            self.env.zenith_cli(['pg', 'stop', self.node_name, f'--tenantid={self.tenant_id}'])
            self.running = False

        return self

    def stop_and_destroy(self) -> 'Postgres':
        """
        Stop the Postgres instance, then destroy it.
        Returns self.
        """

        assert self.node_name is not None
        self.env.zenith_cli(
            ['pg', 'stop', '--destroy', self.node_name, f'--tenantid={self.tenant_id}'])
        self.node_name = None

        return self

    def create_start(
        self,
        node_name: str,
        branch: Optional[str] = None,
        config_lines: Optional[List[str]] = None,
    ) -> 'Postgres':
        """
        Create a Postgres instance, apply config
        and then start it.
        Returns self.
        """

        self.create(
            node_name=node_name,
            branch=branch,
            config_lines=config_lines,
        ).start()

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


class PostgresFactory:
    """ An object representing multiple running postgres daemons. """
    def __init__(self, env: ZenithEnv):
        self.env = env
        self.num_instances = 0
        self.instances: List[Postgres] = []

    def create_start(self,
                     node_name: str = "main",
                     branch: Optional[str] = None,
                     tenant_id: Optional[str] = None,
                     config_lines: Optional[List[str]] = None) -> Postgres:

        pg = Postgres(
            self.env,
            tenant_id=tenant_id or self.env.initial_tenant,
            port=self.env.port_distributor.get_port(),
        )
        self.num_instances += 1
        self.instances.append(pg)

        return pg.create_start(
            node_name=node_name,
            branch=branch,
            config_lines=config_lines,
        )

    def create(self,
               node_name: str = "main",
               branch: Optional[str] = None,
               tenant_id: Optional[str] = None,
               config_lines: Optional[List[str]] = None) -> Postgres:

        pg = Postgres(
            self.env,
            tenant_id=tenant_id or self.env.initial_tenant,
            port=self.env.port_distributor.get_port(),
        )

        self.num_instances += 1
        self.instances.append(pg)

        return pg.create(
            node_name=node_name,
            branch=branch,
            config_lines=config_lines,
        )

    def stop_all(self) -> 'PostgresFactory':
        for pg in self.instances:
            pg.stop()

        return self


def read_pid(path: Path) -> int:
    """ Read content of file into number """
    return int(path.read_text())


@dataclass
class SafekeeperPort:
    pg: int
    http: int


@dataclass
class Safekeeper:
    """ An object representing a running safekeeper daemon. """
    env: ZenithEnv
    port: SafekeeperPort
    name: str  # identifier for logging
    auth_token: Optional[str] = None

    def start(self) -> 'Safekeeper':
        self.env.zenith_cli(['safekeeper', 'start', self.name])

        # wait for wal acceptor start by checking its status
        started_at = time.time()
        while True:
            try:
                http_cli = self.http_client()
                http_cli.check_status()
            except Exception as e:
                elapsed = time.time() - started_at
                if elapsed > 3:
                    raise RuntimeError(
                        f"timed out waiting {elapsed:.0f}s for wal acceptor start: {e}")
                time.sleep(0.5)
            else:
                break  # success
        return self

    def stop(self, immediate=False) -> 'Safekeeper':
        cmd = ['safekeeper', 'stop']
        if immediate:
            cmd.extend(['-m', 'immediate'])
        cmd.append(self.name)

        log.info('Stopping safekeeper {}'.format(self.name))
        self.env.zenith_cli(cmd)
        return self

    def append_logical_message(self, tenant_id: str, timeline_id: str,
                               request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send JSON_CTRL query to append LogicalMessage to WAL and modify
        safekeeper state. It will construct LogicalMessage from provided
        prefix and message, and then will write it to WAL.
        """

        # "replication=0" hacks psycopg not to send additional queries
        # on startup, see https://github.com/psycopg/psycopg2/pull/482
        connstr = f"host=localhost port={self.port.pg} replication=0 options='-c ztimelineid={timeline_id} ztenantid={tenant_id}'"

        with closing(psycopg2.connect(connstr)) as conn:
            # server doesn't support transactions
            conn.autocommit = True
            with conn.cursor() as cur:
                request_json = json.dumps(request)
                log.info(f"JSON_CTRL request on port {self.port.pg}: {request_json}")
                cur.execute("JSON_CTRL " + request_json)
                all = cur.fetchall()
                log.info(f"JSON_CTRL response: {all[0][0]}")
                res = json.loads(all[0][0])
                assert isinstance(res, dict)
                return res

    def http_client(self):
        return SafekeeperHttpClient(port=self.port.http)


@dataclass
class SafekeeperTimelineStatus:
    acceptor_epoch: int
    flush_lsn: str


class SafekeeperHttpClient(requests.Session):
    def __init__(self, port: int) -> None:
        super().__init__()
        self.port = port

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def timeline_status(self, tenant_id: str, timeline_id: str) -> SafekeeperTimelineStatus:
        res = self.get(f"http://localhost:{self.port}/v1/timeline/{tenant_id}/{timeline_id}")
        res.raise_for_status()
        resj = res.json()
        return SafekeeperTimelineStatus(acceptor_epoch=resj['acceptor_state']['epoch'],
                                        flush_lsn=resj['flush_lsn'])


def get_test_output_dir(request: Any) -> str:
    """ Compute the working directory for an individual test. """
    test_name = request.node.name
    test_dir = os.path.join(str(top_output_dir), test_name)
    log.info(f'get_test_output_dir is {test_dir}')
    return test_dir


# This is autouse, so the test output directory always gets created, even
# if a test doesn't put anything there. It also solves a problem with the
# zenith_simple_env fixture: if TEST_SHARED_FIXTURES is not set, it
# creates the repo in the test output directory. But it cannot depend on
# 'test_output_dir' fixture, because when TEST_SHARED_FIXTURES is not set,
# it has 'session' scope and cannot access fixtures with 'function'
# scope. So it uses the get_test_output_dir() function to get the path, and
# this fixture ensures that the directory exists.  That works because
# 'autouse' fixtures are run before other fixtures.
@pytest.fixture(scope='function', autouse=True)
def test_output_dir(request: Any) -> str:
    """ Create the working directory for an individual test. """

    # one directory per test
    test_dir = get_test_output_dir(request)
    log.info(f'test_output_dir is {test_dir}')
    shutil.rmtree(test_dir, ignore_errors=True)
    mkdir_if_needed(test_dir)
    return test_dir


#
# Test helpers
#
def list_files_to_compare(pgdata_dir: str):
    pgdata_files = []
    for root, _file, filenames in os.walk(pgdata_dir):
        for filename in filenames:
            rel_dir = os.path.relpath(root, pgdata_dir)
            # Skip some dirs and files we don't want to compare
            skip_dirs = ['pg_wal', 'pg_stat', 'pg_stat_tmp', 'pg_subtrans', 'pg_logical']
            skip_files = [
                'pg_internal.init',
                'pg.log',
                'zenith.signal',
                'postgresql.conf',
                'postmaster.opts',
                'postmaster.pid',
                'pg_control'
            ]
            if rel_dir not in skip_dirs and filename not in skip_files:
                rel_file = os.path.join(rel_dir, filename)
                pgdata_files.append(rel_file)

    pgdata_files.sort()
    log.info(pgdata_files)
    return pgdata_files


# pg is the existing and running compute node, that we want to compare with a basebackup
def check_restored_datadir_content(test_output_dir: str, env: ZenithEnv, pg: Postgres):

    # Get the timeline ID of our branch. We need it for the 'basebackup' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

    # stop postgres to ensure that files won't change
    pg.stop()

    # Take a basebackup from pageserver
    restored_dir_path = os.path.join(env.repo_dir, f"{pg.node_name}_restored_datadir")
    mkdir_if_needed(restored_dir_path)

    pg_bin = PgBin(test_output_dir)
    psql_path = os.path.join(pg_bin.pg_bin_path, 'psql')

    cmd = rf"""
        {psql_path}                                    \
            --no-psqlrc                                \
            postgres://localhost:{env.pageserver.service_port.pg}  \
            -c 'basebackup {pg.tenant_id} {timeline}'  \
         | tar -x -C {restored_dir_path}
    """

    subprocess.check_call(cmd, shell=True)

    # list files we're going to compare
    assert pg.pgdata_dir
    pgdata_files = list_files_to_compare(pg.pgdata_dir)
    restored_files = list_files_to_compare(restored_dir_path)

    # check that file sets are equal
    assert pgdata_files == restored_files

    # compare content of the files
    # filecmp returns (match, mismatch, error) lists
    # We've already filtered all mismatching files in list_files_to_compare(),
    # so here expect that the content is identical
    (match, mismatch, error) = filecmp.cmpfiles(pg.pgdata_dir,
                                                restored_dir_path,
                                                pgdata_files,
                                                shallow=False)
    log.info(f'filecmp result mismatch and error lists:\n\t mismatch={mismatch}\n\t error={error}')

    for f in mismatch:

        f1 = os.path.join(pg.pgdata_dir, f)
        f2 = os.path.join(restored_dir_path, f)
        stdout_filename = "{}.filediff".format(f2)

        with open(stdout_filename, 'w') as stdout_f:
            subprocess.run("xxd -b {} > {}.hex ".format(f1, f1), shell=True)
            subprocess.run("xxd -b {} > {}.hex ".format(f2, f2), shell=True)

            cmd = 'diff {}.hex {}.hex'.format(f1, f2)
            subprocess.run([cmd], stdout=stdout_f, shell=True)

    assert (mismatch, error) == ([], [])
