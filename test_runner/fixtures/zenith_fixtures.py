from dataclasses import dataclass
from functools import cached_property
import os
import pathlib
import uuid
import jwt
import psycopg2
import pytest
import shutil
import signal
import subprocess
import time

from contextlib import closing
from pathlib import Path
from dataclasses import dataclass

# Type-related stuff
from psycopg2.extensions import connection as PgConnection
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, cast
from typing_extensions import Literal

import requests

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

DEFAULT_PAGESERVER_PG_PORT = 64000
DEFAULT_PAGESERVER_HTTP_PORT = 9898


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
        self.username = username or "zenith_admin"

    def connstr(self, *, dbname: str = 'postgres', username: Optional[str] = None, password: Optional[str] = None) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """

        username = username or self.username
        res = f'host={self.host} port={self.port} user={username} dbname={dbname}'
        if not password:
            return res
        return f'{res} password={password}'

    # autocommit=True here by default because that's what we need most of the time
    def connect(self, *, autocommit=True, dbname: str = 'postgres', username: Optional[str] = None, password: Optional[str] = None) -> PgConnection:
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


class ZenithPageserverHttpClient(requests.Session):
    def __init__(self, port: int, auth_token: Optional[str] = None) -> None:
        super().__init__()
        self.port = port
        self.auth_token = auth_token

        if auth_token is not None:
            self.headers['Authorization'] = f'Bearer {auth_token}'

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def branch_list(self, tenant_id: uuid.UUID) -> List[Dict]:
        res = self.get(f"http://localhost:{self.port}/v1/branch/{tenant_id.hex}")
        res.raise_for_status()
        return res.json()

    def branch_create(self, tenant_id: uuid.UUID, name: str, start_point: str) -> Dict:
        res = self.post(
            f"http://localhost:{self.port}/v1/branch",
            json={
                'tenant_id': tenant_id.hex,
                'name': name,
                'start_point': start_point,
            }
        )
        res.raise_for_status()
        return res.json()

    def tenant_list(self) -> List[str]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant")
        res.raise_for_status()
        return res.json()

    def tenant_create(self, tenant_id: uuid.UUID):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant",
            json={
                'tenant_id': tenant_id.hex,
            },
        )
        res.raise_for_status()
        return res.json()


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
        token = jwt.encode({"scope": "tenant", "tenant_id": tenant_id}, self.priv, algorithm="RS256")

        if isinstance(token, bytes):
            token = token.decode()

        return token





class ZenithPageserver(PgProtocol):
    """ An object representing a running pageserver. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str):
        super().__init__(host='localhost', port=DEFAULT_PAGESERVER_PG_PORT)
        self.zenith_cli = zenith_cli
        self.running = False
        self.initial_tenant = None
        self.repo_dir = repo_dir

    def init(self, enable_auth: bool = False) -> 'ZenithPageserver':
        """
        Initialize the repository, i.e. run "zenith init".
        Returns self.
        """
        cmd = ['init']
        if enable_auth:
            cmd.append('--enable-auth')
        self.zenith_cli.run(cmd)
        return self

    def start(self) -> 'ZenithPageserver':
        """
        Start the page server.
        Returns self.
        """

        self.zenith_cli.run(['start'])
        self.running = True
        # get newly created tenant id
        self.initial_tenant = self.zenith_cli.run(['tenant', 'list']).stdout.strip()
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

    @cached_property
    def auth_keys(self) -> AuthKeys:
        pub = (Path(self.repo_dir) / 'auth_public_key.pem').read_bytes()
        priv = (Path(self.repo_dir) / 'auth_private_key.pem').read_bytes()
        return AuthKeys(pub=pub, priv=priv)

    def http_client(self, auth_token: Optional[str] = None):
        return ZenithPageserverHttpClient(
            port=DEFAULT_PAGESERVER_HTTP_PORT,
            auth_token=auth_token,
        )


@zenfixture
def pageserver(zenith_cli: ZenithCli, repo_dir: str) -> Iterator[ZenithPageserver]:
    """
    The 'pageserver' fixture provides a Page Server that's up and running.

    If TEST_SHARED_FIXTURES is set, the Page Server instance is shared by all
    the tests. To avoid clashing with other tests, don't use the 'main' branch in
    the tests directly. Instead, create a branch off the 'empty' branch and use
    that.

    By convention, the test branches are named after the tests. For example,
    test called 'test_foo' would create and use branches with the 'test_foo' prefix.
    """

    ps = ZenithPageserver(zenith_cli, repo_dir).init().start()
    # For convenience in tests, create a branch from the freshly-initialized cluster.
    zenith_cli.run(["branch", "empty", "main"])

    yield ps

    # After the yield comes any cleanup code we need.
    print('Starting pageserver cleanup')
    ps.stop()


@pytest.fixture
def pageserver_auth_enabled(zenith_cli: ZenithCli, repo_dir: str):
    with ZenithPageserver(zenith_cli, repo_dir).init(enable_auth=True).start() as ps:
        # For convenience in tests, create a branch from the freshly-initialized cluster.
        zenith_cli.run(["branch", "empty", "main"])
        yield ps


class Postgres(PgProtocol):
    """ An object representing a running postgres daemon. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str, tenant_id: str, port: int):
        super().__init__(host='localhost', port=port)

        self.zenith_cli = zenith_cli
        self.running = False
        self.repo_dir = repo_dir
        self.branch: Optional[str] = None  # dubious, see asserts below
        self.tenant_id = tenant_id
        # path to conf is <repo_dir>/pgdatadirs/tenants/<tenant_id>/<branch_name>/postgresql.conf

    def create(
        self,
        branch: str,
        wal_acceptors: Optional[str] = None,
        config_lines: Optional[List[str]] = None,
        config_only: bool = False,
    ) -> 'Postgres':
        """
        Create the pg data directory.
        If wal_acceptors is not None, node will use wal acceptors; config is
        adjusted accordingly.
        Returns self.
        """

        if not config_lines:
            config_lines = []

        if config_only:
            self.zenith_cli.run(['pg', 'create', '--config-only', branch, f'--tenantid={self.tenant_id}'])
        else:
            self.zenith_cli.run(['pg', 'create', branch, f'--tenantid={self.tenant_id}'])
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
        self.zenith_cli.run(['pg', 'start', self.branch, f'--tenantid={self.tenant_id}'])
        self.running = True

        return self

    def pg_xact_dir_path(self) -> str:
        """ Path to pg_xact dir """
        path = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id / self.branch / 'pg_xact'
        return os.path.join(self.repo_dir, path)

    def pg_twophase_dir_path(self) -> str:
        """ Path to pg_twophase dir """
        print(self.tenant_id)
        print(self.branch)
        path = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id / self.branch / 'pg_twophase'
        return os.path.join(self.repo_dir, path)

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        filename = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id / self.branch / 'postgresql.conf'
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
            assert self.branch is not None
            self.zenith_cli.run(['pg', 'stop', self.branch, f'--tenantid={self.tenant_id}'])
            self.running = False

        return self

    def stop_and_destroy(self) -> 'Postgres':
        """
        Stop the Postgres instance, then destroy it.
        Returns self.
        """

        assert self.branch is not None
        assert self.tenant_id is not None
        self.zenith_cli.run(['pg', 'stop', '--destroy', self.branch, f'--tenantid={self.tenant_id}'])

        return self

    def create_start(
        self,
        branch: str,
        wal_acceptors: Optional[str] = None,
        config_lines: Optional[List[str]] = None,
    ) -> 'Postgres':
        """
        Create a Postgres instance, apply config
        and then start it.
        Returns self.
        """

        self.create(
            branch=branch,
            wal_acceptors=wal_acceptors,
            config_lines=config_lines,
            config_only=True,
        ).start()

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


class PostgresFactory:
    """ An object representing multiple running postgres daemons. """
    def __init__(self, zenith_cli: ZenithCli, repo_dir: str, initial_tenant: str, base_port: int = 55431):
        self.zenith_cli = zenith_cli
        self.repo_dir = repo_dir
        self.num_instances = 0
        self.instances: List[Postgres] = []
        self.initial_tenant: str = initial_tenant
        self.base_port = base_port

    def create_start(
        self,
        branch: str = "main",
        tenant_id: Optional[str] = None,
        wal_acceptors: Optional[str] = None,
        config_lines: Optional[List[str]] = None
    ) -> Postgres:

        pg = Postgres(
            zenith_cli=self.zenith_cli,
            repo_dir=self.repo_dir,
            tenant_id=tenant_id or self.initial_tenant,
            port=self.base_port + self.num_instances + 1,
        )

        self.num_instances += 1
        self.instances.append(pg)

        return pg.create_start(
            branch=branch,
            wal_acceptors=wal_acceptors,
            config_lines=config_lines,
        )

    def create(
        self,
        branch: str = "main",
        tenant_id: Optional[str] = None,
        wal_acceptors: Optional[str] = None,
        config_lines: Optional[List[str]] = None
    ) -> Postgres:

        pg = Postgres(
            zenith_cli=self.zenith_cli,
            repo_dir=self.repo_dir,
            tenant_id=tenant_id or self.initial_tenant,
            port=self.base_port + self.num_instances + 1,
        )

        self.num_instances += 1
        self.instances.append(pg)

        return pg.create(
            branch=branch,
            wal_acceptors=wal_acceptors,
            config_lines=config_lines,
        )

    def config(
        self,
        branch: str = "main",
        tenant_id: Optional[str] = None,
        wal_acceptors: Optional[str] = None,
        config_lines: Optional[List[str]] = None
    ) -> Postgres:

        pg = Postgres(
            zenith_cli=self.zenith_cli,
            repo_dir=self.repo_dir,
            tenant_id=tenant_id or self.initial_tenant,
            port=self.base_port + self.num_instances + 1,
        )

        self.num_instances += 1
        self.instances.append(pg)

        return pg.config(
            branch=branch,
            wal_acceptors=wal_acceptors,
            config_lines=config_lines,
        )

    def stop_all(self) -> 'PostgresFactory':
        for pg in self.instances:
            pg.stop()

        return self

@zenfixture
def initial_tenant(pageserver: ZenithPageserver):
    return pageserver.initial_tenant


@zenfixture
def postgres(zenith_cli: ZenithCli, initial_tenant: str, repo_dir: str) -> Iterator[PostgresFactory]:
    pgfactory = PostgresFactory(zenith_cli, repo_dir, initial_tenant=initial_tenant)

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


def read_pid(path: Path):
    """ Read content of file into number """
    return int(path.read_text())


@dataclass
class WalAcceptor:
    """ An object representing a running wal acceptor daemon. """
    bin_path: Path
    data_dir: Path
    port: int
    num: int # identifier for logging
    auth_token: Optional[str] = None

    def start(self) -> 'WalAcceptor':
        # create data directory if not exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.pidfile.unlink(missing_ok=True)

        cmd = [str(self.bin_path)]
        cmd.extend(["-D", str(self.data_dir)])
        cmd.extend(["-l", "localhost:{}".format(self.port)])
        cmd.append("--daemonize")
        cmd.append("--no-sync")
        # Tell page server it can receive WAL from this WAL safekeeper
        cmd.extend(["--pageserver", "localhost:{}".format(DEFAULT_PAGESERVER_PG_PORT)])
        cmd.extend(["--recall", "1 second"])
        print('Running command "{}"'.format(' '.join(cmd)))
        env = {'PAGESERVER_AUTH_TOKEN': self.auth_token} if self.auth_token else None
        subprocess.run(cmd, check=True, env=env)

        # wait for wal acceptor start by checkking that pid is readable
        for _ in range(3):
            pid = self.get_pid()
            if pid is not None:
                return self
            time.sleep(0.5)

        raise RuntimeError("cannot get wal acceptor pid")

    @property
    def pidfile(self) -> Path:
        return self.data_dir / "wal_acceptor.pid"

    def get_pid(self) -> Optional[int]:
        if not self.pidfile.exists():
            return None

        try:
            pid = read_pid(self.pidfile)
        except ValueError:
            return None

        return pid

    def stop(self) -> 'WalAcceptor':
        print('Stopping wal acceptor {}'.format(self.num))
        pid = self.get_pid()
        if pid is None:
            print("Wal acceptor {} is not running".format(self.num))
            return self

        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            # TODO: cleanup pid file on exit in wal acceptor
            pass # pidfile might be obsolete
        return self


class WalAcceptorFactory:
    """ An object representing multiple running wal acceptors. """
    def __init__(self, zenith_binpath: Path, data_dir: Path):
        self.wa_binpath = zenith_binpath / 'wal_acceptor'
        self.data_dir = data_dir
        self.instances: List[WalAcceptor] = []
        self.initial_port = 54321

    def start_new(self, auth_token: Optional[str] = None) -> WalAcceptor:
        """
        Start new wal acceptor.
        """

        wa_num = len(self.instances)
        wa = WalAcceptor(
            self.wa_binpath,
            self.data_dir / "wal_acceptor_{}".format(wa_num),
            self.initial_port + wa_num,
            wa_num,
            auth_token,
        )
        wa.start()
        self.instances.append(wa)
        return wa

    def start_n_new(self, n: int, auth_token: Optional[str] = None) -> None:
        """
        Start n new wal acceptors.
        """

        for _ in range(n):
            self.start_new(auth_token)

    def stop_all(self) -> 'WalAcceptorFactory':
        for wa in self.instances:
            wa.stop()
        return self

    def get_connstrs(self) -> str:
        """ Get list of wal acceptor endpoints suitable for wal_acceptors GUC  """
        return ','.join(["localhost:{}".format(wa.port) for wa in self.instances])


@zenfixture
def wa_factory(zenith_binpath: str, repo_dir: str) -> Iterator[WalAcceptorFactory]:
    """ Gives WalAcceptorFactory providing wal acceptors. """
    wafactory = WalAcceptorFactory(Path(zenith_binpath), Path(repo_dir) / "wal_acceptors")
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


class TenantFactory:
    def __init__(self, cli: ZenithCli):
        self.cli = cli

    def create(self, tenant_id: Optional[str] = None):
        if tenant_id is None:
            tenant_id = uuid.uuid4().hex
        res = self.cli.run(['tenant', 'create', tenant_id])
        res.check_returncode()
        return tenant_id


@zenfixture
def tenant_factory(zenith_cli: ZenithCli):
    return TenantFactory(zenith_cli)
