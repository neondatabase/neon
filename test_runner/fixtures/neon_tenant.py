from __future__ import annotations

import random
import string
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Type,
    cast,
)

import pytest
from _pytest.config import Config
from _pytest.fixtures import FixtureRequest

from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonEnvBuilder
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import (
    MockS3Server,
)
from fixtures.utils import AuxFileStore

DEFAULT_BRANCH_NAME: str = "main"


@pytest.fixture(scope="function")
def neon_tenant(shared_env: NeonEnv) -> Iterator[NeonTestTenant]:
    tenant = NeonTestTenant(shared_env)
    tenant.create()
    yield tenant
    # TODO:  clean up the tenant


@dataclass(frozen=True)
class NeonEnvDiscriminants:
    """The options that define which environments can be shared"""

    neon_binpath: Path
    pageserver_virtual_file_io_engine: str
    pageserver_aux_file_policy: Optional[AuxFileStore]
    pageserver_default_tenant_config_compaction_algorithm: Optional[Dict[str, Any]]
    pageserver_io_buffer_alignment: Optional[int]


class NeonSharedEnvs:
    def __init__(
        self,
        port_distributor: PortDistributor,
        run_id: uuid.UUID,
        mock_s3_server: MockS3Server,
        pg_distrib_dir: Path,
        top_output_dir: Path,
        preserve_database_files: bool,
    ):
        self.port_distributor = port_distributor
        self.run_id = run_id
        self.mock_s3_server = mock_s3_server
        self.pg_distrib_dir = pg_distrib_dir
        self.top_output_dir = top_output_dir
        self.preserve_database_files = preserve_database_files

        self.lock = threading.Lock()
        self.envs: Dict[NeonEnvDiscriminants, NeonEnv] = {}

        self.builders: List[NeonEnvBuilder] = []

    def get_repo_dir(self, disc: NeonEnvDiscriminants) -> Path:
        # FIXME use discriminants
        randstr = "".join(
            random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10)
        )
        s = Path(f"shared-{randstr}")
        return self.top_output_dir / s

    def get_or_create(self, disc: NeonEnvDiscriminants) -> NeonEnv:
        with self.lock:
            env = self.envs.get(disc)
            if env is None:
                builder = NeonEnvBuilder(
                    repo_dir=self.get_repo_dir(disc),
                    port_distributor=self.port_distributor,
                    run_id=self.run_id,
                    mock_s3_server=self.mock_s3_server,
                    pg_distrib_dir=self.pg_distrib_dir,
                    preserve_database_files=self.preserve_database_files,
                    pg_version=PgVersion("17"),  # FIXME: this should go unused. Pass None?
                    test_name="shared",  # FIXME
                    test_output_dir=Path("shared"),  # FIXME
                    test_overlay_dir=None,
                    top_output_dir=self.top_output_dir,
                    neon_binpath=disc.neon_binpath,
                    pageserver_virtual_file_io_engine=disc.pageserver_virtual_file_io_engine,
                    pageserver_aux_file_policy=disc.pageserver_aux_file_policy,
                    pageserver_default_tenant_config_compaction_algorithm=disc.pageserver_default_tenant_config_compaction_algorithm,
                    # FIXME: only support defaults for these currently
                    # pageserver_remote_storage
                    # pageserver_config_override
                    # num_safekeepers
                    # num_pageservers
                    # safekeepers_id_start
                    # safekeepers_enable_fsync
                    # auth_enabled
                    # rust_log_override
                    # default_branch_name
                    initial_tenant=None,  # FIXME should go unused
                    initial_timeline=None,  # FIXME should go unused
                    # safekeeper_extra_opts: Optional[list[str]] = None,
                    # storage_controller_port_override: Optional[int] = None,
                    # pageserver_io_buffer_alignment: Optional[int] = None,
                )
                env = builder.init_start()

                self.envs[disc] = env
        return env

    def __enter__(self) -> "NeonSharedEnvs":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        for env in self.envs.values():
            env.stop(immediate=True)


@pytest.fixture(scope="session")
def shared_environments(
    # session fixtures
    port_distributor: PortDistributor,
    run_id: uuid.UUID,
    mock_s3_server: MockS3Server,
    pg_distrib_dir: Path,
    top_output_dir: Path,
    pytestconfig: Config,
) -> Iterator[NeonSharedEnvs]:
    with NeonSharedEnvs(
        port_distributor=port_distributor,
        run_id=run_id,
        mock_s3_server=mock_s3_server,
        pg_distrib_dir=pg_distrib_dir,
        top_output_dir=top_output_dir,
        # rust_log_override=rust_log_override, # FIXME
        preserve_database_files=cast(bool, pytestconfig.getoption("--preserve-database-files")),
    ) as envs:
        yield envs


@pytest.fixture(scope="function")
def shared_env(
    request: FixtureRequest,
    # session fixture holding all the envs
    shared_environments: NeonSharedEnvs,
    # other session fixtures
    port_distributor: PortDistributor,
    mock_s3_server: MockS3Server,
    run_id: uuid.UUID,
    top_output_dir: Path,
    pg_distrib_dir: Path,
    # these define the env to use
    neon_binpath: Path,
    pageserver_virtual_file_io_engine: str,
    pageserver_aux_file_policy: Optional[AuxFileStore],
    pageserver_default_tenant_config_compaction_algorithm: Optional[Dict[str, Any]],
    pageserver_io_buffer_alignment: Optional[int],
) -> NeonEnv:
    disc = NeonEnvDiscriminants(
        neon_binpath=neon_binpath,
        # FIXME: There's no difference in e.g. having pageserver_virtual_file_io_engine=None, and
        # explicitly specifying whatever the default is. We could share those envs.
        pageserver_virtual_file_io_engine=pageserver_virtual_file_io_engine,
        pageserver_aux_file_policy=pageserver_aux_file_policy,
        pageserver_default_tenant_config_compaction_algorithm=pageserver_default_tenant_config_compaction_algorithm,
        pageserver_io_buffer_alignment=pageserver_io_buffer_alignment,
    )
    return shared_environments.get_or_create(disc)


class NeonTestTenant:
    """
    An object representing a single Neon tenant, in a shared environment

    Notable functions and fields:

    endpoints - A factory object for creating postgres compute nodes.

    tenant_id - tenant ID of the initial tenant created in the repository

    initial_timeline - timeline ID of the "main" branch

    create_branch() - branch a new timeline from an existing one, returns
        the new timeline id

    create_timeline() - initializes a new timeline by running initdb, returns
        the new timeline id
    """

    def __init__(self, env: NeonEnv):
        self.tenant_id = TenantId.generate()
        self.initial_timeline = TimelineId.generate()
        self.created = False

        self.endpoints = TenantEndpointFactory(self)
        self.env = env

    def create(
        self,
        conf: Optional[Dict[str, Any]] = None,
        shard_count: Optional[int] = None,
        shard_stripe_size: Optional[int] = None,
        placement_policy: Optional[str] = None,
        aux_file_policy: Optional[AuxFileStore] = None,
    ):
        assert not self.created
        self.env.create_tenant(
            tenant_id=self.tenant_id,
            timeline_id=self.initial_timeline,
            conf=conf,
            shard_count=shard_count,
            shard_stripe_size=shard_stripe_size,
            placement_policy=placement_policy,
            set_default=False,
            aux_file_policy=aux_file_policy,
        )
        self.created = True

    # Todo: this could be imeplemented
    # def config_tenant(self, conf: Dict[str, str]):

    def create_branch(
        self,
        new_branch_name: str = DEFAULT_BRANCH_NAME,
        ancestor_branch_name: Optional[str] = None,
        ancestor_start_lsn: Optional[Lsn] = None,
        new_timeline_id: Optional[TimelineId] = None,
    ) -> TimelineId:
        return self.env.create_branch(
            new_branch_name=new_branch_name,
            tenant_id=self.tenant_id,
            ancestor_branch_name=ancestor_branch_name,
            ancestor_start_lsn=ancestor_start_lsn,
            new_timeline_id=new_timeline_id,
        )

    def create_timeline(
        self,
        new_branch_name: str,
        timeline_id: Optional[TimelineId] = None,
    ) -> TimelineId:
        return self.env.create_timeline(new_branch_name=new_branch_name, timeline_id=timeline_id)


class TenantEndpointFactory:
    """An object representing multiple compute endpoints of a single tenant."""

    def __init__(self, tenant: NeonTestTenant):
        self.tenant = tenant
        self.num_instances: int = 0
        self.endpoints: List[Endpoint] = []

    def create(
        self,
        branch_name: str,
        endpoint_id: Optional[str] = None,
        lsn: Optional[Lsn] = None,
        hot_standby: bool = False,
        config_lines: Optional[List[str]] = None,
    ) -> Endpoint:
        ep = Endpoint(
            self.tenant.env,
            tenant_id=self.tenant.tenant_id,
            pg_port=self.tenant.env.port_distributor.get_port(),
            http_port=self.tenant.env.port_distributor.get_port(),
        )

        endpoint_id = endpoint_id or self.tenant.env.generate_endpoint_id()

        self.num_instances += 1
        self.endpoints.append(ep)

        return ep.create(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            lsn=lsn,
            hot_standby=hot_standby,
            config_lines=config_lines,
        )

    # FIXME: extra args for start
    # remote_ext_config: Optional[str] = None,
    # basebackup_request_tries: Optional[int] = None,
    def create_start(self, *args, **kwargs):
        ep = self.create(*args, **kwargs)
        ep.start()
        return ep

    def stop_all(self, fail_on_error=True) -> "TenantEndpointFactory":
        exception = None
        for ep in self.endpoints:
            try:
                ep.stop()
            except Exception as e:
                log.error(f"Failed to stop endpoint {ep.endpoint_id}: {e}")
                exception = e

        if fail_on_error and exception is not None:
            raise exception

        return self

    def new_replica(
        self, origin: Endpoint, endpoint_id: str, config_lines: Optional[List[str]] = None
    ):
        branch_name = origin.branch_name
        assert origin in self.endpoints
        assert branch_name is not None

        return self.create(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            lsn=None,
            hot_standby=True,
            config_lines=config_lines,
        )

    def new_replica_start(
        self, origin: Endpoint, endpoint_id: str, config_lines: Optional[List[str]] = None
    ):
        branch_name = origin.branch_name
        assert origin in self.endpoints
        assert branch_name is not None

        return self.create_start(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            lsn=None,
            hot_standby=True,
            config_lines=config_lines,
        )
