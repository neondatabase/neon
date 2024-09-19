from functools import partial
from pathlib import Path
from typing import Any, Optional, cast, List, Dict

import pytest

from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, PgProtocol, DEFAULT_BRANCH_NAME, tenant_get_shards, \
    check_restored_datadir_content
from fixtures.pageserver.utils import wait_for_last_record_lsn
from fixtures.safekeeper.utils import are_walreceivers_absent
from fixtures.utils import wait_until

"""
In this file most important resources are exposed as function-level fixtures
that depend on session-level resources like pageservers and safekeepers.

The main rationale here is that we don't need to initialize a new SK/PS/etc
every time we want to test something that has branching: we can just as well
reuse still-available PageServers from other tests of the same kind.
"""


@pytest.fixture(scope="session")
def shared_storage_repo_path(build_type: str, base_dir: Path) -> Path:
    return base_dir / f"shared[{build_type}]" / "repo"


class TEndpoint(PgProtocol):
    def clear_shared_buffers(self, cursor: Optional[Any] = None):
        """
        Best-effort way to clear postgres buffers. Pinned buffers will not be 'cleared.'

        Might also clear LFC.
        """
        if cursor is not None:
            cursor.execute("select clear_buffer_cache()")
        else:
            self.safe_psql("select clear_buffer_cache()")

_TEndpoint = Endpoint


class TTimeline:
    __primary: Optional[_TEndpoint]
    __secondary: Optional[_TEndpoint]

    def __init__(self, timeline_id: TimelineId, tenant: "TTenant", name: str):
        self.id = timeline_id
        self.tenant = tenant
        self.name = name
        self.__primary = None
        self.__secondary = None

    @property
    def primary(self) -> TEndpoint:
        if self.__primary is None:
            self.__primary = cast(_TEndpoint, self.tenant.create_endpoint(
                name=self._get_full_endpoint_name("primary"),
                timeline_id=self.id,
                primary=True,
                running=True,
            ))

        return cast(TEndpoint, self.__primary)

    def primary_with_config(self, config_lines: List[str], reboot: bool = False):
        if self.__primary is None:
            self.__primary = cast(_TEndpoint, self.tenant.create_endpoint(
                name=self._get_full_endpoint_name("primary"),
                timeline_id=self.id,
                primary=True,
                running=True,
                config_lines=config_lines,
            ))
        else:
            self.__primary.config(config_lines)
            if reboot:
                if self.__primary.is_running():
                    self.__primary.stop()
                self.__primary.start()

        return cast(TTimeline, self.__primary)

    @property
    def secondary(self) -> TEndpoint:
        if self.__secondary is None:
            self.__secondary = cast(_TEndpoint, self.tenant.create_endpoint(
                name=self._get_full_endpoint_name("secondary"),
                timeline_id=self.id,
                primary=False,
                running=True,
            ))

        return cast(TEndpoint, self.__secondary)

    def secondary_with_config(self, config_lines: List[str], reboot: bool = False) -> TEndpoint:
        if self.__secondary is None:
            self.__secondary = cast(_TEndpoint, self.tenant.create_endpoint(
                name=self._get_full_endpoint_name("secondary"),
                timeline_id=self.id,
                primary=False,
                running=True,
                config_lines=config_lines,
            ))
        else:
            self.__secondary.config(config_lines)
            if reboot:
                if self.__secondary.is_running():
                    self.__secondary.stop()
                self.__secondary.start()

        return cast(TEndpoint, self.__secondary)

    def _get_full_endpoint_name(self, name) -> str:
        return f"{self.name}.{name}"

    def create_branch(self, name: str, lsn: Optional[Lsn]) -> "TTimeline":
        return self.tenant.create_timeline(
            new_name=name,
            parent_name=self.name,
            branch_at=lsn,
        )

    def stop_and_flush(self, endpoint: TEndpoint):
        self.tenant.stop_endpoint(endpoint)
        self.tenant.flush_timeline_data(self)

    def checkpoint(self, **kwargs):
        self.tenant.checkpoint_timeline(self, **kwargs)


class TTenant:
    """
    An object representing a single test case on a shared pageserver.
    All operations here are safe practically safe.
    """

    def __init__(
        self,
        env: NeonEnv,
        name: str,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.id = TenantId.generate()
        self.timelines = []
        self.timeline_names = {}
        self.timeline_ids = {}
        self.name = name

        # publicly inaccessible stuff, used during shutdown
        self.__endpoints: List[Endpoint] = []
        self.__env: NeonEnv = env

        env.neon_cli.create_tenant(
            tenant_id=self.id,
            set_default=False,
            conf=config
        )

        self.first_timeline_id = env.neon_cli.create_branch(
            new_branch_name=f"{self.name}:{DEFAULT_BRANCH_NAME}",
            ancestor_branch_name=DEFAULT_BRANCH_NAME,
            tenant_id=self.id,
        )

        self._add_timeline(
            TTimeline(
                timeline_id=self.first_timeline_id,
                tenant=self,
                name=DEFAULT_BRANCH_NAME,
            )
        )

    def _add_timeline(self, timeline: TTimeline):
        assert timeline.tenant == self
        assert timeline not in self.timelines
        assert timeline.id is not None

        self.timelines.append(timeline)
        self.timeline_ids[timeline.id] = timeline

        if timeline.name is not None:
            self.timeline_names[timeline.name] = timeline

    def create_timeline(
        self,
        new_name: str,
        parent_name: Optional[str] = None,
        parent_id: Optional[TimelineId] = None,
        branch_at: Optional[Lsn] = None,
    ) -> TTimeline:
        if parent_name is not None:
            pass
        elif parent_id is not None:
            assert parent_name is None
            parent = self.timeline_ids[parent_id]
            parent_name = parent.name
        else:
            raise LookupError("Timeline creation requires parent by either ID or name")

        assert parent_name is not None

        new_id = self.__env.neon_cli.create_branch(
            new_branch_name=f"{self.name}:{new_name}",
            ancestor_branch_name=f"{self.name}:{parent_name}",
            tenant_id=self.id,
            ancestor_start_lsn=branch_at,
        )

        new_tl = TTimeline(
            timeline_id=new_id,
            tenant=self,
            name=new_name,
        )
        self._add_timeline(new_tl)

        return new_tl

    @property
    def default_timeline(self) -> TTimeline:
        return self.timeline_ids.get(self.first_timeline_id)

    def start_endpoint(self, ep: TEndpoint):
        if ep not in self.__endpoints:
            return

        ep = cast(Endpoint, ep)

        if not ep.is_running():
            ep.start()

    def stop_endpoint(self, ep: TEndpoint, mode: str = "fast"):
        if ep not in self.__endpoints:
            return

        ep = cast(Endpoint, ep)

        if ep.is_running():
            ep.stop(mode=mode)

    def create_endpoint(
        self,
        name: str,
        timeline_id: TimelineId,
        primary: bool = True,
        running: bool = False,
        port: Optional[int] = None,
        http_port: Optional[int] = None,
        lsn: Optional[Lsn] = None,
        config_lines: Optional[List[str]] = None,
    ) -> TEndpoint:
        endpoint: _TEndpoint

        if port is None:
            port = self.__env.port_distributor.get_port()

        if http_port is None:
            http_port = self.__env.port_distributor.get_port()

        if running:
            endpoint = self.__env.endpoints.create_start(
                branch_name=self.timeline_ids[timeline_id].name,
                endpoint_id=f"{self.name}.{name}",
                tenant_id=self.id,
                lsn=lsn,
                hot_standby=not primary,
                config_lines=config_lines,
            )
        else:
            endpoint = self.__env.endpoints.create(
                branch_name=self.timeline_ids[timeline_id].name,
                endpoint_id=f"{self.name}.{name}",
                tenant_id=self.id,
                lsn=lsn,
                hot_standby=not primary,
                config_lines=config_lines,
            )

        self.__endpoints.append(endpoint)

        return endpoint

    def shutdown_resources(self):
        for ep in self.__endpoints:
            try:
                ep.stop_and_destroy("fast")
            except BaseException as e:
                log.error("Error encountered while shutting down endpoint %s", ep.endpoint_id, exc_info=e)

    def reconfigure(self, endpoint: TEndpoint, lines: List[str], restart: bool):
        if endpoint not in self.__endpoints:
            return

        endpoint = cast(_TEndpoint, endpoint)
        was_running = endpoint.is_running()
        if restart and was_running:
            endpoint.stop()

        endpoint.config(lines)

        if restart:
            endpoint.start()

    def flush_timeline_data(self, timeline: TTimeline) -> Lsn:
        commit_lsn: Lsn = Lsn(0)
        # In principle in the absense of failures polling single sk would be enough.
        for sk in self.__env.safekeepers:
            cli = sk.http_client()
            # wait until compute connections are gone
            wait_until(30, 0.5, partial(are_walreceivers_absent, cli, self.id, timeline.id))
            commit_lsn = max(cli.get_commit_lsn(self.id, timeline.id), commit_lsn)

        # Note: depending on WAL filtering implementation, probably most shards
        # won't be able to reach commit_lsn (unless gaps are also ack'ed), so this
        # is broken in sharded case.
        shards = tenant_get_shards(env=self.__env, tenant_id=self.id)
        for tenant_shard_id, pageserver in shards:
            log.info(
                f"flush_ep_to_pageserver: waiting for {commit_lsn} on shard {tenant_shard_id} on pageserver {pageserver.id})"
            )
            waited = wait_for_last_record_lsn(
                pageserver.http_client(), tenant_shard_id, timeline.id, commit_lsn
            )

            assert waited >= commit_lsn

        return commit_lsn

    def checkpoint_timeline(self, timeline: TTimeline, **kwargs):
        self.__env.pageserver.http_client().timeline_checkpoint(
            tenant_id=self.id,
            timeline_id=timeline.id,
            **kwargs,
        )

    def pgdatadir(self, endpoint: TEndpoint):
        if endpoint not in self.__endpoints:
            return None

        return cast(Endpoint, endpoint).pgdata_dir

    def check_restored_datadir_content(self, path, endpoint: TEndpoint, *args, **kwargs):
        if endpoint not in self.__endpoints:
            return

        check_restored_datadir_content(path, self.__env, cast(Endpoint, endpoint), *args, **kwargs)
