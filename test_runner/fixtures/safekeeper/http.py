import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import pytest
import requests

from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.metrics import Metrics, MetricsGetter, parse_metrics


# Walreceiver as returned by sk's timeline status endpoint.
@dataclass
class Walreceiver:
    conn_id: int
    state: str


@dataclass
class SafekeeperTimelineStatus:
    term: int
    last_log_term: int
    pg_version: int  # Not exactly a PgVersion, safekeeper returns version as int, for example 150002 for 15.2
    flush_lsn: Lsn
    commit_lsn: Lsn
    timeline_start_lsn: Lsn
    backup_lsn: Lsn
    peer_horizon_lsn: Lsn
    remote_consistent_lsn: Lsn
    walreceivers: List[Walreceiver]


class SafekeeperMetrics(Metrics):
    # Helpers to get metrics from tests without hardcoding the metric names there.
    # These are metrics from Prometheus which uses float64 internally.
    # As a consequence, values may differ from real original int64s.

    def __init__(self, m: Metrics):
        self.metrics = m.metrics

    def flush_lsn_inexact(self, tenant_id: TenantId, timeline_id: TimelineId):
        return self.query_one(
            "safekeeper_flush_lsn", {"tenant_id": str(tenant_id), "timeline_id": str(timeline_id)}
        ).value

    def commit_lsn_inexact(self, tenant_id: TenantId, timeline_id: TimelineId):
        return self.query_one(
            "safekeeper_commit_lsn", {"tenant_id": str(tenant_id), "timeline_id": str(timeline_id)}
        ).value


class SafekeeperHttpClient(requests.Session, MetricsGetter):
    HTTPError = requests.HTTPError

    def __init__(self, port: int, auth_token: Optional[str] = None, is_testing_enabled=False):
        super().__init__()
        self.port = port
        self.auth_token = auth_token
        self.is_testing_enabled = is_testing_enabled

        if auth_token is not None:
            self.headers["Authorization"] = f"Bearer {auth_token}"

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def is_testing_enabled_or_skip(self):
        if not self.is_testing_enabled:
            pytest.skip("safekeeper was built without 'testing' feature")

    def configure_failpoints(self, config_strings: Union[Tuple[str, str], List[Tuple[str, str]]]):
        self.is_testing_enabled_or_skip()

        if isinstance(config_strings, tuple):
            pairs = [config_strings]
        else:
            pairs = config_strings

        log.info(f"Requesting config failpoints: {repr(pairs)}")

        res = self.put(
            f"http://localhost:{self.port}/v1/failpoints",
            json=[{"name": name, "actions": actions} for name, actions in pairs],
        )
        log.info(f"Got failpoints request response code {res.status_code}")
        res.raise_for_status()
        res_json = res.json()
        assert res_json is None
        return res_json

    def debug_dump(self, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        params = params or {}
        res = self.get(f"http://localhost:{self.port}/v1/debug_dump", params=params)
        res.raise_for_status()
        res_json = json.loads(res.text)
        assert isinstance(res_json, dict)
        return res_json

    def patch_control_file(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        patch: Dict[str, Any],
    ) -> Dict[str, Any]:
        res = self.patch(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/control_file",
            json={
                "updates": patch,
                "apply_fields": list(patch.keys()),
            },
        )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def pull_timeline(self, body: Dict[str, Any]) -> Dict[str, Any]:
        res = self.post(f"http://localhost:{self.port}/v1/pull_timeline", json=body)
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def copy_timeline(self, tenant_id: TenantId, timeline_id: TimelineId, body: Dict[str, Any]):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/copy",
            json=body,
        )
        res.raise_for_status()

    def timeline_digest(
        self, tenant_id: TenantId, timeline_id: TimelineId, from_lsn: Lsn, until_lsn: Lsn
    ) -> Dict[str, Any]:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/digest",
            params={
                "from_lsn": str(from_lsn),
                "until_lsn": str(until_lsn),
            },
        )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_create(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        pg_version: int,  # Not exactly a PgVersion, safekeeper returns version as int, for example 150002 for 15.2
        commit_lsn: Lsn,
    ):
        body = {
            "tenant_id": str(tenant_id),
            "timeline_id": str(timeline_id),
            "pg_version": pg_version,
            "commit_lsn": str(commit_lsn),
        }
        res = self.post(f"http://localhost:{self.port}/v1/tenant/timeline", json=body)
        res.raise_for_status()

    def timeline_status(
        self, tenant_id: TenantId, timeline_id: TimelineId
    ) -> SafekeeperTimelineStatus:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}")
        res.raise_for_status()
        resj = res.json()
        walreceivers = [Walreceiver(wr["conn_id"], wr["status"]) for wr in resj["walreceivers"]]
        return SafekeeperTimelineStatus(
            term=resj["acceptor_state"]["term"],
            last_log_term=resj["acceptor_state"]["epoch"],
            pg_version=resj["pg_info"]["pg_version"],
            flush_lsn=Lsn(resj["flush_lsn"]),
            commit_lsn=Lsn(resj["commit_lsn"]),
            timeline_start_lsn=Lsn(resj["timeline_start_lsn"]),
            backup_lsn=Lsn(resj["backup_lsn"]),
            peer_horizon_lsn=Lsn(resj["peer_horizon_lsn"]),
            remote_consistent_lsn=Lsn(resj["remote_consistent_lsn"]),
            walreceivers=walreceivers,
        )

    def get_commit_lsn(self, tenant_id: TenantId, timeline_id: TimelineId) -> Lsn:
        return self.timeline_status(tenant_id, timeline_id).commit_lsn

    def record_safekeeper_info(self, tenant_id: TenantId, timeline_id: TimelineId, body):
        res = self.post(
            f"http://localhost:{self.port}/v1/record_safekeeper_info/{tenant_id}/{timeline_id}",
            json=body,
        )
        res.raise_for_status()

    def checkpoint(self, tenant_id: TenantId, timeline_id: TimelineId):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/checkpoint",
            json={},
        )
        res.raise_for_status()

    # only_local doesn't remove segments in the remote storage.
    def timeline_delete(
        self, tenant_id: TenantId, timeline_id: TimelineId, only_local: bool = False
    ) -> Dict[Any, Any]:
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            params={
                "only_local": str(only_local).lower(),
            },
        )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def tenant_delete_force(self, tenant_id: TenantId) -> Dict[Any, Any]:
        res = self.delete(f"http://localhost:{self.port}/v1/tenant/{tenant_id}")
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def get_metrics_str(self) -> str:
        """You probably want to use get_metrics() instead."""
        request_result = self.get(f"http://localhost:{self.port}/metrics")
        request_result.raise_for_status()
        return request_result.text

    def get_metrics(self) -> SafekeeperMetrics:
        res = self.get_metrics_str()
        return SafekeeperMetrics(parse_metrics(res))
