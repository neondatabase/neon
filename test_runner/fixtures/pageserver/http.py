from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from fixtures.log_helper import log
from fixtures.metrics import Metrics, parse_metrics
from fixtures.pg_version import PgVersion
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import Fn


class PageserverApiException(Exception):
    def __init__(self, message, status_code: int):
        super().__init__(message)
        self.status_code = status_code


class TimelineCreate406(PageserverApiException):
    def __init__(self, res: requests.Response):
        assert res.status_code == 406
        super().__init__(res.json()["msg"], res.status_code)


class TimelineCreate409(PageserverApiException):
    def __init__(self, res: requests.Response):
        assert res.status_code == 409
        super().__init__("", res.status_code)


@dataclass
class InMemoryLayerInfo:
    kind: str
    lsn_start: str
    lsn_end: Optional[str]

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> InMemoryLayerInfo:
        return InMemoryLayerInfo(
            kind=d["kind"],
            lsn_start=d["lsn_start"],
            lsn_end=d.get("lsn_end"),
        )


@dataclass(frozen=True)
class HistoricLayerInfo:
    kind: str
    layer_file_name: str
    layer_file_size: Optional[int]
    lsn_start: str
    lsn_end: Optional[str]
    remote: bool

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> HistoricLayerInfo:
        return HistoricLayerInfo(
            kind=d["kind"],
            layer_file_name=d["layer_file_name"],
            layer_file_size=d.get("layer_file_size"),
            lsn_start=d["lsn_start"],
            lsn_end=d.get("lsn_end"),
            remote=d["remote"],
        )


@dataclass
class LayerMapInfo:
    in_memory_layers: List[InMemoryLayerInfo]
    historic_layers: List[HistoricLayerInfo]

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> LayerMapInfo:
        info = LayerMapInfo(in_memory_layers=[], historic_layers=[])

        json_in_memory_layers = d["in_memory_layers"]
        assert isinstance(json_in_memory_layers, List)
        for json_in_memory_layer in json_in_memory_layers:
            info.in_memory_layers.append(InMemoryLayerInfo.from_json(json_in_memory_layer))

        json_historic_layers = d["historic_layers"]
        assert isinstance(json_historic_layers, List)
        for json_historic_layer in json_historic_layers:
            info.historic_layers.append(HistoricLayerInfo.from_json(json_historic_layer))

        return info

    def kind_count(self) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for inmem_layer in self.in_memory_layers:
            counts[inmem_layer.kind] += 1
        for hist_layer in self.historic_layers:
            counts[hist_layer.kind] += 1
        return counts


@dataclass
class TenantConfig:
    tenant_specific_overrides: Dict[str, Any]
    effective_config: Dict[str, Any]

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> TenantConfig:
        return TenantConfig(
            tenant_specific_overrides=d["tenant_specific_overrides"],
            effective_config=d["effective_config"],
        )


class PageserverHttpClient(requests.Session):
    def __init__(self, port: int, is_testing_enabled_or_skip: Fn, auth_token: Optional[str] = None):
        super().__init__()
        self.port = port
        self.auth_token = auth_token
        self.is_testing_enabled_or_skip = is_testing_enabled_or_skip

        if auth_token is not None:
            self.headers["Authorization"] = f"Bearer {auth_token}"

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def verbose_error(self, res: requests.Response):
        try:
            res.raise_for_status()
        except requests.RequestException as e:
            try:
                msg = res.json()["msg"]
            except:  # noqa: E722
                msg = ""
            raise PageserverApiException(msg, res.status_code) from e

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def configure_failpoints(self, config_strings: Tuple[str, str] | List[Tuple[str, str]]):
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
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None
        return res_json

    def tenant_list(self) -> List[Dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_create(
        self, new_tenant_id: TenantId, conf: Optional[Dict[str, Any]] = None
    ) -> TenantId:
        if conf is not None:
            assert "new_tenant_id" not in conf.keys()
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant",
            json={
                "new_tenant_id": str(new_tenant_id),
                **(conf or {}),
            },
        )
        self.verbose_error(res)
        if res.status_code == 409:
            raise Exception(f"could not create tenant: already exists for id {new_tenant_id}")
        new_tenant_id = res.json()
        assert isinstance(new_tenant_id, str)
        return TenantId(new_tenant_id)

    def tenant_attach(
        self, tenant_id: TenantId, config: None | Dict[str, Any] = None, config_null: bool = False
    ):
        if config_null:
            assert config is None
            body = "null"
        else:
            # null-config is prohibited by the API
            config = config or {}
            body = json.dumps({"config": config})
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/attach",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        self.verbose_error(res)

    def tenant_detach(self, tenant_id: TenantId, detach_ignored=False):
        params = {}
        if detach_ignored:
            params["detach_ignored"] = "true"

        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/detach", params=params)
        self.verbose_error(res)

    def tenant_load(self, tenant_id: TenantId):
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/load")
        self.verbose_error(res)

    def tenant_ignore(self, tenant_id: TenantId):
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/ignore")
        self.verbose_error(res)

    def tenant_status(self, tenant_id: TenantId) -> Dict[Any, Any]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def tenant_config(self, tenant_id: TenantId) -> TenantConfig:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/config")
        self.verbose_error(res)
        return TenantConfig.from_json(res.json())

    def set_tenant_config(self, tenant_id: TenantId, config: dict[str, Any]):
        assert "tenant_id" not in config.keys()
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/config",
            json={**config, "tenant_id": str(tenant_id)},
        )
        self.verbose_error(res)

    def patch_tenant_config_client_side(
        self,
        tenant_id: TenantId,
        inserts: Optional[Dict[str, Any]] = None,
        removes: Optional[List[str]] = None,
    ):
        current = self.tenant_config(tenant_id).tenant_specific_overrides
        if inserts is not None:
            current.update(inserts)
        if removes is not None:
            for key in removes:
                del current[key]
        self.set_tenant_config(tenant_id, current)

    def tenant_size(self, tenant_id: TenantId) -> int:
        return self.tenant_size_and_modelinputs(tenant_id)[0]

    def tenant_size_and_modelinputs(self, tenant_id: TenantId) -> Tuple[int, Dict[str, Any]]:
        """
        Returns the tenant size, together with the model inputs as the second tuple item.
        """
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/synthetic_size")
        self.verbose_error(res)
        res = res.json()
        assert isinstance(res, dict)
        assert TenantId(res["id"]) == tenant_id
        size = res["size"]
        assert type(size) == int
        inputs = res["inputs"]
        assert type(inputs) is dict
        return (size, inputs)

    def tenant_size_debug(self, tenant_id: TenantId) -> str:
        """
        Returns the tenant size debug info, as an HTML string
        """
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/synthetic_size",
            headers={"Accept": "text/html"},
        )
        return res.text

    def timeline_list(
        self,
        tenant_id: TenantId,
        include_non_incremental_logical_size: bool = False,
        include_timeline_dir_layer_file_size_sum: bool = False,
    ) -> List[Dict[str, Any]]:
        params = {}
        if include_non_incremental_logical_size:
            params["include-non-incremental-logical-size"] = "true"
        if include_timeline_dir_layer_file_size_sum:
            params["include-timeline-dir-layer-file-size-sum"] = "true"

        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline", params=params
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def timeline_create(
        self,
        pg_version: PgVersion,
        tenant_id: TenantId,
        new_timeline_id: TimelineId,
        ancestor_timeline_id: Optional[TimelineId] = None,
        ancestor_start_lsn: Optional[Lsn] = None,
        **kwargs,
    ) -> Dict[Any, Any]:
        body: Dict[str, Any] = {
            "new_timeline_id": str(new_timeline_id),
            "ancestor_start_lsn": str(ancestor_start_lsn) if ancestor_start_lsn else None,
            "ancestor_timeline_id": str(ancestor_timeline_id) if ancestor_timeline_id else None,
        }
        if pg_version != PgVersion.NOT_SET:
            body["pg_version"] = int(pg_version)

        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline", json=body, **kwargs
        )
        if res.status_code == 409:
            raise TimelineCreate409(res)
        if res.status_code == 406:
            raise TimelineCreate406(res)

        self.verbose_error(res)

        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_detail(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        include_non_incremental_logical_size: bool = False,
        include_timeline_dir_layer_file_size_sum: bool = False,
        **kwargs,
    ) -> Dict[Any, Any]:
        params = {}
        if include_non_incremental_logical_size:
            params["include-non-incremental-logical-size"] = "true"
        if include_timeline_dir_layer_file_size_sum:
            params["include-timeline-dir-layer-file-size-sum"] = "true"

        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            params=params,
            **kwargs,
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_delete(self, tenant_id: TenantId, timeline_id: TimelineId, **kwargs):
        """
        Note that deletion is not instant, it is scheduled and performed mostly in the background.
        So if you need to wait for it to complete use `timeline_delete_wait_completed`.
        For longer description consult with pageserver openapi spec.
        """
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}", **kwargs
        )
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_gc(
        self, tenant_id: TenantId, timeline_id: TimelineId, gc_horizon: Optional[int]
    ) -> dict[str, Any]:
        self.is_testing_enabled_or_skip()

        log.info(
            f"Requesting GC: tenant {tenant_id}, timeline {timeline_id}, gc_horizon {repr(gc_horizon)}"
        )
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/do_gc",
            json={"gc_horizon": gc_horizon},
        )
        log.info(f"Got GC request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is not None
        assert isinstance(res_json, dict)
        return res_json

    def timeline_compact(self, tenant_id: TenantId, timeline_id: TimelineId):
        self.is_testing_enabled_or_skip()

        log.info(f"Requesting compact: tenant {tenant_id}, timeline {timeline_id}")
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/compact"
        )
        log.info(f"Got compact request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_get_lsn_by_timestamp(
        self, tenant_id: TenantId, timeline_id: TimelineId, timestamp
    ):
        log.info(
            f"Requesting lsn by timestamp {timestamp}, tenant {tenant_id}, timeline {timeline_id}"
        )
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/get_lsn_by_timestamp?timestamp={timestamp}",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_checkpoint(self, tenant_id: TenantId, timeline_id: TimelineId):
        self.is_testing_enabled_or_skip()

        log.info(f"Requesting checkpoint: tenant {tenant_id}, timeline {timeline_id}")
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/checkpoint"
        )
        log.info(f"Got checkpoint request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_spawn_download_remote_layers(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        max_concurrent_downloads: int,
    ) -> dict[str, Any]:
        body = {
            "max_concurrent_downloads": max_concurrent_downloads,
        }
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
            json=body,
        )
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is not None
        assert isinstance(res_json, dict)
        return res_json

    def timeline_poll_download_remote_layers_status(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        spawn_response: dict[str, Any],
        poll_state=None,
    ) -> None | dict[str, Any]:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
        )
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is not None
        assert isinstance(res_json, dict)

        # assumption in this API client here is that nobody else spawns the task
        assert res_json["task_id"] == spawn_response["task_id"]

        if poll_state is None or res_json["state"] == poll_state:
            return res_json
        return None

    def timeline_download_remote_layers(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        max_concurrent_downloads: int,
        errors_ok=False,
        at_least_one_download=True,
    ):
        res = self.timeline_spawn_download_remote_layers(
            tenant_id, timeline_id, max_concurrent_downloads
        )
        while True:
            completed = self.timeline_poll_download_remote_layers_status(
                tenant_id, timeline_id, res, poll_state="Completed"
            )
            if not completed:
                time.sleep(0.1)
                continue
            if not errors_ok:
                assert completed["failed_download_count"] == 0
            if at_least_one_download:
                assert completed["successful_download_count"] > 0
            return completed

    def get_metrics_str(self) -> str:
        """You probably want to use get_metrics() instead."""
        res = self.get(f"http://localhost:{self.port}/metrics")
        self.verbose_error(res)
        return res.text

    def get_metrics(self) -> Metrics:
        res = self.get_metrics_str()
        return parse_metrics(res)

    def get_timeline_metric(
        self, tenant_id: TenantId, timeline_id: TimelineId, metric_name: str
    ) -> float:
        metrics = self.get_metrics()
        return metrics.query_one(
            metric_name,
            filter={
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
            },
        ).value

    def get_remote_timeline_client_metric(
        self,
        metric_name: str,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: str,
        op_kind: str,
    ) -> Optional[float]:
        metrics = self.get_metrics()
        matches = metrics.query_all(
            name=metric_name,
            filter={
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
        )
        if len(matches) == 0:
            value = None
        elif len(matches) == 1:
            value = matches[0].value
            assert value is not None
        else:
            assert len(matches) < 2, "above filter should uniquely identify metric"
        return value

    def get_metric_value(
        self, name: str, filter: Optional[Dict[str, str]] = None
    ) -> Optional[float]:
        metrics = self.get_metrics()
        results = metrics.query_all(name, filter=filter)
        if not results:
            log.info(f'could not find metric "{name}"')
            return None
        assert len(results) == 1, f"metric {name} with given filters is not unique, got: {results}"
        return results[0].value

    def layer_map_info(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> LayerMapInfo:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/",
        )
        self.verbose_error(res)
        return LayerMapInfo.from_json(res.json())

    def download_layer(self, tenant_id: TenantId, timeline_id: TimelineId, layer_name: str):
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}",
        )
        self.verbose_error(res)

        assert res.status_code == 200

    def download_all_layers(self, tenant_id: TenantId, timeline_id: TimelineId):
        info = self.layer_map_info(tenant_id, timeline_id)
        for layer in info.historic_layers:
            if not layer.remote:
                continue
            self.download_layer(tenant_id, timeline_id, layer.layer_file_name)

    def evict_layer(self, tenant_id: TenantId, timeline_id: TimelineId, layer_name: str):
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}",
        )
        self.verbose_error(res)

        assert res.status_code == 200

    def evict_all_layers(self, tenant_id: TenantId, timeline_id: TimelineId):
        info = self.layer_map_info(tenant_id, timeline_id)
        for layer in info.historic_layers:
            self.evict_layer(tenant_id, timeline_id, layer.layer_file_name)

    def disk_usage_eviction_run(self, request: dict[str, Any]):
        res = self.put(
            f"http://localhost:{self.port}/v1/disk_usage_eviction/run",
            json=request,
        )
        self.verbose_error(res)
        return res.json()

    def tenant_break(self, tenant_id: TenantId):
        res = self.put(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/break")
        self.verbose_error(res)

    def post_tracing_event(self, level: str, message: str):
        res = self.post(
            f"http://localhost:{self.port}/v1/tracing/event",
            json={
                "level": level,
                "message": message,
            },
        )
        self.verbose_error(res)
