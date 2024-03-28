from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from fixtures.log_helper import log
from fixtures.metrics import Metrics, MetricsGetter, parse_metrics
from fixtures.pg_version import PgVersion
from fixtures.types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.utils import Fn


class PageserverApiException(Exception):
    def __init__(self, message, status_code: int):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class TimelineCreate406(PageserverApiException):
    def __init__(self, res: requests.Response):
        assert res.status_code == 406
        super().__init__(res.json()["msg"], res.status_code)


class TimelineCreate409(PageserverApiException):
    def __init__(self, res: requests.Response):
        assert res.status_code == 409
        super().__init__(res.json()["msg"], res.status_code)


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

    def delta_layers(self) -> List[HistoricLayerInfo]:
        return [x for x in self.historic_layers if x.kind == "Delta"]

    def image_layers(self) -> List[HistoricLayerInfo]:
        return [x for x in self.historic_layers if x.kind == "Image"]

    def historic_by_name(self) -> Set[str]:
        return set(x.layer_file_name for x in self.historic_layers)


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


class PageserverHttpClient(requests.Session, MetricsGetter):
    def __init__(
        self,
        port: int,
        is_testing_enabled_or_skip: Fn,
        auth_token: Optional[str] = None,
        retries: Optional[Retry] = None,
    ):
        super().__init__()
        self.port = port
        self.auth_token = auth_token
        self.is_testing_enabled_or_skip = is_testing_enabled_or_skip

        if retries is None:
            # We apply a retry policy that is different to the default `requests` behavior,
            # because the pageserver has various transiently unavailable states that benefit
            # from a client retrying on 503

            retries = Retry(
                # Status retries are for retrying on 503 while e.g. waiting for tenants to activate
                status=5,
                # Connection retries are for waiting for the pageserver to come up and listen
                connect=5,
                # No read retries: if a request hangs that is not expected behavior
                # (this may change in future if we do fault injection of a kind that causes
                #  requests TCP flows to stick)
                read=False,
                backoff_factor=0.2,
                status_forcelist=[503],
                allowed_methods=None,
                remove_headers_on_redirect=[],
            )

        self.mount("http://", HTTPAdapter(max_retries=retries))

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

    def reload_auth_validation_keys(self):
        res = self.post(f"http://localhost:{self.port}/v1/reload_auth_validation_keys")
        self.verbose_error(res)

    def tenant_list(self) -> List[Dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_create(
        self,
        new_tenant_id: Union[TenantId, TenantShardId],
        conf: Optional[Dict[str, Any]] = None,
        generation: Optional[int] = None,
    ) -> TenantId:
        if conf is not None:
            assert "new_tenant_id" not in conf.keys()

        body: Dict[str, Any] = {
            "new_tenant_id": str(new_tenant_id),
            **(conf or {}),
        }

        if generation is not None:
            body.update({"generation": generation})

        res = self.post(
            f"http://localhost:{self.port}/v1/tenant",
            json=body,
        )
        self.verbose_error(res)
        if res.status_code == 409:
            raise Exception(f"could not create tenant: already exists for id {new_tenant_id}")
        new_tenant_id = res.json()
        assert isinstance(new_tenant_id, str)
        return TenantId(new_tenant_id)

    def tenant_attach(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        config: None | Dict[str, Any] = None,
        config_null: bool = False,
        generation: Optional[int] = None,
    ):
        if config_null:
            assert config is None
            body: Any = None
        else:
            # null-config is prohibited by the API
            config = config or {}
            body = {"config": config}
            if generation is not None:
                body.update({"generation": generation})

        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/attach",
            data=json.dumps(body),
            headers={"Content-Type": "application/json"},
        )
        self.verbose_error(res)

    def tenant_detach(self, tenant_id: TenantId, detach_ignored=False, timeout_secs=None):
        params = {}
        if detach_ignored:
            params["detach_ignored"] = "true"

        kwargs = {}
        if timeout_secs is not None:
            kwargs["timeout"] = timeout_secs

        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/detach", params=params, **kwargs
        )
        self.verbose_error(res)

    def tenant_reset(self, tenant_id: Union[TenantId, TenantShardId], drop_cache: bool):
        params = {}
        if drop_cache:
            params["drop_cache"] = "true"

        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/reset", params=params)
        self.verbose_error(res)

    def tenant_location_conf(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        location_conf=dict[str, Any],
        flush_ms=None,
        lazy: Optional[bool] = None,
    ):
        body = location_conf.copy()
        body["tenant_id"] = str(tenant_id)

        params = {}
        if flush_ms is not None:
            params["flush_ms"] = str(flush_ms)

        if lazy is not None:
            params["lazy"] = "true" if lazy else "false"

        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/location_config",
            json=body,
            params=params,
        )
        self.verbose_error(res)

    def tenant_list_locations(self):
        res = self.get(
            f"http://localhost:{self.port}/v1/location_config",
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json["tenant_shards"], list)
        return res_json

    def tenant_get_location(self, tenant_id: TenantShardId):
        res = self.get(
            f"http://localhost:{self.port}/v1/location_config/{tenant_id}",
        )
        self.verbose_error(res)
        return res.json()

    def tenant_delete(self, tenant_id: Union[TenantId, TenantShardId]):
        res = self.delete(f"http://localhost:{self.port}/v1/tenant/{tenant_id}")
        self.verbose_error(res)
        return res

    def tenant_load(self, tenant_id: TenantId, generation=None):
        body = None
        if generation is not None:
            body = {"generation": generation}
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/load", json=body)
        self.verbose_error(res)

    def tenant_ignore(self, tenant_id: TenantId):
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/ignore")
        self.verbose_error(res)

    def tenant_status(self, tenant_id: Union[TenantId, TenantShardId]) -> Dict[Any, Any]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def tenant_config(self, tenant_id: Union[TenantId, TenantShardId]) -> TenantConfig:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/config")
        self.verbose_error(res)
        return TenantConfig.from_json(res.json())

    def tenant_heatmap_upload(self, tenant_id: Union[TenantId, TenantShardId]):
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/heatmap_upload")
        self.verbose_error(res)

    def tenant_secondary_download(
        self, tenant_id: Union[TenantId, TenantShardId], wait_ms: Optional[int] = None
    ) -> tuple[int, dict[Any, Any]]:
        url = f"http://localhost:{self.port}/v1/tenant/{tenant_id}/secondary/download"
        if wait_ms is not None:
            url = url + f"?wait_ms={wait_ms}"
        res = self.post(url)
        self.verbose_error(res)
        return (res.status_code, res.json())

    def set_tenant_config(self, tenant_id: Union[TenantId, TenantShardId], config: dict[str, Any]):
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

    def tenant_size(self, tenant_id: Union[TenantId, TenantShardId]) -> int:
        return self.tenant_size_and_modelinputs(tenant_id)[0]

    def tenant_size_and_modelinputs(
        self, tenant_id: Union[TenantId, TenantShardId]
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Returns the tenant size, together with the model inputs as the second tuple item.
        """
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/synthetic_size")
        self.verbose_error(res)
        res = res.json()
        assert isinstance(res, dict)
        assert TenantId(res["id"]) == tenant_id
        size = res["size"]
        assert isinstance(size, int)
        inputs = res["inputs"]
        assert isinstance(inputs, dict)
        return (size, inputs)

    def tenant_size_debug(self, tenant_id: Union[TenantId, TenantShardId]) -> str:
        """
        Returns the tenant size debug info, as an HTML string
        """
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/synthetic_size",
            headers={"Accept": "text/html"},
        )
        return res.text

    def tenant_time_travel_remote_storage(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        timestamp: datetime,
        done_if_after: datetime,
        shard_counts: Optional[List[int]] = None,
    ):
        """
        Issues a request to perform time travel operations on the remote storage
        """

        if shard_counts is None:
            shard_counts = []
        body: Dict[str, Any] = {
            "shard_counts": shard_counts,
        }
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/time_travel_remote_storage?travel_to={timestamp.isoformat()}Z&done_if_after={done_if_after.isoformat()}Z",
            json=body,
        )
        self.verbose_error(res)

    def timeline_list(
        self,
        tenant_id: Union[TenantId, TenantShardId],
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
        tenant_id: Union[TenantId, TenantShardId],
        new_timeline_id: TimelineId,
        ancestor_timeline_id: Optional[TimelineId] = None,
        ancestor_start_lsn: Optional[Lsn] = None,
        existing_initdb_timeline_id: Optional[TimelineId] = None,
        **kwargs,
    ) -> Dict[Any, Any]:
        body: Dict[str, Any] = {
            "new_timeline_id": str(new_timeline_id),
            "ancestor_start_lsn": str(ancestor_start_lsn) if ancestor_start_lsn else None,
            "ancestor_timeline_id": str(ancestor_timeline_id) if ancestor_timeline_id else None,
            "existing_initdb_timeline_id": str(existing_initdb_timeline_id)
            if existing_initdb_timeline_id
            else None,
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
        tenant_id: Union[TenantId, TenantShardId],
        timeline_id: TimelineId,
        include_non_incremental_logical_size: bool = False,
        include_timeline_dir_layer_file_size_sum: bool = False,
        force_await_initial_logical_size: bool = False,
        **kwargs,
    ) -> Dict[Any, Any]:
        params = {}
        if include_non_incremental_logical_size:
            params["include-non-incremental-logical-size"] = "true"
        if include_timeline_dir_layer_file_size_sum:
            params["include-timeline-dir-layer-file-size-sum"] = "true"
        if force_await_initial_logical_size:
            params["force-await-initial-logical-size"] = "true"

        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            params=params,
            **kwargs,
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_delete(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId, **kwargs
    ):
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
        self,
        tenant_id: Union[TenantId, TenantShardId],
        timeline_id: TimelineId,
        gc_horizon: Optional[int],
    ) -> dict[str, Any]:
        """
        Unlike most handlers, this will wait for the layers to be actually
        complete registering themselves to the deletion queue.
        """
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

    def timeline_compact(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        timeline_id: TimelineId,
        force_repartition=False,
        force_image_layer_creation=False,
    ):
        self.is_testing_enabled_or_skip()
        query = {}
        if force_repartition:
            query["force_repartition"] = "true"
        if force_image_layer_creation:
            query["force_image_layer_creation"] = "true"

        log.info(f"Requesting compact: tenant {tenant_id}, timeline {timeline_id}")
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/compact",
            params=query,
        )
        log.info(f"Got compact request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_preserve_initdb_archive(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId
    ):
        log.info(
            f"Requesting initdb archive preservation for tenant {tenant_id} and timeline {timeline_id}"
        )
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/preserve_initdb_archive",
        )
        self.verbose_error(res)

    def timeline_get_lsn_by_timestamp(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        timeline_id: TimelineId,
        timestamp: datetime,
    ):
        log.info(
            f"Requesting lsn by timestamp {timestamp}, tenant {tenant_id}, timeline {timeline_id}"
        )
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/get_lsn_by_timestamp?timestamp={timestamp.isoformat()}Z",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_get_timestamp_of_lsn(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId, lsn: Lsn
    ):
        log.info(f"Requesting time range of lsn {lsn}, tenant {tenant_id}, timeline {timeline_id}")
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/get_timestamp_of_lsn?lsn={lsn}",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_layer_map_info(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId
    ):
        log.info(f"Requesting layer map info of tenant {tenant_id}, timeline {timeline_id}")
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_checkpoint(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        timeline_id: TimelineId,
        force_repartition=False,
        force_image_layer_creation=False,
    ):
        self.is_testing_enabled_or_skip()
        query = {}
        if force_repartition:
            query["force_repartition"] = "true"
        if force_image_layer_creation:
            query["force_image_layer_creation"] = "true"

        log.info(f"Requesting checkpoint: tenant {tenant_id}, timeline {timeline_id}")
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/checkpoint",
            params=query,
        )
        log.info(f"Got checkpoint request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_spawn_download_remote_layers(
        self,
        tenant_id: Union[TenantId, TenantShardId],
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
        tenant_id: Union[TenantId, TenantShardId],
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
        tenant_id: Union[TenantId, TenantShardId],
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

    def get_remote_timeline_client_queue_count(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: str,
        op_kind: str,
    ) -> Optional[int]:
        metrics = [
            "pageserver_remote_timeline_client_calls_started_total",
            "pageserver_remote_timeline_client_calls_finished_total",
        ]
        res = self.get_metrics_values(
            metrics,
            filter={
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
            absence_ok=True,
        )
        if len(res) != 2:
            return None
        inc, dec = [res[metric] for metric in metrics]
        queue_count = int(inc) - int(dec)
        assert queue_count >= 0
        return queue_count

    def layer_map_info(
        self,
        tenant_id: Union[TenantId, TenantShardId],
        timeline_id: TimelineId,
    ) -> LayerMapInfo:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/",
        )
        self.verbose_error(res)
        return LayerMapInfo.from_json(res.json())

    def download_layer(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId, layer_name: str
    ):
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}",
        )
        self.verbose_error(res)

        assert res.status_code == 200

    def download_all_layers(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId
    ):
        info = self.layer_map_info(tenant_id, timeline_id)
        for layer in info.historic_layers:
            if not layer.remote:
                continue
            self.download_layer(tenant_id, timeline_id, layer.layer_file_name)

    def detach_ancestor(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId
    ):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/detach_ancestor",
        )
        self.verbose_error(res)

    def evict_layer(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId, layer_name: str
    ):
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}",
        )
        self.verbose_error(res)

        assert res.status_code in (200, 304)

    def evict_all_layers(self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId):
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

    def tenant_break(self, tenant_id: Union[TenantId, TenantShardId]):
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

    def deletion_queue_flush(self, execute: bool = False):
        self.put(
            f"http://localhost:{self.port}/v1/deletion_queue/flush?execute={'true' if execute else 'false'}"
        ).raise_for_status()

    def timeline_wait_logical_size(self, tenant_id: TenantId, timeline_id: TimelineId) -> int:
        detail = self.timeline_detail(
            tenant_id,
            timeline_id,
            include_non_incremental_logical_size=True,
            force_await_initial_logical_size=True,
        )
        current_logical_size = detail["current_logical_size"]
        non_incremental = detail["current_logical_size_non_incremental"]
        assert current_logical_size == non_incremental
        assert isinstance(current_logical_size, int)
        return current_logical_size
