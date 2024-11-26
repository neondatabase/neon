from __future__ import annotations

import time
from typing import TYPE_CHECKING

from mypy_boto3_s3.type_defs import (
    DeleteObjectOutputTypeDef,
    EmptyResponseMetadataTypeDef,
    ListObjectsV2OutputTypeDef,
    ObjectTypeDef,
)

from fixtures.common_types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.log_helper import log
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.remote_storage import RemoteStorage, RemoteStorageKind, S3Storage
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from typing import Any


def assert_tenant_state(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId,
    expected_state: str,
    message: str | None = None,
) -> None:
    tenant_status = pageserver_http.tenant_status(tenant)
    log.info(f"tenant_status: {tenant_status}")
    assert tenant_status["state"]["slug"] == expected_state, message or tenant_status


def remote_consistent_lsn(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId | TenantShardId,
    timeline: TimelineId,
) -> Lsn:
    detail = pageserver_http.timeline_detail(tenant, timeline)

    if detail["remote_consistent_lsn"] is None:
        # No remote information at all. This happens right after creating
        # a timeline, before any part of it has been uploaded to remote
        # storage yet.
        return Lsn(0)
    else:
        lsn_str = detail["remote_consistent_lsn"]
        assert isinstance(lsn_str, str)
        return Lsn(lsn_str)


def wait_for_upload(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId | TenantShardId,
    timeline: TimelineId,
    lsn: Lsn,
):
    """waits for local timeline upload up to specified lsn"""

    current_lsn = Lsn(0)
    for i in range(20):
        current_lsn = remote_consistent_lsn(pageserver_http, tenant, timeline)
        if current_lsn >= lsn:
            log.info("wait finished")
            return
        lr_lsn = last_record_lsn(pageserver_http, tenant, timeline)
        log.info(
            f"waiting for remote_consistent_lsn to reach {lsn}, now {current_lsn}, last_record_lsn={lr_lsn}, iteration {i + 1}"
        )
        time.sleep(1)
    raise Exception(
        f"timed out while waiting for {tenant}/{timeline} remote_consistent_lsn to reach {lsn}, was {current_lsn}"
    )


def _tenant_in_expected_state(tenant_info: dict[str, Any], expected_state: str):
    if tenant_info["state"]["slug"] == expected_state:
        return True
    if tenant_info["state"]["slug"] == "Broken":
        raise RuntimeError(f"tenant became Broken, not {expected_state}")
    return False


def wait_until_tenant_state(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    expected_state: str,
    iterations: int,
    period: float = 1.0,
) -> dict[str, Any]:
    """
    Does not use `wait_until` for debugging purposes
    """
    for _ in range(iterations):
        try:
            tenant = pageserver_http.tenant_status(tenant_id=tenant_id)
        except Exception as e:
            log.debug(f"Tenant {tenant_id} state retrieval failure: {e}")
        else:
            log.debug(f"Tenant {tenant_id} data: {tenant}")
            if _tenant_in_expected_state(tenant, expected_state):
                return tenant

        time.sleep(period)

    raise Exception(
        f"Tenant {tenant_id} did not become {expected_state} within {iterations * period} seconds"
    )


def wait_until_all_tenants_state(
    pageserver_http: PageserverHttpClient,
    expected_state: str,
    iterations: int,
    period: float = 1.0,
    http_error_ok: bool = True,
):
    """
    Like wait_until_tenant_state, but checks all tenants.
    """
    for _ in range(iterations):
        try:
            tenants = pageserver_http.tenant_list()
        except Exception as e:
            if http_error_ok:
                log.debug(f"Failed to list tenants: {e}")
            else:
                raise
        else:
            if all(map(lambda tenant: _tenant_in_expected_state(tenant, expected_state), tenants)):
                return
        time.sleep(period)

    raise Exception(
        f"Not all tenants became active {expected_state} within {iterations * period} seconds"
    )


def wait_until_timeline_state(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId | TenantShardId,
    timeline_id: TimelineId,
    expected_state: str,
    iterations: int,
    period: float = 1.0,
) -> dict[str, Any]:
    """
    Does not use `wait_until` for debugging purposes
    """
    for i in range(iterations):
        try:
            timeline = pageserver_http.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id)
            log.debug(f"Timeline {tenant_id}/{timeline_id} data: {timeline}")
            if isinstance(timeline["state"], str):
                if timeline["state"] == expected_state:
                    return timeline
            elif isinstance(timeline, dict):
                if timeline["state"].get(expected_state):
                    return timeline

        except Exception as e:
            log.debug(f"Timeline {tenant_id}/{timeline_id} state retrieval failure: {e}")

        if i == iterations - 1:
            # do not sleep last time, we already know that we failed
            break
        time.sleep(period)

    raise Exception(
        f"Timeline {tenant_id}/{timeline_id} did not become {expected_state} within {iterations * period} seconds"
    )


def wait_until_tenant_active(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    iterations: int = 30,
    period: float = 1.0,
):
    wait_until_tenant_state(
        pageserver_http,
        tenant_id,
        expected_state="Active",
        iterations=iterations,
        period=period,
    )


def last_record_lsn(
    pageserver_http_client: PageserverHttpClient,
    tenant: TenantId | TenantShardId,
    timeline: TimelineId,
) -> Lsn:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail["last_record_lsn"]
    assert isinstance(lsn_str, str)
    return Lsn(lsn_str)


def wait_for_last_record_lsn(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId | TenantShardId,
    timeline: TimelineId,
    lsn: Lsn,
) -> Lsn:
    """waits for pageserver to catch up to a certain lsn, returns the last observed lsn."""

    current_lsn = Lsn(0)
    for i in range(1000):
        current_lsn = last_record_lsn(pageserver_http, tenant, timeline)
        if current_lsn >= lsn:
            return current_lsn
        if i % 10 == 0:
            log.info(
                f"{tenant}/{timeline} waiting for last_record_lsn to reach {lsn}, now {current_lsn}, iteration {i + 1}"
            )
        time.sleep(0.1)
    raise Exception(
        f"timed out while waiting for last_record_lsn to reach {lsn}, was {current_lsn}"
    )


def wait_for_upload_queue_empty(
    pageserver_http: PageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    wait_period_secs = 0.2
    while True:
        all_metrics = pageserver_http.get_metrics()
        started = all_metrics.query_all(
            "pageserver_remote_timeline_client_calls_started_total",
            {
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
            },
        )
        finished = all_metrics.query_all(
            "pageserver_remote_timeline_client_calls_finished_total",
            {
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
            },
        )

        # this is `started left join finished`; if match, subtracting start from finished, resulting in queue depth
        remaining_labels = ["shard_id", "file_kind", "op_kind"]
        tl: list[tuple[Any, float]] = []
        for s in started:
            found = False
            for f in finished:
                if all([s.labels[label] == f.labels[label] for label in remaining_labels]):
                    assert (
                        not found
                    ), "duplicate match, remaining_labels don't uniquely identify sample"
                    tl.append((s.labels, int(s.value) - int(f.value)))
                    found = True
            if not found:
                tl.append((s.labels, int(s.value)))
        assert len(tl) == len(started), "something broken with join logic"
        log.info(f"upload queue for {tenant_id}/{timeline_id}:")
        for labels, queue_count in tl:
            log.info(f"  {labels}: {queue_count}")
        if all(queue_count == 0 for (_, queue_count) in tl):
            return
        time.sleep(wait_period_secs)


def wait_timeline_detail_404(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId | TenantShardId,
    timeline_id: TimelineId,
    iterations: int,
    interval: float | None = None,
):
    if interval is None:
        interval = 0.25

    def timeline_is_missing():
        data = {}
        try:
            data = pageserver_http.timeline_detail(tenant_id, timeline_id)
            log.info(f"timeline detail {data}")
        except PageserverApiException as e:
            log.debug(e)
            if e.status_code == 404:
                return

        raise RuntimeError(f"Timeline exists state {data.get('state')}")

    wait_until(iterations, interval, func=timeline_is_missing)


def timeline_delete_wait_completed(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId | TenantShardId,
    timeline_id: TimelineId,
    iterations: int = 20,
    interval: float | None = None,
    **delete_args,
) -> None:
    pageserver_http.timeline_delete(tenant_id=tenant_id, timeline_id=timeline_id, **delete_args)
    wait_timeline_detail_404(pageserver_http, tenant_id, timeline_id, iterations, interval)


# remote_storage must not be None, but that's easier for callers to make mypy happy
def assert_prefix_empty(
    remote_storage: RemoteStorage | None,
    prefix: str | None = None,
    allowed_postfix: str | None = None,
    delimiter: str = "/",
) -> None:
    assert remote_storage is not None
    response = list_prefix(remote_storage, prefix, delimiter)
    keys = response["KeyCount"]
    objects: list[ObjectTypeDef] = response.get("Contents", [])
    common_prefixes = response.get("CommonPrefixes", [])

    is_mock_s3 = isinstance(remote_storage, S3Storage) and not remote_storage.cleanup

    if is_mock_s3:
        if keys == 1 and len(objects) == 0 and len(common_prefixes) == 1:
            # this has been seen in the wild by tests with the below contradicting logging
            # https://neon-github-public-dev.s3.amazonaws.com/reports/pr-5322/6207777020/index.html#suites/3556ed71f2d69272a7014df6dcb02317/53b5c368b5a68865
            # this seems like a mock_s3 issue
            log.warning(
                f"contradicting ListObjectsV2 response with KeyCount={keys} and Contents={objects}, CommonPrefixes={common_prefixes}, assuming this means KeyCount=0"
            )
            keys = 0
        elif keys != 0 and len(objects) == 0:
            # this has been seen in one case with mock_s3:
            # https://neon-github-public-dev.s3.amazonaws.com/reports/pr-4938/6000769714/index.html#suites/3556ed71f2d69272a7014df6dcb02317/ca01e4f4d8d9a11f
            # looking at moto impl, it might be there's a race with common prefix (sub directory) not going away with deletes
            log.warning(
                f"contradicting ListObjectsV2 response with KeyCount={keys} and Contents={objects}, CommonPrefixes={common_prefixes}"
            )

    filtered_count = 0
    if allowed_postfix is None:
        filtered_count = len(objects)
    else:
        for _obj in objects:
            key: str = str(response.get("Key", []))
            if not (allowed_postfix.endswith(key)):
                filtered_count += 1

    assert filtered_count == 0, f"remote prefix {prefix} is not empty: {objects}"


# remote_storage must not be None, but that's easier for callers to make mypy happy
def assert_prefix_not_empty(
    remote_storage: RemoteStorage | None,
    prefix: str | None = None,
    delimiter: str = "/",
):
    assert remote_storage is not None
    response = list_prefix(remote_storage, prefix)
    assert response["KeyCount"] != 0, f"remote prefix {prefix} is empty: {response}"


def list_prefix(
    remote: RemoteStorage, prefix: str | None = None, delimiter: str = "/"
) -> ListObjectsV2OutputTypeDef:
    """
    Note that this function takes into account prefix_in_bucket.
    """
    # For local_fs we need to properly handle empty directories, which we currently dont, so for simplicity stick to s3 api.
    assert isinstance(remote, S3Storage), "localfs is currently not supported"

    prefix_in_bucket = remote.prefix_in_bucket or ""
    if not prefix:
        prefix = prefix_in_bucket
    else:
        # real s3 tests have uniqie per test prefix
        # mock_s3 tests use special pageserver prefix for pageserver stuff
        prefix = "/".join((prefix_in_bucket, prefix))

    # Note that this doesnt use pagination, so list is not guaranteed to be exhaustive.
    response = remote.client.list_objects_v2(
        Delimiter=delimiter,
        Bucket=remote.bucket_name,
        Prefix=prefix,
    )
    return response


def remote_storage_delete_key(
    remote: RemoteStorage,
    key: str,
) -> DeleteObjectOutputTypeDef:
    """
    Note that this function takes into account prefix_in_bucket.
    """
    # For local_fs we need to use a different implementation. As we don't need local_fs, just don't support it for now.
    assert isinstance(remote, S3Storage), "localfs is currently not supported"

    prefix_in_bucket = remote.prefix_in_bucket or ""

    # real s3 tests have uniqie per test prefix
    # mock_s3 tests use special pageserver prefix for pageserver stuff
    key = "/".join((prefix_in_bucket, key))

    response = remote.client.delete_object(
        Bucket=remote.bucket_name,
        Key=key,
    )
    return response


def enable_remote_storage_versioning(
    remote: RemoteStorage,
) -> EmptyResponseMetadataTypeDef:
    """
    Enable S3 versioning for the remote storage
    """
    # local_fs has no support for versioning
    assert isinstance(remote, S3Storage), "localfs is currently not supported"

    # The SDK supports enabling versioning on normal S3 as well but we don't want to change
    # these settings from a test in a live bucket (also, our access isn't enough nor should it be)
    assert not remote.real, "Enabling storage versioning only supported on Mock S3"

    # Workaround to enable self-copy until upstream bug is fixed: https://github.com/getmoto/moto/issues/7300
    remote.client.put_bucket_encryption(
        Bucket=remote.bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    "BucketKeyEnabled": False,
                },
            ]
        },
    )
    # Note that this doesnt use pagination, so list is not guaranteed to be exhaustive.
    response = remote.client.put_bucket_versioning(
        Bucket=remote.bucket_name,
        VersioningConfiguration={
            "MFADelete": "Disabled",
            "Status": "Enabled",
        },
    )
    return response


def many_small_layers_tenant_config() -> dict[str, Any]:
    """
    Create a new dict to avoid issues with deleting from the global value.
    In python, the global is mutable.
    """
    return {
        "gc_period": "0s",
        "compaction_period": "0s",
        "checkpoint_distance": 1024**2,
        "image_creation_threshold": 100,
    }


def poll_for_remote_storage_iterations(remote_storage_kind: RemoteStorageKind) -> int:
    return 40 if remote_storage_kind is RemoteStorageKind.REAL_S3 else 15
