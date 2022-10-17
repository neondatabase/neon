import json
import time
from typing import Any, Dict

from fixtures.log_helper import log
from fixtures.neon_fixtures import LocalFsStorage, NeonPageserverHttpClient
from fixtures.types import Lsn, TenantId, TimelineId


def assert_no_in_progress_downloads_for_tenant(
    pageserver_http_client: NeonPageserverHttpClient,
    tenant: TenantId,
):
    tenant_status = pageserver_http_client.tenant_status(tenant)
    assert tenant_status["has_in_progress_downloads"] is False, tenant_status


def remote_consistent_lsn(
    pageserver_http_client: NeonPageserverHttpClient, tenant: TenantId, timeline: TimelineId
) -> Lsn:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail["remote_consistent_lsn"]
    if lsn_str is None:
        # No remote information at all. This happens right after creating
        # a timeline, before any part of it has been uploaded to remote
        # storage yet.
        return Lsn(0)
    assert isinstance(lsn_str, str)
    return Lsn(lsn_str)


def wait_for_upload(
    pageserver_http_client: NeonPageserverHttpClient,
    tenant: TenantId,
    timeline: TimelineId,
    lsn: Lsn,
):
    """waits for local timeline upload up to specified lsn"""
    for i in range(20):
        current_lsn = remote_consistent_lsn(pageserver_http_client, tenant, timeline)
        if current_lsn >= lsn:
            return
        log.info(
            "waiting for remote_consistent_lsn to reach {}, now {}, iteration {}".format(
                lsn, current_lsn, i + 1
            )
        )
        time.sleep(1)
    raise Exception(
        "timed out while waiting for remote_consistent_lsn to reach {}, was {}".format(
            lsn, current_lsn
        )
    )


def read_local_fs_index_part(env, tenant_id: TenantId, timeline_id: TimelineId):
    """
    Return json.load parsed index_part.json of tenant and timeline from LOCAL_FS
    """
    timeline_path = local_fs_index_part_path(env, tenant_id, timeline_id)
    return json.loads(timeline_path.read_text())


def write_local_fs_index_part(
    env, tenant_id: TenantId, timeline_id: TimelineId, index_part: Dict[str, Any]
):
    timeline_path = local_fs_index_part_path(env, tenant_id, timeline_id)
    timeline_path.write_text(json.dumps(index_part))


def local_fs_index_part_path(env, tenant_id: TenantId, timeline_id: TimelineId):
    """
    Return path to the LOCAL_FS index_part.json of the tenant and timeline.
    """
    assert isinstance(env.remote_storage, LocalFsStorage)
    return (
        env.remote_storage.root
        / "tenants"
        / str(tenant_id)
        / "timelines"
        / str(timeline_id)
        / "index_part.json"
    )
