from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonPageserver, Safekeeper
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_for_upload
from fixtures.utils import get_dir_size


def is_segment_offloaded(
    sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, seg_end: Lsn
):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.backup_lsn >= seg_end


def is_flush_lsn_caught_up(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.flush_lsn >= lsn


def is_wal_trimmed(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, target_size_mb):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    sk_wal_size = get_dir_size(sk.timeline_dir(tenant_id, timeline_id))
    sk_wal_size_mb = sk_wal_size / 1024 / 1024
    log.info(f"Safekeeper id={sk.id} wal_size={sk_wal_size_mb:.2f}MB status={tli_status}")
    return sk_wal_size_mb <= target_size_mb


def wait_lsn_force_checkpoint(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    endpoint: Endpoint,
    ps: NeonPageserver,
    pageserver_conn_options=None,
):
    pageserver_conn_options = pageserver_conn_options or {}
    lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    log.info(f"pg_current_wal_flush_lsn is {lsn}, waiting for it on pageserver")

    wait_lsn_force_checkpoint_at(lsn, tenant_id, timeline_id, ps, pageserver_conn_options)


def wait_lsn_force_checkpoint_at_sk(
    safekeeper: Safekeeper,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ps: NeonPageserver,
    pageserver_conn_options=None,
):
    sk_flush_lsn = safekeeper.get_flush_lsn(tenant_id, timeline_id)
    wait_lsn_force_checkpoint_at(sk_flush_lsn, tenant_id, timeline_id, ps, pageserver_conn_options)


def wait_lsn_force_checkpoint_at(
    lsn: Lsn,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ps: NeonPageserver,
    pageserver_conn_options=None,
):
    """
    Wait until pageserver receives given lsn, force checkpoint and wait for
    upload, i.e. remote_consistent_lsn advancement.
    """
    pageserver_conn_options = pageserver_conn_options or {}

    auth_token = None
    if "password" in pageserver_conn_options:
        auth_token = pageserver_conn_options["password"]

    # wait for the pageserver to catch up
    wait_for_last_record_lsn(
        ps.http_client(auth_token=auth_token),
        tenant_id,
        timeline_id,
        lsn,
    )

    # force checkpoint to advance remote_consistent_lsn
    ps.http_client(auth_token).timeline_checkpoint(tenant_id, timeline_id)

    # ensure that remote_consistent_lsn is advanced
    wait_for_upload(
        ps.http_client(auth_token=auth_token),
        tenant_id,
        timeline_id,
        lsn,
    )
