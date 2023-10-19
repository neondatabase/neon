# safekeeper utils

import os
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import Safekeeper, get_dir_size
from fixtures.types import Lsn, TenantId, TimelineId


# my grammar guard startles at the name, but let's consider it is a shortening from "is it true that ...."
def is_segs_not_exist(segs, http_cli, tenant_id, timeline_id):
    segs_existense = [f"{f}: {os.path.exists(f)}" for f in segs]
    log.info(
        f"waiting for segments removal, sk info: {http_cli.timeline_status(tenant_id=tenant_id, timeline_id=timeline_id)}, segments_existence: {segs_existense}"
    )
    return all(not os.path.exists(p) for p in segs)


def is_flush_lsn_caught_up(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.flush_lsn >= lsn


def is_commit_lsn_equals_flush_lsn(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.flush_lsn == tli_status.commit_lsn


def is_segment_offloaded(
    sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, seg_end: Lsn
):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.backup_lsn >= seg_end


def is_wal_trimmed(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, target_size_mb):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    sk_wal_size = get_dir_size(os.path.join(sk.data_dir(), str(tenant_id), str(timeline_id)))
    sk_wal_size_mb = sk_wal_size / 1024 / 1024
    log.info(f"Safekeeper id={sk.id} wal_size={sk_wal_size_mb:.2f}MB status={tli_status}")
    return sk_wal_size_mb <= target_size_mb


# Wait for something, defined as f() returning True, raising error if this
# doesn't happen without timeout seconds.
# TODO: unite with wait_until preserving good logs
def wait(f, desc, timeout=30):
    started_at = time.time()
    while True:
        if f():
            break
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(f"timed out waiting {elapsed:.0f}s for {desc}")
        time.sleep(0.5)
