from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.safekeeper.http import SafekeeperHttpClient


def are_walreceivers_absent(
    sk_http_cli: SafekeeperHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    status = sk_http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"waiting for walreceivers to be gone, currently {status.walreceivers}")
    return len(status.walreceivers) == 0
