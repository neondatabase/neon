from __future__ import annotations

from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.safekeeper.http import SafekeeperHttpClient
from fixtures.utils import wait_until


def wait_walreceivers_absent(
    sk_http_cli: SafekeeperHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    """
    Wait until there is no walreceiver connections from the compute(s) on the
    safekeeper.
    """

    def walreceivers_absent():
        status = sk_http_cli.timeline_status(tenant_id, timeline_id)
        log.info(f"waiting for walreceivers to be gone, currently {status.walreceivers}")
        assert len(status.walreceivers) == 0

    wait_until(walreceivers_absent)
