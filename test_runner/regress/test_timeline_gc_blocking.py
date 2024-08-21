import time

from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pageserver.utils import wait_timeline_detail_404


def test_gc_blocking_by_timeline(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start(
        initial_tenant_conf={"gc_period": "1s", "lsn_lease_length": "0s"}
    )
    ps = env.pageserver
    http = ps.http_client()

    foo_branch = env.neon_cli.create_branch("foo", "main", env.initial_tenant)

    gc_active_line = ".* gc_loop.*: [12] timelines need GC"
    gc_skipped_line = ".* gc_loop.*: Skipping GC: .*"
    init_gc_skipped = ".*: initialized with gc blocked.*"

    tenant_before = http.tenant_status(env.initial_tenant)

    wait_for_another_gc_round()
    _, offset = ps.assert_log_contains(gc_active_line)

    assert ps.log_contains(gc_skipped_line, offset) is None

    http.timeline_block_gc(env.initial_tenant, foo_branch)

    tenant_after = http.tenant_status(env.initial_tenant)
    assert tenant_before != tenant_after
    gc_blocking = tenant_after["gc_blocking"]
    assert gc_blocking == "BlockingReasons { timelines: 1, reasons: EnumSet(Manual) }"

    wait_for_another_gc_round()
    _, offset = ps.assert_log_contains(gc_skipped_line, offset)

    ps.restart()
    ps.quiesce_tenants()

    _, offset = env.pageserver.assert_log_contains(init_gc_skipped, offset)

    wait_for_another_gc_round()
    _, offset = ps.assert_log_contains(gc_skipped_line, offset)

    # deletion unblocks gc
    http.timeline_delete(env.initial_tenant, foo_branch)
    wait_timeline_detail_404(http, env.initial_tenant, foo_branch, 10, 1.0)

    wait_for_another_gc_round()
    _, offset = ps.assert_log_contains(gc_active_line, offset)

    http.timeline_block_gc(env.initial_tenant, env.initial_timeline)

    wait_for_another_gc_round()
    _, offset = ps.assert_log_contains(gc_skipped_line, offset)

    # removing the manual block also unblocks gc
    http.timeline_unblock_gc(env.initial_tenant, env.initial_timeline)

    wait_for_another_gc_round()
    _, offset = ps.assert_log_contains(gc_active_line, offset)


def wait_for_another_gc_round():
    time.sleep(2)
