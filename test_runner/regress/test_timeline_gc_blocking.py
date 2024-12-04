from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    LogCursor,
    NeonEnvBuilder,
    NeonPageserver,
)
from fixtures.pageserver.utils import wait_timeline_detail_404


@pytest.mark.parametrize("sharded", [True, False])
def test_gc_blocking_by_timeline(neon_env_builder: NeonEnvBuilder, sharded: bool):
    neon_env_builder.num_pageservers = 2 if sharded else 1
    env = neon_env_builder.init_start(
        initial_tenant_conf={"gc_period": "1s", "lsn_lease_length": "0s"},
        initial_tenant_shard_count=2 if sharded else None,
    )

    if sharded:
        http = env.storage_controller.pageserver_api()
    else:
        http = env.pageserver.http_client()

    pss = ManyPageservers(list(map(lambda ps: ScrollableLog(ps, None), env.pageservers)))

    foo_branch = env.create_branch("foo", ancestor_branch_name="main", tenant_id=env.initial_tenant)

    gc_active_line = ".* gc_loop.*: [12] timelines need GC"
    gc_skipped_line = ".* gc_loop.*: Skipping GC: .*"
    init_gc_skipped = ".*: initialized with gc blocked.*"

    tenant_before = http.tenant_status(env.initial_tenant)

    wait_for_another_gc_round()
    pss.assert_log_contains(gc_active_line)
    pss.assert_log_does_not_contain(gc_skipped_line)

    http.timeline_block_gc(env.initial_tenant, foo_branch)

    tenant_after = http.tenant_status(env.initial_tenant)
    assert tenant_before != tenant_after
    gc_blocking = tenant_after["gc_blocking"]
    assert gc_blocking == "BlockingReasons { timelines: 1, reasons: EnumSet(Manual) }"

    wait_for_another_gc_round()
    pss.assert_log_contains(gc_skipped_line)

    pss.restart()
    pss.quiesce_tenants()

    pss.assert_log_contains(init_gc_skipped)

    wait_for_another_gc_round()
    pss.assert_log_contains(gc_skipped_line)

    # deletion unblocks gc
    http.timeline_delete(env.initial_tenant, foo_branch)
    wait_timeline_detail_404(http, env.initial_tenant, foo_branch)

    wait_for_another_gc_round()
    pss.assert_log_contains(gc_active_line)

    http.timeline_block_gc(env.initial_tenant, env.initial_timeline)

    wait_for_another_gc_round()
    pss.assert_log_contains(gc_skipped_line)

    # removing the manual block also unblocks gc
    http.timeline_unblock_gc(env.initial_tenant, env.initial_timeline)

    wait_for_another_gc_round()
    pss.assert_log_contains(gc_active_line)


def wait_for_another_gc_round():
    time.sleep(2)


@dataclass
class ScrollableLog:
    pageserver: NeonPageserver
    offset: LogCursor | None

    def assert_log_contains(self, what: str):
        msg, offset = self.pageserver.assert_log_contains(what, offset=self.offset)
        old = self.offset
        self.offset = offset
        log.info(f"{old} -> {offset}: {msg}")

    def assert_log_does_not_contain(self, what: str):
        assert self.pageserver.log_contains(what) is None


@dataclass(frozen=True)
class ManyPageservers:
    many: list[ScrollableLog]

    def assert_log_contains(self, what: str):
        for one in self.many:
            one.assert_log_contains(what)

    def assert_log_does_not_contain(self, what: str):
        for one in self.many:
            one.assert_log_does_not_contain(what)

    def restart(self):
        def do_restart(x: ScrollableLog):
            x.pageserver.restart()

        with ThreadPoolExecutor(max_workers=len(self.many)) as rt:
            rt.map(do_restart, self.many)
            rt.shutdown(wait=True)

    def quiesce_tenants(self):
        def do_quiesce(x: ScrollableLog):
            x.pageserver.quiesce_tenants()

        with ThreadPoolExecutor(max_workers=len(self.many)) as rt:
            rt.map(do_quiesce, self.many)
            rt.shutdown(wait=True)
