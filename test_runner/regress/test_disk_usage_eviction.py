import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    LayerMapInfo,
    NeonEnv,
    NeonEnvBuilder,
    PageserverHttpClient,
    PgBin,
    RemoteStorageKind,
    wait_for_last_flush_lsn,
)
from fixtures.types import TenantId, TimelineId

GLOBAL_LRU_LOG_LINE = "tenant_min_resident_size-respecting LRU would not relieve pressure, evicting more following global LRU policy"


@pytest.mark.parametrize("config_level_override", [None, 400])
def test_min_resident_size_override_handling(
    neon_env_builder: NeonEnvBuilder, config_level_override: int
):
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    def assert_config(tenant_id, expect_override, expect_effective):
        config = ps_http.tenant_config(tenant_id)
        assert config.tenant_specific_overrides.get("min_resident_size_override") == expect_override
        assert config.effective_config.get("min_resident_size_override") == expect_effective

    def assert_overrides(tenant_id, default_tenant_conf_value):
        ps_http.set_tenant_config(tenant_id, {"min_resident_size_override": 200})
        assert_config(tenant_id, 200, 200)

        ps_http.set_tenant_config(tenant_id, {"min_resident_size_override": 0})
        assert_config(tenant_id, 0, 0)

        ps_http.set_tenant_config(tenant_id, {})
        assert_config(tenant_id, None, default_tenant_conf_value)

    env.pageserver.stop()
    if config_level_override is not None:
        env.pageserver.start(
            overrides=(
                "--pageserver-config-override=tenant_config={ min_resident_size_override =  "
                + str(config_level_override)
                + " }",
            )
        )
    else:
        env.pageserver.start()

    tenant_id, _ = env.neon_cli.create_tenant()
    assert_overrides(tenant_id, config_level_override)

    # Also ensure that specifying the paramter to create_tenant works, in addition to http-level recconfig.
    tenant_id, _ = env.neon_cli.create_tenant(conf={"min_resident_size_override": "100"})
    assert_config(tenant_id, 100, 100)
    ps_http.set_tenant_config(tenant_id, {})
    assert_config(tenant_id, None, config_level_override)


@dataclass
class EvictionEnv:
    timelines: list[Tuple[TenantId, TimelineId, LayerMapInfo]]
    neon_env: NeonEnv
    pg_bin: PgBin
    pageserver_http: PageserverHttpClient
    layer_size: int

    def timelines_du(self) -> Tuple[int, int, int]:
        return poor_mans_du(self.neon_env, [(tid, tlid) for tid, tlid, _ in self.timelines])

    def du_by_timeline(self) -> Dict[Tuple[TenantId, TimelineId], int]:
        return {
            (tid, tlid): poor_mans_du(self.neon_env, [(tid, tlid)])[0]
            for tid, tlid, _ in self.timelines
        }


@pytest.fixture
def eviction_env(request, neon_env_builder: NeonEnvBuilder, pg_bin: PgBin) -> Iterator[EvictionEnv]:
    """
    Creates two tenants, one somewhat larger than the other.
    """

    log.info(f"setting up eviction_env for test {request.node.name}")

    neon_env_builder.enable_remote_storage(RemoteStorageKind.LOCAL_FS, f"{request.node.name}")

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # allow because we are invoking this manually; we always warn on executing disk based eviction
    env.pageserver.allowed_errors.append(r".* running disk usage based eviction due to pressure.*")
    env.pageserver.allowed_errors.append(
        r".* Changing Active tenant to Broken state, reason: broken from test"
    )

    # break the difficult to use initial default tenant, later assert that it has not been evicted
    broken_tenant_id, broken_timeline_id = (env.initial_tenant, env.initial_timeline)
    assert broken_timeline_id is not None
    pageserver_http.tenant_break(env.initial_tenant)
    (broken_on_disk_before, _, _) = poor_mans_du(
        env, timelines=[(broken_tenant_id, broken_timeline_id)]
    )
    env.pageserver.allowed_errors.append(
        f".*extend_lru_candidates.*Tenant {broken_tenant_id} is not active. Current state: Broken"
    )

    timelines = []

    # Choose small layer_size so that we can use low pgbench_scales and still get a large count of layers.
    # Large count of layers and small layer size is good for testing because it makes evictions predictable.
    # Predictable in the sense that many layer evictions will be required to reach the eviction target, because
    # each eviction only makes small progress. That means little overshoot, and thereby stable asserts.
    pgbench_scales = [4, 6]
    layer_size = 5 * 1024**2

    for scale in pgbench_scales:
        tenant_id, timeline_id = env.neon_cli.create_tenant(
            conf={
                "gc_period": "0s",
                "compaction_period": "0s",
                "checkpoint_distance": f"{layer_size}",
                "image_creation_threshold": "100",
                "compaction_target_size": f"{layer_size}",
            }
        )

        with env.postgres.create_start("main", tenant_id=tenant_id) as pg:
            pg_bin.run(["pgbench", "-i", f"-s{scale}", pg.connstr()])
            wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
        log.info(f"{layers}")
        assert len(layers.historic_layers) >= 4

        timelines.append((tenant_id, timeline_id, layers))

    eviction_env = EvictionEnv(
        timelines=timelines,
        neon_env=env,
        pageserver_http=pageserver_http,
        layer_size=layer_size,
        pg_bin=pg_bin,
    )

    yield eviction_env

    (broken_on_disk_after, _, _) = poor_mans_du(
        eviction_env.neon_env, [(broken_tenant_id, broken_timeline_id)]
    )

    assert (
        broken_on_disk_before == broken_on_disk_after
    ), "only touch active tenants with disk_usage_eviction"


def test_pageserver_evicts_until_pressure_is_relieved(eviction_env: EvictionEnv):
    """
    Basic test to ensure that we evict enough to relieve pressure.
    """
    env = eviction_env
    pageserver_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()

    target = total_on_disk // 2

    response = pageserver_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du()

    actual_change = total_on_disk - later_total_on_disk

    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "must evict more than half"
    assert (
        response["Finished"]["assumed"]["projected_after"]["freed_bytes"] >= actual_change
    ), "report accurately evicted bytes"
    assert response["Finished"]["assumed"]["failed"]["count"] == 0, "zero failures expected"


def test_pageserver_respects_overridden_resident_size(eviction_env: EvictionEnv):
    """
    Override tenant min resident and ensure that it will be respected by eviction.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()
    du_by_timeline = env.du_by_timeline()
    log.info("du_by_timeline: %s", du_by_timeline)

    assert len(du_by_timeline) == 2, "this test assumes two tenants"
    large_tenant = max(du_by_timeline, key=du_by_timeline.__getitem__)
    small_tenant = min(du_by_timeline, key=du_by_timeline.__getitem__)
    assert du_by_timeline[large_tenant] > du_by_timeline[small_tenant]
    assert (
        du_by_timeline[large_tenant] - du_by_timeline[small_tenant] > 5 * env.layer_size
    ), "ensure this test will do more than 1 eviction"

    # Give the larger tenant a haircut while preventing the smaller tenant from getting one.
    # To prevent the smaller from getting a haircut, we set min_resident_size to its current size.
    # To ensure the larger tenant is getting a haircut, any non-zero `target` will do.
    min_resident_size = du_by_timeline[small_tenant]
    target = 1
    assert any(
        [du > min_resident_size for du in du_by_timeline.values()]
    ), "ensure the larger tenant will get a haircut"

    ps_http.set_tenant_config(small_tenant[0], {"min_resident_size_override": min_resident_size})
    ps_http.set_tenant_config(large_tenant[0], {"min_resident_size_override": min_resident_size})

    # Make the large tenant more-recently used. An incorrect implemention would try to evict
    # from the smaller tenant first, since its layers would be the least-recently-used
    with env.neon_env.postgres.create_start("main", tenant_id=large_tenant[0]) as pg:
        env.pg_bin.run(["pgbench", "-S", pg.connstr()])

    # do one run
    response = ps_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    time.sleep(1)  # give log time to flush
    assert not env.neon_env.pageserver.log_contains(
        GLOBAL_LRU_LOG_LINE,
    ), "this test is pointless if it fell back to global LRU"

    (later_total_on_disk, _, _) = env.timelines_du()
    later_du_by_timeline = env.du_by_timeline()
    log.info("later_du_by_timeline: %s", later_du_by_timeline)

    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"
    assert (
        response["Finished"]["assumed"]["projected_after"]["freed_bytes"] >= actual_change
    ), "report accurately evicted bytes"
    assert response["Finished"]["assumed"]["failed"]["count"] == 0, "zero failures expected"

    assert (
        later_du_by_timeline[small_tenant] == du_by_timeline[small_tenant]
    ), "small tenant sees no haircut"
    assert (
        later_du_by_timeline[large_tenant] < du_by_timeline[large_tenant]
    ), "large tenant gets a haircut"
    assert du_by_timeline[large_tenant] - later_du_by_timeline[large_tenant] >= target


def test_pageserver_falls_back_to_global_lru(eviction_env: EvictionEnv):
    """
    If we can't relieve pressure using tenant_min_resident_size-respecting eviction,
    we should continue to evict layers following global LRU.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()
    target = total_on_disk

    response = ps_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du()
    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"

    time.sleep(1)  # give log time to flush
    assert env.neon_env.pageserver.log_contains(GLOBAL_LRU_LOG_LINE)
    env.neon_env.pageserver.allowed_errors.append(".*" + GLOBAL_LRU_LOG_LINE)


def test_partial_evict_tenant(eviction_env: EvictionEnv):
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()
    du_by_timeline = env.du_by_timeline()

    # pick any tenant
    [our_tenant, other_tenant] = list(du_by_timeline.keys())
    (tenant_id, timeline_id) = our_tenant
    tenant_usage = du_by_timeline[our_tenant]

    # make our tenant more recently used than the other one
    with env.neon_env.postgres.create_start("main", tenant_id=tenant_id) as pg:
        env.pg_bin.run(["pgbench", "-S", pg.connstr()])

    target = total_on_disk - (tenant_usage // 2)
    response = ps_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du()
    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"

    later_du_by_timeline = env.du_by_timeline()
    for tenant, later_tenant_usage in later_du_by_timeline.items():
        assert (
            later_tenant_usage < du_by_timeline[tenant]
        ), "all tenants should have lost some layers"

    assert (
        later_du_by_timeline[our_tenant] > 0.4 * tenant_usage
    ), "our warmed up tenant should be at about half capacity"
    assert (
        later_du_by_timeline[other_tenant] < 2 * env.layer_size
    ), "the other tenant should be completely evicted"


def poor_mans_du(
    env: NeonEnv, timelines: list[Tuple[TenantId, TimelineId]]
) -> Tuple[int, int, int]:
    """
    Disk usage, largest, smallest layer for layer files over the given (tenant, timeline) tuples;
    this could be done over layers endpoint just as well.
    """
    total_on_disk = 0
    largest_layer = 0
    smallest_layer = None
    for tenant_id, timeline_id in timelines:
        dir = Path(env.repo_dir) / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
        assert dir.exists(), f"timeline dir does not exist: {dir}"
        sum = 0
        for file in dir.iterdir():
            if "__" not in file.name:
                continue
            size = file.stat().st_size
            sum += size
            largest_layer = max(largest_layer, size)
            if smallest_layer:
                smallest_layer = min(smallest_layer, size)
            else:
                smallest_layer = size
            log.info(f"{tenant_id}/{timeline_id} => {file.name} {size}")

        log.info(f"{tenant_id}/{timeline_id}: sum {sum}")
        total_on_disk += sum

    assert smallest_layer is not None or total_on_disk == 0 and largest_layer == 0
    return (total_on_disk, largest_layer, smallest_layer or 0)
