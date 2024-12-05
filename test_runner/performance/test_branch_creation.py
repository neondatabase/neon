from __future__ import annotations

import random
import re
import statistics
import threading
import time
import timeit
from contextlib import closing

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.compare_fixtures import NeonCompare
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonPageserver
from fixtures.pageserver.utils import wait_for_last_record_lsn
from fixtures.utils import wait_until
from prometheus_client.samples import Sample


def _record_branch_creation_durations(neon_compare: NeonCompare, durs: list[float]):
    neon_compare.zenbenchmark.record(
        "branch_creation_duration_max", max(durs), "s", MetricReport.LOWER_IS_BETTER
    )
    neon_compare.zenbenchmark.record(
        "branch_creation_duration_avg", statistics.mean(durs), "s", MetricReport.LOWER_IS_BETTER
    )
    neon_compare.zenbenchmark.record(
        "branch_creation_duration_stdev", statistics.stdev(durs), "s", MetricReport.LOWER_IS_BETTER
    )


@pytest.mark.parametrize("n_branches", [20])
# Test measures the latency of branch creation during a heavy [1] workload.
#
# [1]: to simulate a heavy workload, the test tweaks the GC and compaction settings
# to increase the task's frequency. The test runs `pgbench` in each new branch.
# Each branch is created from a randomly picked source branch.
def test_branch_creation_heavy_write(neon_compare: NeonCompare, n_branches: int):
    env = neon_compare.env
    pg_bin = neon_compare.pg_bin

    # Use aggressive GC and checkpoint settings, so GC and compaction happen more often during the test
    tenant, _ = env.create_tenant(
        conf={
            "gc_period": "5 s",
            "gc_horizon": f"{4 * 1024 ** 2}",
            "checkpoint_distance": f"{2 * 1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            "compaction_threshold": "2",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "5 s",
        }
    )

    def run_pgbench(branch: str):
        log.info(f"Start a pgbench workload on branch {branch}")

        endpoint = env.endpoints.create_start(branch, tenant_id=tenant)
        connstr = endpoint.connstr()

        pg_bin.run_capture(["pgbench", "-i", connstr])
        pg_bin.run_capture(["pgbench", "-c10", "-T10", connstr])

        endpoint.stop()

    env.create_branch("b0", tenant_id=tenant)

    threads: list[threading.Thread] = []
    threads.append(threading.Thread(target=run_pgbench, args=("b0",), daemon=True))
    threads[-1].start()

    branch_creation_durations = []
    for i in range(n_branches):
        time.sleep(1.0)

        # random a source branch
        p = random.randint(0, i)

        timer = timeit.default_timer()
        env.create_branch(f"b{i + 1}", ancestor_branch_name=f"b{p}", tenant_id=tenant)
        dur = timeit.default_timer() - timer

        log.info(f"Creating branch b{i+1} took {dur}s")
        branch_creation_durations.append(dur)

        threads.append(threading.Thread(target=run_pgbench, args=(f"b{i+1}",), daemon=True))
        threads[-1].start()

    for thread in threads:
        thread.join()

    _record_branch_creation_durations(neon_compare, branch_creation_durations)


@pytest.mark.parametrize("n_branches", [500, 1024])
@pytest.mark.parametrize("shape", ["one_ancestor", "random"])
def test_branch_creation_many(neon_compare: NeonCompare, n_branches: int, shape: str):
    """
    Test measures the latency of branch creation when creating a lot of branches.
    """
    env = neon_compare.env

    # seed the prng so we will measure the same structure every time
    rng = random.Random("2024-02-29")

    env.create_branch("b0")

    endpoint = env.endpoints.create_start("b0")
    neon_compare.pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", "-s10", endpoint.connstr()])

    branch_creation_durations = []

    for i in range(n_branches):
        if shape == "random":
            parent = f"b{rng.randint(0, i)}"
        elif shape == "one_ancestor":
            parent = "b0"
        else:
            raise RuntimeError(f"unimplemented shape: {shape}")

        timer = timeit.default_timer()
        # each of these uploads to remote storage before completion
        env.create_branch(f"b{i + 1}", ancestor_branch_name=parent)
        dur = timeit.default_timer() - timer
        branch_creation_durations.append(dur)

    _record_branch_creation_durations(neon_compare, branch_creation_durations)

    endpoint.stop_and_destroy()

    with neon_compare.record_duration("shutdown"):
        # this sleeps 100ms between polls
        env.pageserver.stop()

    startup_line = "INFO version: git(-env)?:"

    # find the first line of the log file so we can find the next start later
    _, first_start = wait_until(lambda: env.pageserver.assert_log_contains(startup_line))

    # start without gc so we can time compaction with less noise; use shorter
    # period for compaction so it starts earlier
    def patch_default_tenant_config(config):
        tenant_config = config.setdefault("tenant_config", {})
        tenant_config["compaction_period"] = "3s"
        tenant_config["gc_period"] = "0s"

    env.pageserver.edit_config_toml(patch_default_tenant_config)
    env.pageserver.start(
        # this does print more than we want, but the number should be comparable between runs
        extra_env_vars={
            "RUST_LOG": f"[compaction_loop{{tenant_id={env.initial_tenant}}}]=debug,info"
        },
    )

    _, second_start = wait_until(
        lambda: env.pageserver.assert_log_contains(startup_line, first_start),
    )
    env.pageserver.quiesce_tenants()

    wait_and_record_startup_metrics(env.pageserver, neon_compare.zenbenchmark, "restart_after")

    # wait for compaction to complete, which most likely has already done so multiple times
    msg, _ = wait_until(
        lambda: env.pageserver.assert_log_contains(
            f".*tenant_id={env.initial_tenant}.*: compaction iteration complete.*", second_start
        ),
    )
    needle = re.search(" elapsed_ms=([0-9]+)", msg)
    assert needle is not None, "failed to find the elapsed time"
    duration = int(needle.group(1)) / 1000.0
    neon_compare.zenbenchmark.record("compaction", duration, "s", MetricReport.LOWER_IS_BETTER)


def wait_and_record_startup_metrics(
    pageserver: NeonPageserver, target: NeonBenchmarker, prefix: str
):
    """
    Waits until all startup metrics have non-zero values on the pageserver, then records them on the target
    """

    client = pageserver.http_client()

    expected_labels = set(
        [
            "background_jobs_can_start",
            "complete",
            "initial",
            "initial_tenant_load",
            "initial_tenant_load_remote",
        ]
    )

    def metrics_are_filled() -> list[Sample]:
        m = client.get_metrics()
        samples = m.query_all("pageserver_startup_duration_seconds")
        # we should not have duplicate labels
        matching = [
            x for x in samples if x.labels.get("phase") in expected_labels and x.value > 0.0
        ]
        assert len(matching) == len(expected_labels)
        return matching

    samples = wait_until(metrics_are_filled)

    for sample in samples:
        phase = sample.labels["phase"]
        name = f"{prefix}.{phase}"
        target.record(name, sample.value, "s", MetricReport.LOWER_IS_BETTER)


# Test measures the branch creation time when branching from a timeline with a lot of relations.
#
# This test measures the latency of branch creation under two scenarios
# 1. The ancestor branch is not under any workloads
# 2. The ancestor branch is under a workload (busy)
#
# To simulate the workload, the test runs a concurrent insertion on the ancestor branch right before branching.
def test_branch_creation_many_relations(neon_compare: NeonCompare):
    env = neon_compare.env

    timeline_id = env.create_branch("root")

    endpoint = env.endpoints.create_start("root")
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            for i in range(10000):
                cur.execute(f"CREATE TABLE t{i} as SELECT g FROM generate_series(1, 1000) g")

    # Wait for the pageserver to finish processing all the pending WALs,
    # as we don't want the LSN wait time to be included during the branch creation
    flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    wait_for_last_record_lsn(
        env.pageserver.http_client(), env.initial_tenant, timeline_id, flush_lsn
    )

    with neon_compare.record_duration("create_branch_time_not_busy_root"):
        env.create_branch("child_not_busy", ancestor_branch_name="root")

    # run a concurrent insertion to make the ancestor "busy" during the branch creation
    thread = threading.Thread(
        target=endpoint.safe_psql, args=("INSERT INTO t0 VALUES (generate_series(1, 100000))",)
    )
    thread.start()

    with neon_compare.record_duration("create_branch_time_busy_root"):
        env.create_branch("child_busy", ancestor_branch_name="root")

    thread.join()
