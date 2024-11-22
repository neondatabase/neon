import dataclasses
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.utils import humantime_to_ms

TARGET_RUNTIME = 60


@pytest.mark.skip("See https://github.com/neondatabase/neon/pull/9820#issue-2675856095")
@pytest.mark.parametrize(
    "tablesize_mib, batch_timeout, target_runtime, effective_io_concurrency, readhead_buffer_size, name",
    [
        # the next 4 cases demonstrate how not-batchable workloads suffer from batching timeout
        (50, None, TARGET_RUNTIME, 1, 128, "not batchable no batching"),
        (50, "10us", TARGET_RUNTIME, 1, 128, "not batchable 10us timeout"),
        (50, "1ms", TARGET_RUNTIME, 1, 128, "not batchable 1ms timeout"),
        # the next 4 cases demonstrate how batchable workloads benefit from batching
        (50, None, TARGET_RUNTIME, 100, 128, "batchable no batching"),
        (50, "10us", TARGET_RUNTIME, 100, 128, "batchable 10us timeout"),
        (50, "100us", TARGET_RUNTIME, 100, 128, "batchable 100us timeout"),
        (50, "1ms", TARGET_RUNTIME, 100, 128, "batchable 1ms timeout"),
    ],
)
def test_getpage_merge_smoke(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    tablesize_mib: int,
    batch_timeout: str | None,
    target_runtime: int,
    effective_io_concurrency: int,
    readhead_buffer_size: int,
    name: str,
):
    """
    Do a bunch of sequential scans and ensure that the pageserver does some merging.
    """

    #
    # record perf-related parameters as metrics to simplify processing of results
    #
    params: dict[str, tuple[float | int, dict[str, Any]]] = {}

    params.update(
        {
            "tablesize_mib": (tablesize_mib, {"unit": "MiB"}),
            "batch_timeout": (
                -1 if batch_timeout is None else 1e3 * humantime_to_ms(batch_timeout),
                {"unit": "us"},
            ),
            # target_runtime is just a polite ask to the workload to run for this long
            "effective_io_concurrency": (effective_io_concurrency, {}),
            "readhead_buffer_size": (readhead_buffer_size, {}),
            # name is not a metric
        }
    )

    log.info("params: %s", params)

    for param, (value, kwargs) in params.items():
        zenbenchmark.record(
            param,
            metric_value=value,
            unit=kwargs.pop("unit", ""),
            report=MetricReport.TEST_PARAM,
            **kwargs,
        )

    #
    # Setup
    #

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")
    conn = endpoint.connect()
    cur = conn.cursor()

    cur.execute("SET max_parallel_workers_per_gather=0")  # disable parallel backends
    cur.execute(f"SET effective_io_concurrency={effective_io_concurrency}")
    cur.execute(
        f"SET neon.readahead_buffer_size={readhead_buffer_size}"
    )  # this is the current default value, but let's hard-code that

    cur.execute("CREATE EXTENSION IF NOT EXISTS neon;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")

    log.info("Filling the table")
    cur.execute("CREATE TABLE t (data char(1000)) with (fillfactor=10)")
    tablesize = tablesize_mib * 1024 * 1024
    npages = tablesize // (8 * 1024)
    cur.execute("INSERT INTO t SELECT generate_series(1, %s)", (npages,))
    # TODO: can we force postgres to do sequential scans?

    #
    # Run the workload, collect `Metrics` before and after, calculate difference, normalize.
    #

    @dataclass
    class Metrics:
        time: float
        pageserver_getpage_count: float
        pageserver_vectored_get_count: float
        compute_getpage_count: float
        pageserver_cpu_seconds_total: float

        def __sub__(self, other: "Metrics") -> "Metrics":
            return Metrics(
                time=self.time - other.time,
                pageserver_getpage_count=self.pageserver_getpage_count
                - other.pageserver_getpage_count,
                pageserver_vectored_get_count=self.pageserver_vectored_get_count
                - other.pageserver_vectored_get_count,
                compute_getpage_count=self.compute_getpage_count - other.compute_getpage_count,
                pageserver_cpu_seconds_total=self.pageserver_cpu_seconds_total
                - other.pageserver_cpu_seconds_total,
            )

        def normalize(self, by) -> "Metrics":
            return Metrics(
                time=self.time / by,
                pageserver_getpage_count=self.pageserver_getpage_count / by,
                pageserver_vectored_get_count=self.pageserver_vectored_get_count / by,
                compute_getpage_count=self.compute_getpage_count / by,
                pageserver_cpu_seconds_total=self.pageserver_cpu_seconds_total / by,
            )

    def get_metrics() -> Metrics:
        with conn.cursor() as cur:
            cur.execute(
                "select value from neon_perf_counters where metric='getpage_wait_seconds_count';"
            )
            compute_getpage_count = cur.fetchall()[0][0]
            pageserver_metrics = ps_http.get_metrics()
            return Metrics(
                time=time.time(),
                pageserver_getpage_count=pageserver_metrics.query_one(
                    "pageserver_smgr_query_seconds_count", {"smgr_query_type": "get_page_at_lsn"}
                ).value,
                pageserver_vectored_get_count=pageserver_metrics.query_one(
                    "pageserver_get_vectored_seconds_count", {"task_kind": "PageRequestHandler"}
                ).value,
                compute_getpage_count=compute_getpage_count,
                pageserver_cpu_seconds_total=pageserver_metrics.query_one(
                    "libmetrics_process_cpu_seconds_highres"
                ).value,
            )

    def workload() -> Metrics:
        start = time.time()
        iters = 0
        while time.time() - start < target_runtime or iters < 2:
            log.info("Seqscan %d", iters)
            if iters == 1:
                # round zero for warming up
                before = get_metrics()
            cur.execute(
                "select clear_buffer_cache()"
            )  # TODO: what about LFC? doesn't matter right now because LFC isn't enabled by default in tests
            cur.execute("select sum(data::bigint) from t")
            assert cur.fetchall()[0][0] == npages * (npages + 1) // 2
            iters += 1
        after = get_metrics()
        return (after - before).normalize(iters - 1)

    env.pageserver.patch_config_toml_nonrecursive({"server_side_batch_timeout": batch_timeout})
    env.pageserver.restart()
    metrics = workload()

    log.info("Results: %s", metrics)

    #
    # Sanity-checks on the collected data
    #
    # assert that getpage counts roughly match between compute and ps
    assert metrics.pageserver_getpage_count == pytest.approx(
        metrics.compute_getpage_count, rel=0.01
    )

    #
    # Record the results
    #

    for metric, value in dataclasses.asdict(metrics).items():
        zenbenchmark.record(f"counters.{metric}", value, unit="", report=MetricReport.TEST_PARAM)

    zenbenchmark.record(
        "perfmetric.batching_factor",
        metrics.pageserver_getpage_count / metrics.pageserver_vectored_get_count,
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )


@pytest.mark.skip("See https://github.com/neondatabase/neon/pull/9820#issue-2675856095")
@pytest.mark.parametrize(
    "batch_timeout", [None, "10us", "20us", "50us", "100us", "200us", "500us", "1ms"]
)
def test_timer_precision(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    batch_timeout: str | None,
):
    """
    Determine the batching timeout precision (mean latency) and tail latency impact.

    The baseline is `None`; an ideal batching timeout implementation would increase
    the mean latency by exactly `batch_timeout`.

    That is not the case with the current implementation, will be addressed in future changes.
    """

    #
    # Setup
    #

    def patch_ps_config(ps_config):
        ps_config["server_side_batch_timeout"] = batch_timeout

    neon_env_builder.pageserver_config_override = patch_ps_config

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")
    conn = endpoint.connect()
    cur = conn.cursor()

    cur.execute("SET max_parallel_workers_per_gather=0")  # disable parallel backends
    cur.execute("SET effective_io_concurrency=1")

    cur.execute("CREATE EXTENSION IF NOT EXISTS neon;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")

    log.info("Filling the table")
    cur.execute("CREATE TABLE t (data char(1000)) with (fillfactor=10)")
    tablesize = 50 * 1024 * 1024
    npages = tablesize // (8 * 1024)
    cur.execute("INSERT INTO t SELECT generate_series(1, %s)", (npages,))
    # TODO: can we force postgres to do sequential scans?

    cur.close()
    conn.close()

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    endpoint.stop()

    for sk in env.safekeepers:
        sk.stop()

    #
    # Run single-threaded pagebench (TODO: dedup with other benchmark code)
    #

    env.pageserver.allowed_errors.append(
        # https://github.com/neondatabase/neon/issues/6925
        r".*query handler for.*pagestream.*failed: unexpected message: CopyFail during COPY.*"
    )

    ps_http = env.pageserver.http_client()

    cmd = [
        str(env.neon_binpath / "pagebench"),
        "get-page-latest-lsn",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--page-service-connstring",
        env.pageserver.connstr(password=None),
        "--num-clients",
        "1",
        "--runtime",
        "10s",
    ]
    log.info(f"command: {' '.join(cmd)}")
    basepath = pg_bin.run_capture(cmd, with_command_header=False)
    results_path = Path(basepath + ".stdout")
    log.info(f"Benchmark results at: {results_path}")

    with open(results_path) as f:
        results = json.load(f)
    log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")

    total = results["total"]

    metric = "latency_mean"
    zenbenchmark.record(
        metric,
        metric_value=humantime_to_ms(total[metric]),
        unit="ms",
        report=MetricReport.LOWER_IS_BETTER,
    )

    metric = "latency_percentiles"
    for k, v in total[metric].items():
        zenbenchmark.record(
            f"{metric}.{k}",
            metric_value=humantime_to_ms(v),
            unit="ms",
            report=MetricReport.LOWER_IS_BETTER,
        )
