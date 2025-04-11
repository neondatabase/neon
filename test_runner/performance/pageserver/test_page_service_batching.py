import concurrent.futures
import dataclasses
import json
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.utils import humantime_to_ms

TARGET_RUNTIME = 30


@dataclass
class PageServicePipeliningConfig:
    pass


@dataclass
class PageServicePipeliningConfigSerial(PageServicePipeliningConfig):
    mode: str = "serial"


@dataclass
class PageServicePipeliningConfigPipelined(PageServicePipeliningConfig):
    max_batch_size: int
    execution: str
    mode: str = "pipelined"


EXECUTION = ["concurrent-futures"]

NON_BATCHABLE: list[PageServicePipeliningConfig] = [PageServicePipeliningConfigSerial()]
for max_batch_size in [1, 32]:
    for execution in EXECUTION:
        NON_BATCHABLE.append(PageServicePipeliningConfigPipelined(max_batch_size, execution))

BATCHABLE: list[PageServicePipeliningConfig] = []
for max_batch_size in [32]:
    for execution in EXECUTION:
        BATCHABLE.append(PageServicePipeliningConfigPipelined(max_batch_size, execution))


@pytest.mark.parametrize(
    "tablesize_mib, pipelining_config, target_runtime, effective_io_concurrency, readhead_buffer_size, name",
    [
        # batchable workloads should show throughput and CPU efficiency improvements
        *[
            (
                50,
                config,
                TARGET_RUNTIME,
                100,
                128,
                f"batchable {dataclasses.asdict(config)}",
            )
            for config in BATCHABLE
        ],
    ],
)
def test_throughput(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    tablesize_mib: int,
    pipelining_config: PageServicePipeliningConfig,
    target_runtime: int,
    effective_io_concurrency: int,
    readhead_buffer_size: int,
    name: str,
):
    """
    Do a bunch of sequential scans with varying compute and pipelining configurations.
    Primary performance metrics are the achieved batching factor and throughput (wall clock time).
    Resource utilization is also interesting - we currently measure CPU time.

    The test is a fixed-runtime based type of test (target_runtime).
    Hence, the results are normalized to the number of iterations completed within target runtime.

    If the compute doesn't provide pipeline depth (effective_io_concurrency=1),
    performance should be about identical in all configurations.
    Pipelining can still yield improvements in these scenarios because it parses the
    next request while the current one is still being executed.

    If the compute provides pipeline depth (effective_io_concurrency=100), then
    pipelining configs, especially with max_batch_size>1 should yield dramatic improvements
    in all performance metrics.
    """

    #
    # record perf-related parameters as metrics to simplify processing of results
    #
    params: dict[str, tuple[float | int, dict[str, Any]]] = {}

    params.update(
        {
            "tablesize_mib": (tablesize_mib, {"unit": "MiB"}),
            # target_runtime is just a polite ask to the workload to run for this long
            "effective_io_concurrency": (effective_io_concurrency, {}),
            "readhead_buffer_size": (readhead_buffer_size, {}),
            # name is not a metric, we just use it to identify the test easily in the `test_...[...]`` notation
        }
    )
    # For storing configuration as a metric, insert a fake 0 with labels with actual data
    params.update({"pipelining_config": (0, {"labels": dataclasses.asdict(pipelining_config)})})

    log.info("params: %s", params)

    for param, (value, kwargs) in params.items():
        zenbenchmark.record(
            param,
            metric_value=float(value),
            unit=kwargs.pop("unit", ""),
            report=MetricReport.TEST_PARAM,
            labels=kwargs.pop("labels", None),
            **kwargs,
        )

    #
    # Setup
    #

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            # minimal lfc & small shared buffers to force requests to pageserver
            "neon.max_file_cache_size=1MB",
            "shared_buffers=10MB",
        ],
    )
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

    #
    # Run the workload, collect `Metrics` before and after, calculate difference, normalize.
    #

    @dataclass
    class Metrics:
        time: float
        pageserver_batch_size_histo_sum: float
        pageserver_batch_size_histo_count: float
        compute_getpage_count: float
        pageserver_cpu_seconds_total: float

        def __sub__(self, other: "Metrics") -> "Metrics":
            return Metrics(
                time=self.time - other.time,
                pageserver_batch_size_histo_sum=self.pageserver_batch_size_histo_sum
                - other.pageserver_batch_size_histo_sum,
                pageserver_batch_size_histo_count=self.pageserver_batch_size_histo_count
                - other.pageserver_batch_size_histo_count,
                compute_getpage_count=self.compute_getpage_count - other.compute_getpage_count,
                pageserver_cpu_seconds_total=self.pageserver_cpu_seconds_total
                - other.pageserver_cpu_seconds_total,
            )

        def normalize(self, by) -> "Metrics":
            return Metrics(
                time=self.time / by,
                pageserver_batch_size_histo_sum=self.pageserver_batch_size_histo_sum / by,
                pageserver_batch_size_histo_count=self.pageserver_batch_size_histo_count / by,
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
                pageserver_batch_size_histo_sum=pageserver_metrics.query_one(
                    "pageserver_page_service_batch_size_sum"
                ).value,
                pageserver_batch_size_histo_count=pageserver_metrics.query_one(
                    "pageserver_page_service_batch_size_count"
                ).value,
                compute_getpage_count=compute_getpage_count,
                pageserver_cpu_seconds_total=pageserver_metrics.query_one(
                    "libmetrics_process_cpu_seconds_highres"
                ).value,
            )

    def workload(disruptor_started: threading.Event) -> Metrics:
        disruptor_started.wait()
        start = time.time()
        iters = 0
        while time.time() - start < target_runtime or iters < 2:
            if iters == 1:
                # round zero for warming up
                before = get_metrics()
            cur.execute("select sum(data::bigint) from t")
            assert cur.fetchall()[0][0] == npages * (npages + 1) // 2
            iters += 1
        after = get_metrics()
        return (after - before).normalize(iters - 1)

    def disruptor(disruptor_started: threading.Event, stop_disruptor: threading.Event):
        conn = endpoint.connect()
        cur = conn.cursor()
        iters = 0
        while True:
            cur.execute("SELECT pg_logical_emit_message(true, 'test', 'advancelsn')")
            if stop_disruptor.is_set():
                break
            disruptor_started.set()
            iters += 1
            time.sleep(0.001)
        return iters

    env.pageserver.patch_config_toml_nonrecursive(
        {"page_service_pipelining": dataclasses.asdict(pipelining_config)}
    )

    # set trace for log analysis below
    env.pageserver.restart(extra_env_vars={"RUST_LOG": "info,pageserver::page_service=trace"})

    log.info("Starting workload")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        disruptor_started = threading.Event()
        stop_disruptor = threading.Event()
        disruptor_fut = executor.submit(disruptor, disruptor_started, stop_disruptor)
        workload_fut = executor.submit(workload, disruptor_started)
        metrics = workload_fut.result()
        stop_disruptor.set()
        ndisruptions = disruptor_fut.result()
        log.info("Disruptor issued %d disrupting requests", ndisruptions)

    log.info("Results: %s", metrics)

    since_last_start: list[str] = []
    for line in env.pageserver.logfile.read_text().splitlines():
        if "git:" in line:
            since_last_start = []
        since_last_start.append(line)

    stopping_batching_because_re = re.compile(
        r"stopping batching because (LSN changed|of batch size|timeline object mismatch|batch key changed|same page was requested at different LSNs|.*)"
    )
    reasons_for_stopping_batching = {}
    for line in since_last_start:
        match = stopping_batching_because_re.search(line)
        if match:
            if match.group(1) not in reasons_for_stopping_batching:
                reasons_for_stopping_batching[match.group(1)] = 0
            reasons_for_stopping_batching[match.group(1)] += 1

    log.info("Reasons for stopping batching: %s", reasons_for_stopping_batching)

    #
    # Sanity-checks on the collected data
    #
    # assert that getpage counts roughly match between compute and ps
    assert metrics.pageserver_batch_size_histo_sum == pytest.approx(
        metrics.compute_getpage_count, rel=0.01
    )

    #
    # Record the results
    #

    for metric, value in dataclasses.asdict(metrics).items():
        zenbenchmark.record(f"counters.{metric}", value, unit="", report=MetricReport.TEST_PARAM)

    zenbenchmark.record(
        "perfmetric.batching_factor",
        metrics.pageserver_batch_size_histo_sum / metrics.pageserver_batch_size_histo_count,
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )


PRECISION_CONFIGS: list[PageServicePipeliningConfig] = [PageServicePipeliningConfigSerial()]
for max_batch_size in [1, 32]:
    for execution in EXECUTION:
        PRECISION_CONFIGS.append(PageServicePipeliningConfigPipelined(max_batch_size, execution))


@pytest.mark.parametrize(
    "pipelining_config,name",
    [(config, f"{dataclasses.asdict(config)}") for config in PRECISION_CONFIGS],
)
def test_latency(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    pipelining_config: PageServicePipeliningConfig,
    name: str,
):
    """
    Measure the latency impact of pipelining in an un-batchable workloads.

    An ideal implementation should not increase average or tail latencies for such workloads.

    We don't have support in pagebench to create queue depth yet.
    => https://github.com/neondatabase/neon/issues/9837
    """

    #
    # Setup
    #

    def patch_ps_config(ps_config):
        if pipelining_config is not None:
            ps_config["page_service_pipelining"] = dataclasses.asdict(pipelining_config)

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
