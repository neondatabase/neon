import concurrent.futures
import dataclasses
import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, PgBin
from fixtures.pageserver.makelayers import l0stack
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
    batching: str
    mode: str = "pipelined"


PS_IO_CONCURRENCY = ["sidecar-task"]
PIPELINING_CONFIGS: list[PageServicePipeliningConfig] = []
for max_batch_size in [32]:
    for execution in ["concurrent-futures"]:
        for batching in ["scattered-lsn"]:
            PIPELINING_CONFIGS.append(
                PageServicePipeliningConfigPipelined(max_batch_size, execution, batching)
            )


@pytest.mark.parametrize(
    "tablesize_mib, pipelining_config, target_runtime, ps_io_concurrency, effective_io_concurrency, readhead_buffer_size, name",
    [
        # batchable workloads should show throughput and CPU efficiency improvements
        *[
            (
                50,
                config,
                TARGET_RUNTIME,
                ps_io_concurrency,
                100,
                128,
                f"batchable {dataclasses.asdict(config)}",
            )
            for config in PIPELINING_CONFIGS
            for ps_io_concurrency in PS_IO_CONCURRENCY
        ],
    ],
)
def test_postgres_seqscan(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    tablesize_mib: int,
    pipelining_config: PageServicePipeliningConfig,
    target_runtime: int,
    ps_io_concurrency: str,
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

    We advance the LSN from a disruptor thread to simulate the effect of a workload with concurrent writes
    in another table. The `scattered-lsn` batching mode handles this well whereas the
    initial implementatin (`uniform-lsn`) would break the batch.
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
    params.update(
        {
            "config": (
                0,
                {
                    "labels": {
                        "pipelining_config": dataclasses.asdict(pipelining_config),
                        "ps_io_concurrency": ps_io_concurrency,
                    }
                },
            )
        }
    )

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
        pageserver_batch_breaks_reason_count: dict[str, int]
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
                pageserver_batch_breaks_reason_count={
                    reason: count - other.pageserver_batch_breaks_reason_count.get(reason, 0)
                    for reason, count in self.pageserver_batch_breaks_reason_count.items()
                },
            )

        def normalize(self, by) -> "Metrics":
            return Metrics(
                time=self.time / by,
                pageserver_batch_size_histo_sum=self.pageserver_batch_size_histo_sum / by,
                pageserver_batch_size_histo_count=self.pageserver_batch_size_histo_count / by,
                compute_getpage_count=self.compute_getpage_count / by,
                pageserver_cpu_seconds_total=self.pageserver_cpu_seconds_total / by,
                pageserver_batch_breaks_reason_count={
                    reason: count / by
                    for reason, count in self.pageserver_batch_breaks_reason_count.items()
                },
            )

    def get_metrics() -> Metrics:
        with conn.cursor() as cur:
            cur.execute(
                "select value from neon_perf_counters where metric='getpage_wait_seconds_count';"
            )
            compute_getpage_count = cur.fetchall()[0][0]
            pageserver_metrics = ps_http.get_metrics()
            for name, samples in pageserver_metrics.metrics.items():
                for sample in samples:
                    log.info(f"{name=} labels={sample.labels} {sample.value}")

            raw_batch_break_reason_count = pageserver_metrics.query_all(
                "pageserver_page_service_batch_break_reason_total",
                filter={"timeline_id": str(env.initial_timeline)},
            )

            batch_break_reason_count = {
                sample.labels["reason"]: int(sample.value)
                for sample in raw_batch_break_reason_count
            }

            return Metrics(
                time=time.time(),
                pageserver_batch_size_histo_sum=pageserver_metrics.query_one(
                    "pageserver_page_service_batch_size_sum"
                ).value,
                pageserver_batch_size_histo_count=pageserver_metrics.query_one(
                    "pageserver_page_service_batch_size_count"
                ).value,
                pageserver_batch_breaks_reason_count=batch_break_reason_count,
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
        {
            "page_service_pipelining": dataclasses.asdict(pipelining_config),
            "get_vectored_concurrent_io": {"mode": ps_io_concurrency},
        }
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
        if metric == "pageserver_batch_breaks_reason_count":
            assert isinstance(value, dict)
            for reason, count in value.items():
                zenbenchmark.record(
                    f"counters.{metric}_{reason}", count, unit="", report=MetricReport.TEST_PARAM
                )
        else:
            zenbenchmark.record(
                f"counters.{metric}", value, unit="", report=MetricReport.TEST_PARAM
            )

    zenbenchmark.record(
        "perfmetric.batching_factor",
        metrics.pageserver_batch_size_histo_sum / metrics.pageserver_batch_size_histo_count,
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )


@pytest.mark.parametrize(
    "pipelining_config,ps_io_concurrency,l0_stack_height,queue_depth,name",
    [
        (config, ps_io_concurrency, l0_stack_height, queue_depth, f"{dataclasses.asdict(config)}")
        for config in PIPELINING_CONFIGS
        for ps_io_concurrency in PS_IO_CONCURRENCY
        for queue_depth in [1, 2, 32]
        for l0_stack_height in [0, 20]
    ],
)
def test_random_reads(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    pipelining_config: PageServicePipeliningConfig,
    ps_io_concurrency: str,
    l0_stack_height: int,
    queue_depth: int,
    name: str,
):
    """
    Throw pagebench random getpage at latest lsn workload from a single client against pageserver.
    """

    #
    # Setup
    #

    def build_snapshot_cb(neon_env_builder: NeonEnvBuilder) -> NeonEnv:
        env = neon_env_builder.init_start()
        endpoint = env.endpoints.create_start("main")
        l0stack.make_l0_stack(
            endpoint,
            l0stack.L0StackShape(logical_table_size_mib=50, delta_stack_height=l0_stack_height),
        )
        return env

    env = neon_env_builder.build_and_use_snapshot(
        f"test_page_service_batching--test_pagebench-{l0_stack_height}", build_snapshot_cb
    )

    def patch_ps_config(ps_config):
        ps_config["page_service_pipelining"] = dataclasses.asdict(pipelining_config)
        ps_config["get_vectored_concurrent_io"] = {"mode": ps_io_concurrency}

    env.pageserver.edit_config_toml(patch_ps_config)

    env.start()

    lsn = env.safekeepers[0].get_commit_lsn(env.initial_tenant, env.initial_timeline)
    ep = env.endpoints.create_start("main", lsn=lsn)
    data_table_relnode_oid = ep.safe_psql_scalar("SELECT 'data'::regclass::oid")
    ep.stop_and_destroy()

    for sk in env.safekeepers:
        sk.stop()

    env.pageserver.allowed_errors.append(
        # https://github.com/neondatabase/neon/issues/6925
        r".*query handler for.*pagestream.*failed: unexpected message: CopyFail during COPY.*"
    )

    ps_http = env.pageserver.http_client()

    metrics_before = ps_http.get_metrics()

    cmd = [
        str(env.neon_binpath / "pagebench"),
        "get-page-latest-lsn",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--page-service-connstring",
        env.pageserver.connstr(password=None),
        "--num-clients",
        "1",
        "--queue-depth",
        str(queue_depth),
        "--only-relnode",
        str(data_table_relnode_oid),
        "--runtime",
        "10s",
    ]
    log.info(f"command: {' '.join(cmd)}")
    basepath = pg_bin.run_capture(cmd, with_command_header=False)
    results_path = Path(basepath + ".stdout")
    log.info(f"Benchmark results at: {results_path}")

    metrics_after = ps_http.get_metrics()

    with open(results_path) as f:
        results = json.load(f)
    log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")

    total = results["total"]

    metric = "request_count"
    zenbenchmark.record(
        metric,
        metric_value=total[metric],
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )

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

    reads_before = metrics_before.query_one(
        "pageserver_io_operations_seconds_count", filter={"operation": "read"}
    )
    reads_after = metrics_after.query_one(
        "pageserver_io_operations_seconds_count", filter={"operation": "read"}
    )

    zenbenchmark.record(
        "virtual_file_reads",
        metric_value=reads_after.value - reads_before.value,
        unit="",
        report=MetricReport.LOWER_IS_BETTER,
    )
