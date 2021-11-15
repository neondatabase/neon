from io import BytesIO
import asyncio
import asyncpg
from fixtures.zenith_fixtures import ZenithEnv, Postgres
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


async def repeat_bytes(buf, repetitions: int):
    for i in range(repetitions):
        yield buf


async def copy_test_data_to_table(pg: Postgres, worker_id: int, table_name: str):
    buf = BytesIO()
    for i in range(1000):
        buf.write(
            f"{i}\tLoaded by worker {worker_id}. Long string to consume some space.\n".encode())
    buf.seek(0)

    copy_input = repeat_bytes(buf.read(), 5000)

    pg_conn = await pg.connect_async()
    await pg_conn.copy_to_table(table_name, source=copy_input)


async def parallel_load_different_tables(pg: Postgres, n_parallel: int):
    workers = []
    for worker_id in range(n_parallel):
        worker = copy_test_data_to_table(pg, worker_id, f'copytest_{worker_id}')
        workers.append(asyncio.create_task(worker))

    # await all workers
    await asyncio.gather(*workers)


# Load 5 different tables in parallel with COPY TO
def test_parallel_copy_different_tables(zenith_simple_env: ZenithEnv,
                                        zenbenchmark: ZenithBenchmarker,
                                        n_parallel=5):

    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_parallel_copy_different_tables", "empty"])

    pg = env.postgres.create_start('test_parallel_copy_different_tables')
    log.info("postgres is running on 'test_parallel_copy_different_tables' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    conn = pg.connect()
    cur = conn.cursor()
    cur.execute("SHOW zenith.zenith_timeline")
    timeline = cur.fetchone()[0]

    for worker_id in range(n_parallel):
        cur.execute(f'CREATE TABLE copytest_{worker_id} (i int, t text)')

    with zenbenchmark.record_pageserver_writes(env.pageserver, 'pageserver_writes'):
        with zenbenchmark.record_duration('load'):
            asyncio.run(parallel_load_different_tables(pg, n_parallel))

            # Flush the layers from memory to disk. This is included in the reported
            # time and I/O
            pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")

    # Record peak memory usage
    zenbenchmark.record("peak_mem",
                        zenbenchmark.get_peak_mem(env.pageserver) / 1024,
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)

    # Report disk space used by the repository
    timeline_size = zenbenchmark.get_timeline_size(env.repo_dir, env.initial_tenant, timeline)
    zenbenchmark.record('size',
                        timeline_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)


async def parallel_load_same_table(pg: Postgres, n_parallel: int):
    workers = []
    for worker_id in range(n_parallel):
        worker = copy_test_data_to_table(pg, worker_id, f'copytest')
        workers.append(asyncio.create_task(worker))

    # await all workers
    await asyncio.gather(*workers)


# Load data into one table with COPY TO from 5 parallel connections
def test_parallel_copy_same_table(zenith_simple_env: ZenithEnv,
                                  zenbenchmark: ZenithBenchmarker,
                                  n_parallel=5):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_parallel_copy_same_table", "empty"])

    pg = env.postgres.create_start('test_parallel_copy_same_table')
    log.info("postgres is running on 'test_parallel_copy_same_table' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    conn = pg.connect()
    cur = conn.cursor()
    cur.execute("SHOW zenith.zenith_timeline")
    timeline = cur.fetchone()[0]

    cur.execute(f'CREATE TABLE copytest (i int, t text)')

    with zenbenchmark.record_pageserver_writes(env.pageserver, 'pageserver_writes'):
        with zenbenchmark.record_duration('load'):
            asyncio.run(parallel_load_same_table(pg, n_parallel))

            # Flush the layers from memory to disk. This is included in the reported
            # time and I/O
            pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")

    # Record peak memory usage
    zenbenchmark.record("peak_mem",
                        zenbenchmark.get_peak_mem(env.pageserver) / 1024,
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)

    # Report disk space used by the repository
    timeline_size = zenbenchmark.get_timeline_size(env.repo_dir, env.initial_tenant, timeline)
    zenbenchmark.record('size',
                        timeline_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)
