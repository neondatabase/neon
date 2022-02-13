from io import BytesIO
import asyncio
from fixtures.zenith_fixtures import ZenithEnv, Postgres, PgProtocol
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare


async def repeat_bytes(buf, repetitions: int):
    for i in range(repetitions):
        yield buf


async def copy_test_data_to_table(pg: PgProtocol, worker_id: int, table_name: str):
    buf = BytesIO()
    for i in range(1000):
        buf.write(
            f"{i}\tLoaded by worker {worker_id}. Long string to consume some space.\n".encode())
    buf.seek(0)

    copy_input = repeat_bytes(buf.read(), 5000)

    pg_conn = await pg.connect_async()
    await pg_conn.copy_to_table(table_name, source=copy_input)


async def parallel_load_different_tables(pg: PgProtocol, n_parallel: int):
    workers = []
    for worker_id in range(n_parallel):
        worker = copy_test_data_to_table(pg, worker_id, f'copytest_{worker_id}')
        workers.append(asyncio.create_task(worker))

    # await all workers
    await asyncio.gather(*workers)


# Load 5 different tables in parallel with COPY TO
def test_parallel_copy_different_tables(zenith_with_baseline: PgCompare, n_parallel=5):

    env = zenith_with_baseline
    conn = env.pg.connect()
    cur = conn.cursor()

    for worker_id in range(n_parallel):
        cur.execute(f'CREATE TABLE copytest_{worker_id} (i int, t text)')

    with env.record_pageserver_writes('pageserver_writes'):
        with env.record_duration('load'):
            asyncio.run(parallel_load_different_tables(env.pg, n_parallel))
            env.flush()

    env.report_peak_memory_use()
    env.report_size()


async def parallel_load_same_table(pg: PgProtocol, n_parallel: int):
    workers = []
    for worker_id in range(n_parallel):
        worker = copy_test_data_to_table(pg, worker_id, f'copytest')
        workers.append(asyncio.create_task(worker))

    # await all workers
    await asyncio.gather(*workers)


# Load data into one table with COPY TO from 5 parallel connections
def test_parallel_copy_same_table(zenith_with_baseline: PgCompare, n_parallel=5):
    env = zenith_with_baseline
    conn = env.pg.connect()
    cur = conn.cursor()

    cur.execute(f'CREATE TABLE copytest (i int, t text)')

    with env.record_pageserver_writes('pageserver_writes'):
        with env.record_duration('load'):
            asyncio.run(parallel_load_same_table(env.pg, n_parallel))
            env.flush()

    env.report_peak_memory_use()
    env.report_size()
