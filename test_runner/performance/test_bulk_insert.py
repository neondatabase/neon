import pytest
import os
import shutil
from contextlib import closing
from fixtures.log_helper import log

from fixtures.compare_fixtures import NeonCompare, PgCompare
from fixtures.pg_version import PgVersion


#
# Run bulk INSERT test.
#
# Collects metrics:
#
# 1. Time to INSERT 5 million rows
# 2. Disk writes
# 3. Disk space used
# 4. Peak memory usage
#
def test_bulk_insert(neon_with_baseline: PgCompare):
    env = neon_with_baseline

    # Number of times to run the write query. One run creates 350MB of wal.
    n_writes = 100

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table huge (i int, j int);")

            # Run INSERT, recording the time and I/O it takes
            with env.record_pageserver_writes("pageserver_writes"):
                with env.record_duration("insert"):
                    for i in range(n_writes):
                        if n_writes > 1:
                            log.info(f"running query {i}/{n_writes}")
                        cur.execute("insert into huge values (generate_series(1, 5000000), 0);")
                    env.flush()

            env.report_peak_memory_use()
            env.report_size()

    # When testing neon, also check how long it takes the pageserver to reingest the
    # wal from safekeepers. If this number is close to total runtime, then the pageserver
    # is the bottleneck.
    if isinstance(env, NeonCompare):
        measure_recovery_time(env)


def measure_recovery_time(env: NeonCompare):
    # Hmm why is pageserver less ready to respond to http when the datadir is large?
    from urllib3.util.retry import Retry
    client = env.env.pageserver.http_client(retries=Retry(1000))
    pg_version = PgVersion(client.timeline_detail(env.tenant, env.timeline)["pg_version"])

    # Stop pageserver and remove tenant data
    env.env.pageserver.stop()
    timeline_dir = env.env.pageserver.timeline_dir(env.tenant, env.timeline)
    shutil.rmtree(timeline_dir)

    # Start pageserver
    env.env.pageserver.start()

    # Measure recovery time
    with env.record_duration("wal_recovery"):
        # Create the tenant, which will start walingest
        client.timeline_create(pg_version, env.tenant, env.timeline)

        # Flush, which will also wait for lsn to catch up
        env.flush()


# This test is meant for local iteration only. The use case is when you want to re-run
# the measure_recovery_time part of test_bulk_insert, but without running the setup.
# It allows you to iterate on results 2x faster while trying to improve wal ingestion
# performance.
@pytest.mark.skip("this is a convenience test for local dev only")
def test_recovery(neon_env_builder):
    env = neon_env_builder.init_start()
    measure_recovery_time(env)
