from __future__ import annotations

from contextlib import closing

from fixtures.benchmark_fixture import MetricReport
from fixtures.common_types import Lsn
from fixtures.compare_fixtures import NeonCompare, PgCompare
from fixtures.log_helper import log
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

    start_lsn = Lsn(env.pg.safe_psql("SELECT pg_current_wal_lsn()")[0][0])

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table huge (i int, j int);")

            # Run INSERT, recording the time and I/O it takes
            with env.record_pageserver_writes("pageserver_writes"):
                with env.record_duration("insert"):
                    cur.execute("insert into huge values (generate_series(1, 20000000), 0);")
                    env.flush(compact=False, gc=False)

            env.report_peak_memory_use()
            env.report_size()

    # Report amount of wal written. Useful for comparing vanilla wal format vs
    # neon wal format, measuring neon write amplification, etc.
    end_lsn = Lsn(env.pg.safe_psql("SELECT pg_current_wal_lsn()")[0][0])
    wal_written_bytes = end_lsn - start_lsn
    wal_written_mb = round(wal_written_bytes / (1024 * 1024))
    env.zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)

    # When testing neon, also check how long it takes the pageserver to reingest the
    # wal from safekeepers. If this number is close to total runtime, then the pageserver
    # is the bottleneck.
    if isinstance(env, NeonCompare):
        measure_recovery_time(env)

    with env.record_duration("compaction"):
        env.compact()


def measure_recovery_time(env: NeonCompare):
    client = env.env.pageserver.http_client()
    pg_version = PgVersion(str(client.timeline_detail(env.tenant, env.timeline)["pg_version"]))

    # Delete the Tenant in the pageserver: this will drop local and remote layers, such that
    # when we "create" the Tenant again, we will replay the WAL from the beginning.
    #
    # This is a "weird" thing to do, and can confuse the storage controller as we're re-using
    # the same tenant ID for a tenant that is logically different from the pageserver's point
    # of view, but the same as far as the safekeeper/WAL is concerned.  To work around that,
    # we will explicitly create the tenant in the same generation that it was previously
    # attached in.
    attach_status = env.env.storage_controller.inspect(tenant_shard_id=env.tenant)
    assert attach_status is not None
    (attach_gen, _) = attach_status

    client.tenant_delete(env.tenant)
    env.env.pageserver.tenant_create(tenant_id=env.tenant, generation=attach_gen)

    # Measure recovery time
    with env.record_duration("wal_recovery"):
        log.info("Entering recovery...")
        client.timeline_create(pg_version, env.tenant, env.timeline)

        # Flush, which will also wait for lsn to catch up
        env.flush(compact=False, gc=False)
        log.info("Finished recovery.")
