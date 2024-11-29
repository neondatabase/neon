from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin

#
# Test UNLOGGED tables/relations. Postgres copies init fork contents to main
# fork to reset them during recovery. In Neon, pageserver directly sends init
# fork contents as main fork during basebackup.
#


def test_check_visibility_map(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Runs pgbench across a few databases on a sharded tenant, then performs a visibility map
    consistency check. Regression test for https://github.com/neondatabase/neon/issues/9914.
    """

    # Use a large number of shards with small stripe sizes, to ensure the visibility
    # map will end up on non-zero shards.
    SHARD_COUNT = 8
    STRIPE_SIZE = 32  # in 8KB pages
    PGBENCH_RUNS = 4

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=SHARD_COUNT, initial_tenant_shard_stripe_size=STRIPE_SIZE
    )
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "shared_buffers = 64MB",
        ],
    )

    # Run pgbench in 4 different databases, to exercise different shards.
    dbnames = [f"pgbench{i}" for i in range(PGBENCH_RUNS)]
    for i, dbname in enumerate(dbnames):
        log.info(f"pgbench run {i+1}/{PGBENCH_RUNS}")
        endpoint.safe_psql(f"create database {dbname}")
        connstr = endpoint.connstr(dbname=dbname)
        # pgbench -i will automatically vacuum the tables. This creates the visibility map.
        pg_bin.run(["pgbench", "-i", "-s", "10", connstr])
        pg_bin.run(["pgbench", "-c", "32", "-j", "8", "-T", "10", connstr])

    # Restart the endpoint to flush the compute page cache. We want to make sure we read VM pages
    # from storage, not cache.
    endpoint.stop()
    endpoint.start()

    # Check that the visibility map matches the heap contents for pg_accounts (the main table).
    for dbname in dbnames:
        log.info(f"Checking VM visibility for {dbname}")
        with endpoint.cursor(dbname=dbname) as cur:
            cur.execute("create extension pg_visibility")
            cur.execute("select count(*) from pg_check_visible('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (visible)"

    # Freeze the tuples and check that the freeze bit is set correctly.
    for dbname in dbnames:
        log.info(f"Checking VM frozen for {dbname}")
        with endpoint.cursor(dbname=dbname) as cur:
            cur.execute("select count(*) from pg_check_frozen('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (frozen)"

            cur.execute("vacuum freeze pgbench_accounts")
            cur.execute("select count(*) from pg_check_frozen('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (frozen)"
