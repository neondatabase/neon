from contextlib import closing

import pytest
from fixtures.compare_fixtures import PgCompare
from pytest_lazyfixture import lazy_fixture  # type: ignore


@pytest.mark.parametrize(
    "env",
    [
        # The test is too slow to run in CI, but fast enough to run with remote tests
        pytest.param(lazy_fixture("neon_compare"), id="neon", marks=pytest.mark.slow),
        pytest.param(lazy_fixture("vanilla_compare"), id="vanilla", marks=pytest.mark.slow),
        pytest.param(lazy_fixture("remote_compare"), id="remote", marks=pytest.mark.remote_cluster),
    ],
)
def test_index_vacuum_perf(env: PgCompare):
    # Update the same page many times, then measure read performance
    # At 24 bytes/index record this is ~ 192MB of data; which should not fit in
    # shared buffers, especially if we just scanned the table.
    # The table itself will be at least 256MB large (= ntups * 24B tuple header + ntups * 1maxalign data)
    table_size = 8 * 1024 * 1024

    with closing(env.pg.connect()) as conn:
        run_test(env, conn, table_size, "btree",  "bigint",    "seed")
        run_test(env, conn, table_size, "brin",   "bigint",    "seed")
        run_test(env, conn, table_size, "spgist", "text",      "md5(seed::text)::text")
        run_test(env, conn, table_size, "hash",   "text",      "md5(seed::text)::text")
        run_test(env, conn, table_size, "gin",    "bigint[]",  "ARRAY[seed, random() * 1000::bigint]::bigint[]")
        run_test(env, conn, table_size, "gist",   "int8range", "int8range(seed, seed + (random() * 1000)::bigint, '[]')")


def run_test(env: PgCompare, conn, table_size: int, itype: str, coltype: str, gencol: str):
    with conn.cursor() as cur:
        # just in case we didn't finish last run
        cur.execute("DROP TABLE IF EXISTS index_testdata")
        # We want/need full control over vacuum.
        cur.execute(f"CREATE TABLE index_testdata (icol {coltype}) WITH (autovacuum_enabled = off)")
        cur.execute(f"INSERT INTO index_testdata " +
                    f"(SELECT ({gencol}) as icol " +
                    f"from generate_series(1, {table_size}) gen(seed))")
        
        cur.execute("SELECT pg_prewarm('index_testdata')")

        cur.execute(f"CREATE INDEX index_testdata_idx_{itype} "
                    f"   ON index_testdata "
                    f"       USING {itype} (icol)")

        # generate a lot of dead tuples
        cur.execute("WITH max_ctid AS (SELECT MAX(ctid) FROM index_testdata) "
                    "DELETE FROM index_testdata it WHERE it.ctid < max_ctid")

        # VACUUM
        with env.record_duration(f"vacuum-{itype}"):
            cur.execute("VACUUM (FREEZE, INDEX_CLEANUP on, PARALLEL 0) index_testdata")
        cur.execute("DROP TABLE index_testdata")
