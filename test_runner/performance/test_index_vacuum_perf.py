from contextlib import closing
from typing import Tuple

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
@pytest.mark.parametrize(
    "index_method",
    [
        # index method, column type, column definition // parameter "seed" is out of generate_series(1, N)
        pytest.param(["btree",  "bigint",    "seed"], id="btree"),
        pytest.param(["brin",   "bigint",    "seed"], id="brin"),
        pytest.param(["spgist", "text",      "md5(seed::text)::text"], id="spgist"),
        pytest.param(["hash",   "text",      "md5(seed::text)::text"], id="hash"),
        pytest.param(["gin",    "bigint[]",  "ARRAY[seed, random() * 1000::bigint]::bigint[]"], id="gin"),
        pytest.param(["gist",   "int8range", "int8range(seed, seed + (random() * 1000)::bigint, '[]')"], id="gist"),
    ]
)
def test_index_vacuum_perf(env: PgCompare, index_method: Tuple[str, str, str]):
    # Update the same page many times, then measure read performance
    # At 24 bytes/index record this is ~ 192MB of data; which should not fit in
    # shared buffers, especially if we just scanned the table.
    # The table itself will be at least 256MB large (= ntups * 24B tuple header + ntups * 1maxalign data)
    table_size = 8 * 1024 * 1024
    [index_method, coltype, coldef] = index_method

    table = f"index_testdata_{index_method}"
    index = f"idx__index_testdata_{index_method}__icol"

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            # just in case we didn't finish last run
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            # We want/need full control over vacuum.
            cur.execute(f"CREATE TABLE {table} (icol {coltype}) WITH (autovacuum_enabled = off)")
            cur.execute(f"INSERT INTO {table} " +
                        f"(SELECT ({coldef}) as icol " +
                        f"from generate_series(1, {table_size}) gen(seed))")

            cur.execute("SELECT pg_prewarm('index_testdata')")

            cur.execute(f"CREATE INDEX {index} "
                        f"   ON {table} "
                        f"       USING {index_method} (icol)")

            # generate a lot of dead tuples
            cur.execute(f"WITH max_ctid AS (SELECT MAX(ctid) FROM {table}) "
                        f"DELETE FROM {table} it WHERE it.ctid < max_ctid")

            # VACUUM
            with env.record_duration(f"vacuum"):
                cur.execute(f"VACUUM (FREEZE, INDEX_CLEANUP on, PARALLEL 0) {table}")
            cur.execute(f"DROP TABLE {table}")
