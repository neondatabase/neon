from __future__ import annotations

import threading
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from pytest_lazyfixture import lazy_fixture

if TYPE_CHECKING:
    from fixtures.compare_fixtures import PgCompare


#
# This test demonstrates effect of relpersistence cache. Postgres doesn't store relation persistence in shared buffer tag.
# It means that if page is evicted from shared buffers and relation is not in the backend's relation cache, then persistence=0 (auto) is used.
# For vanilla Postgres it is not important, because in both cases we need to write changes to the file.
# In Neon, neon_write does nothing nothing for a permanent relation, while for an unlogged relation, it writes the page to the local file.
# Originally Neon always called `mdexists` to check if the local file exists and determine if it's an unlogged relation. Now we check the cache first.
# mdexists is not so cheap: it closes and opens the file.
#
# This test tries to recreate the situation that most of writes are with persistence=0.
# We open multiple connections to the database and in each fill its own table. So each backends writes only its own table and other table's
# descriptors are not cached. At the same time all backends perform eviction from shared buffers. Probability that backends evicts page of its own
# relation is 1/N when N is number of relations=number of backends. The more relations, the smaller probability.
# For large enough number of relations most of writes are with unknown persistence.
#
# On Linux, introducing the relpersistence cache shows about 2x time speed improvement in this test.
#
@pytest.mark.timeout(1000)
@pytest.mark.parametrize(
    "env",
    [
        pytest.param(lazy_fixture("neon_compare"), id="neon"),
        pytest.param(lazy_fixture("vanilla_compare"), id="vanilla"),
    ],
)
def test_unlogged(env: PgCompare):
    n_tables = 20
    n_records = 1000
    n_updates = 1000

    with env.record_duration("insert"):
        with closing(env.pg.connect()) as conn:
            with conn.cursor() as cur:
                for i in range(n_tables):
                    cur.execute(
                        f"create unlogged table t{i}(pk integer primary key, sk integer, fillter text default repeat('x', 1000)) with (fillfactor=10)"
                    )
                    cur.execute(f"insert into t{i} values (generate_series(1,{n_records}),0)")

    def do_updates(table_id: int):
        with closing(env.pg.connect()) as conn:
            with conn.cursor() as cur:
                for _ in range(n_updates):
                    cur.execute(f"update t{table_id} set sk=sk+1")

    with env.record_duration("update"):
        threads = [threading.Thread(target=do_updates, args=(i,)) for i in range(n_tables)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
