from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


#
# This test demonstrates effect of relkind cache. Postgres doesn't store relation persistence in shared buffer tag.
# It means that if page is evicted from shared buffers and relation is not cache in relation cache, then persistence=0 (auto) is used.
# For vanilla Postgres it is not important, because in both cases we need to write changes to the file.
# In Neon for permanent relations neon_write does nothing, while for unlogged relation - should store data in local file.
# Originally Neon uses `mdexists` call to check if local file exists and so determine if it is unlogged relation.
# mdexists is not so cheap: it closes and opens file. Relkind cache allow to eliminate this checks.
#
# This test tries to emulate situation when most of writes are with persistence=0.
# We create multiple connections to the database and in each fill it's own table. So each backends writes only it's own table and other tables
# descriptors are not cached. At the same time all backends perform eviction from shared buffers. Probability that backends evicts page of it's own
# relation is 1/N when N is number of relations=number of backends. The more relations, the smaller probability.
# For large enough number of relations most of writes are with unknown persistence.
#
# At Linux this test shows about 2x time speed improvement.
#
@pytest.mark.timeout(10000)
def test_unlogged(neon_env_builder: NeonEnvBuilder):
    n_tables = 20
    n_records = 1000
    n_updates = 1000
    shared_buffers = 1

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main", config_lines=[f"shared_buffers='{shared_buffers}MB'"]
    )

    with endpoint.connect().cursor() as cur:
        for i in range(n_tables):
            cur.execute(
                f"create unlogged table t{i}(pk integer primary key, sk integer, fillter text default repeat('x', 1000)) with (fillfactor=10)"
            )
            cur.execute(f"insert into t{i} values (generate_series(1,{n_records}),0)")

    def do_updates(table_id: int):
        with endpoint.connect().cursor() as cur:
            for _ in range(n_updates):
                cur.execute(f"update t{table_id} set sk=sk+1")

    threads = [threading.Thread(target=do_updates, args=(i,)) for i in range(n_tables)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
