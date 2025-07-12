from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


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
