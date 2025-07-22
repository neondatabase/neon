from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


# Checks that the is no race between end of unlogged build and backends evicting pages of this index.
# We need to create quite large  index (more than one gigabyte segment) to reproduce write error caused by this race condition
# (backend completed unlogged build removes local files while backend evicting page tries to write to the file).
# If index size is smaller than segment size, the problem is avoided by file descriptor cache which prevents file deletion.
def test_unlogged_build(neon_env_builder: NeonEnvBuilder):
    n_connections = 4
    shared_buffers = 1024
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main", config_lines=[f"shared_buffers='{shared_buffers}MB'"]
    )

    def unlogged_build(i: int):
        con = endpoint.connect()
        cur = con.cursor()
        cur.execute("set statement_timeout=0")
        cur.execute(f"CREATE TABLE quad_box_tbl_{i} (id int, b box)")
        cur.execute(
            f"INSERT INTO quad_box_tbl_{i} SELECT (x - 1) * 100 + y, box(point(x * 10, y * 10), point(x * 10 + 5, y * 10 + 5)) FROM generate_series(1, 100) x, generate_series(1, 1200 * 100) y"
        )
        cur.execute(f"CREATE INDEX quad_box_tbl_idx_{i} ON quad_box_tbl_{i} USING spgist(b)")
        cur.execute(
            f"EXPLAIN (COSTS OFF) SELECT rank() OVER (ORDER BY b <-> point '123,456') n, b <-> point '123,456' dist, id FROM quad_box_tbl_{i}"
        )

    threads = [threading.Thread(target=unlogged_build, args=(i,)) for i in range(n_connections)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
