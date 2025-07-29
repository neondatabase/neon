from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.utils import query_scalar, skip_in_debug_build

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.timeout(600)
@skip_in_debug_build("only run with release build")
def test_unlogged_build(neon_env_builder: NeonEnvBuilder):
    """
    Check for race conditions between end of unlogged build and backends
    evicting pages of this index. We used to have a race condition where a
    backend completed unlogged build and removed local files, while another
    backend is evicting a page belonging to the newly-built index and tries to
    write to the file that's removed.
    """
    n_connections = 4

    # A small shared_buffers setting forces backends to evict more pages from
    # the buffer cache during the build, which is needed to expose the bug with
    # concurrent eviction.
    shared_buffers = "128MB"

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main", config_lines=[f"shared_buffers='{shared_buffers}'"]
    )

    def unlogged_build(i: int):
        con = endpoint.connect()
        cur = con.cursor()
        cur.execute("set statement_timeout=0")
        cur.execute(f"CREATE TABLE ginarray_tbl_{i} (arr text[])")
        cur.execute(
            f"INSERT INTO ginarray_tbl_{i} SELECT array_agg(repeat((x*1000000 + y)::text, 40)) FROM generate_series(1, 100) x, generate_series(1, 200 * 100) y group by y"
        )
        cur.execute("SET log_min_messages='debug1'")
        cur.execute(f"CREATE INDEX ginarray_tbl_idx_{i} ON ginarray_tbl_{i} USING gin(arr)")
        cur.execute("RESET log_min_messages")

        # Check that the index is larger than 1 GB, so that it doesn't fit into
        # a single segment file. Otherwise, the race condition is avoided,
        # because when the backend evicts and writes out the page, it already
        # has the file descriptor opened and the write succeeds even if the
        # underlying file is deleted concurrently.
        assert (
            query_scalar(cur, f"select pg_relation_size('ginarray_tbl_idx_{i}')")
            > 1024 * 1024 * 1024
        )

        # Run a simple query that uses the index as a sanity check. (In the
        # original bug, the CREATE INDEX step failed already. But since we
        # already went through all the effort to build the index, might as well
        # check that it also works.)
        qry = f"SELECT count(*) FROM ginarray_tbl_{i} WHERE ARRAY[repeat('1012345', 40)] <@ arr"
        result = query_scalar(cur, qry)
        assert result == 1

        # Verify that the query actually uses the index
        cur.execute(f"EXPLAIN (COSTS OFF) {qry}")
        rows = cur.fetchall()
        plan = "\n".join(r[0] for r in rows)
        log.debug(plan)
        assert "Bitmap Index Scan on ginarray_tbl_idx" in plan

    threads = [threading.Thread(target=unlogged_build, args=(i,)) for i in range(n_connections)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Sanity check that the indexes were buitl with the "unlogged build"
    # method. GIN always indexes use that currently, but if a different, more
    # efficient, method is invented later, that might invalidate this test.
    assert endpoint.log_contains("starting unlogged build of relation")
