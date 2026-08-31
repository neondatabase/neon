from __future__ import annotations

import io
from typing import TYPE_CHECKING

from fixtures.log_helper import log
from fixtures.neon_fixtures import wait_for_last_flush_lsn
from fixtures.utils import query_scalar

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder

EXTENSIONS = ["neon_test_utils", "pageinspect"]

# One row is engineered to occupy exactly one heap block: a 26-char unit
# repeated 196 times is 5096 bytes, which is more than half of an 8 KB page, so
# only a single such tuple ever fits per block. STORAGE plain is mandatory --
# without it the (highly compressible) text is TOAST-compressed down to a few
# bytes and many rows would share a block, breaking the one-row-per-block
# invariant the test relies on for locating gap blocks.
UNIT = "abcdefghijklmnopqrstuvwxyz"
ROW_BODY = UNIT * 196  # 5096 bytes

# Small but enough for COPY's bulk-extension ramp to reach a multi-block
# smgrzeroextend whose final extension over-allocates past the last row.
NROWS = 100


def test_rel_extend_gap_blocks_read_as_zero(neon_env_builder: NeonEnvBuilder):
    """
    Regression coverage for the marker-FPI relation-extend optimization.

    When a relation is bulk-extended by N blocks at once, the compute no longer
    emits one zero-page WAL record per block; it emits a single PageInit'd
    marker FPI for the *last* block of the extension and relies on the
    pageserver to materialize the intermediate (un-WAL'd) blocks as all-zero
    pages at ingest. This test verifies that, end to end, purely through
    observable behavior (no internal metric):

      1. a multi-block extension actually happened, and the extension
         over-allocated past the data we wrote -- so some in-bounds blocks were
         never touched by a heap-insert and remain pure gap blocks; and
      2. every such gap block reads back from the pageserver as a valid
         all-zero page after the local caches are evicted -- a successful read
         (not a MissingKey / I/O error) of a zeroed page, which is exactly what
         the marker-FPI path delegates to the pageserver's gap-fill.

    How the gap is constructed deterministically:

    Each row is exactly one block wide (see ROW_BODY / STORAGE plain), so the
    data occupies precisely blocks [0, NROWS). COPY uses heap_multi_insert,
    which ramps its relation-extension chunk size up to 64 blocks
    (RelationGetBufferForTuple -> RelationAddBlocks -> ExtendBufferedRelBy ->
    smgrzeroextend), and the last chunk grabs many blocks but only fills the
    few remaining rows. Because blocks are always appended at the end and filled
    in order, every block with index >= NROWS is guaranteed never-written: those
    are the surviving gap blocks. A plain INSERT would extend one block at a
    time (bistate is NULL), never hitting the marker-FPI multi-block path, so we
    deliberately use COPY.
    """
    env = neon_env_builder.init_start()

    # Bulk relation extension is gated by LimitAdditionalPins(), which clamps the
    # number of blocks extended at once to roughly NBuffers / max_backends. With
    # the default tiny shared_buffers the clamp is 1, so the relation would only
    # ever be extended one block at a time and the marker-FPI multi-block path
    # would never fire. Give the compute enough buffers (and few enough backends)
    # that a single smgrzeroextend can cover many blocks.
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=["shared_buffers=1GB", "max_connections=20"],
    )
    with endpoint.connect() as con:
        con.autocommit = True
        with con.cursor() as c:
            for e in EXTENSIONS:
                c.execute(f"CREATE EXTENSION IF NOT EXISTS {e}")
            c.execute("CREATE TABLE t (v text) WITH (autovacuum_enabled = false)")
            # Keep every value inline and uncompressed so each row fills exactly
            # one block.
            c.execute("ALTER TABLE t ALTER COLUMN v SET STORAGE plain")
            buf = io.StringIO("".join(f"{ROW_BODY}{i}\n" for i in range(NROWS)))
            c.copy_expert("COPY t (v) FROM STDIN", buf)

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    with endpoint.connect() as con:
        con.autocommit = True
        with con.cursor() as c:
            nblocks = query_scalar(
                c, "SELECT pg_relation_size('t') / current_setting('block_size')::int"
            )
    log.info(f"relation t has {nblocks} blocks for {NROWS} one-block rows")

    # Guarantee 1: the bulk extension over-allocated past our data. With one row
    # per block, data lives in [0, NROWS); anything beyond is a never-written
    # gap block produced by the marker-FPI multi-block extension.
    assert nblocks > NROWS, (
        f"expected the bulk extension to over-allocate past {NROWS} data blocks "
        f"(got {nblocks}); the marker-FPI multi-block path did not leave a gap"
    )

    with endpoint.connect() as con:
        con.autocommit = True
        with con.cursor() as c:
            # Evict shared buffers and the LFC so every read below is served by
            # the pageserver, not by a locally cached copy.
            endpoint.clear_buffers()

            # Guarantee 2: every over-allocated tail block reads back from the
            # pageserver as an *empty* page, and at least one is an all-zero
            # gap-filled page.
            #
            # The data occupies [0, NROWS). Each tail block (>= NROWS) was never
            # touched by a heap-insert, so it must read back as one of exactly
            # two empty shapes:
            #   - (lower, upper) == (0, 0)        -> an all-zero gap block the
            #     pageserver zero-filled at ingest, OR
            #   - (lower, upper) == (24, 8192)    -> the PageInit'd marker FPI
            #     for the last block of an extension (SizeOfPageHeaderData=24,
            #     BLCKSZ=8192).
            # Anything else (a partially-filled data page, or a failed read)
            # means the marker-FPI gap-fill regressed: a lost gap-fill surfaces
            # as a psycopg2 IoError ("could not read block ...") on the read
            # itself, and a stale/garbage page would have a different header.
            zero_gap_blocks = 0
            for blk in range(NROWS, nblocks):
                c.execute(
                    "SELECT lower, upper FROM page_header("
                    f"get_raw_page_at_lsn('t', 'main', {blk}, NULL, NULL))"
                )
                row = c.fetchone()
                assert row is not None, f"tail block {blk} read back NULL"
                lower, upper = row
                assert (lower, upper) in {(0, 0), (24, 8192)}, (
                    f"tail block {blk} is neither an all-zero gap page nor the "
                    f"PageInit'd marker (lower={lower}, upper={upper})"
                )
                if (lower, upper) == (0, 0):
                    zero_gap_blocks += 1

            assert zero_gap_blocks > 0, (
                "expected at least one all-zero gap block among the over-allocated "
                "tail; the pageserver did not zero-fill any relation-extend gap"
            )

    log.info(
        f"verified {nblocks - NROWS} tail blocks ({zero_gap_blocks} zero-filled gaps) "
        "after cache eviction"
    )
