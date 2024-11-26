from __future__ import annotations

from contextlib import closing

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import query_scalar
from psycopg2.errors import IoError, UndefinedTable

pytest_plugins = "fixtures.neon_fixtures"

extensions = ["pageinspect", "neon_test_utils", "pg_buffercache"]


#
# Validation of reading different page versions
#
def test_read_validation(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")
    with closing(endpoint.connect()) as con:
        with con.cursor() as c:
            for e in extensions:
                c.execute(f"create extension if not exists {e};")

            c.execute("create table foo (c int) with (autovacuum_enabled = false)")
            c.execute("insert into foo values (1)")

            c.execute("select lsn, lower, upper from page_header(get_raw_page('foo', 'main', 0));")
            first = c.fetchone()
            assert first is not None

            relfilenode = query_scalar(c, "select relfilenode from pg_class where relname = 'foo'")

            c.execute("insert into foo values (2);")
            c.execute("select lsn, lower, upper from page_header(get_raw_page('foo', 'main', 0));")
            second = c.fetchone()

            assert first != second, "Failed to update page"

            log.info("Test table is populated, validating buffer cache")

            cache_entries = query_scalar(
                c, f"select count(*) from pg_buffercache where relfilenode = {relfilenode}"
            )
            assert cache_entries > 0, "No buffers cached for the test relation"

            c.execute(
                f"select reltablespace, reldatabase, relfilenode from pg_buffercache where relfilenode = {relfilenode}"
            )
            reln = c.fetchone()
            assert reln is not None

            log.info("Clear buffer cache to ensure no stale pages are brought into the cache")

            endpoint.clear_buffers(cursor=c)

            cache_entries = query_scalar(
                c, f"select count(*) from pg_buffercache where relfilenode = {relfilenode}"
            )
            assert cache_entries == 0, "Failed to clear buffer cache"

            log.info("Cache is clear, reading stale page version")

            c.execute(
                f"select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 0, '{first[0]}', NULL))"
            )
            direct_first = c.fetchone()
            assert first == direct_first, "Failed fetch page at historic lsn"

            cache_entries = query_scalar(
                c, f"select count(*) from pg_buffercache where relfilenode = {relfilenode}"
            )
            assert cache_entries == 0, "relation buffers detected after invalidation"

            log.info("Cache is clear, reading latest page version without cache")

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 0, NULL, NULL))"
            )
            direct_latest = c.fetchone()
            assert second == direct_latest, "Failed fetch page at latest lsn"

            cache_entries = query_scalar(
                c, f"select count(*) from pg_buffercache where relfilenode = {relfilenode}"
            )
            assert cache_entries == 0, "relation buffers detected after invalidation"

            log.info(
                "Cache is clear, reading stale page version without cache using relation identifiers"
            )

            c.execute(
                f"select lsn, lower, upper from page_header(get_raw_page_at_lsn({reln[0]}, {reln[1]}, {reln[2]}, 0, 0, '{first[0]}', NULL))"
            )
            direct_first = c.fetchone()
            assert first == direct_first, "Failed fetch page at historic lsn using oid"

            log.info(
                "Cache is clear, reading latest page version without cache using relation identifiers"
            )

            c.execute(
                f"select lsn, lower, upper from page_header(get_raw_page_at_lsn({reln[0]}, {reln[1]}, {reln[2]}, 0, 0, NULL, NULL))"
            )
            direct_latest = c.fetchone()
            assert second == direct_latest, "Failed fetch page at latest lsn"

            c.execute("drop table foo;")

            log.info(
                "Relation dropped, attempting reading stale page version without cache using relation identifiers"
            )

            c.execute(
                f"select lsn, lower, upper from page_header(get_raw_page_at_lsn({reln[0]}, {reln[1]}, {reln[2]}, 0, 0, '{first[0]}', NULL))"
            )
            direct_first = c.fetchone()
            assert first == direct_first, "Failed fetch page at historic lsn using oid"

            log.info("Validation page inspect won't allow reading pages of dropped relations")
            try:
                c.execute("select * from page_header(get_raw_page('foo', 'main', 0));")
                raise AssertionError("query should have failed")
            except UndefinedTable as e:
                log.info(f"Caught an expected failure: {e}")


def test_read_validation_neg(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.pageserver.allowed_errors.append(".*invalid LSN\\(0\\) in request.*")

    endpoint = env.endpoints.create_start("main")

    with closing(endpoint.connect()) as con:
        with con.cursor() as c:
            for e in extensions:
                c.execute(f"create extension if not exists {e};")

            log.info("read a page of a missing relation")
            try:
                c.execute(
                    "select lsn, lower, upper from page_header(get_raw_page_at_lsn('Unknown', 'main', 0, '0/0', NULL))"
                )
                raise AssertionError("query should have failed")
            except UndefinedTable as e:
                log.info(f"Caught an expected failure: {e}")

            c.execute("create table foo (c int) with (autovacuum_enabled = false)")
            c.execute("insert into foo values (1)")

            log.info("read a page at lsn 0")
            try:
                c.execute(
                    "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 0, '0/0', NULL))"
                )
                raise AssertionError("query should have failed")
            except IoError as e:
                log.info(f"Caught an expected failure: {e}")

            log.info("Pass NULL as an input")
            expected = (None, None, None)
            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn(NULL, 'main', 0, '0/0', NULL))"
            )
            assert c.fetchone() == expected, "Expected null output"

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', NULL, 0, '0/0', NULL))"
            )
            assert c.fetchone() == expected, "Expected null output"

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', NULL, '0/0', NULL))"
            )
            assert c.fetchone() == expected, "Expected null output"

            # This check is currently failing, reading beyond EOF is returning a 0-page
            log.info("Read beyond EOF")
            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 1, NULL, NULL))"
            )
