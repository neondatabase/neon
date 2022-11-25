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
    env.neon_cli.create_branch("test_read_validation", "empty")

    pg = env.postgres.create_start("test_read_validation")
    log.info("postgres is running on 'test_read_validation' branch")

    with closing(pg.connect()) as con:
        with con.cursor() as c:

            for e in extensions:
                c.execute("create extension if not exists {};".format(e))

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
                c, "select count(*) from pg_buffercache where relfilenode =  {}".format(relfilenode)
            )
            assert cache_entries > 0, "No buffers cached for the test relation"

            c.execute(
                "select reltablespace, reldatabase, relfilenode from pg_buffercache where relfilenode = {}".format(
                    relfilenode
                )
            )
            reln = c.fetchone()
            assert reln is not None

            log.info("Clear buffer cache to ensure no stale pages are brought into the cache")

            c.execute("select clear_buffer_cache()")

            cache_entries = query_scalar(
                c, "select count(*) from pg_buffercache where relfilenode =  {}".format(relfilenode)
            )
            assert cache_entries == 0, "Failed to clear buffer cache"

            log.info("Cache is clear, reading stale page version")

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 0, '{}'))".format(
                    first[0]
                )
            )
            direct_first = c.fetchone()
            assert first == direct_first, "Failed fetch page at historic lsn"

            cache_entries = query_scalar(
                c, "select count(*) from pg_buffercache where relfilenode =  {}".format(relfilenode)
            )
            assert cache_entries == 0, "relation buffers detected after invalidation"

            log.info("Cache is clear, reading latest page version without cache")

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 0, NULL))"
            )
            direct_latest = c.fetchone()
            assert second == direct_latest, "Failed fetch page at latest lsn"

            cache_entries = query_scalar(
                c, "select count(*) from pg_buffercache where relfilenode =  {}".format(relfilenode)
            )
            assert cache_entries == 0, "relation buffers detected after invalidation"

            log.info(
                "Cache is clear, reading stale page version without cache using relation identifiers"
            )

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn( {}, {}, {}, 0, 0, '{}' ))".format(
                    reln[0], reln[1], reln[2], first[0]
                )
            )
            direct_first = c.fetchone()
            assert first == direct_first, "Failed fetch page at historic lsn using oid"

            log.info(
                "Cache is clear, reading latest page version without cache using relation identifiers"
            )

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn( {}, {}, {}, 0, 0, NULL ))".format(
                    reln[0], reln[1], reln[2]
                )
            )
            direct_latest = c.fetchone()
            assert second == direct_latest, "Failed fetch page at latest lsn"

            c.execute("drop table foo;")

            log.info(
                "Relation dropped, attempting reading stale page version without cache using relation identifiers"
            )

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn( {}, {}, {}, 0, 0, '{}' ))".format(
                    reln[0], reln[1], reln[2], first[0]
                )
            )
            direct_first = c.fetchone()
            assert first == direct_first, "Failed fetch page at historic lsn using oid"

            log.info("Validation page inspect won't allow reading pages of dropped relations")
            try:
                c.execute("select * from page_header(get_raw_page('foo', 'main', 0));")
                assert False, "query should have failed"
            except UndefinedTable as e:
                log.info("Caught an expected failure: {}".format(e))


def test_read_validation_neg(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_read_validation_neg", "empty")

    env.pageserver.allowed_errors.append(".*invalid LSN\\(0\\) in request.*")

    pg = env.postgres.create_start("test_read_validation_neg")
    log.info("postgres is running on 'test_read_validation_neg' branch")

    with closing(pg.connect()) as con:
        with con.cursor() as c:

            for e in extensions:
                c.execute("create extension if not exists {};".format(e))

            log.info("read a page of a missing relation")
            try:
                c.execute(
                    "select lsn, lower, upper from page_header(get_raw_page_at_lsn('Unknown', 'main', 0, '0/0'))"
                )
                assert False, "query should have failed"
            except UndefinedTable as e:
                log.info("Caught an expected failure: {}".format(e))

            c.execute("create table foo (c int) with (autovacuum_enabled = false)")
            c.execute("insert into foo values (1)")

            log.info("read a page at lsn 0")
            try:
                c.execute(
                    "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 0, '0/0'))"
                )
                assert False, "query should have failed"
            except IoError as e:
                log.info("Caught an expected failure: {}".format(e))

            log.info("Pass NULL as an input")
            expected = (None, None, None)
            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn(NULL, 'main', 0, '0/0'))"
            )
            assert c.fetchone() == expected, "Expected null output"

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', NULL, 0, '0/0'))"
            )
            assert c.fetchone() == expected, "Expected null output"

            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', NULL, '0/0'))"
            )
            assert c.fetchone() == expected, "Expected null output"

            # This check is currently failing, reading beyond EOF is returning a 0-page
            log.info("Read beyond EOF")
            c.execute(
                "select lsn, lower, upper from page_header(get_raw_page_at_lsn('foo', 'main', 1, NULL))"
            )
