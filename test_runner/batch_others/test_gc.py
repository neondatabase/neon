import pytest

from contextlib import closing
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver
import psycopg2.extras

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test Garbage Collection of old page versions.
#
# This test is pretty tightly coupled with the current implementation of page version storage
# and garbage collection in object_repository.rs.
#
@pytest.mark.skip(reason=""""
    Current GC test is flaky and overly strict. Since we are migrating to the layered repo format
    with different GC implementation let's just silence this test for now. This test only
    works with the RocksDB implementation.
""")
def test_gc(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, pg_bin):
    zenith_cli.run(["branch", "test_gc", "empty"])
    pg = postgres.create_start('test_gc')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            with closing(pageserver.connect()) as psconn:
                with psconn.cursor(cursor_factory = psycopg2.extras.DictCursor) as pscur:

                    # Get the timeline ID of our branch. We need it for the 'do_gc' command
                    cur.execute("SHOW zenith.zenith_timeline")
                    timeline = cur.fetchone()[0]

                    # Create a test table
                    cur.execute("CREATE TABLE foo(x integer)")

                    # Run GC, to clear out any old page versions left behind in the catalogs by
                    # the CREATE TABLE command. We want to have a clean slate with no garbage
                    # before running the actual tests below, otherwise the counts won't match
                    # what we expect.
                    print("Running GC before test")
                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print("GC duration {elapsed} ms, relations: {n_relations}, dropped {dropped}, truncated: {truncated}, deleted: {deleted}".format_map(row))
                    # remember the number of relations
                    n_relations = row['n_relations']
                    assert n_relations > 0

                    # Insert a row. The first insert will also create a metadata entry for the
                    # relation, with size == 1 block. Hence, bump up the expected relation count.
                    n_relations += 1
                    print("Inserting one row and running GC")
                    cur.execute("INSERT INTO foo VALUES (1)")
                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print("GC duration {elapsed} ms, relations: {n_relations}, dropped {dropped}, truncated: {truncated}, deleted: {deleted}".format_map(row))
                    assert row['n_relations'] == n_relations
                    assert row['dropped'] == 0
                    assert row['truncated'] == 31
                    assert row['deleted'] == 4

                    # Insert two more rows and run GC.
                    print("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print("GC duration {elapsed} ms, relations: {n_relations}, dropped {dropped}, truncated: {truncated}, deleted: {deleted}".format_map(row))
                    assert row['n_relations'] == n_relations
                    assert row['dropped'] == 0
                    assert row['truncated'] == 31
                    assert row['deleted'] == 4

                    # Insert one more row. It creates one more page version, but doesn't affect the
                    # relation size.
                    print("Inserting one more row")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print("GC duration {elapsed} ms, relations: {n_relations}, dropped {dropped}, truncated: {truncated}, deleted: {deleted}".format_map(row))
                    assert row['n_relations'] == n_relations
                    assert row['dropped'] == 0
                    assert row['truncated'] == 31
                    assert row['deleted'] == 2

                    # Run GC again, with no changes in the database. Should not remove anything.
                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print("GC duration {elapsed} ms, relations: {n_relations}, dropped {dropped}, truncated: {truncated}, deleted: {deleted}".format_map(row))
                    assert row['n_relations'] == n_relations
                    assert row['dropped'] == 0
                    assert row['truncated'] == 31
                    assert row['deleted'] == 0

                    #
                    # Test DROP TABLE checks that relation data and metadata was deleted by GC from object storage
                    #
                    cur.execute("DROP TABLE foo")

                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print("GC duration {elapsed} ms, relations: {n_relations}, dropped {dropped}, truncated: {truncated}, deleted: {deleted}".format_map(row))
                    # Each relation fork is counted separately, hence 3.
                    assert row['dropped'] == 3
