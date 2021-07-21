from contextlib import closing
import psycopg2.extras
import time;

pytest_plugins = ("fixtures.zenith_fixtures")

def print_gc_result(row):
    print("GC duration {elapsed} ms".format_map(row));
    print("  REL    total: {snapshot_relfiles_total}, needed_by_cutoff {snapshot_relfiles_needed_by_cutoff}, needed_by_branches: {snapshot_relfiles_needed_by_branches}, not_updated: {snapshot_relfiles_not_updated}, removed: {snapshot_relfiles_removed}, dropped: {snapshot_relfiles_dropped}".format_map(row))
    print("  NONREL total: {snapshot_nonrelfiles_total}, needed_by_cutoff {snapshot_nonrelfiles_needed_by_cutoff}, needed_by_branches: {snapshot_nonrelfiles_needed_by_branches}, not_updated: {snapshot_nonrelfiles_not_updated}, removed: {snapshot_nonrelfiles_removed}, dropped: {snapshot_nonrelfiles_dropped}".format_map(row))


#
# Test Garbage Collection of old snapshot files
#
# This test is pretty tightly coupled with the current implementation of layered
# storage, in layered_repository.rs.
#
def test_snapfiles_gc(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_snapfiles_gc", "empty"])
    pg = postgres.create_start('test_snapfiles_gc')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            with closing(pageserver.connect()) as psconn:
                with psconn.cursor(cursor_factory = psycopg2.extras.DictCursor) as pscur:

                    # Get the timeline ID of our branch. We need it for the 'do_gc' command
                    cur.execute("SHOW zenith.zenith_timeline")
                    timeline = cur.fetchone()[0]

                    # Create a test table
                    cur.execute("CREATE TABLE foo(x integer)")

                    # Run GC, to clear out any garbage left behind in the catalogs by
                    # the CREATE TABLE command. We want to have a clean slate with no garbage
                    # before running the actual tests below, otherwise the counts won't match
                    # what we expect.
                    #
                    # Also run vacuum first to make it less likely that autovacuum or pruning
                    # kicks in and confuses our numbers.
                    cur.execute("VACUUM")

                    print("Running GC before test")
                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    # remember the number of files
                    snapshot_relfiles_remain = row['snapshot_relfiles_total'] - row['snapshot_relfiles_removed']
                    assert snapshot_relfiles_remain > 0

                    # Insert a row. The first insert will also create a metadata entry for the
                    # relation, with size == 1 block. Hence, bump up the expected relation count.
                    snapshot_relfiles_remain += 1;
                    print("Inserting one row and running GC")
                    cur.execute("INSERT INTO foo VALUES (1)")
                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_relfiles_total'] == snapshot_relfiles_remain
                    assert row['snapshot_relfiles_removed'] == 0
                    assert row['snapshot_relfiles_dropped'] == 0

                    # Insert two more rows and run GC.
                    # This should create a new snapshot file with the new contents, and
                    # remove the old one.
                    print("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_relfiles_total'] == snapshot_relfiles_remain + 1
                    assert row['snapshot_relfiles_removed'] == 1
                    assert row['snapshot_relfiles_dropped'] == 0

                    # Do it again. Should again create a new snapshot file and remove old one.
                    print("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_relfiles_total'] == snapshot_relfiles_remain + 1
                    assert row['snapshot_relfiles_removed'] == 1
                    assert row['snapshot_relfiles_dropped'] == 0

                    # Run GC again, with no changes in the database. Should not remove anything.
                    print("Run GC again, with nothing to do")
                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_relfiles_total'] == snapshot_relfiles_remain
                    assert row['snapshot_relfiles_removed'] == 0
                    assert row['snapshot_relfiles_dropped'] == 0

                    #
                    # Test DROP TABLE checks that relation data and metadata was deleted by GC from object storage
                    #
                    print("Drop table and run GC again");
                    cur.execute("DROP TABLE foo")

                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);

                    # Each relation fork is counted separately, hence 3.
                    assert row['snapshot_relfiles_dropped'] == 3

                    # The catalog updates also create new snapshot files of the catalogs, which
                    # are counted as 'removed'
                    assert row['snapshot_relfiles_removed'] > 0

                    # TODO: perhaps we should count catalog and user relations separately,
                    # to make this kind of testing more robust
