from contextlib import closing
import psycopg2.extras
import time;

pytest_plugins = ("fixtures.zenith_fixtures")

def print_gc_result(row):
    print("GC duration {elapsed} ms, total: {snapshot_files_total}, needed_by_cutoff {snapshot_files_needed_by_cutoff}, needed_by_branches: {snapshot_files_needed_by_branches}, not_updated: {snapshot_files_not_updated}, removed: {snapshot_files_removed}".format_map(row))


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
                    snapshot_files_total = row['snapshot_files_total']
                    assert snapshot_files_total > 0

                    # Insert a row. The first insert will also create a metadata entry for the
                    # relation, with size == 1 block. Hence, bump up the expected relation count.
                    snapshot_files_total += 1;
                    print("Inserting one row and running GC")
                    cur.execute("INSERT INTO foo VALUES (1)")
                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_files_total'] == snapshot_files_total
                    assert row['snapshot_files_removed'] == 0

                    # Insert two more rows and run GC.
                    # This should create a new snapshot file with the new contents, and
                    # remove the old one.
                    print("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_files_total'] == snapshot_files_total + 1
                    assert row['snapshot_files_removed'] == 1

                    # Do it again. Should again create a new snapshot file and remove old one.
                    print("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_files_total'] == snapshot_files_total + 1
                    assert row['snapshot_files_removed'] == 1

                    # Run GC again, with no changes in the database. Should not remove anything.
                    print("Run GC again, with nothing to do")
                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);
                    assert row['snapshot_files_total'] == snapshot_files_total
                    assert row['snapshot_files_removed'] == 0

                    #
                    # Test DROP TABLE checks that relation data and metadata was deleted by GC from object storage
                    #
                    print("Drop table and run GC again");
                    cur.execute("DROP TABLE foo")

                    pscur.execute(f"do_gc {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row);

                    # Each relation fork is counted separately, hence 3. But the catalog
                    # updates also create new snapshot files of the catalogs.

                    # TODO: perhaps we should count catalog and user relations separately,
                    # to make this kind of testing more robust

                    # FIXME: Unlinking relations hasn't been implemented yet
                    #assert row['snapshot_files_removed'] == 3
                    #assert row['snapshot_files_removed'] == 5
