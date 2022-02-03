from contextlib import closing
import psycopg2.extras
import time
from fixtures.utils import print_gc_result
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test Garbage Collection of old layer files
#
# This test is pretty tightly coupled with the current implementation of layered
# storage, in layered_repository.rs.
#
def test_layerfiles_gc(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli(["branch", "test_layerfiles_gc", "empty"])
    pg = env.postgres.create_start('test_layerfiles_gc')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            with closing(env.pageserver.connect()) as psconn:
                with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:

                    # Get the timeline ID of our branch. We need it for the 'do_gc' command
                    cur.execute("SHOW zenith.zenith_timeline")
                    timeline = cur.fetchone()[0]

                    # Create a test table
                    cur.execute("CREATE TABLE foo(x integer)")
                    cur.execute("INSERT INTO foo VALUES (1)")

                    cur.execute("select relfilenode from pg_class where oid = 'foo'::regclass")
                    row = cur.fetchone()
                    log.info(f"relfilenode is {row[0]}")

                    # Run GC, to clear out any garbage left behind in the catalogs by
                    # the CREATE TABLE command. We want to have a clean slate with no garbage
                    # before running the actual tests below, otherwise the counts won't match
                    # what we expect.
                    #
                    # Also run vacuum first to make it less likely that autovacuum or pruning
                    # kicks in and confuses our numbers.
                    cur.execute("VACUUM")

                    # delete the row, to update the Visibility Map. We don't want the VM
                    # update to confuse our numbers either.
                    cur.execute("DELETE FROM foo")

                    log.info("Running GC before test")
                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row)
                    # remember the number of files
                    layer_relfiles_remain = (row['layer_relfiles_total'] -
                                             row['layer_relfiles_removed'])
                    assert layer_relfiles_remain > 0

                    # Insert a row and run GC. Checkpoint should freeze the layer
                    # so that there is only the most recent image layer left for the rel,
                    # removing the old image and delta layer.
                    log.info("Inserting one row and running GC")
                    cur.execute("INSERT INTO foo VALUES (1)")
                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row)
                    assert row['layer_relfiles_total'] == layer_relfiles_remain + 2
                    assert row['layer_relfiles_removed'] == 2
                    assert row['layer_relfiles_dropped'] == 0

                    # Insert two more rows and run GC.
                    # This should create new image and delta layer file with the new contents, and
                    # then remove the old one image and the just-created delta layer.
                    log.info("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row)
                    assert row['layer_relfiles_total'] == layer_relfiles_remain + 2
                    assert row['layer_relfiles_removed'] == 2
                    assert row['layer_relfiles_dropped'] == 0

                    # Do it again. Should again create two new layer files and remove old ones.
                    log.info("Inserting two more rows and running GC")
                    cur.execute("INSERT INTO foo VALUES (2)")
                    cur.execute("INSERT INTO foo VALUES (3)")

                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row)
                    assert row['layer_relfiles_total'] == layer_relfiles_remain + 2
                    assert row['layer_relfiles_removed'] == 2
                    assert row['layer_relfiles_dropped'] == 0

                    # Run GC again, with no changes in the database. Should not remove anything.
                    log.info("Run GC again, with nothing to do")
                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row)
                    assert row['layer_relfiles_total'] == layer_relfiles_remain
                    assert row['layer_relfiles_removed'] == 0
                    assert row['layer_relfiles_dropped'] == 0

                    #
                    # Test DROP TABLE checks that relation data and metadata was deleted by GC from object storage
                    #
                    log.info("Drop table and run GC again")
                    cur.execute("DROP TABLE foo")

                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                    row = pscur.fetchone()
                    print_gc_result(row)

                    # We still cannot remove the latest layers
                    # because they serve as tombstones for earlier layers.
                    assert row['layer_relfiles_dropped'] == 0
                    # Each relation fork is counted separately, hence 3.
                    assert row['layer_relfiles_needed_as_tombstone'] == 3

                    # The catalog updates also create new layer files of the catalogs, which
                    # are counted as 'removed'
                    assert row['layer_relfiles_removed'] > 0

                    # TODO Change the test to check actual CG of dropped layers.
                    # Each relation fork is counted separately, hence 3.
                    #assert row['layer_relfiles_dropped'] == 3

                    # TODO: perhaps we should count catalog and user relations separately,
                    # to make this kind of testing more robust
