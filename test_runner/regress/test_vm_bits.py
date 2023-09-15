from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn


#
# Test that the VM bit is cleared correctly at a HEAP_DELETE and
# HEAP_UPDATE record.
#
def test_vm_bit_clear(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.neon_cli.create_branch("test_vm_bit_clear", "empty")
    endpoint = env.endpoints.create_start("test_vm_bit_clear")

    log.info("postgres is running on 'test_vm_bit_clear' branch")
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    # Create a test table for a few different scenarios and freeze it to set the VM bits.
    cur.execute("CREATE TABLE vmtest_delete (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_delete VALUES (1)")
    cur.execute("VACUUM FREEZE vmtest_delete")

    cur.execute("CREATE TABLE vmtest_hot_update (id integer PRIMARY KEY, filler text)")
    cur.execute("INSERT INTO vmtest_hot_update VALUES (1, 'x')")
    cur.execute("VACUUM FREEZE vmtest_hot_update")

    cur.execute("CREATE TABLE vmtest_cold_update (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_cold_update SELECT g FROM generate_series(1, 1000) g")
    cur.execute("VACUUM FREEZE vmtest_cold_update")

    cur.execute(
        "CREATE TABLE vmtest_cold_update2 (id integer PRIMARY KEY, filler text) WITH (fillfactor=100)"
    )
    cur.execute("INSERT INTO vmtest_cold_update2 SELECT g, '' FROM generate_series(1, 1000) g")
    cur.execute("VACUUM FREEZE vmtest_cold_update2")

    # DELETE and UPDATE the rows.
    cur.execute("DELETE FROM vmtest_delete WHERE id = 1")
    cur.execute("UPDATE vmtest_hot_update SET filler='x' WHERE id = 1")
    cur.execute("UPDATE vmtest_cold_update SET id = 5000 WHERE id = 1")

    # Clear the VM bit on the last page with an INSERT. Then clear the VM bit on
    # the page where row 1 is (block 0), by doing an UPDATE. The UPDATE is a
    # cold update, and the new tuple goes to the last page, which already had
    # its VM bit cleared. The point is that the UPDATE *only* clears the VM bit
    # on the page containing the old tuple. We had a bug where we got the old
    # and new pages mixed up, and that only shows up when one of the bits is
    # cleared, but not the other one.
    cur.execute("INSERT INTO vmtest_cold_update2 VALUES (9999, 'x')")
    # Clears the VM bit on the old page
    cur.execute("UPDATE vmtest_cold_update2 SET id = 5000, filler=repeat('x', 200) WHERE id = 1")

    # Branch at this point, to test that later
    fork_at_current_lsn(env, endpoint, "test_vm_bit_clear_new", "test_vm_bit_clear")

    # Clear the buffer cache, to force the VM page to be re-fetched from
    # the page server
    cur.execute("SELECT clear_buffer_cache()")

    # Check that an index-only scan doesn't see the deleted row. If the
    # clearing of the VM bit was not replayed correctly, this would incorrectly
    # return deleted row.
    cur.execute(
        """
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    """
    )

    cur.execute("SELECT id FROM vmtest_delete WHERE id = 1")
    assert cur.fetchall() == []
    cur.execute("SELECT id FROM vmtest_hot_update WHERE id = 1")
    assert cur.fetchall() == [(1,)]
    cur.execute("SELECT id FROM vmtest_cold_update WHERE id = 1")
    assert cur.fetchall() == []
    cur.execute("SELECT id FROM vmtest_cold_update2 WHERE id = 1")
    assert cur.fetchall() == []

    cur.close()

    # Check the same thing on the branch that we created right after the DELETE
    #
    # As of this writing, the code in smgrwrite() creates a full-page image whenever
    # a dirty VM page is evicted. If the VM bit was not correctly cleared by the
    # earlier WAL record, the full-page image hides the problem. Starting a new
    # server at the right point-in-time avoids that full-page image.
    endpoint_new = env.endpoints.create_start("test_vm_bit_clear_new")

    log.info("postgres is running on 'test_vm_bit_clear_new' branch")
    pg_new_conn = endpoint_new.connect()
    cur_new = pg_new_conn.cursor()

    cur_new.execute(
        """
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    """
    )

    cur_new.execute("SELECT id FROM vmtest_delete WHERE id = 1")
    assert cur_new.fetchall() == []
    cur_new.execute("SELECT id FROM vmtest_hot_update WHERE id = 1")
    assert cur_new.fetchall() == [(1,)]
    cur_new.execute("SELECT id FROM vmtest_cold_update WHERE id = 1")
    assert cur_new.fetchall() == []
    cur_new.execute("SELECT id FROM vmtest_cold_update2 WHERE id = 1")
    assert cur_new.fetchall() == []


#
# Test that the ALL_FROZEN VM bit is cleared correctly at a HEAP_LOCK
# record.
#
def test_vm_bit_clear_on_heap_lock(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.neon_cli.create_branch("test_vm_bit_clear_on_heap_lock", "empty")
    endpoint = env.endpoints.create_start(
        "test_vm_bit_clear_on_heap_lock",
        config_lines=[
            "log_autovacuum_min_duration = 0",
            # Perform anti-wraparound vacuuming aggressively
            "autovacuum_naptime='1 s'",
            "autovacuum_freeze_max_age = 1000000",
        ],
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    cur.execute("SELECT pg_switch_wal()")

    # Create a test table and freeze it to set the all-frozen VM bit on all pages.
    cur.execute("CREATE TABLE vmtest_lock (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_lock SELECT g FROM generate_series(1, 50000) g")
    cur.execute("VACUUM FREEZE vmtest_lock")

    # Lock a row. This clears the all-frozen VM bit for that page.
    cur.execute("SELECT * FROM vmtest_lock WHERE id = 40000 FOR UPDATE")

    # Remember the XID. We will use it later to verify that we have consumed a lot of
    # XIDs after this.
    cur.execute("select pg_current_xact_id()")
    locking_xid = cur.fetchall()[0][0]

    # Stop and restart postgres, to clear the buffer cache.
    #
    # NOTE: clear_buffer_cache() will not do, because it evicts the dirty pages
    # in a "clean" way. Our neon extension will write a full-page image of the VM
    # page, and we want to avoid that.
    endpoint.stop()
    endpoint.start()
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("select xmin, xmax, * from vmtest_lock where id = 40000 ")
    tup = cur.fetchall()
    xmax_before = tup[0][1]

    # Consume a lot of XIDs, so that anti-wraparound autovacuum kicks
    # in and the clog gets truncated. We set autovacuum_freeze_max_age to a very
    # low value, so it doesn't take all that many XIDs for autovacuum to kick in.
    for i in range(1000):
        cur.execute(
            """
        CREATE TEMP TABLE othertable (i int) ON COMMIT DROP;
        do $$
        begin
          for i in 1..100000 loop
            -- Use a begin-exception block to generate a new subtransaction on each iteration
            begin
              insert into othertable values (i);
            exception when others then
              raise 'not expected %', sqlerrm;
            end;
          end loop;
        end;
        $$;
        """
        )
        cur.execute("select xmin, xmax, * from vmtest_lock where id = 40000 ")
        tup = cur.fetchall()
        log.info(f"tuple = {tup}")
        xmax = tup[0][1]
        assert xmax == xmax_before

        if i % 50 == 0:
            cur.execute("select datfrozenxid from pg_database where datname='postgres'")
            datfrozenxid = cur.fetchall()[0][0]
            if datfrozenxid > locking_xid:
                break

    cur.execute("select pg_current_xact_id()")
    curr_xid = cur.fetchall()[0][0]
    assert int(curr_xid) - int(locking_xid) >= 100000

    # Now, if the VM all-frozen bit was not correctly cleared on
    # replay, we will try to fetch the status of the XID that was
    # already truncated away.
    #
    # ERROR: could not access status of transaction 1027
    cur.execute("select xmin, xmax, * from vmtest_lock where id = 40000 for update")
    tup = cur.fetchall()
    log.info(f"tuple = {tup}")
