from pathlib import Path

from fixtures.neon_fixtures import NeonEnvBuilder, flush_ep_to_pageserver, test_output_dir
from fixtures.shared_fixtures import TTimeline


def do_combocid_op(timeline: TTimeline, op):
    endpoint = timeline.primary_with_config(config_lines=[
        "shared_buffers='1MB'",
    ])

    conn = endpoint.connect()
    cur = conn.cursor()
    n_records = 1000

    cur.execute("CREATE EXTENSION neon_test_utils")

    cur.execute("create table t(id integer, val integer)")

    cur.execute("begin")
    cur.execute("insert into t values (1, 0)")
    cur.execute("insert into t values (2, 0)")
    cur.execute(f"insert into t select g, 0 from generate_series(3,{n_records}) g")

    # Open a cursor that scroll it halfway through
    cur.execute("DECLARE c1 NO SCROLL CURSOR WITHOUT HOLD FOR SELECT * FROM t")
    cur.execute("fetch 500 from c1")
    rows = cur.fetchall()
    assert len(rows) == 500

    # Perform specified operation
    cur.execute(op)

    # Clear the cache, so that we exercise reconstructing the pages
    # from WAL
    endpoint.clear_shared_buffers()

    # Check that the cursor opened earlier still works. If the
    # combocids are not restored correctly, it won't.
    cur.execute("fetch all from c1")
    rows = cur.fetchall()
    assert len(rows) == 500

    cur.execute("rollback")
    timeline.stop_and_flush(endpoint)
    timeline.checkpoint(compact=False, wait_until_uploaded=True)


def test_combocid_delete(timeline: TTimeline):
    do_combocid_op(timeline, "delete from t")


def test_combocid_update(timeline: TTimeline):
    do_combocid_op(timeline, "update t set val=val+1")


def test_combocid_lock(timeline: TTimeline):
    do_combocid_op(timeline, "select * from t for update")


def test_combocid_multi_insert(timeline: TTimeline, test_output_dir: Path):
    endpoint = timeline.primary_with_config(config_lines=[
        "shared_buffers='1MB'",
    ])

    conn = endpoint.connect()
    cur = conn.cursor()
    n_records = 1000

    cur.execute("CREATE EXTENSION neon_test_utils")

    cur.execute("create table t(id integer, val integer)")
    file_path = str(test_output_dir / "t.csv")
    cur.execute(f"insert into t select g, 0 from generate_series(1,{n_records}) g")
    cur.execute(f"copy t to '{file_path}'")
    cur.execute("truncate table t")

    cur.execute("begin")
    cur.execute(f"copy t from '{file_path}'")

    # Open a cursor that scroll it halfway through
    cur.execute("DECLARE c1 NO SCROLL CURSOR WITHOUT HOLD FOR SELECT * FROM t")
    cur.execute("fetch 500 from c1")
    rows = cur.fetchall()
    assert len(rows) == 500

    # Delete all the rows. Because all of the rows were inserted earlier in the
    # same transaction, all the rows will get a combocid.
    cur.execute("delete from t")
    # Clear the cache, so that we exercise reconstructing the pages
    # from WAL
    endpoint.clear_shared_buffers()

    # Check that the cursor opened earlier still works. If the
    # combocids are not restored correctly, it won't.
    cur.execute("fetch all from c1")
    rows = cur.fetchall()
    assert len(rows) == 500

    cur.execute("rollback")

    timeline.stop_and_flush(endpoint)
    timeline.checkpoint(compact=False, wait_until_uploaded=True)


def test_combocid(timeline: TTimeline):
    endpoint = timeline.primary

    conn = endpoint.connect()
    cur = conn.cursor()
    n_records = 100000

    cur.execute("create table t(id integer, val integer)")
    cur.execute(f"insert into t values (generate_series(1,{n_records}), 0)")

    cur.execute("begin")

    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records

    cur.execute("delete from t")
    assert cur.rowcount == n_records
    cur.execute("delete from t")
    assert cur.rowcount == 0

    cur.execute(f"insert into t values (generate_series(1,{n_records}), 0)")
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records

    cur.execute("rollback")

    timeline.stop_and_flush(endpoint)
    timeline.checkpoint(compact=False, wait_until_uploaded=True)
