import time

from fixtures.neon_fixtures import NeonEnv, wait_replica_caughtup
from fixtures.shared_fixtures import TTimeline


#
# Test that redo of XLOG_GIN_VACUUM_PAGE doesn't produce error
#
def test_gin_redo(timeline: TTimeline):
    primary = timeline.primary
    secondary = timeline.secondary
    con = primary.connect()
    cur = con.cursor()
    cur.execute("create table gin_test_tbl(id integer, i int4[])")
    cur.execute("create index gin_test_idx on gin_test_tbl using gin (i)")
    cur.execute("insert into gin_test_tbl select g,array[3, 1, g] from generate_series(1, 10000) g")
    cur.execute("delete from gin_test_tbl where id % 2 = 0")
    cur.execute("vacuum gin_test_tbl")
    wait_replica_caughtup(primary, secondary)
