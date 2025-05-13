from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


#
# Test unlogged build for GIST index
#
def test_gist(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    con = endpoint.connect()
    cur = con.cursor()
    iterations = 100

    for _ in range(iterations):
        cur.execute(
            "CREATE TABLE pvactst (i INT, a INT[], p POINT) with (autovacuum_enabled = off)"
        )
        cur.execute(
            "INSERT INTO pvactst SELECT i, array[1,2,3], point(i, i+1) FROM generate_series(1,1000) i"
        )
        cur.execute("CREATE INDEX gist_pvactst ON pvactst USING gist (p)")
        cur.execute("VACUUM pvactst")
        cur.execute("DROP TABLE pvactst")
